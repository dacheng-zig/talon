//! Http1Protocol: the HTTP/1.1 connection loop (design doc §5.5), a `Proto`
//! implementation for talon-core's StreamServer.
//!
//! Per request: accumulate head (hand-written Accumulator specialization,
//! §8) → copy to arena → pure-function parse → handler → drain body →
//! keep-alive or close. Hot path allocates only from the per-connection
//! arena, which resets between requests with retained capacity — steady
//! state is malloc-free (§5.4).
//!
//! Why the arena copy of the head: header slices must stay valid while the
//! handler reads the body through the same `std.Io.Reader`, whose buffer
//! rebases on refill. One memcpy of a few hundred bytes buys lifetime
//! correctness without giving up the zero-copy parse.

const std = @import("std");
const parser = @import("parser.zig");
const body_mod = @import("body.zig");
const encode = @import("encode.zig");
const request_mod = @import("request.zig");
const response_mod = @import("response.zig");

pub const Request = request_mod.Request;
pub const Response = response_mod.Response;
pub const Status = encode.Status;

const body_buffer_size = 4 * 1024;

pub fn Http1Protocol(comptime App: type) type {
    comptime {
        if (!std.meta.hasFn(App, "handle")) {
            @compileError("talon.http.Server: App type '" ++ @typeName(App) ++
                "' must declare 'pub fn handle(self: *App, req: *talon.http.Request, res: *talon.http.Response) !void'");
        }
    }

    return struct {
        pub fn serve(conn: anytype, app: *App) anyerror!void {
            var date_cache: encode.DateCache = .{};
            const r = conn.reader();
            const w = conn.writer();
            // Reused across requests on this connection: header slices stay
            // valid only for the current request (the documented contract),
            // so per-request allocation would buy nothing.
            var headers_storage: [parser.max_headers]parser.Header = undefined;
            // Minimal buffer so peek()/fill() on a bodyless reader hit the
            // vtable (and get a clean EndOfStream) instead of spinning on a
            // zero-capacity buffer.
            var empty_body_buffer: [1]u8 = undefined;

            while (true) {
                // Ship pending responses only when the read side is about
                // to park (no buffered request bytes left). While pipelined
                // requests remain buffered, responses keep accumulating in
                // the write buffer and leave in one vectored syscall —
                // findHeadEnd flushes before any blocking refill, so the
                // peer is never left waiting on queued output.
                if (r.bufferedLen() == 0) w.flush() catch return;

                // Request-boundary idle wait: interruptible by shutdown,
                // bounded by the keep-alive budget (§5.6/§5.8).
                conn.waitReadable(conn.limits.keep_alive_timeout) catch return;
                conn.setReadTimeout(conn.limits.header_read_timeout);

                const head_len = findHeadEnd(r, w, conn.limits.max_header_size) catch |err| switch (err) {
                    error.CleanClose => return,
                    error.HeadersTooLarge => {
                        return respondErrorAndClose(w, &date_cache, .request_header_fields_too_large);
                    },
                    else => return, // truncated head / read failure / timeout
                };

                _ = conn.arena.reset(.retain_capacity);
                const arena = conn.arena.allocator();

                // Pin the head for the request's lifetime (see file doc).
                const head_bytes = arena.dupe(u8, r.buffered()[0..head_len]) catch
                    return respondErrorAndClose(w, &date_cache, .internal_server_error);
                r.toss(head_len);

                const head = parser.parse(head_bytes, &headers_storage) catch |err| {
                    return respondErrorAndClose(w, &date_cache, statusForParseError(err));
                };

                if (head.content_length) |cl| {
                    if (conn.limits.max_body_size) |max| {
                        if (cl > max) return respondErrorAndClose(w, &date_cache, .payload_too_large);
                    }
                }

                // Bodyless requests (the hot path) skip the body buffer.
                const has_body = head.transfer_chunked or (head.content_length orelse 0) != 0;
                const body_buffer: []u8 = if (has_body)
                    arena.alloc(u8, body_buffer_size) catch
                        return respondErrorAndClose(w, &date_cache, .internal_server_error)
                else
                    &empty_body_buffer;
                var body = body_mod.BodyReader.init(r, &head, conn.limits.max_body_size, body_buffer);

                if (head.expect_continue) {
                    // Eager 100-continue (Kestrel defers to first body read;
                    // M1 keeps it simple).
                    w.writeAll("HTTP/1.1 100 Continue\r\n\r\n") catch return;
                    w.flush() catch return;
                }

                var req: Request = .{ .head = head, .arena = arena, .body = &body };
                var res: Response = .{
                    .out = w,
                    .date = &date_cache,
                    .keep_alive = head.keep_alive,
                    .suppress_body = head.method == .HEAD,
                };

                app.handle(&req, &res) catch |err| {
                    if (!res.written) {
                        respondErrorAndClose(w, &date_cache, .internal_server_error) catch {};
                    } else {
                        w.flush() catch {};
                    }
                    return err; // surface handler errors to the server log
                };

                if (!res.written) {
                    // Handler contract violation: never leave the client hanging.
                    return respondErrorAndClose(w, &date_cache, .internal_server_error);
                }

                // Drain unread body so the next request parses cleanly; a
                // body that fails to frame poisons the connection — close.
                if (has_body) body.discard() catch return;

                if (!head.keep_alive or !res.keep_alive or conn.isShuttingDown()) {
                    w.flush() catch {};
                    return;
                }
            }
        }
    };
}

const HeadEndError = error{
    /// Peer closed before sending anything: normal keep-alive end.
    CleanClose,
    HeadersTooLarge,
    TruncatedHead,
    ReadFailed,
};

/// Scans the buffered window for the end-of-head terminator, refilling as
/// needed. Returns the head length INCLUDING the final "\r\n\r\n".
///
/// `pending` (the connection writer, nullable for tests) is flushed before
/// any blocking refill: the peer may be waiting for those responses before
/// it sends the rest of this head — never park on read with queued output.
fn findHeadEnd(r: *std.Io.Reader, pending: ?*std.Io.Writer, max_header_size: u32) HeadEndError!usize {
    var search_start: usize = 0;
    while (true) {
        const window = r.buffered();
        if (std.mem.indexOfPos(u8, window, search_start, "\r\n\r\n")) |idx| {
            const head_len = idx + 4;
            if (head_len > max_header_size) return error.HeadersTooLarge;
            return head_len;
        }
        if (window.len > max_header_size) return error.HeadersTooLarge;
        search_start = window.len -| 3;
        if (pending) |w| w.flush() catch return error.ReadFailed;
        r.fillMore() catch |err| switch (err) {
            error.EndOfStream => {
                if (r.bufferedLen() == 0) return error.CleanClose;
                return error.TruncatedHead;
            },
            error.ReadFailed => return error.ReadFailed,
        };
    }
}

fn statusForParseError(err: parser.ParseError) Status {
    return switch (err) {
        error.BadVersion => .http_version_not_supported,
        error.UnsupportedTransferEncoding => .not_implemented,
        error.TooManyHeaders => .request_header_fields_too_large,
        error.MalformedRequestLine,
        error.BadTarget,
        error.MalformedHeader,
        error.BadContentLength,
        error.ConflictingFraming,
        error.MissingHost,
        => .bad_request,
    };
}

/// Minimal error response; the connection is closed afterwards by the
/// caller returning out of the connection loop.
fn respondErrorAndClose(w: *std.Io.Writer, date: *encode.DateCache, status: Status) anyerror!void {
    const phrase = status.phrase() orelse "Error";
    encode.writeHead(w, date, .{
        .status = status,
        .content_length = phrase.len,
        .keep_alive = false,
    }) catch return;
    w.writeAll(phrase) catch return;
    w.flush() catch return;
}

// ── Tests ────────────────────────────────────────────────────────────────

test "findHeadEnd: locates terminator across refills" {
    var r: std.Io.Reader = .fixed("GET / HTTP/1.1\r\nHost: h\r\n\r\nBODY");
    const len = try findHeadEnd(&r, null, 1024);
    try std.testing.expectEqual(27, len);
    try std.testing.expectEqualStrings("GET / HTTP/1.1\r\nHost: h\r\n\r\n", r.buffered()[0..len]);
}

test "findHeadEnd: oversized head rejected, clean close detected" {
    const big = [_]u8{'a'} ** 128;
    var r: std.Io.Reader = .fixed(&big);
    try std.testing.expectError(error.HeadersTooLarge, findHeadEnd(&r, null, 64));

    var r2: std.Io.Reader = .fixed("");
    try std.testing.expectError(error.CleanClose, findHeadEnd(&r2, null, 64));

    var r3: std.Io.Reader = .fixed("GET / HT");
    try std.testing.expectError(error.TruncatedHead, findHeadEnd(&r3, null, 64));
}

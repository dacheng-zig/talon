//! Http1Protocol: the HTTP/1.1 connection loop, a `Proto`
//! implementation for talon-core's StreamServer.
//!
//! Per request: accumulate head (hand-written Accumulator
//! specialization) → copy to arena → pure-function parse → handler → drain
//! body → keep-alive or close. Hot path allocates only from the per-connection
//! arena, which resets between requests with retained capacity — steady
//! state is malloc-free.
//!
//! Why the arena copy of the head: header slices must stay valid while the
//! handler reads the body through the same `std.Io.Reader`, whose buffer
//! rebases on refill. One memcpy of a few hundred bytes buys lifetime
//! correctness without giving up the zero-copy parse.

const std = @import("std");
const parser = @import("codec/request_parser.zig");
const body_mod = @import("codec/body.zig");
const encode = @import("codec/response_encode.zig");
const head_scan = @import("codec/head.zig");
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
                // bounded by the keep-alive budget.
                conn.waitReadable(conn.limits.keep_alive_timeout) catch return;
                conn.setReadTimeout(conn.limits.header_read_timeout);

                const head_len = head_scan.findHeadEnd(r, conn.limits.max_header_size, w) catch |err| switch (err) {
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
                    // this keeps it simple).
                    w.writeAll("HTTP/1.1 100 Continue\r\n\r\n") catch return;
                    w.flush() catch return;
                }

                var req: Request = .{ .head = head, .arena = arena, .body = &body };
                var res: Response = .{
                    .out = w,
                    .date = &date_cache,
                    // HTTP/1.0 persistence requires an explicit
                    // `Connection: keep-alive` in the response, but the encoder
                    // emits an HTTP/1.1 status line and no such header — so a 1.0
                    // connection cannot be correctly persisted. Treat 1.0 as
                    // non-persistent (RFC 9112 §9.3): the response then carries
                    // `connection: close` and the loop below closes, instead of
                    // holding a socket the 1.0 peer believes is already closed.
                    // `respond()` may only narrow this, never re-enable it.
                    .keep_alive = head.keep_alive and head.version == .@"HTTP/1.1",
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
                // body that fails to frame (or exceeds max_body_size) poisons
                // the connection — flush the response already produced (e.g. a
                // 413) so it still reaches the client, then close.
                if (has_body) body.discard() catch {
                    w.flush() catch {};
                    return;
                };

                if (!head.keep_alive or !res.keep_alive or conn.isShuttingDown()) {
                    w.flush() catch {};
                    return;
                }
            }
        }
    };
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

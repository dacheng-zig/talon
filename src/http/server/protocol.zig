//! Http1Protocol: the HTTP/1.1 connection loop, a `Proto`
//! implementation for talon-core's StreamServer.
//!
//! Per request: accumulate head (hand-written Accumulator
//! specialization) → copy to arena → pure-function parse → handler → drain
//! body → keep-alive or close. Hot path allocates only from the per-connection
//! arena, which resets between requests with bounded retained capacity.
//! Requests fitting that retained capacity reuse it without malloc.
//!
//! Why the arena copy of the head: header slices must stay valid while the
//! handler reads the body through the same `std.Io.Reader`, whose buffer
//! rebases on refill. One memcpy of a few hundred bytes buys lifetime
//! correctness without giving up the zero-copy parse.

const std = @import("std");
const zio = @import("zio");
const parser = @import("../codec/request_parser.zig");
const body_mod = @import("../codec/body.zig");
const encode = @import("../codec/response_encode.zig");
const head_scan = @import("../codec/head.zig");
const request_mod = @import("request.zig");
const response_mod = @import("response.zig");

pub const Request = request_mod.Request;
pub const Upgrade = request_mod.Upgrade;
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

            // Connection-upgrade handle, reused across requests. The reader and
            // writer are already type-erased; only setReadTimeout is
            // transport-specific, reached through a thunk over `conn`.
            var upgrade_taken = false;
            const UpgradeThunk = struct {
                fn setReadTimeout(ctx: *anyopaque, timeout: zio.Timeout) void {
                    const c: @TypeOf(conn) = @ptrCast(@alignCast(ctx));
                    c.setReadTimeout(timeout);
                }
            };
            var upgrade_ctx: Upgrade = .{
                .reader = r,
                .writer = w,
                .taken = &upgrade_taken,
                .ctx = conn,
                .set_read_timeout_fn = UpgradeThunk.setReadTimeout,
            };

            var first_request = true;
            conn.setWriteTimeout(conn.limits.write_timeout);
            while (true) {
                // Release unusually large request allocations before idling.
                resetRequestArena(conn.arena, conn.limits.max_retained_arena);
                // Ship pending responses only when the read side is about
                // to park (no buffered request bytes left). While pipelined
                // requests remain buffered, responses keep accumulating in
                // the write buffer and leave in one vectored syscall —
                // findHeadEnd flushes before any blocking refill, so the
                // peer is never left waiting on queued output.
                if (r.bufferedLen() == 0) w.flush() catch return;

                // Request-boundary idle wait: interruptible by shutdown,
                // bounded by the keep-alive budget.
                const initial_deadline = conn.limits.header_read_timeout.toDeadline();
                conn.waitReadable(if (first_request) initial_deadline else conn.limits.keep_alive_timeout) catch return;
                conn.setReadTimeout(if (first_request) initial_deadline else conn.limits.header_read_timeout.toDeadline());
                first_request = false;

                const head_len = head_scan.findHeadEnd(r, conn.limits.max_header_size, w) catch |err| switch (err) {
                    error.CleanClose => return,
                    error.HeadersTooLarge => {
                        return respondErrorAndClose(w, &date_cache, .request_header_fields_too_large, false);
                    },
                    else => return, // truncated head / read failure / timeout
                };

                const arena = conn.arena.allocator();

                // Pin the head for the request's lifetime (see file doc).
                const head_bytes = arena.dupe(u8, r.buffered()[0..head_len]) catch
                    return respondErrorAndClose(w, &date_cache, .internal_server_error, false);
                r.toss(head_len);

                const head = parser.parse(head_bytes, &headers_storage) catch |err| {
                    return respondErrorAndClose(w, &date_cache, statusForParseError(err), false);
                };

                if (head.content_length) |cl| {
                    if (conn.limits.max_body_size) |max| {
                        if (cl > max) return respondErrorAndClose(w, &date_cache, .payload_too_large, head.method == .HEAD);
                    }
                }

                // Bodyless requests (the hot path) skip the body buffer.
                const has_body = head.transfer_chunked or (head.content_length orelse 0) != 0;
                const body_buffer: []u8 = if (has_body)
                    arena.alloc(u8, body_buffer_size) catch
                        return respondErrorAndClose(w, &date_cache, .internal_server_error, head.method == .HEAD)
                else
                    &empty_body_buffer;
                var body = body_mod.BodyReader.init(r, &head, conn.limits.max_body_size, body_buffer);
                // The head deadline ends here. Body reads have an independent
                // cumulative network-wait budget, not a repeatedly reset timer.
                conn.setReadTimeout(.none);
                var body_rate = BodyRate(@TypeOf(conn)){ .conn = conn };
                if (has_body and conn.limits.min_body_data_rate != null) {
                    body.read_policy = .{ .ctx = &body_rate, .before = BodyRate(@TypeOf(conn)).before, .after = BodyRate(@TypeOf(conn)).after };
                }

                if (head.expect_continue) {
                    // Eager 100-continue (Kestrel defers to first body read;
                    // this keeps it simple).
                    w.writeAll("HTTP/1.1 100 Continue\r\n\r\n") catch return;
                    w.flush() catch return;
                }

                var req: Request = .{ .head = head, .arena = arena, .body = &body, .upgrade = &upgrade_ctx };
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
                        respondErrorAndClose(w, &date_cache, bodyFailureStatus(&req), head.method == .HEAD) catch {};
                    } else {
                        w.flush() catch {};
                    }
                    return err; // surface handler errors to the server log
                };

                // The handler upgraded the connection (e.g. WebSocket) and drove
                // it to completion; it is no longer an HTTP connection, so leave
                // the loop instead of reading another request.
                if (upgrade_taken) return;

                if (!res.written) {
                    // Handler contract violation: never leave the client hanging.
                    return respondErrorAndClose(w, &date_cache, bodyFailureStatus(&req), head.method == .HEAD);
                }

                // A closing response need not drain an upload the application
                // rejected; publish it promptly instead of waiting for the peer.
                if (!res.keep_alive or conn.isShuttingDown()) {
                    w.flush() catch {};
                    return;
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

fn bodyFailureStatus(req: *const Request) Status {
    if (req.bodyError()) |err| {
        if (err == error.BodyTooSlow) return .request_timeout;
    }
    return .internal_server_error;
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
fn respondErrorAndClose(w: *std.Io.Writer, date: *encode.DateCache, status: Status, suppress_body: bool) anyerror!void {
    const phrase = status.phrase() orelse "Error";
    encode.writeHead(w, date, .{
        .status = status,
        .content_length = phrase.len,
        .keep_alive = false,
    }) catch return;
    // HEAD semantics apply to protocol-generated errors as well as app output.
    if (!suppress_body) w.writeAll(phrase) catch return;
    w.flush() catch return;
}

/// Cumulative network-wait accounting. Buffered payload earns credit, but
/// handler CPU time, application sleeps, and downstream backpressure do not
/// spend the client's upload budget. One policy lives for the entire body.
fn BodyRate(comptime Conn: type) type {
    return struct {
        conn: Conn,
        waited_ns: u64 = 0,
        started_ns: u64 = 0,
        allowance_ns: u64 = 0,
        const Self = @This();

        fn before(ctx: *anyopaque, produced: u64) bool {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const rate = self.conn.limits.min_body_data_rate.?;
            const credit: u128 = @as(u128, produced) * std.time.ns_per_s / rate.bytes_per_sec;
            const allowance: u64 = @intCast(@min(std.math.maxInt(u64), credit + rate.grace.toNanoseconds()));
            if (self.waited_ns >= allowance) return false;
            self.allowance_ns = allowance;
            self.started_ns = zio.Timestamp.now(.monotonic).toNanoseconds();
            self.conn.setReadTimeout(zio.Timeout.fromNanoseconds(allowance - self.waited_ns).toDeadline());
            return true;
        }

        fn after(ctx: *anyopaque) bool {
            const self: *Self = @ptrCast(@alignCast(ctx));
            self.waited_ns +|= zio.Timestamp.now(.monotonic).toNanoseconds() -| self.started_ns;
            self.conn.setReadTimeout(.none);
            return self.waited_ns >= self.allowance_ns;
        }
    };
}

fn resetRequestArena(arena: *std.heap.ArenaAllocator, retained: ?usize) void {
    if (!arena.reset(if (retained) |max| .{ .retain_with_limit = max } else .retain_capacity)) {
        // std's reset may retain the old oversized allocation if shrinking
        // requires a new allocation and that allocation fails.
        _ = arena.reset(.free_all);
    }
}

test "request arena: retention cap holds when shrinking allocation fails" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    var arena = std.heap.ArenaAllocator.init(failing.allocator());
    defer arena.deinit();
    _ = try arena.allocator().alloc(u8, 128 * 1024);
    failing.fail_index = failing.alloc_index;
    resetRequestArena(&arena, 4096);
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 0), arena.queryCapacity());
}

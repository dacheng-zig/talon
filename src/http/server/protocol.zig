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
const timeout_mod = @import("../../core/timeout.zig");
const request_mod = @import("request.zig");
const response_mod = @import("response.zig");
const limits_mod = @import("limits.zig");
const EngineLimits = @import("../../core/limits.zig").Limits;
const AllocationBudget = @import("../../core/allocation_budget.zig");

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
        pub const Limits = limits_mod.Limits;

        pub fn engineLimits(limits: Limits) !EngineLimits {
            return limits.engineLimits();
        }

        pub fn validateTransport(capabilities: timeout_mod.Capabilities, limits: Limits) timeout_mod.Error!void {
            try capabilities.require(limits.header_read_timeout != .none or limits.keep_alive_timeout != .none or
                limits.min_body_data_rate != null, limits.write_timeout != .none);
        }

        pub fn serve(conn: anytype, app: *App) anyerror!void {
            return serveWithConfig(conn, app, .{});
        }

        pub fn serveWithConfig(conn: anytype, app: *App, limits: Limits) anyerror!void {
            _ = try limits.engineLimits();
            try validateTransport(@TypeOf(conn.*).timeout_capabilities, limits);
            var date_cache: encode.DateCache = .{};
            const r = conn.reader();
            const w = conn.writer();
            if (r.buffer.len <= limits.max_header_size) return error.HeaderBufferTooSmall;
            // Retained capacity stays charged across request resets. The
            // budget must outlive the arena and any upgraded protocol users.
            var request_budget = AllocationBudget.init(conn.gpa, limits.max_request_arena_bytes orelse 0);
            defer request_budget.deinit();
            var request_arena = std.heap.ArenaAllocator.init(if (limits.max_request_arena_bytes != null)
                request_budget.allocator()
            else
                conn.gpa);
            defer request_arena.deinit();
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
                fn setReadTimeout(ctx: *anyopaque, timeout: zio.Timeout) timeout_mod.Error!void {
                    const c: @TypeOf(conn) = @ptrCast(@alignCast(ctx));
                    try c.setReadTimeout(timeout);
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
            try conn.setWriteTimeout(limits.write_timeout);
            while (true) {
                // Release unusually large request allocations before idling.
                resetRequestArena(&request_arena, limits.max_retained_arena);
                // Ship pending responses only when the read side is about
                // to park (no buffered request bytes left). While pipelined
                // requests remain buffered, responses keep accumulating in
                // the write buffer and leave in one vectored syscall —
                // findHeadEnd flushes before any blocking refill, so the
                // peer is never left waiting on queued output.
                if (r.bufferedLen() == 0) try w.flush();

                // Request-boundary idle wait: interruptible by shutdown,
                // bounded by the keep-alive budget.
                const initial_request = first_request;
                const initial_deadline = conn.initialReadTimeout(limits.header_read_timeout).toDeadline();
                conn.waitReadable(if (first_request) initial_deadline else limits.keep_alive_timeout) catch return;
                try conn.setReadTimeout(if (first_request) initial_deadline else limits.header_read_timeout.toDeadline());
                first_request = false;

                const head_len = head_scan.findHeadEnd(r, limits.max_header_size, w) catch |err| switch (err) {
                    error.CleanClose => return,
                    error.WriteFailed => return err,
                    error.HeadersTooLarge => {
                        return respondErrorAndClose(w, &date_cache, .request_header_fields_too_large, isHeadPrefix(r.buffered()));
                    },
                    else => return, // truncated head / read failure / timeout
                };
                if (initial_request) conn.checkEstablishmentDeadline() catch return;

                const arena = request_arena.allocator();
                const suppress_error_body = isHeadPrefix(r.buffered()[0..head_len]);

                // Pin the head for the request's lifetime (see file doc).
                const head_bytes = arena.dupe(u8, r.buffered()[0..head_len]) catch
                    return respondErrorAndClose(w, &date_cache, .internal_server_error, suppress_error_body);
                r.toss(head_len);

                const head = parser.parse(head_bytes, &headers_storage) catch |err| {
                    return respondErrorAndClose(w, &date_cache, statusForParseError(err), suppress_error_body);
                };

                if (head.content_length) |cl| {
                    if (limits.max_body_size) |max| {
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
                var body = body_mod.BodyReader.init(r, &head, limits.max_body_size, body_buffer);
                // The head deadline ends here. Body reads have an independent
                // cumulative network-wait budget, not a repeatedly reset timer.
                try conn.setReadTimeout(.none);
                var body_rate = BodyRate(@TypeOf(conn)){ .conn = conn, .rate = limits.min_body_data_rate };
                if (has_body and limits.min_body_data_rate != null) {
                    body.read_policy = .{ .ctx = &body_rate, .before = BodyRate(@TypeOf(conn)).before, .after = BodyRate(@TypeOf(conn)).after };
                }

                if (head.expect_continue) {
                    // Eager 100-continue (Kestrel defers to first body read;
                    // this keeps it simple).
                    try w.writeAll("HTTP/1.1 100 Continue\r\n\r\n");
                    try w.flush();
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
                    // Committed upgrade output (even a failed 101 prefix)
                    // and terminal output/cancellation errors cannot be
                    // followed by HTTP fallback or another flush.
                    if (upgrade_taken or err == error.WriteFailed or err == error.Canceled) return err;
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
                    try w.flush();
                    return;
                }

                // Drain unread body so the next request parses cleanly; a
                // body that fails to frame (or exceeds max_body_size) poisons
                // the connection — flush the response already produced (e.g. a
                // 413) so it still reaches the client, then close.
                if (has_body) body.discard() catch {
                    try w.flush();
                    return;
                };

                if (!head.keep_alive or !res.keep_alive or conn.isShuttingDown()) {
                    try w.flush();
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

/// Error paths can precede a parsed Head. Recognize only the complete,
/// case-sensitive method token; this does not validate or accept the request.
fn isHeadPrefix(bytes: []const u8) bool {
    return std.mem.startsWith(u8, bytes, "HEAD ");
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
    try encode.writeHead(w, date, .{
        .status = status,
        .content_length = phrase.len,
        .keep_alive = false,
    });
    // HEAD semantics apply to protocol-generated errors as well as app output.
    if (!suppress_body) try w.writeAll(phrase);
    try w.flush();
}

/// Cumulative network-wait accounting. Buffered payload earns credit, but
/// handler CPU time, application sleeps, and downstream backpressure do not
/// spend the client's upload budget. One policy lives for the entire body.
fn BodyRate(comptime Conn: type) type {
    return struct {
        conn: Conn,
        rate: ?limits_mod.DataRate,
        waited_ns: u64 = 0,
        started_ns: u64 = 0,
        allowance_ns: u64 = 0,
        const Self = @This();

        fn before(ctx: *anyopaque, produced: u64) bool {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const rate = self.rate.?;
            const credit: u128 = @as(u128, produced) * std.time.ns_per_s / rate.bytes_per_sec;
            const allowance: u64 = @intCast(@min(std.math.maxInt(u64), credit + rate.grace.toNanoseconds()));
            if (self.waited_ns >= allowance) return false;
            self.allowance_ns = allowance;
            self.started_ns = zio.Timestamp.now(.monotonic).toNanoseconds();
            self.conn.setReadTimeout(zio.Timeout.fromNanoseconds(allowance - self.waited_ns).toDeadline()) catch return false;
            return true;
        }

        fn after(ctx: *anyopaque) bool {
            const self: *Self = @ptrCast(@alignCast(ctx));
            self.waited_ns +|= zio.Timestamp.now(.monotonic).toNanoseconds() -| self.started_ns;
            self.conn.setReadTimeout(.none) catch unreachable;
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

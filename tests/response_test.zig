const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

test "Response: HEAD and bodyless statuses preserve the next response boundary" {
    for ([_]bool{ false, true }) |head| {
        for ([_]talon.http.Status{ .ok, .no_content, .reset_content, .not_modified }) |status| {
            for ([_]bool{ false, true }) |chunked| {
                if (!head and status == .ok) continue;
                var buf: [2048]u8 = undefined;
                var out: std.Io.Writer = .fixed(&buf);
                var date: talon.http.codec.DateCache = .{};
                var res: talon.http.Response = .{ .out = &out, .date = &date, .keep_alive = true, .suppress_body = head };
                if (chunked) {
                    var staging: [2]u8 = undefined;
                    var body = try res.startChunked(.{ .status = status }, &staging);
                    try body.interface.writeAll("hello");
                    try body.finish();
                } else {
                    try res.respond("hello", .{ .status = status });
                }
                const head_end = std.mem.indexOf(u8, out.buffered(), "\r\n\r\n").? + 4;
                try std.testing.expectEqual(head_end, out.buffered().len);
                if (status == .no_content or status == .not_modified) {
                    try std.testing.expect(std.mem.indexOf(u8, out.buffered(), "content-length:") == null);
                    try std.testing.expect(std.mem.indexOf(u8, out.buffered(), "transfer-encoding:") == null);
                }
                if (status == .reset_content) {
                    try std.testing.expect(std.mem.indexOf(u8, out.buffered(), "content-length: 0\r\n") != null);
                    try std.testing.expect(std.mem.indexOf(u8, out.buffered(), "transfer-encoding:") == null);
                } else if (head and status == .ok and !chunked) {
                    try std.testing.expect(std.mem.indexOf(u8, out.buffered(), "content-length: 5\r\n") != null);
                }
                var next: talon.http.Response = .{ .out = &out, .date = &date, .keep_alive = true };
                try next.respond("next", .{});
                try std.testing.expect(std.mem.startsWith(u8, out.buffered()[head_end..], "HTTP/1.1 200 OK\r\n"));
            }
        }
    }
}

test "Response: HEAD event stream emits neither events nor terminating chunk" {
    var buf: [1024]u8 = undefined;
    var out: std.Io.Writer = .fixed(&buf);
    var date: talon.http.codec.DateCache = .{};
    var res: talon.http.Response = .{ .out = &out, .date = &date, .keep_alive = true, .suppress_body = true };
    var staging: [32]u8 = undefined;
    var events = try res.startEventStream(&staging);
    try events.send(.{ .data = "hello" });
    try events.comment("heartbeat");
    try events.finish();
    try std.testing.expect(std.mem.endsWith(u8, out.buffered(), "\r\n\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, out.buffered(), "hello") == null);
}

test "Response: informational status cannot complete a response" {
    for (100..200) |code| {
        for ([_]bool{ false, true }) |chunked| {
            var buf: [1024]u8 = undefined;
            var out: std.Io.Writer = .fixed(&buf);
            var date: talon.http.codec.DateCache = .{};
            var res: talon.http.Response = .{ .out = &out, .date = &date, .keep_alive = true };
            const options: talon.http.Response.RespondOptions = .{ .status = @fromBackingInt(@intCast(code)) };
            if (chunked) {
                var staging: [2]u8 = undefined;
                try std.testing.expectError(error.InvalidStatus, res.startChunked(options, &staging));
            } else {
                try std.testing.expectError(error.InvalidStatus, res.respond("", options));
            }
            try std.testing.expect(!res.written);
            try std.testing.expectEqual(@as(usize, 0), out.buffered().len);
            try res.respond("ok", .{});
        }
    }
}

test "Response: rejected headers leave room for a valid error response" {
    for ([_]bool{ false, true }) |chunked| {
        var buf: [1024]u8 = undefined;
        var out: std.Io.Writer = .fixed(&buf);
        var date: talon.http.codec.DateCache = .{};
        var res: talon.http.Response = .{ .out = &out, .date = &date, .keep_alive = true };
        const options: talon.http.Response.RespondOptions = .{
            .extra_headers = &.{.{ .name = "x", .value = "bad\r\nvalue" }},
        };
        if (chunked) {
            var staging: [2]u8 = undefined;
            try std.testing.expectError(error.InvalidHeader, res.startChunked(options, &staging));
        } else {
            try std.testing.expectError(error.InvalidHeader, res.respond("bad", options));
        }
        try std.testing.expect(!res.written);
        try std.testing.expectEqual(@as(usize, 0), out.buffered().len);
        try res.respond("error", .{ .status = .internal_server_error });
        try std.testing.expect(std.mem.startsWith(u8, out.buffered(), "HTTP/1.1 500"));
    }
}

// Drive the actual protocol loop with deterministic input and captured output.
const ProtocolConnection = struct {
    pub const timeout_capabilities: talon.core.TimeoutCapabilities = .{ .read = true, .write = true };
    pub fn initialReadTimeout(_: *const ProtocolConnection, timeout: zio.Timeout) zio.Timeout {
        return timeout;
    }
    pub fn checkEstablishmentDeadline(_: *const ProtocolConnection) !void {}
    input: std.Io.Reader,
    output: std.Io.Writer.Allocating,
    gpa: std.mem.Allocator = std.testing.allocator,
    limits: talon.Limits = .{ .max_header_size = 128, .min_body_data_rate = null },
    shutting_down: bool = false,
    faulty_output: ?*FlushFailure = null,

    pub fn reader(self: *@This()) *std.Io.Reader {
        return &self.input;
    }
    pub fn writer(self: *@This()) *std.Io.Writer {
        if (self.faulty_output) |fault| return &fault.interface;
        return &self.output.writer;
    }
    pub fn setReadTimeout(_: *@This(), _: zio.Timeout) talon.core.TimeoutError!void {}
    pub fn setWriteTimeout(_: *@This(), _: zio.Timeout) talon.core.TimeoutError!void {}
    pub fn waitReadable(_: *@This(), _: zio.Timeout) !void {}
    pub fn isShuttingDown(self: *@This()) bool {
        return self.shutting_down;
    }
};

const FlushFailure = struct {
    captured: std.Io.Writer.Allocating,
    interface: std.Io.Writer = .{ .vtable = &.{ .drain = drain, .flush = flush }, .buffer = &.{} },
    calls: usize = 0,
    fail_at: usize = 2,

    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *@This() = @fieldParentPtr("interface", w);
        return self.captured.writer.writeSplat(data, splat);
    }
    fn flush(w: *std.Io.Writer) std.Io.Writer.Error!void {
        const self: *@This() = @fieldParentPtr("interface", w);
        self.calls += 1;
        if (self.calls == self.fail_at) return error.WriteFailed;
    }
};

test "HTTP protocol preserves terminal output errors without another flush" {
    const App = struct {
        const Mode = enum { idle, close, failed_write, canceled, business_error, missing };
        mode: Mode,
        pub fn handle(self: *@This(), _: *talon.http.Request, res: *talon.http.Response) !void {
            if (self.mode == .missing) return;
            try res.respond("body", .{ .keep_alive = self.mode != .close });
            switch (self.mode) {
                .failed_write => try res.out.flush(),
                .canceled => return error.Canceled,
                .business_error => return error.HandlerFailed,
                else => {},
            }
        }
    };
    for ([_]App.Mode{ .idle, .close, .failed_write, .canceled, .business_error, .missing }) |mode| {
        var bytes: [256]u8 = undefined;
        const request = "GET / HTTP/1.1\r\nhost: test\r\n\r\n";
        @memcpy(bytes[0..request.len], request);
        var fault: FlushFailure = .{
            .captured = .init(std.testing.allocator),
            .fail_at = 1,
        };
        defer fault.captured.deinit();
        var conn: ProtocolConnection = .{
            .input = .fixed(&bytes),
            .output = .init(std.testing.allocator),
            .faulty_output = &fault,
        };
        defer conn.output.deinit();
        conn.input.end = request.len;
        var app: App = .{ .mode = mode };
        const result = talon.http.Http1Protocol(App).serveWithConfig(&conn, &app, conn.limits);
        try std.testing.expectError(switch (mode) {
            .canceled => error.Canceled,
            .business_error => error.HandlerFailed,
            else => error.WriteFailed,
        }, result);
        try std.testing.expectEqual(@as(usize, switch (mode) {
            .canceled => 0,
            else => 1,
        }), fault.calls);
    }
}

test "HTTP protocol propagates generated and pending output failures" {
    const App = struct {
        calls: usize = 0,
        pub fn handle(self: *@This(), _: *talon.http.Request, res: *talon.http.Response) !void {
            self.calls += 1;
            try res.respond("body", .{});
        }
    };
    for ([_][]const u8{
        "POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 1\r\nexpect: 100-continue\r\n\r\nx",
        "GET / HTTP/9.9\r\nhost: test\r\n\r\n",
        "GET / HTTP/1.1\r\nhost: test\r\n\r\nGET / HT",
        "POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 4\r\n\r\nx",
    }, 0..) |request, index| {
        var bytes: [256]u8 = undefined;
        @memcpy(bytes[0..request.len], request);
        var fault: FlushFailure = .{ .captured = .init(std.testing.allocator), .fail_at = 1 };
        defer fault.captured.deinit();
        var conn: ProtocolConnection = .{
            .input = .fixed(&bytes),
            .output = .init(std.testing.allocator),
            .faulty_output = &fault,
        };
        defer conn.output.deinit();
        conn.input.end = request.len;
        var app: App = .{};
        try std.testing.expectError(error.WriteFailed, talon.http.Http1Protocol(App).serveWithConfig(&conn, &app, conn.limits));
        try std.testing.expectEqual(@as(usize, 1), fault.calls);
        try std.testing.expectEqual(@as(usize, if (index < 2) 0 else 1), app.calls);
    }
}

test "HEAD protocol errors never emit a response body" {
    const App = struct {
        pub fn handle(_: *@This(), req: *talon.http.Request, res: *talon.http.Response) !void {
            if (std.mem.eql(u8, req.target(), "/missing")) return;
            if (std.mem.eql(u8, req.target(), "/invalid")) {
                return res.respond("bad", .{ .extra_headers = &.{.{ .name = "x", .value = "bad\r\nvalue" }} });
            }
            return error.HandlerFailed;
        }
    };
    for ([_][]const u8{ "HEAD", "GET" }) |method| {
        for ([_][]const u8{ "/error", "/missing", "/invalid", "/oversized" }) |target| {
            var request_buf: [256]u8 = undefined;
            const oversized = std.mem.eql(u8, target, "/oversized");
            const request = try std.fmt.bufPrint(&request_buf, "{s} {s} HTTP/1.1\r\nhost: test\r\ncontent-length: {d}\r\n\r\n", .{ method, target, @as(usize, if (oversized) 2 else 0) });
            var conn: ProtocolConnection = .{
                .input = .fixed(&request_buf),
                .output = .init(std.testing.allocator),
                .limits = .{ .max_header_size = 128, .max_body_size = 1, .min_body_data_rate = null },
            };
            conn.input.end = request.len;
            defer conn.output.deinit();
            var app: App = .{};
            const result = talon.http.Http1Protocol(App).serveWithConfig(&conn, &app, conn.limits);
            if (std.mem.eql(u8, target, "/error")) {
                try std.testing.expectError(error.HandlerFailed, result);
            } else if (std.mem.eql(u8, target, "/invalid")) {
                try std.testing.expectError(error.InvalidHeader, result);
            } else try result;
            const wire = conn.output.written();
            try std.testing.expect(std.mem.startsWith(u8, wire, if (oversized) "HTTP/1.1 413" else "HTTP/1.1 500"));
            const end = std.mem.indexOf(u8, wire, "\r\n\r\n").? + 4;
            if (std.mem.eql(u8, method, "HEAD")) {
                try std.testing.expectEqual(end, wire.len);
            } else {
                try std.testing.expectEqualStrings(if (oversized) "Payload Too Large" else "Internal Server Error", wire[end..]);
            }
        }
    }
}

test "closing response skips unread upload while keep-alive drains it" {
    const App = struct {
        close: bool,
        pub fn handle(self: *@This(), _: *talon.http.Request, res: *talon.http.Response) !void {
            try res.respond("rejected", .{ .status = .payload_too_large, .keep_alive = !self.close });
        }
    };
    for ([_]bool{ false, true }) |close| {
        for ([_]bool{ false, true }) |shutdown| {
            var request_buf: [256]u8 = undefined;
            const request = "POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 4\r\n\r\ndata";
            @memcpy(request_buf[0..request.len], request);
            var conn: ProtocolConnection = .{
                .input = .fixed(&request_buf),
                .output = .init(std.testing.allocator),
                .shutting_down = shutdown,
            };
            conn.input.end = request.len;
            defer conn.output.deinit();
            var app: App = .{ .close = close };
            try talon.http.Http1Protocol(App).serveWithConfig(&conn, &app, conn.limits);
            try std.testing.expectEqualStrings(if (close or shutdown) "data" else "", conn.input.buffered());
            try std.testing.expect(std.mem.endsWith(u8, conn.output.written(), "\r\n\r\nrejected"));
        }
    }
}

test "Response: failed output is committed and cannot receive a fallback response" {
    for ([_]bool{ false, true }) |chunked| {
        // A fixed writer this small fails partway through the response head.
        var buf: [16]u8 = undefined;
        var out: std.Io.Writer = .fixed(&buf);
        var date: talon.http.codec.DateCache = .{};
        var res: talon.http.Response = .{ .out = &out, .date = &date, .keep_alive = true };
        if (chunked) {
            var staging: [2]u8 = undefined;
            try std.testing.expectError(error.WriteFailed, res.startChunked(.{}, &staging));
        } else {
            try std.testing.expectError(error.WriteFailed, res.respond("hello", .{}));
        }
        try std.testing.expect(res.written);
    }
}

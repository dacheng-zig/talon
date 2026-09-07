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
            const options: talon.http.Response.RespondOptions = .{ .status = @enumFromInt(code) };
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
    input: std.Io.Reader,
    output: std.Io.Writer.Allocating,
    arena: *std.heap.ArenaAllocator,
    limits: talon.Limits = .{ .min_body_data_rate = null },
    shutting_down: bool = false,

    pub fn reader(self: *@This()) *std.Io.Reader {
        return &self.input;
    }
    pub fn writer(self: *@This()) *std.Io.Writer {
        return &self.output.writer;
    }
    pub fn setReadTimeout(_: *@This(), _: zio.Timeout) void {}
    pub fn setWriteTimeout(_: *@This(), _: zio.Timeout) void {}
    pub fn waitReadable(_: *@This(), _: zio.Timeout) !void {}
    pub fn isShuttingDown(self: *@This()) bool {
        return self.shutting_down;
    }
};

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
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            var request_buf: [256]u8 = undefined;
            const oversized = std.mem.eql(u8, target, "/oversized");
            const request = try std.fmt.bufPrint(&request_buf, "{s} {s} HTTP/1.1\r\nhost: test\r\ncontent-length: {d}\r\n\r\n", .{ method, target, @as(usize, if (oversized) 2 else 0) });
            var conn: ProtocolConnection = .{
                .input = .fixed(request),
                .output = .init(std.testing.allocator),
                .arena = &arena,
                .limits = .{ .max_body_size = 1, .min_body_data_rate = null },
            };
            defer conn.output.deinit();
            var app: App = .{};
            const result = talon.http.Http1Protocol(App).serve(&conn, &app);
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
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            var conn: ProtocolConnection = .{
                .input = .fixed("POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 4\r\n\r\ndata"),
                .output = .init(std.testing.allocator),
                .arena = &arena,
                .shutting_down = shutdown,
            };
            defer conn.output.deinit();
            var app: App = .{ .close = close };
            try talon.http.Http1Protocol(App).serve(&conn, &app);
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

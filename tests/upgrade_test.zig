//! Connection upgrade: a handler calls `req.upgrade.accept` to send 101 and
//! take over the connection, then speaks a post-HTTP protocol on the same
//! socket. Driven with a raw memory connection (the client speaks the upgrade
//! handshake by hand, since the HTTP client never requests one).

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const Server = talon.http.Server;
const Request = talon.http.Request;
const Response = talon.http.Response;
const MemoryListener = talon.MemoryListener;

// After 101, echoes one line back over the upgraded connection.
const EchoUpgradeApp = struct {
    pub fn handle(_: *EchoUpgradeApp, req: *Request, _: *Response) !void {
        try req.upgrade.accept(.{
            .protocol = "echo",
            .extra_headers = &.{.{ .name = "x-test", .value = "1" }},
        });
        const line = try req.upgrade.reader.takeDelimiterInclusive('\n');
        try req.upgrade.writer.writeAll(line);
        try req.upgrade.writer.flush();
    }
};

// Reads a response head, asserting the status line and returning nothing else.
fn expect101(r: *std.Io.Reader) !void {
    const status = try r.takeDelimiterInclusive('\n');
    try std.testing.expect(std.mem.indexOf(u8, status, "101 Switching Protocols") != null);
    var saw_upgrade = false;
    var saw_test = false;
    while (true) {
        const line = try r.takeDelimiterInclusive('\n');
        if (std.mem.eql(u8, line, "\r\n")) break; // end of head
        if (std.mem.indexOf(u8, line, "upgrade: echo") != null) saw_upgrade = true;
        if (std.mem.indexOf(u8, line, "x-test: 1") != null) saw_test = true;
    }
    try std.testing.expect(saw_upgrade and saw_test);
}

test "upgrade: handler sends 101 and takes over the connection" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: EchoUpgradeApp = .{};
    const Srv = Server(EchoUpgradeApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn client(l: *MemoryListener, s: *Srv) !void {
            const conn = try l.connect();
            defer conn.close();
            var wbuf: [256]u8 = undefined;
            var cw = conn.writer(&wbuf);
            var rbuf: [256]u8 = undefined;
            var cr = conn.reader(&rbuf);

            // Include protocol bytes in the same write as the head: upgrade
            // must keep the original reader's prefetched input.
            try cw.interface.writeAll("GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: echo\r\nConnection: upgrade\r\n\r\nhello upgraded\n");
            try cw.interface.flush();
            try expect101(&cr.interface);

            // The prefetched protocol bytes are echoed after the 101.
            const echo = try cr.interface.takeDelimiterInclusive('\n');
            try std.testing.expectEqualStrings("hello upgraded\n", echo);

            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.client, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

test "upgrade: handler errors preserve protocol ownership and pre-upgrade HTTP fallback" {
    const FailureApp = struct {
        upgraded: bool,
        pub fn handle(self: *@This(), req: *Request, _: *Response) !void {
            if (self.upgraded) {
                try req.upgrade.accept(.{
                    .protocol = "echo",
                    .extra_headers = &.{.{ .name = "x-test", .value = "1" }},
                });
                try req.upgrade.writer.writeAll("opaque protocol output\n");
                try req.upgrade.writer.flush();
            }
            return error.HandlerFailed;
        }
    };
    const Srv = Server(FailureApp);
    const Fns = struct {
        fn serve(server: *Srv, listener: *MemoryListener) !void {
            try server.serve(listener);
        }
        fn client(listener: *MemoryListener, server: *Srv, upgraded: bool) !void {
            defer server.shutdown();
            const conn = try listener.connect();
            defer conn.close();
            var wbuf: [256]u8 = undefined;
            var writer = conn.writer(&wbuf);
            var rbuf: [256]u8 = undefined;
            var reader = conn.reader(&rbuf);
            try writer.interface.writeAll("GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: echo\r\nConnection: upgrade\r\n\r\n");
            try writer.interface.flush();
            if (upgraded) {
                try expect101(&reader.interface);
                try std.testing.expectEqualStrings("opaque protocol output\n", try reader.interface.takeDelimiterInclusive('\n'));
            } else {
                try std.testing.expectEqualStrings("HTTP/1.1 500 Internal Server Error\r\n", try reader.interface.takeDelimiterInclusive('\n'));
                while (!std.mem.eql(u8, try reader.interface.takeDelimiterInclusive('\n'), "\r\n")) {}
                var body: [21]u8 = undefined;
                try reader.interface.readSliceAll(&body);
                try std.testing.expectEqualStrings("Internal Server Error", &body);
            }
            try std.testing.expectError(error.EndOfStream, reader.interface.takeByte());
        }
    };
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    for ([_]bool{ false, true }) |upgraded| {
        var listener = try MemoryListener.init(std.testing.allocator, .{});
        defer listener.deinit();
        var app: FailureApp = .{ .upgraded = upgraded };
        var server = try Srv.init(std.testing.allocator, &app, .{});
        defer server.deinit();
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(Fns.serve, .{ &server, &listener });
        try group.spawn(Fns.client, .{ &listener, &server, upgraded });
        try group.wait();
        try std.testing.expect(!group.hasFailed());
        const stats = server.stats();
        try std.testing.expectEqual(@as(u64, 1), stats.connection_errors);
        try std.testing.expectEqual(@as(u64, 1), stats.finished_tasks);
        try std.testing.expectEqual(@as(u64, 0), stats.active_tasks);
    }
}

// Recovers after exposing an initial prefix, so a fallback or retry is visible.
const RecoveringUpgradeWriter = struct {
    interface: std.Io.Writer,
    wire: [4096]u8 = undefined,
    written: usize = 0,
    attempts: usize = 0,
    failed_from_buffer: bool = false,

    fn record(self: *@This(), bytes: []const u8) void {
        std.debug.assert(bytes.len <= self.wire.len - self.written);
        @memcpy(self.wire[self.written..][0..bytes.len], bytes);
        self.written += bytes.len;
    }

    fn drain(writer: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *@This() = @alignCast(@fieldParentPtr("interface", writer));
        self.attempts += 1;
        if (self.attempts == 1) {
            self.failed_from_buffer = writer.end != 0;
            const prefix = if (writer.end != 0) writer.buffered() else data[0];
            self.record(prefix[0..@min(16, prefix.len)]);
            writer.end = 0;
            return error.WriteFailed;
        }
        self.record(writer.buffered());
        writer.end = 0;
        var consumed: usize = 0;
        for (data[0 .. data.len - 1]) |part| {
            self.record(part);
            consumed += part.len;
        }
        for (0..splat) |_| {
            const part = data[data.len - 1];
            self.record(part);
            consumed += part.len;
        }
        return consumed;
    }
};

test "upgrade: partial write and failed flush prevent fallback and repeated accept" {
    const Connection = struct {
        pub const timeout_capabilities: talon.core.TimeoutCapabilities = .{ .read = true, .write = true };
        input: std.Io.Reader,
        output: *std.Io.Writer,
        gpa: std.mem.Allocator = std.testing.allocator,
        limits: talon.Limits = .{ .max_header_size = 128, .min_body_data_rate = null },

        pub fn reader(self: *@This()) *std.Io.Reader {
            return &self.input;
        }
        pub fn writer(self: *@This()) *std.Io.Writer {
            return self.output;
        }
        pub fn initialReadTimeout(_: *const @This(), timeout: zio.Timeout) zio.Timeout {
            return timeout;
        }
        pub fn checkEstablishmentDeadline(_: *@This()) !void {}
        pub fn setReadTimeout(_: *@This(), _: zio.Timeout) talon.core.TimeoutError!void {}
        pub fn setWriteTimeout(_: *@This(), _: zio.Timeout) talon.core.TimeoutError!void {}
        pub fn waitReadable(_: *@This(), _: zio.Timeout) !void {}
        pub fn isShuttingDown(_: *@This()) bool {
            return false;
        }
    };
    const App = struct {
        taken: bool = false,
        swallow: bool,
        pub fn handle(self: *@This(), req: *Request, _: *Response) !void {
            req.upgrade.accept(.{ .protocol = "echo" }) catch |err| {
                self.taken = req.upgrade.taken.*;
                if (self.taken) try std.testing.expectError(error.AlreadyUpgraded, req.upgrade.accept(.{ .protocol = "echo" }));
                if (self.swallow) return;
                return err;
            };
            return error.ExpectedWriteFailure;
        }
    };
    for ([_]usize{ 16, 512 }) |capacity| {
        for ([_]bool{ false, true }) |swallow| {
            var input_buf: [256]u8 = undefined;
            const request = "GET /ws HTTP/1.1\r\nhost: x\r\nconnection: upgrade\r\nupgrade: echo\r\n\r\n";
            @memcpy(input_buf[0..request.len], request);
            var output_buf: [512]u8 = undefined;
            var writer: RecoveringUpgradeWriter = .{
                .interface = .{ .vtable = &.{ .drain = RecoveringUpgradeWriter.drain }, .buffer = output_buf[0..capacity] },
            };
            var conn: Connection = .{ .input = .fixed(&input_buf), .output = &writer.interface };
            conn.input.end = request.len;
            var app: App = .{ .swallow = swallow };
            const result = talon.http.Http1Protocol(App).serveWithConfig(&conn, &app, conn.limits);
            if (swallow) try result else try std.testing.expectError(error.WriteFailed, result);
            try std.testing.expect(app.taken);
            try std.testing.expectEqual(@as(usize, 1), writer.attempts);
            try std.testing.expectEqual(capacity == 512, writer.failed_from_buffer);
            try std.testing.expectEqualStrings("HTTP/1.1 101 Swi", writer.wire[0..writer.written]);
        }
    }
}

test "upgrade: validation is recoverable but successful accept cannot be repeated" {
    const Upgrade = std.meta.Child(@FieldType(Request, "upgrade"));
    const Setter = struct {
        fn set(ctx: *anyopaque, _: zio.Timeout) talon.core.TimeoutError!void {
            const calls: *usize = @ptrCast(@alignCast(ctx));
            calls.* += 1;
        }
    };
    const rejected = [_]Upgrade.AcceptOptions{
        .{ .protocol = "bad protocol" },
        .{ .protocol = "echo", .extra_headers = &.{.{ .name = "x", .value = "bad\r\nvalue" }} },
        .{ .protocol = "echo", .extra_headers = &.{.{ .name = "content-length", .value = "0" }} },
    };
    for (rejected) |options| {
        var input: std.Io.Reader = .fixed("");
        var buf: [512]u8 = undefined;
        var writer: std.Io.Writer = .fixed(&buf);
        var taken = false;
        var calls: usize = 0;
        var upgrade: Upgrade = .{ .reader = &input, .writer = &writer, .taken = &taken, .ctx = &calls, .set_read_timeout_fn = Setter.set };
        try std.testing.expectError(error.InvalidHeader, upgrade.accept(options));
        try std.testing.expect(!taken);
        try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
        try upgrade.accept(.{ .protocol = "echo" });
        try std.testing.expect(taken);
        const end = writer.end;
        const timeout_calls = calls;
        try std.testing.expectError(error.AlreadyUpgraded, upgrade.accept(.{ .protocol = "echo" }));
        try std.testing.expectEqual(end, writer.end);
        try std.testing.expectEqual(timeout_calls, calls);
    }
}

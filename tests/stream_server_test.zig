//! Integration tests for talon.StreamServer: the protocol-agnostic engine
//! serving real connections over MemoryListener, exercising graceful shutdown,
//! drain cancellation, and connection hijacking through the public API.

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const StreamServer = talon.StreamServer;
const MemoryListener = talon.MemoryListener;

const TestApp = struct {
    requests: u32 = 0,
};

/// Line-echo test protocol: one reply line per request line.
const LineEchoProto = struct {
    pub fn serve(conn: anytype, app: *TestApp) anyerror!void {
        const r = conn.reader();
        const w = conn.writer();
        while (true) {
            const line = r.takeDelimiterInclusive('\n') catch |err| switch (err) {
                error.EndOfStream => return,
                else => return err,
            };
            app.requests += 1;
            try w.writeAll("> ");
            try w.writeAll(line);
            try w.flush();
            if (conn.isShuttingDown()) return;
        }
    }
};

test "StreamServer: serves connections over MemoryListener and shuts down gracefully" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: TestApp = .{};
    const Server = StreamServer(LineEchoProto, TestApp);
    var server = try Server.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Server, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn client(l: *MemoryListener, s: *Server) !void {
            const conn = try l.connect();
            defer conn.close();

            var wbuf: [64]u8 = undefined;
            var w = conn.writer(&wbuf);
            var rbuf: [64]u8 = undefined;
            var r = conn.reader(&rbuf);

            try w.interface.writeAll("hello\n");
            try w.interface.flush();
            const reply1 = try r.interface.takeDelimiterInclusive('\n');
            try std.testing.expectEqualStrings("> hello\n", reply1);

            try w.interface.writeAll("again\n");
            try w.interface.flush();
            const reply2 = try r.interface.takeDelimiterInclusive('\n');
            try std.testing.expectEqualStrings("> again\n", reply2);

            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.client, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(2, app.requests);
}

test "StreamServer: drain cancels connections that ignore shutdown" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    // Protocol that never returns on its own: blocks reading a line that
    // never arrives. Shutdown must hard-cancel it after drain_timeout.
    const StuckProto = struct {
        pub fn serve(conn: anytype, app: *TestApp) anyerror!void {
            _ = app;
            _ = try conn.reader().takeDelimiterInclusive('\n');
        }
    };

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: TestApp = .{};
    const Server = StreamServer(StuckProto, TestApp);
    var server = try Server.init(std.testing.allocator, &app, .{
        .limits = .{ .drain_timeout = .fromMilliseconds(50) },
    });
    defer server.deinit();

    var served: zio.ResetEvent = .init;

    const Fns = struct {
        fn runServer(s: *Server, l: *MemoryListener, done: *zio.ResetEvent) !void {
            try s.serve(l);
            done.set();
        }
        fn client(l: *MemoryListener, s: *Server, done: *zio.ResetEvent) !void {
            const conn = try l.connect();
            // Keep the connection open across shutdown so the stuck handler
            // can only exit via drain-timeout cancellation.
            defer conn.close();
            try zio.sleep(.fromMilliseconds(10));
            s.shutdown();
            try done.wait();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener, &served });
    try group.spawn(Fns.client, .{ &listener, &server, &served });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

test "StreamServer: hijacked connection is not closed by the server" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const HijackProto = struct {
        pub fn serve(conn: anytype, app: *TestApp) anyerror!void {
            _ = app;
            const raw = conn.hijack();
            // Hijacker owns the connection: write a farewell and close it.
            var wbuf: [32]u8 = undefined;
            var w = raw.writer(&wbuf);
            try w.interface.writeAll("bye\n");
            try w.interface.flush();
            raw.close();
        }
    };

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: TestApp = .{};
    const Server = StreamServer(HijackProto, TestApp);
    var server = try Server.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Server, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn client(l: *MemoryListener, s: *Server) !void {
            const conn = try l.connect();
            defer conn.close();
            var rbuf: [32]u8 = undefined;
            var r = conn.reader(&rbuf);
            const line = try r.interface.takeDelimiterInclusive('\n');
            try std.testing.expectEqualStrings("bye\n", line);
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
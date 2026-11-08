const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

test "server stats: real TCP permit backpressure preserves coherent snapshots on two executors" {
    const App = struct {
        first_entered: zio.ResetEvent = .init,
        release_first: zio.ResetEvent = .init,
        calls: std.atomic.Value(usize) = .init(0),
    };
    const Proto = struct {
        pub fn serve(conn: anytype, app: *App) !void {
            const first = app.calls.fetchAdd(1, .acq_rel) == 0;
            if (first) {
                app.first_entered.set();
                try app.release_first.wait();
            }
            try conn.writer().writeAll("ok");
            try conn.writer().flush();
        }
    };
    const Server = talon.StreamServer(Proto, App);
    const F = struct {
        fn serve(s: *Server, l: *talon.TcpListener) !void {
            try s.serve(l);
        }
        fn client(s: *Server, app: *App, port: u16) !void {
            defer s.shutdown();
            defer app.release_first.set();
            const dialer: talon.core.TcpDialer = .{};
            const first = try dialer.dial(.{ .host = "127.0.0.1", .port = port }, .fromSeconds(2));
            defer first.close();
            try app.first_entered.timedWait(.fromSeconds(2));
            const deadline = zio.Timeout.fromSeconds(2).toDeadline();
            while (!s.stats().waiting_for_permit) {
                if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline.deadline.toNanoseconds()) return error.TestUnexpectedResult;
                try zio.yield();
            }
            const second = try dialer.dial(.{ .host = "127.0.0.1", .port = port }, .fromSeconds(2));
            defer second.close();
            try zio.sleep(.fromMilliseconds(10));
            for (0..100) |_| {
                const snapshot = s.stats();
                try std.testing.expectEqual(snapshot.accepted, snapshot.finished_tasks + snapshot.active_tasks);
                try std.testing.expectEqual(@as(u64, 1), snapshot.accepted);
                try std.testing.expectEqual(@as(usize, 1), snapshot.active_tasks);
                try std.testing.expect(snapshot.waiting_for_permit);
            }
            app.release_first.set();
            var buf: [2]u8 = undefined;
            var reader = second.reader(&buf);
            reader.setTimeout(.fromSeconds(2));
            try std.testing.expectEqualStrings("ok", try reader.interface.take(2));
        }
    };
    for ([_]u8{ 1, 2 }) |executors| {
        const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
        defer rt.deinit();
        var listener = try talon.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
        var app: App = .{};
        var server = try Server.init(std.testing.allocator, &app, .{ .limits = .{ .max_connections = 1 } });
        defer server.deinit();
        try std.testing.expectEqual(@as(u64, 0), server.stats().accepted);
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.serve, .{ &server, &listener });
        try group.spawn(F.client, .{ &server, &app, listener.server.socket.address.ip.getPort() });
        try group.wait();
        try std.testing.expect(!group.hasFailed());
        const snapshot = server.stats();
        try std.testing.expectEqual(@as(u64, 2), snapshot.accepted);
        try std.testing.expectEqual(snapshot.accepted, snapshot.finished_tasks);
        try std.testing.expectEqual(@as(usize, 0), snapshot.active_tasks);
        try std.testing.expectEqual(@as(usize, 1), snapshot.peak_active_tasks);
        try std.testing.expect(!snapshot.waiting_for_permit);
        try std.testing.expect(snapshot.permit_wait_ns >= 10 * std.time.ns_per_ms);
        try std.testing.expectEqual(@as(u64, 0), snapshot.connection_errors);
        try std.testing.expectEqual(@as(u64, 0), snapshot.forced_drains);
    }
}

test "server stats: FD backoff is distinct from fatal accept and does not create tasks" {
    const App = struct {};
    const Proto = struct {
        pub fn serve(_: anytype, _: *App) !void {}
    };
    const Listener = struct {
        pub const RawConnection = talon.core.MemoryConnection;
        calls: usize = 0,
        closed: bool = false,
        pub fn accept(self: *@This()) !RawConnection {
            self.calls += 1;
            return switch (self.calls) {
                1 => error.ProcessFdQuotaExceeded,
                2 => error.SystemFdQuotaExceeded,
                else => error.TestListenerFailed,
            };
        }
        pub fn close(self: *@This()) void {
            self.closed = true;
        }
    };
    const Server = talon.StreamServer(Proto, App);
    const F = struct {
        fn run(s: *Server, l: *Listener) !void {
            try std.testing.expectError(error.TestListenerFailed, s.serve(l));
        }
    };
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var app: App = .{};
    var server = try Server.init(std.testing.allocator, &app, .{});
    defer server.deinit();
    var listener: Listener = .{};
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.run, .{ &server, &listener });
    try group.wait();
    try std.testing.expect(!group.hasFailed() and listener.closed);
    const snapshot = server.stats();
    try std.testing.expectEqual(@as(u64, 2), snapshot.accept_fd_backoffs);
    try std.testing.expectEqual(@as(u64, 1), snapshot.accept_failures);
    try std.testing.expectEqual(@as(u64, 0), snapshot.accepted);
    try std.testing.expectEqual(@as(usize, 0), snapshot.active_tasks);
    try std.testing.expect(!snapshot.waiting_for_permit);
}

test "server stats: buffer denial and handshake rejection finish accepted ownership" {
    const App = struct { cancel: bool = false };
    const Proto = struct {
        pub fn handshake(_: anytype, app: *App) !void {
            if (app.cancel) return error.Canceled;
            return error.HandshakeTimeout;
        }
        pub fn serve(_: anytype, _: *App) !void {
            return error.TestUnexpectedResult;
        }
    };
    const Server = talon.StreamServer(Proto, App);
    const F = struct {
        fn serve(s: *Server, l: *talon.MemoryListener) !void {
            try s.serve(l);
        }
        fn client(s: *Server, l: *talon.MemoryListener, allocator: *std.testing.FailingAllocator, deny: bool) !void {
            defer s.shutdown();
            if (deny) allocator.fail_index = allocator.alloc_index;
            const peer = try l.connect();
            defer peer.close();
            var buffer: [1]u8 = undefined;
            var reader = peer.reader(&buffer);
            reader.setTimeout(.fromSeconds(2));
            try std.testing.expectError(error.EndOfStream, reader.interface.takeByte());
        }
    };
    for ([_]enum { timeout, buffer_denied, canceled }{ .timeout, .buffer_denied, .canceled }) |mode| {
        const deny = mode == .buffer_denied;
        const rt = try zio.Runtime.init(std.testing.allocator, .{});
        defer rt.deinit();
        var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
        defer listener.deinit();
        var allocator = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        var app: App = .{ .cancel = mode == .canceled };
        var server = try Server.init(allocator.allocator(), &app, .{});
        defer server.deinit();
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.serve, .{ &server, &listener });
        try group.spawn(F.client, .{ &server, &listener, &allocator, deny });
        try group.wait();
        try std.testing.expect(!group.hasFailed());
        const snapshot = server.stats();
        try std.testing.expectEqual(@as(u64, 1), snapshot.accepted);
        try std.testing.expectEqual(snapshot.accepted, snapshot.finished_tasks);
        try std.testing.expectEqual(@as(usize, 0), snapshot.active_tasks);
        try std.testing.expectEqual(@as(u64, if (deny) 1 else 0), snapshot.buffer_failures);
        try std.testing.expectEqual(@as(u64, if (mode == .timeout) 1 else 0), snapshot.connection_errors);
        try std.testing.expectEqual(@as(u64, if (mode == .timeout) 1 else 0), snapshot.handshake_timeouts);
        try std.testing.expectEqual(@as(u64, if (mode == .canceled) 1 else 0), snapshot.canceled_returns);
    }
}

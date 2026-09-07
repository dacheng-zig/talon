const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

test "limits: reject zero body rate or grace before serving" {
    const App = struct {
        pub fn handle(_: *@This(), _: *talon.http.Request, _: *talon.http.Response) !void {}
    };
    const Srv = talon.http.Server(App);
    var app: App = .{};
    for ([_]talon.core.DataRate{
        .{ .bytes_per_sec = 0, .grace = .fromSeconds(1) },
        .{ .bytes_per_sec = 1, .grace = .fromNanoseconds(0) },
    }) |rate| {
        try std.testing.expectError(error.InvalidDataRate, Srv.init(std.testing.allocator, &app, .{
            .limits = .{ .min_body_data_rate = rate },
        }));
    }
    // Disabling the policy remains supported.
    var server = try Srv.init(std.testing.allocator, &app, .{ .limits = .{ .min_body_data_rate = null } });
    defer server.deinit();
}

test "limits: fragmented TCP headers cannot renew the total deadline" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try talon.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    const port = listener.server.socket.address.ip.getPort();
    const App = struct {
        hits: usize = 0,
        pub fn handle(self: *@This(), _: *talon.http.Request, res: *talon.http.Response) !void {
            self.hits += 1;
            try res.respond("ok", .{ .keep_alive = false });
        }
    };
    const Srv = talon.http.Server(App);
    var app: App = .{};
    var server = try Srv.init(std.testing.allocator, &app, .{ .limits = .{ .header_read_timeout = .fromMilliseconds(200) } });
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *talon.TcpListener) !void {
            try s.serve(l);
        }
        fn client(pn: u16, s: *Srv) !void {
            defer s.shutdown();
            const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", pn);
            const stream = try addr.connect(.{});
            defer stream.close();
            for ([_][]const u8{ "GET / HTTP/1.1\r\n", "Host: test\r\n", "X-A: a\r\n", "X-B: b\r\n", "\r\n" }) |part| {
                stream.writeAll(part, .fromSeconds(1)) catch return;
                try zio.sleep(.fromMilliseconds(80));
            }
            var buf: [1024]u8 = undefined;
            _ = stream.read(&buf, .fromSeconds(1)) catch return;
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener });
    try group.spawn(F.client, .{ port, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(@as(usize, 0), app.hits);
}

test "limits: body network wait is cumulative and excludes handler delay" {
    for ([_]bool{ false, true }) |chunked| {
        for ([_]bool{ false, true }) |slow| {
            const rt = try zio.Runtime.init(std.testing.allocator, .{});
            defer rt.deinit();
            var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
            defer listener.deinit();
            const App = struct {
                slow: bool,
                completed: bool = false,
                too_slow: bool = false,
                pub fn handle(self: *@This(), req: *talon.http.Request, res: *talon.http.Response) !void {
                    if (!self.slow) try zio.sleep(.fromMilliseconds(250));
                    req.body.discard() catch |err| {
                        self.too_slow = err == error.BodyTooSlow;
                        return;
                    };
                    self.completed = true;
                    try res.respond("ok", .{ .keep_alive = false });
                }
            };
            const Srv = talon.http.Server(App);
            var app: App = .{ .slow = slow };
            var server = try Srv.init(std.testing.allocator, &app, .{ .limits = .{
                .min_body_data_rate = .{ .bytes_per_sec = 100, .grace = .fromMilliseconds(100) },
                .drain_timeout = .fromSeconds(1),
            } });
            defer server.deinit();
            const F = struct {
                fn serve(s: *Srv, l: *talon.MemoryListener) !void {
                    try s.serve(l);
                }
                fn client(l: *talon.MemoryListener, s: *Srv, is_chunked: bool, is_slow: bool) !void {
                    defer s.shutdown();
                    const conn = try l.connect();
                    defer conn.close();
                    var wb: [128]u8 = undefined;
                    var w = conn.writer(&wb);
                    try w.interface.writeAll(if (is_chunked)
                        "POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n"
                    else
                        "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\n");
                    try w.interface.flush();
                    const bytes = if (is_chunked) "5\r\nhello\r\n0\r\n\r\n" else "hello";
                    for (bytes) |byte| {
                        if (is_slow) try zio.sleep(.fromMilliseconds(40));
                        w.interface.writeByte(byte) catch return;
                        w.interface.flush() catch return;
                    }
                    var rb: [256]u8 = undefined;
                    var r = conn.reader(&rb);
                    r.setTimeout(.fromSeconds(2));
                    _ = r.interface.takeDelimiterInclusive('\n') catch return;
                }
            };
            var group: zio.Group = .init;
            defer group.cancel();
            try group.spawn(F.serve, .{ &server, &listener });
            try group.spawn(F.client, .{ &listener, &server, chunked, slow });
            try group.wait();
            try std.testing.expect(!group.hasFailed());
            try std.testing.expectEqual(!slow, app.completed);
            try std.testing.expectEqual(slow, app.too_slow);
        }
    }
}

test "limits: response write timeout releases a connection whose peer never reads" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try talon.MemoryListener.init(std.testing.allocator, .{ .pipe_buffer_size = 128 });
    defer listener.deinit();
    const App = struct {
        done: zio.ResetEvent = .init,
        write_failed: bool = false,
        pub fn handle(self: *@This(), _: *talon.http.Request, res: *talon.http.Response) !void {
            defer self.done.set();
            const payload = [_]u8{'x'} ** 8192;
            res.respond(&payload, .{}) catch {
                self.write_failed = true;
            };
        }
    };
    const Srv = talon.http.Server(App);
    var app: App = .{};
    var server = try Srv.init(std.testing.allocator, &app, .{ .limits = .{ .write_timeout = .fromMilliseconds(50) } });
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *talon.MemoryListener) !void {
            try s.serve(l);
        }
    };
    var task = try zio.spawn(F.serve, .{ &server, &listener });
    defer {
        server.shutdown();
        task.join() catch {};
    }
    const conn = try listener.connect();
    defer conn.close();
    var buf: [128]u8 = undefined;
    var w = conn.writer(&buf);
    try w.interface.writeAll("GET / HTTP/1.1\r\nHost: h\r\n\r\n");
    try w.interface.flush();
    try app.done.timedWait(.fromSeconds(2));
    try std.testing.expect(app.write_failed);
}

test "limits: request arena releases exceptional allocations before reuse" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    const App = struct {
        arena: ?*std.heap.ArenaAllocator = null,
        requests: usize = 0,
        capacity_after: usize = 0,
        pub fn handle(self: *@This(), req: *talon.http.Request, res: *talon.http.Response) !void {
            self.requests += 1;
            if (self.requests == 1) {
                _ = try req.arena.alloc(u8, 2 * 1024 * 1024);
            } else self.capacity_after = self.arena.?.queryCapacity();
            try res.respond("", .{});
        }
    };
    const Proto = struct {
        pub fn serve(conn: anytype, app: *App) !void {
            app.arena = conn.arena;
            try talon.http.Http1Protocol(App).serve(conn, app);
        }
    };
    const Srv = talon.StreamServer(Proto, App);
    var app: App = .{};
    var server = try Srv.init(std.testing.allocator, &app, .{ .limits = .{ .max_retained_arena = 8192 } });
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *talon.MemoryListener) !void {
            try s.serve(l);
        }
    };
    var task = try zio.spawn(F.serve, .{ &server, &listener });
    const conn = try listener.connect();
    defer conn.close();
    var wb: [128]u8 = undefined;
    var rb: [1024]u8 = undefined;
    var w = conn.writer(&wb);
    var r = conn.reader(&rb);
    r.setTimeout(.fromSeconds(2));
    for (0..2) |_| {
        try w.interface.writeAll("GET / HTTP/1.1\r\nHost: h\r\n\r\n");
        try w.interface.flush();
        while (true) {
            const line = try r.interface.takeDelimiterInclusive('\n');
            if (std.mem.eql(u8, line, "\r\n")) break;
        }
    }
    server.shutdown();
    try task.join();
    try std.testing.expectEqual(@as(usize, 2), app.requests);
    try std.testing.expect(app.capacity_after <= 8192);
}

test "limits: unhandled body rate failure returns 408 before any response is committed" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    const App = struct {
        pub fn handle(_: *@This(), req: *talon.http.Request, res: *talon.http.Response) !void {
            try req.body.discard();
            try res.respond("ok", .{});
        }
    };
    const Srv = talon.http.Server(App);
    var app: App = .{};
    var server = try Srv.init(std.testing.allocator, &app, .{ .limits = .{
        .min_body_data_rate = .{ .bytes_per_sec = 100, .grace = .fromMilliseconds(50) },
    } });
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *talon.MemoryListener) !void {
            try s.serve(l);
        }
    };
    var task = try zio.spawn(F.serve, .{ &server, &listener });
    defer {
        server.shutdown();
        task.join() catch {};
    }
    const conn = try listener.connect();
    defer conn.close();
    var wb: [128]u8 = undefined;
    var rb: [256]u8 = undefined;
    var w = conn.writer(&wb);
    var r = conn.reader(&rb);
    r.setTimeout(.fromSeconds(2));
    try w.interface.writeAll("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\n\r\n");
    try w.interface.flush();
    try std.testing.expectEqualStrings("HTTP/1.1 408 Request Timeout\r\n", try r.interface.takeDelimiterInclusive('\n'));
}

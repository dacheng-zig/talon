const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");
const client = talon.http.client;
const C = client.Client(client.MemoryConnector);
const Origin = client.Origin;
const a: Origin = .{ .host = "a", .port = 80 };
const b: Origin = .{ .host = "b", .port = 80 };

test "pool budget: decoder byte capacities are fixed at initialization" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    var c = C.init(std.testing.allocator, .{ .listener = &listener }, .{});
    defer c.deinit();
    const limits = c.pool.config;
    c.pool.config.max_decoder_bytes = 0;
    try std.testing.expectError(error.ResourceLimitsChanged, c.pool.checkout(a, .none));
    c.pool.config = limits;
    c.pool.config.max_idle_decoder_bytes = 0;
    try std.testing.expectError(error.ResourceLimitsChanged, c.pool.checkout(a, .none));
    try std.testing.expectEqual(@as(u64, 0), c.poolStats().created);
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
}

fn awaitWaiting(c: anytype, count: usize) !void {
    for (0..1000) |_| {
        if (c.poolStats().waiting == count) return;
        try zio.sleep(.fromMilliseconds(1));
    }
    return error.WaiterNotQueued;
}

test "pool budget: global cap covers distinct origins and reuse independently of idle retention" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    var c = C.init(std.testing.allocator, .{ .listener = &listener }, .{ .pool = .{
        .max_active = 1,
        .max_waiters = 0,
    } });
    defer c.deinit();
    const first = try c.pool.checkout(a, .none);
    const peer = try listener.accept();
    defer peer.close();
    try std.testing.expectError(error.PoolQueueFull, c.pool.checkout(b, .none));
    const long_host = @as([600]u8, @splat('h'));
    try std.testing.expectError(error.PoolQueueFull, c.pool.checkout(.{ .host = &long_host, .port = 80 }, .none));
    try std.testing.expectEqual(@as(u64, 1), c.poolStats().created);
    c.pool.checkin(first.conn, true);
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
    const reused = try c.pool.checkout(a, .none);
    try std.testing.expect(reused.reused);
    try std.testing.expectEqual(@as(usize, 1), c.poolStats().active);
    try std.testing.expectError(error.PoolQueueFull, c.pool.checkout(b, .none));
    c.pool.checkin(reused.conn, true);
    const other = try c.pool.checkout(b, .none);
    const other_peer = try listener.accept();
    defer other_peer.close();
    try std.testing.expectEqual(@as(usize, 1), c.pool.idle_total);
    c.pool.checkin(other.conn, false);
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
}

test "pool budget: bounded queue rejects excess and cancellation frees capacity and origin state" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    var c = C.init(std.testing.allocator, .{ .listener = &listener }, .{ .pool = .{
        .max_active = 1,
        .max_waiters = 1,
        .max_per_origin = 1,
        .pool_wait = .none,
    } });
    defer c.deinit();
    const held = try c.pool.checkout(a, .none);
    const peer = try listener.accept();
    defer peer.close();
    const F = struct {
        fn canceled(cl: *C) !void {
            try std.testing.expectError(error.Canceled, cl.pool.checkout(b, .none));
        }
        fn handoff(cl: *C) !void {
            const next = try cl.pool.checkout(a, .none);
            try std.testing.expect(next.reused);
            cl.pool.checkin(next.conn, false);
        }
    };
    var task = try zio.spawn(F.canceled, .{&c});
    defer task.cancel();
    try awaitWaiting(&c, 1);
    try std.testing.expectError(error.PoolQueueFull, c.pool.checkout(.{ .host = "excess", .port = 80 }, .none));
    try std.testing.expectEqual(@as(u64, 1), c.poolStats().created);
    task.cancel();
    try task.join();
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().waiting);
    try std.testing.expectEqual(@as(usize, 1), c.pool.origins.count());
    var handoff = try zio.spawn(F.handoff, .{&c});
    defer handoff.cancel();
    try awaitWaiting(&c, 1);
    c.pool.checkin(held.conn, true);
    try handoff.join();
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().waiting);
    try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
}

test "pool budget: saturated origin never reserves global capacity while waiting" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    var c = C.init(std.testing.allocator, .{ .listener = &listener }, .{ .pool = .{
        .max_active = 2,
        .max_per_origin = 1,
        .max_waiters = 2,
        .pool_wait = .none,
    } });
    defer c.deinit();
    const held = try c.pool.checkout(a, .none);
    const peer = try listener.accept();
    defer peer.close();
    const other_held = try c.pool.checkout(b, .none);
    const held_peer = try listener.accept();
    defer held_peer.close();
    const F = struct {
        fn run(cl: *C) !void {
            try std.testing.expectError(error.Canceled, cl.pool.checkout(a, .none));
        }
        fn eligible(cl: *C) !void {
            const next = try cl.pool.checkout(b, .none);
            cl.pool.checkin(next.conn, false);
        }
    };
    var waiter = try zio.spawn(F.run, .{&c});
    defer waiter.cancel();
    try awaitWaiting(&c, 1);
    var eligible = try zio.spawn(F.eligible, .{&c});
    defer eligible.cancel();
    try awaitWaiting(&c, 2);
    c.pool.checkin(other_held.conn, false);
    try eligible.join();
    const new_peer = try listener.accept();
    new_peer.close();
    try std.testing.expectEqual(@as(usize, 1), c.poolStats().waiting);
    for (0..3) |_| {
        const other = try c.pool.checkout(b, .none);
        const other_peer = try listener.accept();
        other_peer.close();
        try std.testing.expectEqual(@as(usize, 2), c.poolStats().active);
        c.pool.checkin(other.conn, false);
    }
    waiter.cancel();
    try waiter.join();
    c.pool.checkin(held.conn, false);
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().waiting);
}

test "pool budget: cancellation racing a grant never leaks reservations or queue nodes" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    const F = struct {
        fn run(cl: *C) !void {
            const next = cl.pool.checkout(a, .none) catch |err| {
                try std.testing.expectEqual(error.Canceled, err);
                return;
            };
            cl.pool.checkin(next.conn, false);
        }
    };
    for (0..100) |i| {
        var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
        defer listener.deinit();
        var c = C.init(std.testing.allocator, .{ .listener = &listener }, .{ .pool = .{
            .max_active = 1,
            .max_per_origin = 1,
            .pool_wait = .none,
        } });
        defer c.deinit();
        const held = try c.pool.checkout(a, .none);
        const peer = try listener.accept();
        defer peer.close();
        var waiter = try zio.spawn(F.run, .{&c});
        defer waiter.cancel();
        try awaitWaiting(&c, 1);
        if (i % 2 == 0) waiter.cancel();
        c.pool.checkin(held.conn, false);
        waiter.cancel();
        try waiter.join();
        try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
        try std.testing.expectEqual(@as(usize, 0), c.poolStats().waiting);
        try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
    }
}

test "pool budget: dial cancellation and failure return active reservations" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    const Connector = struct {
        pub const RawConnection = talon.core.MemoryConnection;
        started: *zio.ResetEvent,
        finish: *zio.ResetEvent,
        pub fn connect(self: @This(), _: Origin, _: zio.Timeout) !RawConnection {
            self.started.set();
            try self.finish.wait();
            return error.DialFailed;
        }
    };
    const Cl = client.Client(Connector);
    var started: zio.ResetEvent = .init;
    var finish: zio.ResetEvent = .init;
    var c = Cl.init(std.testing.allocator, .{ .started = &started, .finish = &finish }, .{ .pool = .{
        .max_active = 1,
        .max_per_origin = 1,
        .pool_wait = .fromMilliseconds(30),
    } });
    defer c.deinit();
    const F = struct {
        fn run(cl: *Cl) !void {
            try std.testing.expectError(error.Canceled, cl.pool.checkout(a, .none));
        }
    };
    var dial = try zio.spawn(F.run, .{&c});
    defer dial.cancel();
    try started.timedWait(.fromSeconds(2));
    try std.testing.expectEqual(@as(usize, 1), c.poolStats().active);
    try std.testing.expectError(error.PoolWaitTimeout, c.pool.checkout(b, .none));
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().waiting);
    dial.cancel();
    try dial.join();
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
    try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
    finish.set();
    try std.testing.expectError(error.DialFailed, c.pool.checkout(a, .none));
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
    try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
}

test "pool budget: explicit unlimited and long-origin limits cannot be bypassed" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    var c = C.init(std.testing.allocator, .{ .listener = &listener }, .{ .pool = .{
        .max_active = null,
        .max_per_origin = 1,
        .max_waiters = 0,
    } });
    defer c.deinit();
    const long_host = @as([600]u8, @splat('h'));
    const origin: Origin = .{ .host = &long_host, .port = 80 };
    const held = try c.pool.checkout(origin, .none);
    const peer = try listener.accept();
    defer peer.close();
    try std.testing.expectEqual(@as(usize, 0), held.conn.origin_key.len);
    try std.testing.expectError(error.PoolQueueFull, c.pool.checkout(origin, .none));
    const other = try c.pool.checkout(a, .none);
    const other_peer = try listener.accept();
    other_peer.close();
    try std.testing.expectEqual(@as(usize, 2), c.poolStats().active);
    c.pool.checkin(other.conn, false);
    c.pool.checkin(held.conn, false);
    try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
    for ([_]client.PoolConfig{ .{ .max_active = 0 }, .{ .max_per_origin = 0 } }) |config| {
        var invalid = C.init(std.testing.allocator, .{ .listener = &listener }, .{ .pool = config });
        defer invalid.deinit();
        try std.testing.expectError(error.InvalidPoolLimit, invalid.pool.checkout(a, .none));
        try std.testing.expectEqual(@as(u64, 0), invalid.poolStats().created);
    }
}

test "pool budget: zero wait permits immediate acquisition but expired total rejects it" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    var c = C.init(std.testing.allocator, .{ .listener = &listener }, .{ .pool = .{
        .max_active = 1,
        .pool_wait = .fromNanoseconds(0),
    } });
    defer c.deinit();
    const held = try c.pool.checkout(a, .none);
    const peer = try listener.accept();
    defer peer.close();
    try std.testing.expectError(error.PoolWaitTimeout, c.pool.checkout(b, .none));
    c.pool.checkin(held.conn, false);
    try std.testing.expectError(error.PoolWaitTimeout, c.pool.checkoutWithDeadline(a, .none, zio.Timeout.fromNanoseconds(0).toDeadline()));
    try std.testing.expectEqual(@as(u64, 1), c.poolStats().created);
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
}

test "pool budget: Client total deadline includes admission and streamed Response ownership" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    const App = struct {
        pub fn handle(_: *@This(), _: *talon.http.Request, res: *talon.http.Response) !void {
            try res.respond("body", .{});
        }
    };
    const Srv = talon.http.Server(App);
    var app: App = .{};
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *talon.MemoryListener) !void {
            try s.serve(l);
        }
        fn run(s: *Srv, l: *talon.MemoryListener) !void {
            defer s.shutdown();
            var c = C.init(std.testing.allocator, .{ .listener = l }, .{ .pool = .{
                .max_active = 1,
                .pool_wait = .fromSeconds(2),
            } });
            defer c.deinit();
            var held = try c.get(a, "/");
            try std.testing.expectEqual(@as(usize, 1), c.poolStats().active);
            c.timeouts.total = .fromMilliseconds(40);
            var elapsed = zio.Stopwatch.start();
            try std.testing.expectError(error.PoolWaitTimeout, c.get(b, "/"));
            try std.testing.expect(elapsed.read().toMilliseconds() < 500);
            try std.testing.expectEqual(@as(u64, 1), c.poolStats().created);
            try std.testing.expectEqual(@as(usize, 0), c.poolStats().waiting);
            held.deinit();
            c.timeouts.total = .fromSeconds(2);
            var next = try c.get(a, "/");
            next.deinit();
            try std.testing.expectEqual(@as(u64, 1), c.poolStats().reused);
            try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener });
    try group.spawn(F.run, .{ &server, &listener });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

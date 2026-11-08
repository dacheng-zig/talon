const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");
const core = talon.core;
const http = talon.http.client;
const a: http.Origin = .{ .host = "a", .port = 80 };
const b: http.Origin = .{ .host = "b", .port = 80 };
const c: http.Origin = .{ .host = "c", .port = 80 };

test "idle reclaim: cached client connections release budget for a previously denied origin" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var budget = core.AllocationBudget.init(std.testing.allocator, 128 * 1024);
    defer budget.deinit();
    {
        // Memory pipe allocations are listener-owned and deliberately separate.
        var listener = try core.MemoryListener.init(std.testing.allocator, .{});
        defer listener.deinit();
        const PlainClient = http.Client(http.MemoryConnector);
        var cl = PlainClient.init(budget.allocator(), .{ .listener = &listener }, .{ .pool = .{ .max_per_origin = 1 } });
        defer cl.deinit();
        const first = try cl.pool.checkout(a, .none);
        _ = try first.conn.arena.allocator().alloc(u8, 512);
        cl.pool.checkin(first.conn, true);
        const second = try cl.pool.checkout(b, .none);
        cl.pool.checkin(second.conn, true);
        try std.testing.expect(cl.poolStats().idle_response_arena_bytes > 0);
        const fill = try budget.allocator().alloc(u8, 128 * 1024 - budget.stats().reserved_bytes);
        defer budget.allocator().free(fill);
        try std.testing.expectError(error.OutOfMemory, cl.pool.checkout(c, .none));
        try std.testing.expectEqual(@as(usize, 0), cl.poolStats().active);
        try std.testing.expectEqual(@as(usize, 2), cl.trimIdleConnections());
        try std.testing.expectEqual(@as(usize, 0), cl.pool.buckets.count());
        try std.testing.expectEqual(@as(usize, 0), cl.pool.origins.count());
        try std.testing.expectEqual(@as(usize, 0), cl.poolStats().idle_response_arena_bytes);
        try std.testing.expectEqual(fill.len, budget.stats().reserved_bytes);
        const recovered = try cl.pool.checkout(c, .none);
        try std.testing.expect(!recovered.reused);
        cl.pool.checkin(recovered.conn, false);
        try std.testing.expectEqual(@as(usize, 0), cl.trimIdleConnections());
        try std.testing.expectEqual(fill.len, budget.stats().reserved_bytes);
        try std.testing.expectEqual(@as(u64, 2), cl.poolStats().evicted);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
}

const Probe = struct {
    next_id: std.atomic.Value(usize) = .init(0),
    closed: std.atomic.Value(usize) = .init(0),
    closing: zio.ResetEvent = .init,
    release_close: zio.ResetEvent = .init,
};
const Raw = struct {
    inner: core.MemoryConnection,
    probe: *Probe,
    id: usize,
    pub const Reader = core.MemoryConnection.Reader;
    pub const Writer = core.MemoryConnection.Writer;
    pub fn reader(self: Raw, buffer: []u8) Reader {
        return self.inner.reader(buffer);
    }
    pub fn writer(self: Raw, buffer: []u8) Writer {
        return self.inner.writer(buffer);
    }
    pub fn remoteInfo(_: Raw) core.RemoteInfo {
        return .memory;
    }
    pub fn shutdown(self: Raw) void {
        self.inner.shutdown();
    }
    pub fn close(self: Raw) void {
        if (self.id == 0) {
            self.probe.closing.set();
            self.probe.release_close.wait() catch {};
        }
        _ = self.probe.closed.fetchAdd(1, .monotonic);
        self.inner.close();
    }
};
const Connector = struct {
    listener: *core.MemoryListener,
    probe: *Probe,
    pub const RawConnection = Raw;
    pub fn connect(self: Connector, _: http.Origin, timeout: zio.Timeout) !Raw {
        const inner = try core.MemoryDialer.dial(.{ .listener = self.listener }, {}, timeout);
        return .{ .inner = inner, .probe = self.probe, .id = self.probe.next_id.fetchAdd(1, .monotonic) };
    }
};
const Client = http.Client(Connector);
const TestConnection = @FieldType(Client, "pool").Connection;

test "idle reclaim: suspending close preserves active/waiting owners and newly checked-in caches" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    var budget = core.AllocationBudget.init(std.testing.allocator, 256 * 1024);
    defer budget.deinit();
    var listener = try core.MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    var probe: Probe = .{};
    var cl = Client.init(budget.allocator(), .{ .listener = &listener, .probe = &probe }, .{ .pool = .{ .max_active = 2, .max_per_origin = 1 } });
    defer cl.deinit();
    defer probe.release_close.set();
    const idle = try cl.pool.checkout(a, .none);
    cl.pool.checkin(idle.conn, true);
    const active = try cl.pool.checkout(b, .none);
    var active_owned = true;
    defer if (active_owned) cl.pool.checkin(active.conn, false);
    var held: zio.ResetEvent = .init;
    var release_held: zio.ResetEvent = .init;
    const F = struct {
        fn wait(cl_: *Client, expected: *TestConnection, ready: *zio.ResetEvent, release: *zio.ResetEvent) !void {
            const next = try cl_.pool.checkout(b, .none);
            defer cl_.pool.checkin(next.conn, true);
            try std.testing.expect(next.reused);
            try std.testing.expectEqual(expected, next.conn);
            ready.set();
            try release.wait();
        }
        fn trim(cl_: *Client) !void {
            try std.testing.expectEqual(@as(usize, 1), cl_.trimIdleConnections());
        }
    };
    var waiter = try zio.spawn(F.wait, .{ &cl, active.conn, &held, &release_held });
    var waiter_joined = false;
    defer if (!waiter_joined) {
        release_held.set();
        waiter.cancel();
        waiter.join() catch {};
    };
    const deadline = zio.Timeout.fromSeconds(2).toDeadline();
    while (cl.poolStats().waiting != 1) {
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline.deadline.toNanoseconds()) return error.TestUnexpectedResult;
        try zio.yield();
    }
    var trimming = try zio.spawn(F.trim, .{&cl});
    var trim_joined = false;
    defer if (!trim_joined) {
        probe.release_close.set();
        trimming.cancel();
        trimming.join() catch {};
    };
    try probe.closing.timedWait(.fromSeconds(2));
    try std.testing.expectEqual(@as(usize, 1), cl.poolStats().active);
    try std.testing.expectEqual(@as(usize, 1), cl.poolStats().waiting);
    cl.pool.checkin(active.conn, true);
    active_owned = false;
    try held.timedWait(.fromSeconds(2));
    const newly_idle = try cl.pool.checkout(c, .none);
    cl.pool.checkin(newly_idle.conn, true);
    probe.release_close.set();
    try trimming.join();
    trim_joined = true;
    try std.testing.expectEqual(@as(usize, 1), probe.closed.load(.acquire));
    const reused = try cl.pool.checkout(c, .none);
    try std.testing.expect(reused.reused);
    cl.pool.checkin(reused.conn, true);
    release_held.set();
    try waiter.join();
    waiter_joined = true;
    try std.testing.expectEqual(@as(usize, 2), cl.trimIdleConnections());
    try std.testing.expectEqual(@as(usize, 3), probe.closed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), cl.poolStats().active);
    try std.testing.expectEqual(@as(usize, 0), cl.poolStats().waiting);
    try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
}

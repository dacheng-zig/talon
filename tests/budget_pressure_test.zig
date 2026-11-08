//! Repeated real TCP pressure: isolate routed bytes from runtime/client memory.
const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");
const core = talon.core;
const cap = 64 * 1024;
const buffer_size = 1024;
const cycles = 3;
const Mode = enum { no_headroom, read_headroom, payload };

const App = struct {
    replies: std.atomic.Value(u64) = .init(0),
};
const Protocol = struct {
    pub fn serve(conn: anytype, app: *App) !void {
        while (true) {
            const token = conn.reader().takeByte() catch |err| {
                if (err == error.EndOfStream) return;
                return err;
            };
            if (token == 'a') {
                const payload = try conn.gpa.alloc(u8, 16 * 1024);
                defer conn.gpa.free(payload);
                return error.ExpectedAllocationDenial;
            }
            if (token != 'p') return error.UnexpectedRequest;
            try conn.writer().writeAll("ok");
            try conn.writer().flush();
            _ = app.replies.fetchAdd(1, .release);
        }
    }
};
const Server = core.StreamServer(Protocol, App);

const Peer = struct {
    raw: core.TcpConnection,
    buffer: [32]u8 = undefined,
    reader: core.TcpConnection.Reader,
    writer: core.TcpConnection.Writer,

    fn init(self: *Peer, port: u16) !void {
        self.raw = try (core.TcpDialer{}).dial(.{ .host = "127.0.0.1", .port = port }, .fromSeconds(2));
        self.reader = self.raw.reader(&self.buffer);
        self.writer = self.raw.writer(&.{});
        self.reader.setTimeout(.fromSeconds(2));
        self.writer.setTimeout(.fromSeconds(2));
    }
    fn send(self: *Peer, token: u8) !void {
        try self.writer.interface.writeByte(token);
        try self.writer.interface.flush();
    }
    fn ping(self: *Peer) !void {
        try self.send('p');
        try std.testing.expectEqualStrings("ok", try self.reader.interface.take(2));
    }
    fn close(self: *Peer) void {
        self.raw.close();
    }
};

fn waitFinished(server: *Server, count: u64) !void {
    const deadline = zio.Timeout.fromSeconds(2).toDeadline();
    while (server.stats().finished_tasks != count) {
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline.deadline.toNanoseconds())
            return error.PressureTaskDidNotFinish;
        try zio.yield();
    }
}

fn runClient(server: *Server, budget: *core.AllocationBudget, port: u16, mode: Mode) !void {
    defer server.shutdown();
    var hot: [2]Peer = undefined;
    var opened: usize = 0;
    defer for (hot[0..opened]) |*peer| peer.close();
    for (&hot) |*peer| {
        try peer.init(port);
        opened += 1;
        try peer.ping();
    }
    for (0..cycles) |epoch| {
        // A previously recovered cold connection may leave cached buffers.
        // Allocation headroom only forces rent denial when that cache is empty.
        if (mode != .payload) _ = server.trimIdleBuffers();
        var cold: Peer = undefined;
        var cold_open = false;
        defer if (cold_open) cold.close();
        if (mode == .payload) {
            try cold.init(port);
            cold_open = true;
            try cold.ping();
        }
        const allowance: usize = if (mode == .read_headroom) buffer_size else 0;
        const current = budget.stats().reserved_bytes;
        try std.testing.expect(current + allowance < cap);
        const held = try budget.allocator().alloc(u8, cap - current - allowance);
        var held_owned = true;
        defer if (held_owned) budget.allocator().free(held);
        try std.testing.expectEqual(cap - allowance, budget.stats().reserved_bytes);
        if (!cold_open) {
            try cold.init(port);
            cold_open = true;
        }
        if (mode == .payload) try cold.send('a');
        try std.testing.expectError(error.EndOfStream, cold.reader.interface.takeByte());
        cold.close();
        cold_open = false;
        try waitFinished(server, epoch * 2 + 1);
        for (&hot) |*peer| try peer.ping();
        const s = server.stats();
        try std.testing.expectEqual(@as(usize, 2), s.active_tasks);
        try std.testing.expectEqual(@as(u64, epoch + 1), if (mode == .payload) s.connection_errors else s.buffer_failures);
        try std.testing.expect(budget.stats().denied_requests >= epoch + 1);
        const before_trim = budget.stats().reserved_bytes;
        const trimmed = server.trimIdleBuffers();
        try std.testing.expect(before_trim >= budget.stats().reserved_bytes + trimmed);
        for (&hot) |*peer| try peer.ping(); // Same sockets still own borrowed buffers.
        budget.allocator().free(held);
        held_owned = false;
        try cold.init(port);
        cold_open = true;
        try cold.ping();
        cold.close();
        cold_open = false;
        try waitFinished(server, epoch * 2 + 2);
    }
}

test "budget pressure: real TCP preserves active buffers through repeated denial trim and recovery" {
    for ([_]u8{ 1, 2 }) |executors| {
        for ([_]Mode{ .no_headroom, .read_headroom, .payload }) |mode| {
            const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
            defer rt.deinit();
            var budget = core.AllocationBudget.init(std.testing.allocator, cap);
            defer budget.deinit();
            {
                var app: App = .{};
                var server = try Server.init(budget.allocator(), &app, .{ .limits = .{
                    .max_connections = 3,
                    .read_buffer_size = buffer_size,
                    .write_buffer_size = buffer_size,
                    .drain_timeout = .fromSeconds(2),
                } });
                defer server.deinit();
                var listener = try core.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
                const F = struct {
                    fn serve(s: *Server, l: *core.TcpListener) !void {
                        try s.serve(l);
                    }
                };
                var group: zio.Group = .init;
                defer group.cancel();
                try group.spawn(F.serve, .{ &server, &listener });
                try group.spawn(runClient, .{ &server, &budget, listener.server.socket.address.ip.getPort(), mode });
                try group.wait();
                try std.testing.expect(!group.hasFailed());
                const s = server.stats();
                try std.testing.expectEqual(@as(u64, 8), s.accepted);
                try std.testing.expectEqual(s.accepted, s.finished_tasks);
                try std.testing.expectEqual(@as(usize, 0), s.active_tasks);
                try std.testing.expectEqual(@as(u64, if (mode == .payload) 3 else 0), s.connection_errors);
                try std.testing.expectEqual(@as(u64, if (mode == .payload) 0 else 3), s.buffer_failures);
                try std.testing.expectEqual(@as(u64, 0), s.forced_drains + s.canceled_returns + s.transport_failures + s.task_spawn_failures);
                try std.testing.expect(budget.stats().peak_reserved_bytes <= cap);
            }
            try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
        }
    }
}

const storm_connections = 512;
const StormEnd = enum { recover, shutdown };
const Storm = struct {
    server: *Server,
    port: u16,
    ending: StormEnd,
    stop: std.atomic.Value(bool) = .init(false),
    completed: [2]std.atomic.Value(u64) = .{ .init(0), .init(0) },
    before: [2]u64 = .{ 0, 0 },
    after: [2]u64 = .{ 0, 0 },
    last_accepted: zio.ResetEvent = .init,
    shutdown_requested: zio.ResetEvent = .init,

    fn hot(self: *Storm, peer: *Peer, index: usize) !void {
        while (!self.stop.load(.acquire)) {
            try peer.ping();
            _ = self.completed[index].fetchAdd(1, .release);
        }
    }

    fn reject(self: *Storm) !void {
        defer self.stop.store(true, .release);
        for (0..2) |i| self.before[i] = self.completed[i].load(.acquire);
        for (0..storm_connections) |i| {
            var cold: Peer = undefined;
            try cold.init(self.port);
            defer cold.close();
            if (self.ending == .shutdown and i + 1 == storm_connections) {
                const deadline = zio.Timeout.fromSeconds(2).toDeadline();
                while (self.server.stats().accepted != storm_connections + 2 or
                    self.server.stats().buffer_failures != storm_connections or
                    self.server.stats().finished_tasks != storm_connections)
                {
                    if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline.deadline.toNanoseconds())
                        return error.LastPressureConnectionNotAccepted;
                    try zio.yield();
                }
                self.stop.store(true, .release);
                self.last_accepted.set();
                try self.shutdown_requested.timedWait(.fromSeconds(2));
            }
            try std.testing.expectError(error.EndOfStream, cold.reader.interface.takeByte());
            if (self.ending == .shutdown and i + 1 == storm_connections) {
                // Healthy peers can finish after shutdown, so the total now
                // includes their tasks. Keep exact rejection accounting below.
                const deadline = zio.Timeout.fromSeconds(2).toDeadline();
                while (self.server.stats().finished_tasks < i + 1) {
                    if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline.deadline.toNanoseconds())
                        return error.PressureTaskDidNotFinish;
                    try zio.yield();
                }
            } else try waitFinished(self.server, i + 1);
        }
        for (0..2) |i| self.after[i] = self.completed[i].load(.acquire);
    }
};

fn stormClient(server: *Server, budget: *core.AllocationBudget, port: u16, mode: Mode, ending: StormEnd, served: *zio.ResetEvent) !void {
    defer server.shutdown();
    var peers: [2]Peer = undefined;
    var opened: usize = 0;
    defer for (peers[0..opened]) |*peer| peer.close();
    for (&peers) |*peer| {
        try peer.init(port);
        opened += 1;
        try peer.ping();
    }
    _ = server.trimIdleBuffers();
    const allowance: usize = if (mode == .read_headroom) buffer_size else 0;
    const held = try budget.allocator().alloc(u8, cap - budget.stats().reserved_bytes - allowance);
    var held_owned = true;
    defer if (held_owned) budget.allocator().free(held);
    try std.testing.expectEqual(cap - allowance, budget.stats().reserved_bytes);
    var storm: Storm = .{ .server = server, .port = port, .ending = ending };
    var hot_group: zio.Group = .init;
    defer hot_group.cancel();
    for (&peers, 0..) |*peer, i| try hot_group.spawn(Storm.hot, .{ &storm, peer, i });
    var reject_group: zio.Group = .init;
    defer reject_group.cancel();
    try reject_group.spawn(Storm.reject, .{&storm});
    var shutdown_bytes: usize = 0;
    if (ending == .shutdown) {
        try storm.last_accepted.timedWait(.fromSeconds(15));
        try hot_group.wait();
        try std.testing.expect(!hot_group.hasFailed());
        // Budget remains filled and the last rejected peer still owns its FD.
        shutdown_bytes = budget.stats().reserved_bytes;
        try std.testing.expectEqual(cap - allowance, shutdown_bytes);
        server.shutdown();
        for (&peers) |*peer| peer.close();
        opened = 0;
        storm.shutdown_requested.set();
    }
    try reject_group.wait();
    try std.testing.expect(!reject_group.hasFailed());
    try hot_group.wait();
    try std.testing.expect(!hot_group.hasFailed());
    for (0..2) |i| try std.testing.expect(storm.after[i] > storm.before[i]);
    try std.testing.expectEqual(@as(u64, storm_connections), server.stats().buffer_failures);
    try std.testing.expect(budget.stats().denied_requests >= storm_connections);
    if (ending == .recover) {
        budget.allocator().free(held);
        held_owned = false;
        var recovered: Peer = undefined;
        try recovered.init(port);
        defer recovered.close();
        try recovered.ping();
        for (&peers) |*peer| try peer.ping();
    }
    // In shutdown mode hold routed pressure until serve has finished draining.
    if (ending == .shutdown) {
        try served.timedWait(.fromSeconds(2));
    }
}

test "budget storm: existing TCP peers progress through dense rejection recovery and shutdown" {
    for ([_]u8{ 1, 2 }) |executors| {
        for ([_]Mode{ .no_headroom, .read_headroom }) |mode| {
            for ([_]StormEnd{ .recover, .shutdown }) |ending| {
                const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
                defer rt.deinit();
                var budget = core.AllocationBudget.init(std.testing.allocator, cap);
                defer budget.deinit();
                {
                    var app: App = .{};
                    var server = try Server.init(budget.allocator(), &app, .{ .limits = .{
                        .max_connections = 3,
                        .read_buffer_size = buffer_size,
                        .write_buffer_size = buffer_size,
                        .drain_timeout = .fromSeconds(2),
                    } });
                    defer server.deinit();
                    var listener = try core.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
                    var served: zio.ResetEvent = .init;
                    const F = struct {
                        fn serve(s: *Server, l: *core.TcpListener, done: *zio.ResetEvent) !void {
                            defer done.set();
                            try s.serve(l);
                        }
                    };
                    var group: zio.Group = .init;
                    defer group.cancel();
                    try group.spawn(F.serve, .{ &server, &listener, &served });
                    try group.spawn(stormClient, .{ &server, &budget, listener.server.socket.address.ip.getPort(), mode, ending, &served });
                    try group.wait();
                    try std.testing.expect(!group.hasFailed());
                    const s = server.stats();
                    const accepted: u64 = storm_connections + @as(u64, if (ending == .recover) 3 else 2);
                    try std.testing.expectEqual(accepted, s.accepted);
                    try std.testing.expectEqual(accepted, s.finished_tasks);
                    try std.testing.expectEqual(@as(usize, 0), s.active_tasks);
                    try std.testing.expectEqual(@as(u64, storm_connections), s.buffer_failures);
                    try std.testing.expectEqual(@as(u64, 0), s.forced_drains + s.drain_timeouts + s.canceled_returns +
                        s.connection_errors + s.transport_failures + s.task_spawn_failures + s.accept_failures);
                }
                try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
            }
        }
    }
}

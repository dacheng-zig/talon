//! Tests for talon.MemoryListener: in-process connect/accept round-trips and
//! close semantics, used as a first-class transport (not bolted-on test
//! scaffolding). Driven through the public API.

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const MemoryListener = talon.MemoryListener;

test "MemoryListener: connect/accept round-trip in both directions" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    const Fns = struct {
        fn server(l: *MemoryListener) !void {
            const conn = try l.accept();
            defer conn.close();
            var rbuf: [64]u8 = undefined;
            var r = conn.reader(&rbuf);
            const line = try r.interface.takeDelimiterInclusive('\n');

            var wbuf: [64]u8 = undefined;
            var w = conn.writer(&wbuf);
            try w.interface.writeAll("echo: ");
            try w.interface.writeAll(line);
            try w.interface.flush();
        }
        fn client(l: *MemoryListener) !void {
            const conn = try l.connect();
            defer conn.close();
            var wbuf: [64]u8 = undefined;
            var w = conn.writer(&wbuf);
            try w.interface.writeAll("ping\n");
            try w.interface.flush();

            var rbuf: [64]u8 = undefined;
            var r = conn.reader(&rbuf);
            const reply = try r.interface.takeDelimiterInclusive('\n');
            try std.testing.expectEqualStrings("echo: ping\n", reply);
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.server, .{&listener});
    try group.spawn(Fns.client, .{&listener});
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

test "MemoryListener: close unblocks accept with Closed" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    const Fns = struct {
        fn acceptor(l: *MemoryListener) !void {
            try std.testing.expectError(error.Closed, l.accept());
        }
        fn closer(l: *MemoryListener) !void {
            try zio.sleep(.fromMilliseconds(10));
            l.close();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.acceptor, .{&listener});
    try group.spawn(Fns.closer, .{&listener});
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

test "MemoryListener: closed queue rolls back pair ownership" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    listener.close();
    try std.testing.expectError(error.Closed, listener.connect());
    try std.testing.expectEqual(@as(usize, 0), listener.pairs.items.len);
}

test "MemoryListener: cancellation while queue is full rolls back only that pair" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{ .backlog = 1 });
    defer listener.deinit();
    const first = try listener.connect();
    defer first.close();
    const F = struct {
        fn connect(l: *MemoryListener, started: *zio.ResetEvent) !void {
            started.set();
            try std.testing.expectError(error.Canceled, l.connect());
        }
    };
    var started: zio.ResetEvent = .init;
    var task = try zio.spawn(F.connect, .{ &listener, &started });
    try started.wait();
    var queued = false;
    for (0..1000) |_| {
        listener.pairs_mutex.lockUncancelable();
        queued = listener.pairs.items.len == 2;
        listener.pairs_mutex.unlock();
        if (queued) break;
        try zio.sleep(.fromMilliseconds(1));
    }
    task.cancel();
    try task.join();
    try std.testing.expect(queued);
    try std.testing.expectEqual(@as(usize, 1), listener.pairs.items.len);
    const accepted = try listener.accept();
    accepted.close();
    listener.close();
}

test "MemoryListener: allocation failure leaves no registered pair" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    const F = struct {
        fn run(gpa: std.mem.Allocator) !void {
            var listener = try MemoryListener.init(gpa, .{});
            defer listener.deinit();
            const conn = try listener.connect();
            conn.close();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.run, .{});
}

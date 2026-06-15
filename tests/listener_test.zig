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

test "MemoryListener: close unblocks accept with ChannelClosed" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    const Fns = struct {
        fn acceptor(l: *MemoryListener) !void {
            try std.testing.expectError(error.ChannelClosed, l.accept());
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

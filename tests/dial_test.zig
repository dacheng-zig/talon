const std = @import("std");
const zio = @import("zio");
const core = @import("talon").core;

const Echo = struct {
    pub fn serve(conn: anytype, _: *u8) !void {
        var frames = core.framing.OwnedLengthPrefixed(.{ .length_type = u16, .max_frame = 4096 }).init(conn.reader(), conn.gpa);
        var frame = (try frames.next()).?;
        defer frame.deinit();
        try conn.writer().writeInt(u16, @intCast(frame.payload.len), .big);
        try conn.writer().writeAll(frame.payload);
        try conn.writer().flush();
    }
};
const EchoServer = core.StreamServer(Echo, u8);

fn roundTrip(listener: anytype, dialer: anytype, target: @TypeOf(dialer).Target) !void {
    var app: u8 = 0;
    var server = try EchoServer.init(std.testing.allocator, &app, .{ .limits = .{
        .read_buffer_size = 7,
        .write_buffer_size = 11,
    } });
    defer server.deinit();
    const F = struct {
        fn serve(s: *EchoServer, l: @TypeOf(listener)) !void {
            try s.serve(l);
        }
        fn client(s: *EchoServer, d: @TypeOf(dialer), t: @TypeOf(dialer).Target) !void {
            defer s.shutdown();
            const raw = try d.dial(t, .fromSeconds(2));
            defer raw.close();
            var writer = raw.writer(&.{});
            writer.setTimeout(.fromSeconds(2));
            try writer.interface.writeInt(u16, 4096, .big);
            try writer.interface.writeAll(&@as([4096]u8, @splat('x')));
            var buffer: [17]u8 = undefined;
            var reader = raw.reader(&buffer);
            reader.setTimeout(.fromSeconds(2));
            try std.testing.expectEqual(@as(u16, 4096), try reader.interface.takeInt(u16, .big));
            for (0..4096) |_| try std.testing.expectEqual(@as(u8, 'x'), try reader.interface.takeByte());
            try std.testing.expectError(error.EndOfStream, reader.interface.takeByte());
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, listener });
    try group.spawn(F.client, .{ &server, dialer, target });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

test "dial: non-HTTP frames round-trip over TCP literals, hostname and memory" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    for ([_][]const u8{ "127.0.0.1", "localhost" }) |host| {
        var listener = try core.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
        try roundTrip(&listener, core.TcpDialer{}, .{ .host = host, .port = listener.server.socket.address.ip.getPort() });
    }
    var listener = try core.MemoryListener.init(std.testing.allocator, .{ .pipe_buffer_size = 31 });
    defer listener.deinit();
    try roundTrip(&listener, core.MemoryDialer{ .listener = &listener }, {});
}

test "dial: memory backlog timeout/expired deadline rolls back registration and allows later dial" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try core.MemoryListener.init(std.testing.allocator, .{ .backlog = 1 });
    defer listener.deinit();
    const d: core.MemoryDialer = .{ .listener = &listener };
    try std.testing.expectError(error.Timeout, d.dial({}, .fromMilliseconds(0)));
    try std.testing.expectEqual(@as(usize, 0), listener.pairs.items.len);
    const held = try d.dial({}, .none);
    defer held.close();
    try std.testing.expectError(error.Timeout, d.dial({}, .fromMilliseconds(10)));
    try std.testing.expectEqual(@as(usize, 1), listener.pairs.items.len);
    const accepted = try listener.accept();
    defer accepted.close();
    const later = try d.dial({}, .fromSeconds(1));
    defer later.close();
    const accepted_later = try listener.accept();
    defer accepted_later.close();
    try std.testing.expectEqual(@as(usize, 2), listener.pairs.items.len);
    listener.close();
    try std.testing.expectError(error.Closed, d.dial({}, .fromSeconds(1)));
    try std.testing.expectEqual(@as(usize, 2), listener.pairs.items.len);
}

test "dial: canceling a blocked memory connect frees its pair and preserves the queued connection" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try core.MemoryListener.init(std.testing.allocator, .{ .backlog = 1 });
    defer listener.deinit();
    const d: core.MemoryDialer = .{ .listener = &listener };
    const queued = try d.dial({}, .none);
    defer queued.close();
    var entered: zio.ResetEvent = .init;
    const F = struct {
        fn run(dialer: core.MemoryDialer, started: *zio.ResetEvent) !void {
            started.set();
            try std.testing.expectError(error.Canceled, dialer.dial({}, .fromSeconds(5)));
        }
    };
    var task = try zio.spawn(F.run, .{ d, &entered });
    defer task.cancel();
    try entered.wait();
    // Observe a registered pair: the task has reached the full queue. The lock
    // keeps this valid with task migration, rather than relying on scheduling.
    const deadline = zio.Timeout.fromSeconds(2).toDeadline();
    while (true) {
        try listener.pairs_mutex.lock();
        const pending = listener.pairs.items.len == 2;
        listener.pairs_mutex.unlock();
        if (pending) break;
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline.deadline.toNanoseconds())
            return error.TestUnexpectedResult;
        try zio.yield();
    }
    task.cancel();
    try task.join();
    try std.testing.expectEqual(@as(usize, 1), listener.pairs.items.len);
    const accepted = try listener.accept();
    defer accepted.close();
}

test "dial: existing HTTP MemoryConnector inherits the backlog deadline" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try core.MemoryListener.init(std.testing.allocator, .{ .backlog = 1 });
    defer listener.deinit();
    const connector: @import("talon").http.client.MemoryConnector = .{ .listener = &listener };
    const origin: @import("talon").http.client.Origin = .{ .host = "memory", .port = 80 };
    const queued = try connector.connect(origin, .none);
    defer queued.close();
    try std.testing.expectError(error.Timeout, connector.connect(origin, .fromMilliseconds(10)));
    try std.testing.expectEqual(@as(usize, 1), listener.pairs.items.len);
    const accepted = try listener.accept();
    defer accepted.close();
}

test "dial: local binding is visible to the peer and occupied ports never fall back" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var target = try core.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    defer target.close();
    var source = try core.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    var source_open = true;
    defer if (source_open) source.close();
    const local = source.server.socket.address.ip;
    const endpoint: core.TcpEndpoint = .{ .host = "127.0.0.1", .port = target.server.socket.address.ip.getPort() };
    const dialer: core.TcpDialer = .{ .local_address = local };
    try std.testing.expectError(error.Timeout, dialer.dial(endpoint, .fromMilliseconds(0)));
    try std.testing.expectError(error.AddressInUse, dialer.dial(endpoint, .fromSeconds(2)));
    source.close();
    source_open = false;
    const connector: @import("talon").http.client.TcpConnector = .{ .dialer = dialer };
    const raw = try connector.connect(.{ .host = endpoint.host, .port = endpoint.port }, .fromSeconds(2));
    defer raw.close();
    const peer = try target.accept();
    defer peer.close();
    try std.testing.expectEqual(local.getPort(), peer.stream.socket.address.ip.getPort());
}

test "dial: refused connect releases its bound port and port zero remains OS assigned" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var target = try core.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    defer target.close();
    var source = try core.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    const local = source.server.socket.address.ip;
    var refused = try core.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    const refused_port = refused.server.socket.address.ip.getPort();
    refused.close();
    source.close();
    const dialer: core.TcpDialer = .{ .local_address = local };
    try std.testing.expectError(error.ConnectionRefused, dialer.dial(.{ .host = "127.0.0.1", .port = refused_port }, .fromSeconds(2)));
    const endpoint: core.TcpEndpoint = .{ .host = "127.0.0.1", .port = target.server.socket.address.ip.getPort() };
    const raw = try dialer.dial(endpoint, .fromSeconds(2));
    defer raw.close();
    const peer = try target.accept();
    defer peer.close();
    try std.testing.expectEqual(local.getPort(), peer.stream.socket.address.ip.getPort());
    const auto: core.TcpDialer = .{ .local_address = try zio.net.IpAddress.parseIp4("127.0.0.1", 0) };
    const raw_auto = try auto.dial(endpoint, .fromSeconds(2));
    defer raw_auto.close();
    const peer_auto = try target.accept();
    defer peer_auto.close();
    try std.testing.expect(peer_auto.stream.socket.address.ip.getPort() != 0);
    const mismatch: core.TcpDialer = .{ .local_address = try zio.net.IpAddress.parseIp6("::1", 0) };
    try std.testing.expectError(error.AddressFamilyMismatch, mismatch.dial(endpoint, .fromSeconds(2)));
}

test "dial: local IPv6 binding and IPv4 hostname fallback use compatible families" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener6 = core.TcpListener.listen(try zio.net.IpAddress.parseIp6("::1", 0), .{}) catch |err| {
        if (err == error.AddressUnavailable or err == error.AddressFamilyUnsupported) return error.SkipZigTest;
        return err;
    };
    defer listener6.close();
    const dialer6: core.TcpDialer = .{ .local_address = try zio.net.IpAddress.parseIp6("::1", 0) };
    const raw = try dialer6.dial(.{ .host = "::1", .port = listener6.server.socket.address.ip.getPort() }, .fromSeconds(2));
    defer raw.close();
    const peer = try listener6.accept();
    defer peer.close();
    try std.testing.expectEqual(zio.net.IpAddress.Family.ipv6, peer.stream.socket.address.ip.getFamily());
    try std.testing.expect(peer.stream.socket.address.ip.getPort() != 0);
    var listener4 = try core.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    try roundTrip(&listener4, core.TcpDialer{ .local_address = try zio.net.IpAddress.parseIp4("127.0.0.1", 0) }, .{ .host = "localhost", .port = listener4.server.socket.address.ip.getPort() });
}

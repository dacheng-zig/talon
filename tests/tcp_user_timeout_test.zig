const std = @import("std");
const builtin = @import("builtin");
const zio = @import("zio");
const core = @import("talon").core;

fn readTimeout(socket: zio.net.Socket) !i32 {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var value: i32 = -1;
    try zio.os.net.getsockopt(socket.handle, zio.os.net.IPPROTO.TCP, std.os.linux.TCP.USER_TIMEOUT, std.mem.asBytes(&value));
    return value;
}

test "TCP user timeout: unsupported configuration fails before runtime or DNS" {
    if (builtin.os.tag == .linux) return error.SkipZigTest;
    // No runtime exists: entering socket creation, DNS or the deadline machinery
    // would fail. Explicit zero must not masquerade as supported configuration.
    for ([_]u31{ 0, 2000, std.math.maxInt(u31) }) |value| {
        const dialer: core.TcpDialer = .{ .tcp_user_timeout_ms = value };
        try std.testing.expectError(error.TcpUserTimeoutUnsupported, dialer.dial(.{ .host = "invalid.invalid", .port = 1 }, .none));
        try std.testing.expectError(error.TcpUserTimeoutUnsupported, core.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{ .tcp_user_timeout_ms = value }));
    }
}

test "TCP user timeout: Linux listener inheritance and both dial paths" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    for ([_]?u31{ null, 0, 2000, std.math.maxInt(u31) }) |value| {
        var listener = try core.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{ .tcp_user_timeout_ms = value });
        defer listener.close();
        try std.testing.expectEqual(@as(i32, value orelse 0), try readTimeout(listener.server.socket));
        for ([_]?zio.net.IpAddress{ null, try zio.net.IpAddress.parseIp4("127.0.0.1", 0) }) |local| {
            const dialer: core.TcpDialer = .{ .local_address = local, .tcp_user_timeout_ms = value };
            const raw = try dialer.dial(.{ .host = "127.0.0.1", .port = listener.server.socket.address.ip.getPort() }, .fromSeconds(2));
            defer raw.close();
            // The connection is queued before accept; listener policy is already set.
            const peer = try listener.accept();
            defer peer.close();
            try std.testing.expectEqual(@as(i32, value orelse 0), try readTimeout(raw.stream.socket));
            try std.testing.expectEqual(@as(i32, value orelse 0), try readTimeout(peer.stream.socket));
        }
    }
}

test "TCP user timeout: Linux IPv6 child inherits independent dial policy" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = core.TcpListener.listen(try zio.net.IpAddress.parseIp6("::1", 0), .{ .tcp_user_timeout_ms = 2000 }) catch |err| {
        if (err == error.AddressUnavailable or err == error.AddressFamilyUnsupported) return error.SkipZigTest;
        return err;
    };
    defer listener.close();
    const dialer: core.TcpDialer = .{ .local_address = try zio.net.IpAddress.parseIp6("::1", 0), .tcp_user_timeout_ms = 3000 };
    const raw = try dialer.dial(.{ .host = "::1", .port = listener.server.socket.address.ip.getPort() }, .fromSeconds(2));
    defer raw.close();
    const peer = try listener.accept();
    defer peer.close();
    try std.testing.expectEqual(@as(i32, 3000), try readTimeout(raw.stream.socket));
    try std.testing.expectEqual(@as(i32, 2000), try readTimeout(peer.stream.socket));
}

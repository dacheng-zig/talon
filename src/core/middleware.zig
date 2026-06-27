//! Built-in connection middleware.
//!
//! Middleware signature: `fn (conn: *Connection, next: anytype) !void` —
//! around-style; not calling next rejects the connection. TLS will be one
//! of these once implemented; PROXY protocol and logging already are.

const std = @import("std");
const zio = @import("zio");
const RemoteInfo = @import("listener.zig").RemoteInfo;

const log = std.log.scoped(.talon);

/// Logs connection open/close with the (possibly middleware-rewritten)
/// remote identity and the connection's lifetime.
pub fn conn_log(conn: anytype, next: anytype) anyerror!void {
    var stopwatch = zio.Stopwatch.start();
    log.info("connection opened (peer: {f})", .{conn.remoteInfo()});
    defer log.info("connection closed (peer: {f}, lived {d}ms)", .{
        conn.remoteInfo(), stopwatch.read().toMilliseconds(),
    });
    try next.call(conn);
}

pub const ProxyProtocolError = error{
    MalformedProxyHeader,
    UnsupportedProxyVersion,
};

const v2_signature = "\x0d\x0a\x0d\x0a\x00\x0d\x0a\x51\x55\x49\x54\x0a";

/// PROXY protocol v2 (HAProxy spec): consumes the binary preamble that a
/// fronting load balancer prepends to the stream and publishes the real
/// client address via conn.setRemoteInfo(). Malformed preambles reject the
/// connection — when this middleware is configured, direct un-proxied
/// traffic is a misconfiguration, not a fallback.
pub fn proxy_protocol(conn: anytype, next: anytype) anyerror!void {
    const r = conn.reader();

    const header = r.take(16) catch return error.MalformedProxyHeader;
    if (!std.mem.eql(u8, header[0..12], v2_signature)) {
        return error.MalformedProxyHeader;
    }
    const version_command = header[12];
    if (version_command >> 4 != 0x2) return error.UnsupportedProxyVersion;
    const command = version_command & 0x0f;
    const family_protocol = header[13];
    const addr_len = std.mem.readInt(u16, header[14..16], .big);

    const addr_block = r.take(addr_len) catch return error.MalformedProxyHeader;

    switch (command) {
        0x0 => {}, // LOCAL: health check from the proxy itself; keep transport address.
        0x1 => switch (family_protocol >> 4) {
            0x1 => { // AF_INET
                if (addr_len < 12) return error.MalformedProxyHeader;
                const src_addr: [4]u8 = addr_block[0..4].*;
                const src_port = std.mem.readInt(u16, addr_block[8..10], .big);
                conn.setRemoteInfo(.{ .net = .{ .ip = zio.net.IpAddress.initIp4(src_addr, src_port) } });
            },
            0x2 => { // AF_INET6
                if (addr_len < 36) return error.MalformedProxyHeader;
                const src_addr: [16]u8 = addr_block[0..16].*;
                const src_port = std.mem.readInt(u16, addr_block[32..34], .big);
                conn.setRemoteInfo(.{ .net = .{ .ip = zio.net.IpAddress.initIp6(src_addr, src_port, 0, 0) } });
            },
            0x0 => {}, // UNSPEC: keep transport address.
            else => return error.MalformedProxyHeader,
        },
        else => return error.MalformedProxyHeader,
    }

    try next.call(conn);
}

//! Connector: the outbound dial contract — the dual of
//! talon-core's Listener. A Connector turns an `Origin` into a `RawConnection`
//! (the same comptime-duck-typed contract the Listener produces), so the TCP
//! and in-memory transports are shared verbatim with the server side.
//!
//! Comptime contract validated by the client:
//!   RawConnection: type
//!   connect(self, origin, timeout) !RawConnection  — suspends until connected
//!                                                     or `timeout` elapses
//!
//! RawConnection must provide reader()/writer()/close()/shutdown()/remoteInfo()
//! — identical to the Listener's RawConnection, hence the reuse below.

const std = @import("std");
const zio = @import("zio");
const listener_mod = @import("../../core/listener.zig");

/// Connection target. With TLS the scheme will drive ALPN; currently http only.
pub const Origin = struct {
    scheme: Scheme = .http,
    host: []const u8,
    port: u16,

    pub const Scheme = enum { http, https };

    pub fn defaultPort(scheme: Scheme) u16 {
        return switch (scheme) {
            .http => 80,
            .https => 443,
        };
    }
};

/// TCP connector. Reuses the listener's `TcpConnection` as RawConnection —
/// a connected stream is structurally identical to an accepted one.
pub const TcpConnector = struct {
    pub const RawConnection = listener_mod.TcpConnection;

    pub fn connect(self: TcpConnector, origin: Origin, timeout: zio.Timeout) !RawConnection {
        _ = self;
        // No TLS support yet. Fail loudly instead of sending plaintext to an
        // HTTPS endpoint, which would otherwise look like a confusing parse error.
        if (origin.scheme == .https) return error.TlsNotSupported;
        // tcpConnectToHost resolves literal IPs and host names alike; `timeout`
        // bounds DNS + the TCP handshake (returns error.Timeout if it elapses).
        const stream = try zio.net.tcpConnectToHost(origin.host, origin.port, .{ .timeout = timeout });
        // Latency over throughput, matching the server's accept path.
        stream.socket.setNoDelay(true) catch {};
        return .{ .stream = stream };
    }
};

/// In-process connector: dials a `MemoryListener` over the same pipe pair the
/// server accepts (closed-loop testing). No sockets.
pub const MemoryConnector = struct {
    pub const RawConnection = listener_mod.MemoryConnection;

    listener: *listener_mod.MemoryListener,

    pub fn connect(self: MemoryConnector, origin: Origin, timeout: zio.Timeout) !RawConnection {
        _ = origin; // a memory listener has a single implicit endpoint
        _ = timeout; // in-process connect is instantaneous
        return self.listener.connect();
    }
};

test "TcpConnector: rejects https while there is no TLS support" {
    const c = TcpConnector{};
    try std.testing.expectError(error.TlsNotSupported, c.connect(.{
        .scheme = .https,
        .host = "example.com",
        .port = 443,
    }, .none));
}

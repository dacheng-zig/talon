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
const dial_mod = @import("../../core/dial.zig");

/// HTTP origin policy, distinct from a protocol-neutral TCP endpoint.
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
    dialer: dial_mod.TcpDialer = .{},

    pub fn connect(self: TcpConnector, origin: Origin, timeout: zio.Timeout) !RawConnection {
        // No TLS support yet. Fail loudly instead of sending plaintext to an
        // HTTPS endpoint, which would otherwise look like a confusing parse error.
        if (origin.scheme == .https) return error.TlsNotSupported;
        return self.dialer.dial(.{ .host = origin.host, .port = origin.port }, timeout);
    }
};

/// In-process connector: dials a `MemoryListener` over the same pipe pair the
/// server accepts (closed-loop testing). No sockets.
pub const MemoryConnector = struct {
    pub const RawConnection = listener_mod.MemoryConnection;

    listener: *listener_mod.MemoryListener,

    pub fn connect(self: MemoryConnector, origin: Origin, timeout: zio.Timeout) !RawConnection {
        _ = origin; // a memory listener has a single implicit endpoint
        return dial_mod.MemoryDialer.dial(.{ .listener = self.listener }, {}, timeout);
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

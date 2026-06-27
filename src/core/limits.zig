//! Server limits and slow-attack defense knobs.
//!
//! The struct and its counting points are reserved up front so that the
//! data-rate defense lands without rewriting the connection loop.

const zio = @import("zio");

/// Minimum acceptable data rate, used to defend against slow-body attacks
/// (Kestrel MinRequestBodyDataRate equivalent). Enforcement by the server-level
/// heartbeat coroutine is not yet implemented.
pub const DataRate = struct {
    bytes_per_sec: u64,
    /// Grace period before the rate is enforced on a fresh transfer.
    grace: zio.Duration,
};

pub const Limits = struct {
    max_connections: u32 = 65536,
    max_header_size: u32 = 16 * 1024,
    /// null = unlimited
    max_body_size: ?u64 = 16 * 1024 * 1024,
    header_read_timeout: zio.Timeout = .fromSeconds(10),
    keep_alive_timeout: zio.Timeout = .fromSeconds(75),
    drain_timeout: zio.Timeout = .fromSeconds(30),
    /// Slow-attack defense (Slowloris / slow body). Parsed but not yet enforced.
    min_body_data_rate: ?DataRate = .{ .bytes_per_sec = 240, .grace = .fromSeconds(5) },
};

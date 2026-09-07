//! Server limits and slow-attack defense knobs.
//!
//! HTTP time budgets and resource bounds; custom protocols choose the
//! relevant limits explicitly.

const zio = @import("zio");

/// Minimum acceptable data rate, used to defend against slow-body attacks
/// Rate is measured over time spent waiting for body bytes, excluding handler
/// processing time. Chunk framing consumes time but earns no payload credit.
pub const DataRate = struct {
    bytes_per_sec: u64,
    /// Grace period before the rate is enforced on a fresh transfer; must be nonzero.
    grace: zio.Duration,
};

pub const Limits = struct {
    max_connections: u32 = 65536,
    max_header_size: u32 = 16 * 1024,
    /// null = unlimited
    max_body_size: ?u64 = 16 * 1024 * 1024,
    /// Total head budget, including the initial wait on a new connection.
    header_read_timeout: zio.Timeout = .fromSeconds(10),
    keep_alive_timeout: zio.Timeout = .fromSeconds(75),
    drain_timeout: zio.Timeout = .fromSeconds(30),
    /// Maximum wait for response write progress. Also applies to SSE/WS.
    write_timeout: zio.Timeout = .fromSeconds(30),
    /// Maximum request-arena capacity retained at a request boundary.
    /// null retains the high-water capacity; 0 releases all storage.
    max_retained_arena: ?usize = 64 * 1024,
    /// Minimum body rate; null disables. bytes_per_sec and grace must be nonzero.
    min_body_data_rate: ?DataRate = .{ .bytes_per_sec = 240, .grace = .fromSeconds(5) },
};

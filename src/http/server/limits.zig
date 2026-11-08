//! HTTP policies and the convenience mapping to engine resources.
const std = @import("std");
const zio = @import("zio");
const core = @import("../../core/limits.zig");

/// Upload credit counts payload; waiting on framing spends time too.
/// Handler processing time is excluded. Both fields must be nonzero.
pub const DataRate = struct {
    bytes_per_sec: u64,
    grace: zio.Duration,
};

pub const Limits = struct {
    max_connections: u32 = 65536,
    handshake_timeout: zio.Timeout = .fromSeconds(10),
    drain_timeout: zio.Timeout = .fromSeconds(30),
    max_header_size: u32 = 16 * 1024,
    /// null means unlimited.
    max_body_size: ?u64 = 16 * 1024 * 1024,
    header_read_timeout: zio.Timeout = .fromSeconds(10),
    keep_alive_timeout: zio.Timeout = .fromSeconds(75),
    /// Maximum wait for response progress, including SSE/WS.
    write_timeout: zio.Timeout = .fromSeconds(30),
    /// null retains high-water capacity; 0 releases all request storage.
    max_retained_arena: ?usize = 64 * 1024,
    /// Live requested bytes backing the request arena, including retained
    /// capacity and nodes. null disables; 0 rejects nonzero arena allocations.
    /// Does not limit wire body size or allocations outside Request.arena.
    max_request_arena_bytes: ?usize = null,
    min_body_data_rate: ?DataRate = .{ .bytes_per_sec = 240, .grace = .fromSeconds(5) },
    /// null derives a window with head-scanner slack. Explicit capacity must
    /// exceed max_header_size; the scanner needs room to observe overflow.
    read_buffer_size: ?usize = null,
    write_buffer_size: usize = 4 * 1024,

    pub fn engineLimits(self: Limits) !core.Limits {
        if (self.min_body_data_rate) |rate| {
            if (rate.bytes_per_sec == 0 or rate.grace.toNanoseconds() == 0) return error.InvalidDataRate;
        }
        const read_size = self.read_buffer_size orelse
            try std.math.add(usize, @max(@as(usize, self.max_header_size), 1024), 1024);
        if (read_size <= self.max_header_size) return error.HeaderBufferTooSmall;
        const engine: core.Limits = .{
            .max_connections = self.max_connections,
            .handshake_timeout = self.handshake_timeout,
            .drain_timeout = self.drain_timeout,
            .read_buffer_size = read_size,
            .write_buffer_size = self.write_buffer_size,
        };
        try engine.validate();
        return engine;
    }
};

//! Protocol-independent connection and buffer limits.

const zio = @import("zio");

pub const Limits = struct {
    max_connections: u32 = 65536,
    /// Absolute budget from accept through connection middleware and optional
    /// Proto.handshake; HTTP also caps its first head to this deadline.
    /// Requires read/write timeout support. .none disables.
    handshake_timeout: zio.Timeout = .fromSeconds(10),
    drain_timeout: zio.Timeout = .fromSeconds(30),
    /// Pool capacities fixed at init. Protocols choose their own framing window.
    read_buffer_size: usize = 4 * 1024,
    write_buffer_size: usize = 4 * 1024,

    pub fn validate(self: Limits) error{InvalidBufferSize}!void {
        if (self.read_buffer_size == 0 or self.write_buffer_size == 0) return error.InvalidBufferSize;
    }
};

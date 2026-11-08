const zio = @import("zio");

pub const Error = error{ UnsupportedReadTimeout, UnsupportedWriteTimeout };

/// Whether the transport can enforce deadlines on its I/O. Protocol policy
/// decides which capabilities it needs; disabling a timeout needs neither.
pub const Capabilities = struct {
    read: bool,
    write: bool,

    pub fn require(self: Capabilities, read: bool, write: bool) Error!void {
        if (read and !self.read) return error.UnsupportedReadTimeout;
        if (write and !self.write) return error.UnsupportedWriteTimeout;
    }

    pub fn checkRead(self: Capabilities, timeout: zio.Timeout) Error!void {
        try self.require(timeout != .none, false);
    }

    pub fn checkWrite(self: Capabilities, timeout: zio.Timeout) Error!void {
        try self.require(false, timeout != .none);
    }
};

//! Linux's transport data lifetime is separate from Talon's I/O deadlines.
//! Keep platform validation, wire representation and mandatory failure here.
const std = @import("std");
const builtin = @import("builtin");
const zio = @import("zio");

pub fn validate(milliseconds: ?u31) error{TcpUserTimeoutUnsupported}!void {
    if (milliseconds != null and builtin.os.tag != .linux)
        return error.TcpUserTimeoutUnsupported;
}

pub fn apply(socket: zio.net.Socket, milliseconds: ?u31) !void {
    try validate(milliseconds);
    if (builtin.os.tag == .linux) {
        if (milliseconds) |value| {
            const encoded: i32 = value;
            zio.os.net.setsockopt(socket.handle, zio.os.net.IPPROTO.TCP, std.os.linux.TCP.USER_TIMEOUT, std.mem.asBytes(&encoded)) catch
                return error.TcpUserTimeoutFailed;
        }
    }
}

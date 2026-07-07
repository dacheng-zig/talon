//! Server-side WebSocket: validate the handshake, send 101, then read and
//! write messages in a straight-line loop over the upgraded connection.
//!
//! `upgrade` turns a request into a `WebSocket`; `read` returns whole messages
//! (fragments reassembled) and answers ping/close automatically, so a handler
//! is just `while (try ws.read()) |msg| { ... }`. Control frames never surface
//! unless the caller wants them.

const std = @import("std");
const zio = @import("zio");
const ws = @import("../codec/ws.zig");
const request_mod = @import("request.zig");
const Request = request_mod.Request;
const Header = @import("../codec/codec.zig").Header;

pub const Opcode = ws.Opcode;
pub const CloseCode = ws.CloseCode;

pub const Message = union(enum) {
    text: []const u8,
    binary: []const u8,
};

pub const Options = struct {
    /// Largest message (all fragments) to accept; a bigger one closes with 1009.
    max_message_size: usize = 64 * 1024,
    /// Read deadline while awaiting frames; none (default) blocks until data or
    /// peer close.
    read_timeout: zio.Timeout = .none,
    /// Subprotocol to confirm in `Sec-WebSocket-Protocol` (must be one the
    /// client offered; not checked here).
    subprotocol: ?[]const u8 = null,
};

pub const UpgradeError = error{
    NotWebSocket,
    MissingKey,
    UnsupportedVersion,
    /// A supplied subprotocol name was not a valid token.
    InvalidHeader,
} || std.mem.Allocator.Error || std.Io.Writer.Error;

pub const ReadError = error{
    ProtocolError,
    MessageTooLarge,
} || std.Io.Reader.Error || std.Io.Writer.Error;

pub const WebSocket = struct {
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    msg_buf: []u8,
    close_sent: bool = false,

    /// Next message, or null when the peer closed (a close is echoed) or the
    /// connection ended. Fragments are reassembled; ping is answered with pong.
    pub fn read(self: *WebSocket) ReadError!?Message {
        var assembled: usize = 0;
        var msg_opcode: ?Opcode = null;
        while (true) {
            const h = ws.readFrameHeader(self.reader) catch |err| switch (err) {
                error.EndOfStream => return null, // peer vanished without close
                error.ProtocolError => return self.fail(.protocol_error),
                error.ReadFailed => return error.ReadFailed,
            };
            // Every client→server frame must be masked (RFC 6455 §5.1).
            if (!h.masked) return self.fail(.protocol_error);
            const len: usize = @intCast(h.payload_len);

            if (h.opcode.isControl()) {
                var ctl: [ws.max_control_payload]u8 = undefined;
                try self.reader.readSliceAll(ctl[0..len]);
                ws.unmask(ctl[0..len], h.mask_key);
                switch (h.opcode) {
                    .ping => try self.writeControl(.pong, ctl[0..len]),
                    .pong => {}, // unsolicited pong: ignore
                    .close => switch (closeValidity(ctl[0..len])) {
                        .ok => {
                            try self.echoClose();
                            return null;
                        },
                        .bad_code => return self.fail(.protocol_error),
                        .bad_reason => return self.fail(.invalid_payload),
                    },
                    else => unreachable,
                }
                continue;
            }

            // Data frame: enforce fragmentation ordering.
            if (h.opcode == .continuation) {
                if (msg_opcode == null) return self.fail(.protocol_error);
            } else {
                if (msg_opcode != null) return self.fail(.protocol_error);
                msg_opcode = h.opcode;
            }

            // Subtraction keeps the bound overflow-safe: a crafted 64-bit frame
            // length would wrap `assembled + len`. assembled never exceeds the
            // buffer, so the right-hand side never underflows.
            if (len > self.msg_buf.len - assembled) return self.fail(.message_too_big);
            try self.reader.readSliceAll(self.msg_buf[assembled..][0..len]);
            ws.unmask(self.msg_buf[assembled..][0..len], h.mask_key);
            assembled += len;

            if (h.fin) {
                const msg = self.msg_buf[0..assembled];
                switch (msg_opcode.?) {
                    // A text message must be valid UTF-8; reject it with 1007
                    // rather than forward bytes the peer promised were text.
                    .text => {
                        if (!std.unicode.utf8ValidateSlice(msg)) return self.fail(.invalid_payload);
                        return .{ .text = msg };
                    },
                    .binary => return .{ .binary = msg },
                    else => unreachable,
                }
            }
        }
    }

    pub fn writeText(self: *WebSocket, data: []const u8) std.Io.Writer.Error!void {
        try self.writeMessage(.text, data);
    }

    pub fn writeBinary(self: *WebSocket, data: []const u8) std.Io.Writer.Error!void {
        try self.writeMessage(.binary, data);
    }

    fn writeMessage(self: *WebSocket, opcode: Opcode, data: []const u8) std.Io.Writer.Error!void {
        try ws.writeFrame(self.writer, opcode, true, data, null);
        try self.writer.flush();
    }

    /// Sends a ping. `data` must be at most 125 bytes (the control-frame limit).
    pub fn writePing(self: *WebSocket, data: []const u8) std.Io.Writer.Error!void {
        try self.writeControl(.ping, data);
    }

    fn writeControl(self: *WebSocket, opcode: Opcode, data: []const u8) std.Io.Writer.Error!void {
        std.debug.assert(data.len <= ws.max_control_payload);
        try ws.writeFrame(self.writer, opcode, true, data, null);
        try self.writer.flush();
    }

    /// Sends a close frame (idempotent). The caller then stops reading; the
    /// server connection loop closes the socket when the handler returns.
    pub fn close(self: *WebSocket, code: CloseCode, reason: []const u8) std.Io.Writer.Error!void {
        if (self.close_sent) return;
        var buf: [2 + 123]u8 = undefined;
        std.mem.writeInt(u16, buf[0..2], @intFromEnum(code), .big);
        const rlen = @min(reason.len, buf.len - 2);
        @memcpy(buf[2..][0..rlen], reason[0..rlen]);
        try ws.writeFrame(self.writer, .close, true, buf[0 .. 2 + rlen], null);
        try self.writer.flush();
        self.close_sent = true;
    }

    fn echoClose(self: *WebSocket) std.Io.Writer.Error!void {
        try self.close(.normal, "");
    }

    // Sends a close with `code` and reports the matching error to the caller.
    fn fail(self: *WebSocket, code: CloseCode) ReadError {
        self.close(code, "") catch {};
        return if (code == .message_too_big) error.MessageTooLarge else error.ProtocolError;
    }
};

/// Completes the WebSocket handshake and returns a live `WebSocket`. Rejects a
/// request that is not a valid `version: 13` upgrade. The message buffer is
/// allocated from the request arena (freed when the connection ends).
pub fn upgrade(req: *Request, options: Options) UpgradeError!WebSocket {
    const upg = req.header("upgrade") orelse return error.NotWebSocket;
    if (!headerHasToken(upg, "websocket")) return error.NotWebSocket;
    const version = req.header("sec-websocket-version") orelse return error.UnsupportedVersion;
    if (!std.mem.eql(u8, version, "13")) return error.UnsupportedVersion;
    const key = req.header("sec-websocket-key") orelse return error.MissingKey;

    var accept: [ws.accept_len]u8 = undefined;
    ws.computeAccept(key, &accept);

    var extra: [2]Header = undefined;
    extra[0] = .{ .name = "sec-websocket-accept", .value = &accept };
    var extra_n: usize = 1;
    if (options.subprotocol) |sp| {
        extra[1] = .{ .name = "sec-websocket-protocol", .value = sp };
        extra_n = 2;
    }

    try req.upgrade.accept(.{
        .protocol = "websocket",
        .extra_headers = extra[0..extra_n],
        .read_timeout = options.read_timeout,
    });

    const msg_buf = try req.arena.alloc(u8, options.max_message_size);
    return .{ .reader = req.upgrade.reader, .writer = req.upgrade.writer, .msg_buf = msg_buf };
}

const CloseValidity = enum { ok, bad_code, bad_reason };

// A received close frame is well-formed when it is empty, or carries a valid
// 2-byte close code followed by a UTF-8 reason. A lone byte cannot hold the
// 2-byte code, so it is malformed.
fn closeValidity(payload: []const u8) CloseValidity {
    if (payload.len == 0) return .ok;
    if (payload.len == 1) return .bad_code;
    const code: u16 = @as(u16, payload[0]) << 8 | payload[1];
    if (!validCloseCode(code)) return .bad_code;
    if (!std.unicode.utf8ValidateSlice(payload[2..])) return .bad_reason;
    return .ok;
}

// Close codes a peer may put on the wire: RFC 6455's application codes, the
// later IANA additions (1012–1014), and the registered/private range
// 3000–4999. Excludes 1004 (reserved) and 1005/1006/1015 (never transmitted).
fn validCloseCode(code: u16) bool {
    return switch (code) {
        1000...1003, 1007...1014, 3000...4999 => true,
        else => false,
    };
}

test "closeValidity: codes and reason" {
    try std.testing.expectEqual(CloseValidity.ok, closeValidity("")); // no code
    try std.testing.expectEqual(CloseValidity.ok, closeValidity(&[_]u8{ 0x03, 0xe8 })); // 1000
    try std.testing.expectEqual(CloseValidity.bad_code, closeValidity(&[_]u8{0x03})); // 1 byte
    try std.testing.expectEqual(CloseValidity.bad_code, closeValidity(&[_]u8{ 0x03, 0xec })); // 1004 reserved
    try std.testing.expectEqual(CloseValidity.bad_code, closeValidity(&[_]u8{ 0x00, 0x00 })); // 0
    try std.testing.expectEqual(CloseValidity.bad_reason, closeValidity(&[_]u8{ 0x03, 0xe8, 0xff })); // bad UTF-8
}

// Case-insensitive token membership in a comma-separated header value.
fn headerHasToken(value: []const u8, token: []const u8) bool {
    var it = std.mem.splitScalar(u8, value, ',');
    while (it.next()) |part| {
        if (std.ascii.eqlIgnoreCase(std.mem.trim(u8, part, " \t"), token)) return true;
    }
    return false;
}

test "headerHasToken: case-insensitive, comma lists" {
    try std.testing.expect(headerHasToken("websocket", "websocket"));
    try std.testing.expect(headerHasToken("Upgrade, WebSocket", "websocket"));
    try std.testing.expect(!headerHasToken("h2c", "websocket"));
}

test "read: oversized continuation length is rejected without overflow" {
    // Fragment 1: text, not fin, "ab" (masked with key 1,2,3,4).
    // Fragment 2: continuation, fin, 64-bit length = 0x7FFF... (valid MSB, still
    // enormous), no payload — the bound check must reject it as too-large
    // instead of wrapping `assembled + len`.
    const input = [_]u8{
        0x01, 0x82, 1, 2, 3, 4, 'a' ^ 1, 'b' ^ 2,
        0x80, 0xFF, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 1, 2, 3, 4,
    };
    var r: std.Io.Reader = .fixed(&input);
    var out: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&out);
    var msg_buf: [16]u8 = undefined;
    var sock: WebSocket = .{ .reader = &r, .writer = &w, .msg_buf = &msg_buf };
    try std.testing.expectError(error.MessageTooLarge, sock.read());
}

test "read: invalid UTF-8 text is rejected with close 1007" {
    // Identity mask (zero key): payload 0xFF 0xFF is never valid UTF-8.
    const input = [_]u8{ 0x81, 0x82, 0, 0, 0, 0, 0xFF, 0xFF };
    var r: std.Io.Reader = .fixed(&input);
    var out: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&out);
    var msg_buf: [16]u8 = undefined;
    var sock: WebSocket = .{ .reader = &r, .writer = &w, .msg_buf = &msg_buf };
    try std.testing.expectError(error.ProtocolError, sock.read());

    // The server answered with a close frame carrying code 1007.
    var rr: std.Io.Reader = .fixed(w.buffered());
    const h = try ws.readFrameHeader(&rr);
    try std.testing.expectEqual(ws.Opcode.close, h.opcode);
    var body: [2]u8 = undefined;
    try rr.readSliceAll(&body);
    try std.testing.expectEqual(@as(u16, @intFromEnum(ws.CloseCode.invalid_payload)), std.mem.readInt(u16, &body, .big));
}

test "read: close with invalid code answers 1002" {
    // Masked close (identity key) carrying reserved code 1004.
    const input = [_]u8{ 0x88, 0x82, 0, 0, 0, 0, 0x03, 0xEC };
    var r: std.Io.Reader = .fixed(&input);
    var out: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&out);
    var msg_buf: [16]u8 = undefined;
    var sock: WebSocket = .{ .reader = &r, .writer = &w, .msg_buf = &msg_buf };
    try std.testing.expectError(error.ProtocolError, sock.read());

    var rr: std.Io.Reader = .fixed(w.buffered());
    const h = try ws.readFrameHeader(&rr);
    try std.testing.expectEqual(ws.Opcode.close, h.opcode);
    var body: [2]u8 = undefined;
    try rr.readSliceAll(&body);
    try std.testing.expectEqual(@as(u16, @intFromEnum(ws.CloseCode.protocol_error)), std.mem.readInt(u16, &body, .big));
}

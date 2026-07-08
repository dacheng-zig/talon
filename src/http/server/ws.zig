//! Server-side WebSocket: validate the handshake, send 101, then read and
//! write messages in a straight-line loop over the upgraded connection.
//!
//! `upgrade` turns a request into a `WebSocket`; the message loop itself (frame
//! reassembly, ping/close automation) is the direction-neutral `codec.ws`
//! `Session(.server)` — the same core the client end builds on. A handler is
//! just `while (try ws.read()) |msg| { ... }`; control frames never surface.

const std = @import("std");
const zio = @import("zio");
const ws = @import("../codec/ws.zig");
const request_mod = @import("request.zig");
const Request = request_mod.Request;
const Header = @import("../codec/codec.zig").Header;

pub const Opcode = ws.Opcode;
pub const CloseCode = ws.CloseCode;
pub const Message = ws.Message;

/// Server-side message loop over an upgraded connection. Reads whole messages
/// (fragments reassembled), answers ping/close automatically, and masks
/// nothing (server→client frames are unmasked, RFC 6455 §5.1).
pub const WebSocket = ws.Session(.server);

pub const ReadError = ws.ReadError;

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

/// Completes the WebSocket handshake and returns a live `WebSocket`. Rejects a
/// request that is not a valid `version: 13` upgrade. The message buffer is
/// allocated from the request arena (freed when the connection ends).
pub fn upgrade(req: *Request, options: Options) UpgradeError!WebSocket {
    const upg = req.header("upgrade") orelse return error.NotWebSocket;
    if (!ws.headerHasToken(upg, "websocket")) return error.NotWebSocket;
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

// ── Tests ────────────────────────────────────────────────────────────────

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

test "read: an unmasked client frame is a protocol error" {
    // Every client→server frame must be masked; an unmasked one closes 1002.
    const input = [_]u8{ 0x81, 0x02, 'h', 'i' };
    var r: std.Io.Reader = .fixed(&input);
    var out: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&out);
    var msg_buf: [16]u8 = undefined;
    var sock: WebSocket = .{ .reader = &r, .writer = &w, .msg_buf = &msg_buf };
    try std.testing.expectError(error.ProtocolError, sock.read());
}

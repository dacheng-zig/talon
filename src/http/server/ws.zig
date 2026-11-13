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
const parser = @import("../codec/request_parser.zig");
const timeout_mod = @import("../../core/timeout.zig");

pub const Opcode = ws.Opcode;
pub const CloseCode = ws.CloseCode;
pub const Message = ws.Message;
pub const SendLimits = ws.SendLimits;
pub const SendStats = ws.SendStats;
pub const SendOptions = ws.SendOptions;

/// Server-side message loop over an upgraded connection. Reads whole messages
/// (fragments reassembled), answers ping/close automatically, and masks
/// nothing (server→client frames are unmasked, RFC 6455 §5.1).
/// One read may overlap senders. Use Session methods for every write, keep its
/// address stable during operations, and join tasks before the handler exits.
pub const WebSocket = ws.Session(.server);

pub const ReadError = ws.ReadError;
pub const WriteError = ws.WriteError;

pub const Options = struct {
    /// Largest message (all fragments) to accept; a bigger one closes with 1009.
    max_message_size: usize = 64 * 1024,
    /// Read deadline while awaiting frames; none (default) blocks until data or
    /// peer close.
    read_timeout: zio.Timeout = .none,
    /// Subprotocol to confirm in `Sec-WebSocket-Protocol` (must be one the
    /// client offered, compared case-sensitively).
    subprotocol: ?[]const u8 = null,
    /// Bounds unfinished business sends; controls do not consume this budget.
    send_limits: SendLimits = .{},
    /// Optional data fragmentation; default sends each message as one frame.
    send_options: SendOptions = .{},
};

pub const UpgradeError = error{
    /// Upgrade output was already committed; no second attempt is allowed.
    AlreadyUpgraded,
    NotWebSocket,
    MissingKey,
    InvalidKey,
    UnsupportedVersion,
    UnsupportedSubprotocol,
    /// A single-value field was repeated or a subprotocol was not a token.
    InvalidHeader,
} || std.mem.Allocator.Error || std.Io.Writer.Error || timeout_mod.Error || SendOptions.Error;

/// Completes the WebSocket handshake and returns a live `WebSocket`. Rejects a
/// request that is not a valid `version: 13` upgrade. The message buffer is
/// allocated from the request arena (freed when the connection ends).
/// The handler must cancel/join every task using this Session or its payloads
/// before returning, including on cancellation. Server shutdown doesn't send
/// a WebSocket close: the application owns stop notification and graceful
/// close; the engine may cancel the handler when its drain deadline expires.
pub fn upgrade(req: *Request, options: Options) UpgradeError!WebSocket {
    try options.send_options.validate();
    if (req.method() != .GET or req.head.version != .@"HTTP/1.1" or req.head.host == null)
        return error.NotWebSocket;
    if (!hasHeaderToken(req, "upgrade", "websocket") or !hasHeaderToken(req, "connection", "upgrade"))
        return error.NotWebSocket;
    const version = (try singleHeader(req, "sec-websocket-version")) orelse return error.UnsupportedVersion;
    if (!std.mem.eql(u8, version, "13")) return error.UnsupportedVersion;
    const key = (try singleHeader(req, "sec-websocket-key")) orelse return error.MissingKey;
    const decoder = std.base64.standard.Decoder;
    if ((decoder.calcSizeForSlice(key) catch return error.InvalidKey) != 16) return error.InvalidKey;
    var nonce: [16]u8 = undefined;
    decoder.decode(&nonce, key) catch return error.InvalidKey;

    // Protocol names are case-sensitive tokens, unlike Connection/Upgrade.
    // Validate the offered list even when no protocol is selected.
    var selected_offered = options.subprotocol == null;
    if (options.subprotocol) |sp| {
        if (!parser.isToken(sp)) return error.InvalidHeader;
    }
    for (req.head.headers) |header| {
        if (!std.ascii.eqlIgnoreCase(header.name, "sec-websocket-protocol")) continue;
        var tokens = std.mem.splitScalar(u8, header.value, ',');
        while (tokens.next()) |part| {
            const token = std.mem.trim(u8, part, " \t");
            if (!parser.isToken(token)) return error.InvalidHeader;
            if (options.subprotocol) |sp| {
                if (std.mem.eql(u8, token, sp)) selected_offered = true;
            }
        }
    }
    if (!selected_offered) return error.UnsupportedSubprotocol;

    var accept: [ws.accept_len]u8 = undefined;
    ws.computeAccept(key, &accept);

    var extra: [2]Header = undefined;
    extra[0] = .{ .name = "sec-websocket-accept", .value = &accept };
    var extra_n: usize = 1;
    if (options.subprotocol) |sp| {
        extra[1] = .{ .name = "sec-websocket-protocol", .value = sp };
        extra_n = 2;
    }

    // Do all fallible preparation before committing the wire to WebSocket.
    const msg_buf = try req.arena.alloc(u8, options.max_message_size);
    errdefer req.arena.free(msg_buf);
    try req.upgrade.accept(.{
        .protocol = "websocket",
        .extra_headers = extra[0..extra_n],
        .read_timeout = options.read_timeout,
    });

    return .{ .reader = req.upgrade.reader, .writer = req.upgrade.writer, .msg_buf = msg_buf, .send_limits = options.send_limits, .send_options = options.send_options };
}

fn singleHeader(req: *const Request, name: []const u8) error{InvalidHeader}!?[]const u8 {
    var value: ?[]const u8 = null;
    for (req.head.headers) |header| {
        if (!std.ascii.eqlIgnoreCase(header.name, name)) continue;
        if (value != null) return error.InvalidHeader;
        value = header.value;
    }
    return value;
}

fn hasHeaderToken(req: *const Request, name: []const u8, token: []const u8) bool {
    for (req.head.headers) |header| {
        if (std.ascii.eqlIgnoreCase(header.name, name) and ws.headerHasToken(header.value, token)) return true;
    }
    return false;
}

// ── Tests ────────────────────────────────────────────────────────────────

const valid_key = "dGhlIHNhbXBsZSBub25jZQ==";
const valid_head = "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n";
const valid_challenge = "Sec-WebSocket-Key: " ++ valid_key ++ "\r\nSec-WebSocket-Version: 13\r\n";

fn checkUpgrade(bytes: []const u8, options: Options, gpa: std.mem.Allocator, expected_error: ?anyerror) !void {
    var headers: [parser.max_headers]Header = undefined;
    const head = try parser.parse(bytes, &headers);
    // Any pre-read protocol data must remain on the original reader.
    var reader: std.Io.Reader = .fixed("pre-read frame");
    var output: [1024]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&output);
    var taken = false;
    var timeout_changed = false;
    const Setter = struct {
        fn set(ctx: *anyopaque, _: zio.Timeout) timeout_mod.Error!void {
            const changed: *bool = @ptrCast(@alignCast(ctx));
            changed.* = true;
        }
    };
    var handle: request_mod.Upgrade = .{
        .reader = &reader,
        .writer = &writer,
        .taken = &taken,
        .ctx = &timeout_changed,
        .set_read_timeout_fn = Setter.set,
    };
    var req: Request = .{ .head = head, .arena = gpa, .body = undefined, .upgrade = &handle };
    if (expected_error) |err| {
        try std.testing.expectError(err, upgrade(&req, options));
        try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
        try std.testing.expect(!taken and !timeout_changed);
    } else {
        const socket = try upgrade(&req, options);
        defer gpa.free(socket.msg_buf);
        try std.testing.expect(taken and timeout_changed);
        try std.testing.expect(socket.reader == &reader and socket.writer == &writer);
        try std.testing.expectEqualStrings("pre-read frame", socket.reader.buffered());
        try std.testing.expect(std.mem.indexOf(u8, writer.buffered(), "101 Switching Protocols") != null);
        try std.testing.expect(std.mem.indexOf(u8, writer.buffered(), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") != null);
        if (options.subprotocol) |sp| {
            try std.testing.expect(std.mem.indexOf(u8, writer.buffered(), sp) != null);
        }
        const end = writer.end;
        try std.testing.expectError(error.AlreadyUpgraded, upgrade(&req, options));
        try std.testing.expectEqual(end, writer.end);
    }
}

test "upgrade: invalid opening handshakes fail before writing or taking the connection" {
    const cases = [_]struct { bytes: []const u8, err: anyerror }{
        .{ .bytes = "POST /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" ++ valid_challenge ++ "\r\n", .err = error.NotWebSocket },
        .{ .bytes = "GET /ws HTTP/1.0\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" ++ valid_challenge ++ "\r\n", .err = error.NotWebSocket },
        .{ .bytes = "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n" ++ valid_challenge ++ "\r\n", .err = error.NotWebSocket },
        .{ .bytes = "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: keep-alive\r\n" ++ valid_challenge ++ "\r\n", .err = error.NotWebSocket },
        .{ .bytes = "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: other\r\nConnection: Upgrade\r\n" ++ valid_challenge ++ "\r\n", .err = error.NotWebSocket },
        .{ .bytes = valid_head ++ "Sec-WebSocket-Version: 13\r\n\r\n", .err = error.MissingKey },
        .{ .bytes = valid_head ++ "Sec-WebSocket-Key: " ++ valid_key ++ "\r\n\r\n", .err = error.UnsupportedVersion },
        .{ .bytes = valid_head ++ "Sec-WebSocket-Key: " ++ valid_key ++ "\r\nSec-WebSocket-Version: 12\r\n\r\n", .err = error.UnsupportedVersion },
        .{ .bytes = valid_head ++ "Sec-WebSocket-Key: \r\nSec-WebSocket-Version: 13\r\n\r\n", .err = error.InvalidKey },
        .{ .bytes = valid_head ++ "Sec-WebSocket-Key: YQ==\r\nSec-WebSocket-Version: 13\r\n\r\n", .err = error.InvalidKey },
        .{ .bytes = valid_head ++ "Sec-WebSocket-Key: !GhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n", .err = error.InvalidKey },
        .{ .bytes = valid_head ++ "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ\r\nSec-WebSocket-Version: 13\r\n\r\n", .err = error.InvalidKey },
        .{ .bytes = valid_head ++ valid_challenge ++ "Sec-WebSocket-Key: " ++ valid_key ++ "\r\n\r\n", .err = error.InvalidHeader },
        .{ .bytes = valid_head ++ valid_challenge ++ "Sec-WebSocket-Version: 13\r\n\r\n", .err = error.InvalidHeader },
        .{ .bytes = valid_head ++ valid_challenge ++ "Sec-WebSocket-Protocol: chat,,other\r\n\r\n", .err = error.InvalidHeader },
    };
    for (cases) |case| try checkUpgrade(case.bytes, .{}, std.testing.allocator, case.err);
}

test "upgrade: token headers span fields, selected subprotocol is an exact offered token" {
    const offered = "Sec-WebSocket-Protocol: other\r\nSec-WebSocket-Protocol: chat, binary\r\n\r\n";
    const split_head = "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: other\r\nUpgrade: WebSocket\r\nConnection: keep-alive\r\nConnection: UpGrAdE\r\n";
    try checkUpgrade(split_head ++ valid_challenge ++ offered, .{ .subprotocol = "chat" }, std.testing.allocator, null);
    try checkUpgrade(valid_head ++ valid_challenge ++ "\r\n", .{}, std.testing.allocator, null);
    try checkUpgrade(valid_head ++ valid_challenge ++ "\r\n", .{ .subprotocol = "chat" }, std.testing.allocator, error.UnsupportedSubprotocol);
    try checkUpgrade(valid_head ++ valid_challenge ++ offered, .{ .subprotocol = "Chat" }, std.testing.allocator, error.UnsupportedSubprotocol);
    try checkUpgrade(valid_head ++ valid_challenge ++ offered, .{ .subprotocol = "cha" }, std.testing.allocator, error.UnsupportedSubprotocol);
    try checkUpgrade(valid_head ++ valid_challenge ++ offered, .{ .subprotocol = "chat\r\nX: y" }, std.testing.allocator, error.InvalidHeader);
}

test "upgrade: allocation failure does not send 101" {
    var alloc = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try checkUpgrade(valid_head ++ valid_challenge ++ "\r\n", .{}, alloc.allocator(), error.OutOfMemory);
    try std.testing.expect(alloc.has_induced_failure);
    try std.testing.expectEqual(alloc.allocated_bytes, alloc.freed_bytes);
}

test "read: oversized continuation length is rejected without overflow" {
    // Fragment 1: text, not fin, "ab" (masked with key 1,2,3,4).
    // Fragment 2: continuation, fin, 64-bit length = 0x7FFF... (valid MSB, still
    // enormous), no payload — the bound check must reject it as too-large
    // instead of wrapping `assembled + len`.
    const input = [_]u8{
        0x01, 0x82, 1,    2,    3,    4,    'a' ^ 1, 'b' ^ 2,
        0x80, 0xFF, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF,    0xFF,
        0xFF, 0xFF, 1,    2,    3,    4,
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
    try std.testing.expectEqual(@as(u16, @backingInt(ws.CloseCode.invalid_payload)), std.mem.readInt(u16, &body, .big));
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
    try std.testing.expectEqual(@as(u16, @backingInt(ws.CloseCode.protocol_error)), std.mem.readInt(u16, &body, .big));
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

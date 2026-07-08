//! WebSocket (RFC 6455) wire codec — direction-neutral, sans-io.
//!
//! `Handshake.computeAccept` derives the `Sec-WebSocket-Accept` response value.
//! `readFrameHeader` / `writeFrame` / `unmask` are the frame primitives; the
//! server- and client-side loops (message assembly, control-frame automation)
//! build on them. Payload bytes are never copied here — the caller reads them
//! into its own buffer and `unmask`s in place.

const std = @import("std");
const zio = @import("zio");

pub const Opcode = enum(u4) {
    continuation = 0x0,
    text = 0x1,
    binary = 0x2,
    close = 0x8,
    ping = 0x9,
    pong = 0xa,
    _,

    pub fn isControl(self: Opcode) bool {
        return @intFromEnum(self) & 0x8 != 0;
    }

    /// Reserved (non-control 0x3–0x7, control 0xb–0xf) opcodes are a protocol
    /// error with no extensions negotiated.
    pub fn isReserved(self: Opcode) bool {
        return switch (self) {
            .continuation, .text, .binary, .close, .ping, .pong => false,
            else => true,
        };
    }
};

/// Control frames carry at most 125 bytes and are never fragmented.
pub const max_control_payload = 125;

/// RFC 6455 close codes (the ones a server commonly sends).
pub const CloseCode = enum(u16) {
    normal = 1000,
    going_away = 1001,
    protocol_error = 1002,
    unsupported_data = 1003,
    invalid_payload = 1007,
    policy_violation = 1008,
    message_too_big = 1009,
    internal_error = 1011,
    _,
};

pub const FrameHeader = struct {
    fin: bool,
    opcode: Opcode,
    masked: bool,
    payload_len: u64,
    mask_key: [4]u8,
};

// ── Handshake ──────────────────────────────────────────────────────────────

/// The RFC 6455 handshake GUID concatenated with the client key before hashing.
const accept_guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

pub const accept_len = 28; // base64 of a 20-byte SHA-1 digest

/// Writes the `Sec-WebSocket-Accept` value for a client `Sec-WebSocket-Key`.
pub fn computeAccept(key: []const u8, out: *[accept_len]u8) void {
    var sha = std.crypto.hash.Sha1.init(.{});
    sha.update(key);
    sha.update(accept_guid);
    var digest: [20]u8 = undefined;
    sha.final(&digest);
    _ = std.base64.standard.Encoder.encode(out, &digest);
}

// ── Frame parse / encode ────────────────────────────────────────────────────

pub const HeaderError = error{
    /// RSV bits set (no extensions), a reserved opcode, or an oversized /
    /// fragmented control frame.
    ProtocolError,
};

/// Reads one frame header (everything before the payload) from `r`.
pub fn readFrameHeader(r: *std.Io.Reader) (HeaderError || std.Io.Reader.Error)!FrameHeader {
    const b = try r.takeArray(2);
    const fin = b[0] & 0x80 != 0;
    const rsv = b[0] & 0x70;
    const opcode: Opcode = @enumFromInt(@as(u4, @truncate(b[0])));
    const masked = b[1] & 0x80 != 0;
    var len: u64 = b[1] & 0x7f;
    if (len == 126) {
        len = std.mem.readInt(u16, try r.takeArray(2), .big);
    } else if (len == 127) {
        len = std.mem.readInt(u64, try r.takeArray(8), .big);
        // RFC 6455 §5.2: the 64-bit length's most significant bit must be 0.
        if (len >> 63 != 0) return error.ProtocolError;
    }
    var key: [4]u8 = @splat(0);
    if (masked) key = (try r.takeArray(4)).*;

    if (rsv != 0 or opcode.isReserved()) return error.ProtocolError;
    if (opcode.isControl() and (!fin or len > max_control_payload)) return error.ProtocolError;
    return .{ .fin = fin, .opcode = opcode, .masked = masked, .payload_len = len, .mask_key = key };
}

/// XORs `data` in place with the repeating 4-byte `key`. Vectorized for long
/// payloads; short ones (control frames, tiny messages) take the scalar path,
/// where the SIMD setup cost would not pay off.
pub fn unmask(data: []u8, key: [4]u8) void {
    const vlen = comptime std.simd.suggestVectorLength(u8) orelse 16;
    comptime std.debug.assert(vlen % 4 == 0); // keeps the key phase aligned

    var i: usize = 0;
    if (data.len >= vlen) {
        var key_tile: [vlen]u8 = undefined;
        for (0..vlen) |j| key_tile[j] = key[j & 3];
        const kv: @Vector(vlen, u8) = key_tile;
        while (i + vlen <= data.len) : (i += vlen) {
            const chunk: @Vector(vlen, u8) = data[i..][0..vlen].*;
            data[i..][0..vlen].* = chunk ^ kv;
        }
    }
    // i is a multiple of 4 here (vlen % 4 == 0), so key[i & 3] stays in phase.
    while (i < data.len) : (i += 1) data[i] ^= key[i & 3];
}

/// Writes one frame. `mask` set means client-side framing (payload is masked
/// into `w` in place-safe chunks); null means server-side (payload written
/// verbatim). Does not flush.
pub fn writeFrame(
    w: *std.Io.Writer,
    opcode: Opcode,
    fin: bool,
    payload: []const u8,
    mask: ?[4]u8,
) std.Io.Writer.Error!void {
    var hdr: [14]u8 = undefined;
    var n: usize = 2;
    hdr[0] = (if (fin) @as(u8, 0x80) else 0) | @intFromEnum(opcode);
    const mask_bit: u8 = if (mask != null) 0x80 else 0;
    if (payload.len < 126) {
        hdr[1] = mask_bit | @as(u8, @intCast(payload.len));
    } else if (payload.len < 0x10000) {
        hdr[1] = mask_bit | 126;
        std.mem.writeInt(u16, hdr[2..4], @intCast(payload.len), .big);
        n = 4;
    } else {
        hdr[1] = mask_bit | 127;
        std.mem.writeInt(u64, hdr[2..10], payload.len, .big);
        n = 10;
    }
    if (mask) |key| {
        @memcpy(hdr[n..][0..4], &key);
        n += 4;
        try w.writeAll(hdr[0..n]);
        // Mask into a stack buffer in chunks so the source stays const. The
        // chunk size is a multiple of 4, so every chunk starts on key phase 0
        // and the vectorized `unmask` applies with the same key unchanged.
        var buf: [1024]u8 = undefined;
        comptime std.debug.assert(buf.len % 4 == 0);
        var off: usize = 0;
        while (off < payload.len) {
            const take = @min(buf.len, payload.len - off);
            @memcpy(buf[0..take], payload[off..][0..take]);
            unmask(buf[0..take], key);
            try w.writeAll(buf[0..take]);
            off += take;
        }
        return;
    }
    try w.writeAll(hdr[0..n]);
    try w.writeAll(payload);
}

// ── Message session (direction-neutral loop) ────────────────────────────────

/// Wire side of a session. Selects two things at comptime: which mask bit an
/// inbound frame must carry (RFC 6455 §5.1 — client→server frames are masked,
/// server→client frames are not), and whether outbound frames are masked.
pub const Role = enum { server, client };

pub const Message = union(enum) {
    text: []const u8,
    binary: []const u8,
};

pub const ReadError = error{
    ProtocolError,
    MessageTooLarge,
} || std.Io.Reader.Error || std.Io.Writer.Error;

/// A masking key from a strong entropy source (RFC 6455 §5.3): each client
/// frame carries a fresh unpredictable key, closing the cache-poisoning vector
/// that predictable masking would open.
fn randomMaskKey() [4]u8 {
    var key: [4]u8 = undefined;
    // The per-executor CSPRNG: unpredictable (defeats proxy cache poisoning)
    // and syscall-free on the hot send path.
    zio.random(&key);
    return key;
}

/// The message-assembly loop shared by the server and client WebSocket ends:
/// reassembles fragments, answers ping with pong, echoes close, and validates
/// UTF-8 on text — a straight-line `while (try read()) |msg|`. `role` fixes the
/// masking direction (see `Role`); everything else is identical on both ends,
/// so this is the single sans-io core both build on.
pub fn Session(comptime role: Role) type {
    return struct {
        reader: *std.Io.Reader,
        writer: *std.Io.Writer,
        /// Reassembly buffer sized to the largest accepted message; a bigger
        /// message closes with 1009.
        msg_buf: []u8,
        close_sent: bool = false,

        const Self = @This();

        /// Outbound mask: a fresh random key on the client, none on the server.
        fn outboundMask() ?[4]u8 {
            return if (role == .client) randomMaskKey() else null;
        }

        /// Next message, or null when the peer closed (a close is echoed) or the
        /// connection ended. Fragments are reassembled; ping is answered with
        /// pong; unsolicited pong is ignored. Control frames never surface.
        pub fn read(self: *Self) ReadError!?Message {
            var assembled: usize = 0;
            var msg_opcode: ?Opcode = null;
            while (true) {
                const h = readFrameHeader(self.reader) catch |err| switch (err) {
                    error.EndOfStream => return null, // peer vanished without close
                    error.ProtocolError => return self.fail(.protocol_error),
                    error.ReadFailed => return error.ReadFailed,
                };
                // Masking is directional: a client→server frame MUST be masked,
                // a server→client frame MUST NOT be (RFC 6455 §5.1).
                const inbound_ok = switch (role) {
                    .server => h.masked,
                    .client => !h.masked,
                };
                if (!inbound_ok) return self.fail(.protocol_error);
                const len: usize = @intCast(h.payload_len);

                if (h.opcode.isControl()) {
                    var ctl: [max_control_payload]u8 = undefined;
                    try self.reader.readSliceAll(ctl[0..len]);
                    if (h.masked) unmask(ctl[0..len], h.mask_key);
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

                // Subtraction keeps the bound overflow-safe: a crafted 64-bit
                // frame length would wrap `assembled + len`. assembled never
                // exceeds the buffer, so the right-hand side never underflows.
                if (len > self.msg_buf.len - assembled) return self.fail(.message_too_big);
                try self.reader.readSliceAll(self.msg_buf[assembled..][0..len]);
                if (h.masked) unmask(self.msg_buf[assembled..][0..len], h.mask_key);
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

        pub fn writeText(self: *Self, data: []const u8) std.Io.Writer.Error!void {
            try self.writeMessage(.text, data);
        }

        pub fn writeBinary(self: *Self, data: []const u8) std.Io.Writer.Error!void {
            try self.writeMessage(.binary, data);
        }

        fn writeMessage(self: *Self, opcode: Opcode, data: []const u8) std.Io.Writer.Error!void {
            try writeFrame(self.writer, opcode, true, data, outboundMask());
            try self.writer.flush();
        }

        /// Sends a ping. `data` must be at most 125 bytes (the control-frame limit).
        pub fn writePing(self: *Self, data: []const u8) std.Io.Writer.Error!void {
            try self.writeControl(.ping, data);
        }

        fn writeControl(self: *Self, opcode: Opcode, data: []const u8) std.Io.Writer.Error!void {
            std.debug.assert(data.len <= max_control_payload);
            try writeFrame(self.writer, opcode, true, data, outboundMask());
            try self.writer.flush();
        }

        /// Sends a close frame (idempotent). The caller then stops reading.
        pub fn close(self: *Self, code: CloseCode, reason: []const u8) std.Io.Writer.Error!void {
            if (self.close_sent) return;
            var buf: [2 + 123]u8 = undefined;
            std.mem.writeInt(u16, buf[0..2], @intFromEnum(code), .big);
            const rlen = @min(reason.len, buf.len - 2);
            @memcpy(buf[2..][0..rlen], reason[0..rlen]);
            try writeFrame(self.writer, .close, true, buf[0 .. 2 + rlen], outboundMask());
            try self.writer.flush();
            self.close_sent = true;
        }

        fn echoClose(self: *Self) std.Io.Writer.Error!void {
            try self.close(.normal, "");
        }

        // Sends a close with `code` and reports the matching error to the caller.
        fn fail(self: *Self, code: CloseCode) ReadError {
            self.close(code, "") catch {};
            return if (code == .message_too_big) error.MessageTooLarge else error.ProtocolError;
        }
    };
}

// ── Close-frame validation (direction-neutral) ──────────────────────────────

pub const CloseValidity = enum { ok, bad_code, bad_reason };

/// A received close frame is well-formed when it is empty, or carries a valid
/// 2-byte close code followed by a UTF-8 reason. A lone byte cannot hold the
/// 2-byte code, so it is malformed.
pub fn closeValidity(payload: []const u8) CloseValidity {
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

/// Case-insensitive token membership in a comma-separated header value — the
/// handshake check for `Upgrade`/`Connection` on both ends.
pub fn headerHasToken(value: []const u8, token: []const u8) bool {
    var it = std.mem.splitScalar(u8, value, ',');
    while (it.next()) |part| {
        if (std.ascii.eqlIgnoreCase(std.mem.trim(u8, part, " \t"), token)) return true;
    }
    return false;
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

test "computeAccept: RFC 6455 example" {
    var out: [accept_len]u8 = undefined;
    computeAccept("dGhlIHNhbXBsZSBub25jZQ==", &out);
    try testing.expectEqualStrings("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", &out);
}

test "unmask: matches scalar reference across lengths" {
    const key = [4]u8{ 0x12, 0x34, 0x56, 0x78 };
    var prng = std.Random.DefaultPrng.init(0xF00D);
    const random = prng.random();
    for ([_]usize{ 0, 1, 3, 4, 7, 15, 16, 17, 63, 64, 65, 200 }) |len| {
        var a: [256]u8 = undefined;
        random.bytes(a[0..len]);
        var b: [256]u8 = undefined;
        @memcpy(b[0..len], a[0..len]);
        unmask(a[0..len], key);
        for (0..len) |i| b[i] ^= key[i & 3];
        try testing.expectEqualSlices(u8, b[0..len], a[0..len]);
    }
}

test "writeFrame → readFrameHeader round-trip, all length classes" {
    const cases = [_]usize{ 0, 5, 125, 126, 200, 0x10000 };
    for (cases) |len| {
        var payload: [0x10000 + 1]u8 = undefined;
        @memset(payload[0..len], 'x');

        var buf: [0x10000 + 32]u8 = undefined;
        var w: std.Io.Writer = .fixed(&buf);
        try writeFrame(&w, .binary, true, payload[0..len], null);

        var r: std.Io.Reader = .fixed(w.buffered());
        const h = try readFrameHeader(&r);
        try testing.expect(h.fin and h.opcode == .binary and !h.masked);
        try testing.expectEqual(@as(u64, len), h.payload_len);
        const body = try r.take(len);
        try testing.expectEqualSlices(u8, payload[0..len], body);
    }
}

test "writeFrame masked → unmask recovers payload" {
    const key = [4]u8{ 1, 2, 3, 4 };
    var buf: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeFrame(&w, .text, true, "hello world", key);

    var r: std.Io.Reader = .fixed(w.buffered());
    const h = try readFrameHeader(&r);
    try testing.expect(h.masked and h.opcode == .text);
    var body: [11]u8 = undefined;
    try r.readSliceAll(&body);
    unmask(&body, h.mask_key);
    try testing.expectEqualStrings("hello world", &body);
}

test "writeFrame masked: multi-chunk payload keeps key phase across chunks" {
    // Larger than the internal masking chunk, so the key phase must carry over.
    var payload: [2500]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(0xABCD);
    prng.random().bytes(&payload);
    const key = [4]u8{ 9, 8, 7, 6 };

    var buf: [payload.len + 32]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeFrame(&w, .binary, true, &payload, key);

    var r: std.Io.Reader = .fixed(w.buffered());
    const h = try readFrameHeader(&r);
    try testing.expect(h.masked);
    var body: [payload.len]u8 = undefined;
    try r.readSliceAll(&body);
    unmask(&body, h.mask_key);
    try testing.expectEqualSlices(u8, &payload, &body);
}

test "readFrameHeader: rejects RSV bits and oversized control frames" {
    // RSV1 set on a text frame.
    var r1: std.Io.Reader = .fixed(&[_]u8{ 0xC1, 0x00 });
    try testing.expectError(error.ProtocolError, readFrameHeader(&r1));
    // Ping (0x89) with a 126-byte length is an illegal control frame.
    var r2: std.Io.Reader = .fixed(&[_]u8{ 0x89, 126, 0x00, 0x7e });
    try testing.expectError(error.ProtocolError, readFrameHeader(&r2));
    // Binary frame whose 64-bit length has the top bit set is illegal.
    var r3: std.Io.Reader = .fixed(&[_]u8{ 0x82, 127, 0x80, 0, 0, 0, 0, 0, 0, 0 });
    try testing.expectError(error.ProtocolError, readFrameHeader(&r3));
}

test "closeValidity: codes and reason" {
    try testing.expectEqual(CloseValidity.ok, closeValidity("")); // no code
    try testing.expectEqual(CloseValidity.ok, closeValidity(&[_]u8{ 0x03, 0xe8 })); // 1000
    try testing.expectEqual(CloseValidity.bad_code, closeValidity(&[_]u8{0x03})); // 1 byte
    try testing.expectEqual(CloseValidity.bad_code, closeValidity(&[_]u8{ 0x03, 0xec })); // 1004 reserved
    try testing.expectEqual(CloseValidity.bad_code, closeValidity(&[_]u8{ 0x00, 0x00 })); // 0
    try testing.expectEqual(CloseValidity.bad_reason, closeValidity(&[_]u8{ 0x03, 0xe8, 0xff })); // bad UTF-8
}

test "headerHasToken: case-insensitive, comma lists" {
    try testing.expect(headerHasToken("websocket", "websocket"));
    try testing.expect(headerHasToken("Upgrade, WebSocket", "websocket"));
    try testing.expect(!headerHasToken("h2c", "websocket"));
}

test "Session(.client): masks outbound, decodes unmasked inbound" {
    // Client writes a text message → framed with the mask bit set.
    var out: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&out);
    // Server sends an unmasked text frame "hi" the client must accept.
    const inbound = [_]u8{ 0x81, 0x02, 'h', 'i' };
    var r: std.Io.Reader = .fixed(&inbound);
    var msg_buf: [16]u8 = undefined;
    var sock: Session(.client) = .{ .reader = &r, .writer = &w, .msg_buf = &msg_buf };

    const msg = (try sock.read()).?;
    try testing.expectEqualStrings("hi", msg.text);

    try sock.writeText("yo");
    var rr: std.Io.Reader = .fixed(w.buffered());
    const h = try readFrameHeader(&rr);
    try testing.expect(h.masked); // client frames are always masked
    var body: [2]u8 = undefined;
    try rr.readSliceAll(&body);
    unmask(&body, h.mask_key);
    try testing.expectEqualStrings("yo", &body);
}

test "Session(.client): a masked server frame is a protocol error" {
    // Server→client frame with the mask bit set is illegal (RFC 6455 §5.1).
    const input = [_]u8{ 0x81, 0x82, 0, 0, 0, 0, 'a', 'b' };
    var r: std.Io.Reader = .fixed(&input);
    var out: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&out);
    var msg_buf: [16]u8 = undefined;
    var sock: Session(.client) = .{ .reader = &r, .writer = &w, .msg_buf = &msg_buf };
    try testing.expectError(error.ProtocolError, sock.read());
}

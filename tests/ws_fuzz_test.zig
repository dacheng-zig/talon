//! Fuzz / property tests for the WebSocket frame codec — the header parser is
//! the untrusted-input boundary (variable length encoding, RSV/opcode/control
//! validation), so it gets the same no-crash net as the other parsers, plus a
//! writeFrame→readFrameHeader round-trip and a mask/unmask involution property.
//! Driven through the public talon.http.codec.ws surface.

const std = @import("std");
const talon = @import("talon");

const ws = talon.http.codec.ws;

test "fuzz-lite: 100k mutated frame headers never crash the parser" {
    var prng = std.Random.DefaultPrng.init(0x1234_5678);
    const random = prng.random();

    const seeds = [_][]const u8{
        &[_]u8{ 0x81, 0x85, 1, 2, 3, 4, 'h', 'e', 'l', 'l', 'o' }, // masked text
        &[_]u8{ 0x82, 0x7e, 0x01, 0x00 }, // binary, 256-byte len prefix
        &[_]u8{ 0x89, 0x00 }, // ping, empty
        &[_]u8{ 0x88, 0x82, 0, 0, 0, 0, 0x03, 0xe8 }, // masked close 1000
        &[_]u8{ 0x01, 0x03, 'a', 'b', 'c' }, // continuation-first (unmasked)
        &[_]u8{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff }, // garbage
    };

    var buf: [64]u8 = undefined;
    for (0..100_000) |_| {
        const seed = seeds[random.uintLessThan(usize, seeds.len)];
        var len = seed.len;
        @memcpy(buf[0..len], seed);
        for (0..random.uintLessThan(usize, 6)) |_| {
            switch (random.uintLessThan(u8, 3)) {
                0 => buf[random.uintLessThan(usize, len)] = random.int(u8),
                1 => len = 1 + random.uintLessThan(usize, len),
                2 => {
                    const grow = random.uintLessThan(usize, buf.len - len);
                    random.bytes(buf[len..][0..grow]);
                    len += grow;
                },
                else => unreachable,
            }
        }
        var r: std.Io.Reader = .fixed(buf[0..len]);
        _ = ws.readFrameHeader(&r) catch {}; // no-crash property
    }
}

test "property: writeFrame→readFrameHeader round-trips fin/opcode/len" {
    var prng = std.Random.DefaultPrng.init(0xC0DE_F00D);
    const random = prng.random();
    const opcodes = [_]ws.Opcode{ .text, .binary, .continuation };

    var payload: [4096]u8 = undefined;
    var enc: [4096 + 32]u8 = undefined;
    for (0..3000) |_| {
        const len = random.uintLessThan(usize, payload.len);
        const op = opcodes[random.uintLessThan(usize, opcodes.len)];
        const fin = random.boolean();
        random.bytes(payload[0..len]);

        var w: std.Io.Writer = .fixed(&enc);
        try ws.writeFrame(&w, op, fin, payload[0..len], null);
        var r: std.Io.Reader = .fixed(w.buffered());
        const h = try ws.readFrameHeader(&r);
        try std.testing.expectEqual(fin, h.fin);
        try std.testing.expectEqual(op, h.opcode);
        try std.testing.expectEqual(@as(u64, len), h.payload_len);
    }
}

test "property: unmask is its own inverse" {
    var prng = std.Random.DefaultPrng.init(0xBEEF);
    const random = prng.random();
    for (0..2000) |_| {
        var key: [4]u8 = undefined;
        random.bytes(&key);
        const len = random.uintLessThan(usize, 512);
        var a: [512]u8 = undefined;
        random.bytes(a[0..len]);
        var b: [512]u8 = undefined;
        @memcpy(b[0..len], a[0..len]);
        ws.unmask(a[0..len], key);
        ws.unmask(a[0..len], key); // twice → identity
        try std.testing.expectEqualSlices(u8, b[0..len], a[0..len]);
    }
}

//! Fuzz / property tests for the SSE codec — the incremental event decoder is
//! a line state machine over three line endings, cross-read boundaries, BOM
//! stripping, and lone-meta events, so it gets the same no-crash net as the
//! other parsers plus a serialize↔decode round-trip. Driven through the public
//! talon.http.codec.sse surface.

const std = @import("std");
const talon = @import("talon");

const sse = talon.http.codec.sse;

/// Drives the decoder over `bytes`, discarding events — the no-crash property
/// only needs the state machine driven. Errors are returned, never panicked.
fn driveSse(bytes: []const u8, scratch: []u8) !void {
    var r: std.Io.Reader = .fixed(bytes);
    var dec = sse.EventDecoder.init(&r, scratch);
    while (try dec.next()) |_| {}
}

test "fuzz-lite: 100k mutated event streams never crash the decoder" {
    var prng = std.Random.DefaultPrng.init(0x55e_c0de);
    const random = prng.random();

    const seeds = [_][]const u8{
        "data: hello\n\n",
        "event: x\nid: 1\ndata: a\ndata: b\n\n",
        "data: a\r\ndata: b\r\n\r\n",
        ": comment\nretry: 3000\ndata: c\n\n",
        "\xEF\xBB\xBFdata: bom\n\n",
        "id: 1\x00bad\ndata: d\n\n",
        "data\ndata: x\n\n",
        "data: trailing-cr\rdata: y\r",
    };

    var buf: [512]u8 = undefined;
    var scratch: [1024]u8 = undefined;
    for (0..100_000) |_| {
        const seed = seeds[random.uintLessThan(usize, seeds.len)];
        var len = seed.len;
        @memcpy(buf[0..len], seed);

        for (0..random.uintLessThan(usize, 8)) |_| {
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
        driveSse(buf[0..len], &scratch) catch {};
    }
}

test "property: serialize→decode round-trips data, event, and sticky id" {
    var prng = std.Random.DefaultPrng.init(0x5eed_e_5e);
    const random = prng.random();

    var name_buf: [16]u8 = undefined;
    var data_buf: [256]u8 = undefined;
    var enc: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer enc.deinit();
    var scratch: [512]u8 = undefined;

    for (0..2000) |_| {
        enc.clearRetainingCapacity();

        // A random but framing-safe event: data bytes avoid CR (LF allowed →
        // multi-line), name is alnum, id is a small number.
        const dn = random.uintLessThan(usize, data_buf.len);
        for (data_buf[0..dn]) |*b| {
            const c = random.int(u8);
            b.* = if (c == '\r') 'x' else c;
        }
        const nn = 1 + random.uintLessThan(usize, name_buf.len - 1);
        for (name_buf[0..nn]) |*b| b.* = 'a' + random.uintLessThan(u8, 26);
        var idbuf: [8]u8 = undefined;
        const id = std.fmt.bufPrint(&idbuf, "{d}", .{random.int(u16)}) catch unreachable;

        const want: sse.Event = .{ .event = name_buf[0..nn], .data = data_buf[0..dn], .id = id };
        try sse.serialize(&enc.writer, want);

        var r: std.Io.Reader = .fixed(enc.written());
        var dec = sse.EventDecoder.init(&r, &scratch);
        const got = (try dec.next()).?;
        try std.testing.expectEqualStrings(want.data, got.data);
        try std.testing.expectEqualStrings(want.event.?, got.event.?);
        try std.testing.expectEqualStrings(want.id.?, got.id.?);
        try std.testing.expectEqual(null, try dec.next());
    }
}

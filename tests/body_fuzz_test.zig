//! Fuzz / property tests for the chunked body decoder — the most complex and
//! security-sensitive state machine in the codec (chunk-size hex parsing, the
//! overflow guard, CRLF/last-chunk/trailer rejection, the line DoS cap). The
//! response *head* is fuzzed in response_parser_fuzz_test.zig; this gives the
//! decoder the same no-crash net, plus an encode↔decode round-trip property.
//! Driven through the public talon.http.codec surface.

const std = @import("std");
const talon = @import("talon");

const BodyReader = talon.http.codec.BodyReader;
const ChunkedBodyWriter = talon.http.codec.ChunkedBodyWriter;

const chunked_framing: BodyReader.ResponseFraming = .{
    .transfer_chunked = true,
    .content_length = null,
    .has_body = true,
};

/// Drives the decoder over `bytes` into a caller sink — the no-crash property
/// only needs the state machine driven, not the bytes kept, so the fuzz loop
/// can pass a discarding sink and stay allocation-free. Errors are returned,
/// never panicked.
fn driveChunked(bytes: []const u8, sink: *std.Io.Writer) !void {
    var r: std.Io.Reader = .fixed(bytes);
    var bbuf: [256]u8 = undefined;
    var body = BodyReader.initResponse(&r, chunked_framing, 1 << 20, &bbuf);
    _ = body.interface.streamRemaining(sink) catch |err| return body.err orelse err;
}

/// Decodes `bytes` into an owned buffer (used by the round-trip property where
/// the exact bytes are asserted).
fn decodeChunked(bytes: []const u8, gpa: std.mem.Allocator) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();
    try driveChunked(bytes, &out.writer);
    return gpa.dupe(u8, out.written());
}

test "fuzz-lite: 100k mutated chunked bodies never crash the decoder" {
    // Deterministic in-process fuzzing (`zig build test --fuzz` is broken in
    // the 0.16 toolchain), the same shape as the response-head fuzz.
    var prng = std.Random.DefaultPrng.init(0xb0d1_f0e5);
    const random = prng.random();

    const seeds = [_][]const u8{
        "5\r\nhello\r\n0\r\n\r\n",
        "a\r\n0123456789\r\n6\r\n world\r\n0\r\n\r\n",
        "5;ext=1\r\nhello\r\n0\r\n\r\n", // extension (rejected)
        "3\r\nabc\r\n0\r\nX-T: v\r\n\r\n", // trailer (rejected)
        "0\r\n\r\n", // empty body
        "fffffffffffffff0\r\nx\r\n0\r\n\r\n", // near-overflow size
        "5\r\nhelloXX0\r\n\r\n", // missing CRLF after data
    };

    var buf: [512]u8 = undefined;
    // Discarding sink: decoded output (≤ input ≤ 512) is thrown away, so the
    // loop allocates nothing and runs 100k iterations fast.
    var sink_buf: [1024]u8 = undefined;
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
        // No-crash property; a framing error (or a full sink) is ignored.
        var sink: std.Io.Writer = .fixed(&sink_buf);
        driveChunked(buf[0..len], &sink) catch {};
    }
}

test "property: chunked encode→decode round-trips the exact body" {
    var prng = std.Random.DefaultPrng.init(0x5eed_c0de);
    const random = prng.random();

    var src: [2048]u8 = undefined;
    var enc: [4096]u8 = undefined;
    var chunk_scratch: [128]u8 = undefined;

    for (0..2000) |_| {
        const n = random.uintLessThan(usize, src.len);
        random.bytes(src[0..n]);

        // Encode the body as chunked.
        var w: std.Io.Writer = .fixed(&enc);
        var cw = ChunkedBodyWriter.init(&w, &chunk_scratch);
        try cw.interface.writeAll(src[0..n]);
        try cw.finish();

        // Decode it back and assert byte-for-byte equality (which implies the
        // decoder produced exactly the original length).
        const got = try decodeChunked(w.buffered(), std.testing.allocator);
        defer std.testing.allocator.free(got);
        try std.testing.expectEqualSlices(u8, src[0..n], got);
    }
}

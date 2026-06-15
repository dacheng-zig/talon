//! Fuzz / property tests for the HTTP head parser (design doc §11): the parser
//! must never crash on arbitrary or mutated input. Driven through the public
//! talon.http.parser surface.

const std = @import("std");
const talon = @import("talon");

const parser = talon.http.parser;
const parse = parser.parse;
const Header = parser.Header;
const max_headers = parser.max_headers;

test "fuzz-lite: 200k mutated inputs never crash the parser" {
    // Deterministic in-process fuzzing: `zig build test --fuzz` is currently
    // broken in the 0.16 toolchain (test_runner fails to rebuild in fuzz
    // mode), so this provides the §11 "fuzz 无 crash" evidence directly.
    var prng = std.Random.DefaultPrng.init(0xdac4e16);
    const random = prng.random();

    const seeds = [_][]const u8{
        "GET / HTTP/1.1\r\nHost: h\r\n\r\n",
        "POST /a/b?c=d HTTP/1.1\r\nHost: h\r\nContent-Length: 11\r\nX-Y: z\r\n\r\n",
        "PUT /u HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n",
        "GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n",
    };

    var buf: [512]u8 = undefined;
    var storage: [max_headers]Header = undefined;
    for (0..200_000) |_| {
        const seed = seeds[random.uintLessThan(usize, seeds.len)];
        var len = seed.len;
        @memcpy(buf[0..len], seed);

        // Mutate: byte flips, truncation, growth with random bytes.
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
        _ = parse(buf[0..len], &storage) catch continue;
    }
}

test "fuzz: parser never crashes on arbitrary input" {
    const Ctx = struct {
        fn testOne(_: @This(), smith: *std.testing.Smith) anyerror!void {
            var buf: [2048]u8 = undefined;
            const len = smith.sliceWithHash(&buf, 0x9e3779b9);
            var storage: [max_headers]Header = undefined;
            _ = parse(buf[0..len], &storage) catch return;
        }
    };
    try std.testing.fuzz(Ctx{}, Ctx.testOne, .{ .corpus = &.{
        "GET / HTTP/1.1\r\nHost: h\r\n\r\n",
        "POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n",
    } });
}

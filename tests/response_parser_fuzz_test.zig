//! Fuzz / property tests for the HTTP response head parser: the parser
//! must never crash on arbitrary or mutated input. Driven
//! through the public talon.http.codec.response_parser surface.

const std = @import("std");
const talon = @import("talon");

const response_parser = talon.http.codec.response_parser;
const parse = response_parser.parse;
const Header = response_parser.Header;
const max_headers = response_parser.max_headers;

test "fuzz-lite: 200k mutated inputs never crash the response parser" {
    // Deterministic in-process fuzzing: `zig build test --fuzz` is broken in
    // the 0.16 toolchain, so this provides the fuzz no-crash evidence.
    var prng = std.Random.DefaultPrng.init(0xc0de_5ee5);
    const random = prng.random();

    const seeds = [_][]const u8{
        "HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\n",
        "HTTP/1.1 404 Not Found\r\ncontent-type: text/plain\r\nconnection: close\r\n\r\n",
        "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\nx-y: z\r\n\r\n",
        "HTTP/1.0 301 Moved\r\nlocation: /a\r\n\r\n",
        "HTTP/1.1 204\r\n\r\n",
    };

    var buf: [512]u8 = undefined;
    var storage: [max_headers]Header = undefined;
    for (0..200_000) |_| {
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
        _ = parse(buf[0..len], &storage) catch continue;
    }
}

test "fuzz: response parser never crashes on arbitrary input" {
    const Ctx = struct {
        fn testOne(_: @This(), smith: *std.testing.Smith) anyerror!void {
            var buf: [2048]u8 = undefined;
            const len = smith.sliceWithHash(&buf, 0x9e3779b9);
            var storage: [max_headers]Header = undefined;
            _ = parse(buf[0..len], &storage) catch return;
        }
    };
    try std.testing.fuzz(Ctx{}, Ctx.testOne, .{ .corpus = &.{
        "HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\n",
        "HTTP/1.1 200 OK\r\ncontent-length: 5\r\ntransfer-encoding: chunked\r\n\r\n",
    } });
}

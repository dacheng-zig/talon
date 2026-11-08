//! Tests for talon.http.codec.BodyReader: content-length and chunked decoding,
//! strictness (smuggling defense), max-body enforcement, and discard — driven
//! over a fixed in-memory reader through the public API.

const std = @import("std");
const talon = @import("talon");

const BodyReader = talon.http.codec.BodyReader;
const Head = talon.http.codec.request_parser.Head;

fn makeHead(content_length: ?u64, chunked: bool) Head {
    return .{
        .method = .POST,
        .method_raw = "POST",
        .target = "/",
        .version = .@"HTTP/1.1",
        .headers = &.{},
        .host = "h",
        .content_length = content_length,
        .transfer_chunked = chunked,
        .keep_alive = true,
        .expect_continue = false,
    };
}

fn readAllBody(body: *BodyReader, gpa: std.mem.Allocator) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();
    _ = body.interface.streamRemaining(&out.writer) catch |err| {
        return body.err orelse err;
    };
    return gpa.dupe(u8, out.written());
}

test "BodyReader: content-length body, leftover stays for next request" {
    var r: std.Io.Reader = .fixed("hello worldNEXT");
    const head = makeHead(11, false);
    var bbuf: [64]u8 = undefined;
    var body = BodyReader.init(&r, &head, null, &bbuf);

    const got = try readAllBody(&body, std.testing.allocator);
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualStrings("hello world", got);
    try std.testing.expectEqualStrings("NEXT", r.buffered());
}

test "BodyReader: truncated content-length body errors" {
    var backing: [64]u8 = undefined;
    @memcpy(backing[0..5], "hello");
    var r: std.Io.Reader = .fixed(&backing);
    r.end = 5;
    const head = makeHead(10, false);
    var bbuf: [16]u8 = undefined;
    var body = BodyReader.init(&r, &head, null, &bbuf);

    try std.testing.expectError(error.TruncatedBody, readAllBody(&body, std.testing.allocator));
}

test "BodyReader: chunked body decodes and leaves pipeline bytes" {
    var r: std.Io.Reader = .fixed("5\r\nhello\r\n6\r\n world\r\n0\r\n\r\nNEXT");
    const head = makeHead(null, true);
    var bbuf: [64]u8 = undefined;
    var body = BodyReader.init(&r, &head, null, &bbuf);

    const got = try readAllBody(&body, std.testing.allocator);
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualStrings("hello world", got);
    try std.testing.expectEqualStrings("NEXT", r.buffered());
}

test "BodyReader: chunked strictness — extensions, trailers, bad sizes" {
    // Chunk extension rejected.
    {
        var r: std.Io.Reader = .fixed("5;ext=1\r\nhello\r\n0\r\n\r\n");
        const head = makeHead(null, true);
        var bbuf: [16]u8 = undefined;
        var body = BodyReader.init(&r, &head, null, &bbuf);
        try std.testing.expectError(error.UnsupportedChunkFeature, readAllBody(&body, std.testing.allocator));
    }
    // Trailer field rejected.
    {
        var r: std.Io.Reader = .fixed("5\r\nhello\r\n0\r\nX-T: v\r\n\r\n");
        const head = makeHead(null, true);
        var bbuf: [16]u8 = undefined;
        var body = BodyReader.init(&r, &head, null, &bbuf);
        try std.testing.expectError(error.UnsupportedChunkFeature, readAllBody(&body, std.testing.allocator));
    }
    // Non-hex size.
    {
        var r: std.Io.Reader = .fixed("5g\r\nhello\r\n0\r\n\r\n");
        const head = makeHead(null, true);
        var bbuf: [16]u8 = undefined;
        var body = BodyReader.init(&r, &head, null, &bbuf);
        try std.testing.expectError(error.MalformedChunk, readAllBody(&body, std.testing.allocator));
    }
    // Missing CRLF after chunk data.
    {
        var r: std.Io.Reader = .fixed("5\r\nhelloXX0\r\n\r\n");
        const head = makeHead(null, true);
        var bbuf: [16]u8 = undefined;
        var body = BodyReader.init(&r, &head, null, &bbuf);
        try std.testing.expectError(error.MalformedChunk, readAllBody(&body, std.testing.allocator));
    }
}

test "BodyReader: chunked body over max_body is rejected" {
    var r: std.Io.Reader = .fixed("a\r\n0123456789\r\n0\r\n\r\n");
    const head = makeHead(null, true);
    var bbuf: [16]u8 = undefined;
    var body = BodyReader.init(&r, &head, 5, &bbuf);
    try std.testing.expectError(error.BodyTooLarge, readAllBody(&body, std.testing.allocator));
}

test "BodyReader: discard consumes unread remainder" {
    var r: std.Io.Reader = .fixed("hello worldNEXT");
    const head = makeHead(11, false);
    var bbuf: [64]u8 = undefined;
    var body = BodyReader.init(&r, &head, null, &bbuf);

    try body.discard();
    try std.testing.expectEqualStrings("NEXT", r.buffered());
}

const FragmentedBodyInput = struct {
    interface: std.Io.Reader,
    source: []const u8,
    position: usize = 0,
    chunk_size: usize,
    payload_refills: usize = 0,

    fn init(source: []const u8, buffer: []u8, chunk_size: usize) FragmentedBodyInput {
        return .{
            .interface = .{ .vtable = &.{ .stream = stream }, .buffer = buffer, .seek = 0, .end = 0 },
            .source = source,
            .chunk_size = chunk_size,
        };
    }

    fn stream(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *FragmentedBodyInput = @fieldParentPtr("interface", r);
        if (self.position == self.source.len) {
            self.payload_refills += 1;
            return error.ReadFailed;
        }
        const n = limit.minInt(@min(self.chunk_size, self.source.len - self.position));
        try w.writeAll(self.source[self.position..][0..n]);
        self.position += n;
        return n;
    }
};

test "BodyReader: oversized chunk declaration rejects before payload refill in both directions" {
    const cases = [_]struct { wire: []const u8, max: u64, prefix: []const u8 }{
        .{ .wire = "9\r\n", .max = 8, .prefix = "" },
        .{ .wire = "3\r\nabc\r\n6\r\n", .max = 8, .prefix = "abc" },
        .{ .wire = "1\r\nx\r\nffffffffffffffff\r\n", .max = std.math.maxInt(u64), .prefix = "x" },
        .{ .wire = "1\r\n", .max = 0, .prefix = "" },
    };
    for (cases) |case| {
        for ([_]bool{ false, true }) |response| {
            for (1..case.wire.len + 1) |chunk_size| {
                var upstream_buffer: [64]u8 = undefined;
                var input = FragmentedBodyInput.init(case.wire, &upstream_buffer, chunk_size);
                var body_buffer: [1]u8 = undefined;
                const head = makeHead(null, true);
                var body = if (response)
                    BodyReader.initResponse(&input.interface, .{ .transfer_chunked = true, .content_length = null, .has_body = true }, case.max, &body_buffer)
                else
                    BodyReader.init(&input.interface, &head, case.max, &body_buffer);
                var output_buffer: [8]u8 = undefined;
                var output: std.Io.Writer = .fixed(&output_buffer);
                try std.testing.expectError(error.ReadFailed, body.interface.streamRemaining(&output));
                try std.testing.expectEqual(error.BodyTooLarge, body.err.?);
                try std.testing.expectEqual(@as(usize, 0), input.payload_refills);
                try std.testing.expectEqualStrings(case.prefix, output.buffered());
                try std.testing.expectEqual(@as(u64, case.prefix.len), body.produced);
            }
        }
    }
}

test "BodyReader: chunk admission preserves exact payload budgets and malformed declarations" {
    {
        var input: std.Io.Reader = .fixed("9\r\nabcdefghi\r\n0\r\n\r\nNEXT");
        const head = makeHead(null, true);
        var body_buffer: [1]u8 = undefined;
        var body = BodyReader.init(&input, &head, 8, &body_buffer);
        var byte: [1]u8 = undefined;
        try std.testing.expectError(error.ReadFailed, body.reader().readSliceAll(&byte));
        try std.testing.expectEqual(error.BodyTooLarge, body.err.?);
        try std.testing.expectEqual(@as(u64, 0), body.produced);
        try std.testing.expectEqualStrings("abcdefghi\r\n0\r\n\r\nNEXT", input.buffered());
    }
    for ([_]?u64{ std.math.maxInt(u64), null }) |max| {
        var input: std.Io.Reader = .fixed("ffffffffffffffff\r\nx");
        const head = makeHead(null, true);
        var body_buffer: [1]u8 = undefined;
        var body = BodyReader.init(&input, &head, max, &body_buffer);
        try std.testing.expectEqual(@as(u8, 'x'), try body.reader().takeByte());
        try std.testing.expectEqual(null, body.err);
    }
    for ([_]?u64{ 5, null }) |max| {
        var input: std.Io.Reader = .fixed("2\r\nab\r\n3\r\ncde\r\n0\r\n\r\nNEXT");
        const head = makeHead(null, true);
        var body_buffer: [1]u8 = undefined;
        var body = BodyReader.init(&input, &head, max, &body_buffer);
        const got = try readAllBody(&body, std.testing.allocator);
        defer std.testing.allocator.free(got);
        try std.testing.expectEqualStrings("abcde", got);
        try std.testing.expectEqualStrings("NEXT", input.buffered());
    }
    {
        var input: std.Io.Reader = .fixed("0\r\n\r\nNEXT");
        const head = makeHead(null, true);
        var body_buffer: [1]u8 = undefined;
        var body = BodyReader.init(&input, &head, 0, &body_buffer);
        try body.discard();
        try std.testing.expectEqualStrings("NEXT", input.buffered());
    }
    const invalid = [_]struct { wire: []const u8, err: talon.http.codec.BodyError }{
        .{ .wire = "9g\r\n", .err = error.MalformedChunk },
        .{ .wire = "10000000000000000\r\n", .err = error.MalformedChunk },
        .{ .wire = "9;ext=1\r\n", .err = error.UnsupportedChunkFeature },
        .{ .wire = "9\r", .err = error.TruncatedBody },
    };
    for (invalid) |case| {
        var input: std.Io.Reader = .fixed(case.wire);
        const head = makeHead(null, true);
        var body_buffer: [1]u8 = undefined;
        var body = BodyReader.init(&input, &head, 0, &body_buffer);
        try std.testing.expectError(case.err, body.discard());
    }
}

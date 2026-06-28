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

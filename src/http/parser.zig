//! HTTP/1.1 head parser (design doc §5.5).
//!
//! Pure function: bytes in, struct out. No allocation, no I/O — all returned
//! slices borrow the input buffer. Single pass (picohttpparser approach)
//! with `std.mem` SIMD-backed scanning and a comptime fast path for common
//! headers. Independently fuzzable.
//!
//! Strict RFC 9112 with request-smuggling defense:
//!   - single-SP request line, no HTAB/multi-SP separators
//!   - token-only header names, no whitespace before the colon
//!   - obs-fold (continuation lines) rejected
//!   - every line terminated by exactly CRLF; bare CR/LF rejected
//!   - Content-Length: digits only; ANY duplicate rejected
//!   - Transfer-Encoding: only exactly "chunked"; rejected on HTTP/1.0
//!   - Content-Length + Transfer-Encoding together rejected
//!   - HTTP/1.1 requires exactly one Host header

const std = @import("std");

pub const max_headers = 64;

pub const Method = enum {
    GET,
    HEAD,
    POST,
    PUT,
    DELETE,
    CONNECT,
    OPTIONS,
    TRACE,
    PATCH,
    /// Any other token method; see Head.method_raw.
    other,
};

pub const Version = enum {
    @"HTTP/1.0",
    @"HTTP/1.1",
};

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

pub const Head = struct {
    method: Method,
    method_raw: []const u8,
    target: []const u8,
    version: Version,
    /// Sub-slice of the caller-provided storage.
    headers: []const Header,
    host: ?[]const u8,
    content_length: ?u64,
    transfer_chunked: bool,
    keep_alive: bool,
    expect_continue: bool,

    /// Case-insensitive single-header lookup.
    pub fn header(self: *const Head, name: []const u8) ?[]const u8 {
        for (self.headers) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
        }
        return null;
    }
};

pub const ParseError = error{
    MalformedRequestLine,
    BadVersion,
    BadTarget,
    MalformedHeader,
    TooManyHeaders,
    BadContentLength,
    /// Duplicate Content-Length, CL+TE combination, or duplicate Host —
    /// the classic smuggling vectors.
    ConflictingFraming,
    /// Transfer-Encoding other than exactly "chunked" (or TE on HTTP/1.0).
    UnsupportedTransferEncoding,
    MissingHost,
};

// RFC 9110 token characters.
const tchar_table = blk: {
    var t = [_]bool{false} ** 256;
    for ("!#$%&'*+-.^_`|~") |c| t[c] = true;
    for ('0'..'9' + 1) |c| t[c] = true;
    for ('a'..'z' + 1) |c| t[c] = true;
    for ('A'..'Z' + 1) |c| t[c] = true;
    break :blk t;
};

// field-content: VCHAR (0x21-0x7E), obs-text (0x80-0xFF), SP, HTAB.
const fieldchar_table = blk: {
    var t = [_]bool{false} ** 256;
    t[' '] = true;
    t['\t'] = true;
    for (0x21..0x7F) |c| t[c] = true;
    for (0x80..0x100) |c| t[c] = true;
    break :blk t;
};

// request-target: VCHAR + obs-text, no SP/HTAB/controls.
const targetchar_table = blk: {
    var t = [_]bool{false} ** 256;
    for (0x21..0x7F) |c| t[c] = true;
    for (0x80..0x100) |c| t[c] = true;
    break :blk t;
};

fn isToken(bytes: []const u8) bool {
    if (bytes.len == 0) return false;
    for (bytes) |c| {
        if (!tchar_table[c]) return false;
    }
    return true;
}

fn validCharset(bytes: []const u8, comptime table: *const [256]bool) bool {
    for (bytes) |c| {
        if (!table[c]) return false;
    }
    return true;
}

/// Parses a complete request head: everything from the request line up to
/// and including the blank-line CRLF. `headers_storage` must hold at least
/// `max_headers` entries; returned slices borrow `bytes`.
pub fn parse(bytes: []const u8, headers_storage: []Header) ParseError!Head {
    var it = LineIterator{ .bytes = bytes };

    const request_line = (it.next() catch return error.MalformedRequestLine) orelse
        return error.MalformedRequestLine;

    var head = try parseRequestLine(request_line);

    var header_count: usize = 0;
    var seen_host = false;
    var seen_content_length = false;
    var seen_transfer_encoding = false;
    var connection_close = false;
    var connection_keep_alive = false;

    while (true) {
        const line = (it.next() catch return error.MalformedHeader) orelse
            return error.MalformedHeader; // missing blank-line terminator
        if (line.len == 0) break; // end of head

        // obs-fold / leading whitespace: reject (RFC 9112 §5.2).
        if (line[0] == ' ' or line[0] == '\t') return error.MalformedHeader;

        const colon = std.mem.indexOfScalar(u8, line, ':') orelse
            return error.MalformedHeader;
        const name = line[0..colon];
        // Token-only name also rejects "Name :" (space before colon).
        if (!isToken(name)) return error.MalformedHeader;
        const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
        if (!validCharset(value, &fieldchar_table)) return error.MalformedHeader;

        if (header_count >= headers_storage.len) return error.TooManyHeaders;
        headers_storage[header_count] = .{ .name = name, .value = value };
        header_count += 1;

        // Comptime fast path for semantically-load-bearing headers: dispatch
        // on length first, then case-insensitive compare.
        switch (name.len) {
            4 => if (std.ascii.eqlIgnoreCase(name, "host")) {
                if (seen_host) return error.ConflictingFraming;
                seen_host = true;
                head.host = value;
            },
            6 => if (std.ascii.eqlIgnoreCase(name, "expect")) {
                if (std.ascii.eqlIgnoreCase(value, "100-continue")) {
                    head.expect_continue = true;
                }
            },
            10 => if (std.ascii.eqlIgnoreCase(name, "connection")) {
                var tokens = std.mem.splitScalar(u8, value, ',');
                while (tokens.next()) |raw_token| {
                    const token = std.mem.trim(u8, raw_token, " \t");
                    if (std.ascii.eqlIgnoreCase(token, "close")) connection_close = true;
                    if (std.ascii.eqlIgnoreCase(token, "keep-alive")) connection_keep_alive = true;
                }
            },
            14 => if (std.ascii.eqlIgnoreCase(name, "content-length")) {
                if (seen_content_length) return error.ConflictingFraming;
                seen_content_length = true;
                head.content_length = parseContentLength(value) catch
                    return error.BadContentLength;
            },
            17 => if (std.ascii.eqlIgnoreCase(name, "transfer-encoding")) {
                if (seen_transfer_encoding) return error.ConflictingFraming;
                seen_transfer_encoding = true;
                // Only the exact final-chunked form is accepted; anything
                // else (chains, identity, unknown codings) is rejected.
                if (!std.ascii.eqlIgnoreCase(value, "chunked")) {
                    return error.UnsupportedTransferEncoding;
                }
                if (head.version == .@"HTTP/1.0") return error.UnsupportedTransferEncoding;
                head.transfer_chunked = true;
            },
            else => {},
        }
    }

    if (it.rest().len != 0) return error.MalformedHeader; // bytes after blank line

    // Smuggling keystone: never accept both framing mechanisms.
    if (head.transfer_chunked and head.content_length != null) {
        return error.ConflictingFraming;
    }

    if (head.version == .@"HTTP/1.1" and !seen_host) return error.MissingHost;

    head.keep_alive = switch (head.version) {
        .@"HTTP/1.1" => !connection_close,
        .@"HTTP/1.0" => connection_keep_alive and !connection_close,
    };
    head.headers = headers_storage[0..header_count];
    return head;
}

const LineIterator = struct {
    bytes: []const u8,
    pos: usize = 0,

    /// Next CRLF-terminated line (CRLF stripped). Errors on bare LF;
    /// null when input is exhausted.
    fn next(self: *LineIterator) error{BareLineFeed}!?[]const u8 {
        if (self.pos >= self.bytes.len) return null;
        const nl = std.mem.indexOfScalarPos(u8, self.bytes, self.pos, '\n') orelse
            return null; // unterminated tail: callers treat as malformed
        if (nl == self.pos or self.bytes[nl - 1] != '\r') return error.BareLineFeed;
        const line = self.bytes[self.pos .. nl - 1];
        self.pos = nl + 1;
        return line;
    }

    fn rest(self: *const LineIterator) []const u8 {
        return self.bytes[self.pos..];
    }
};

fn parseRequestLine(line: []const u8) ParseError!Head {
    const sp1 = std.mem.indexOfScalar(u8, line, ' ') orelse
        return error.MalformedRequestLine;
    const method_raw = line[0..sp1];
    if (!isToken(method_raw)) return error.MalformedRequestLine;

    const sp2 = std.mem.indexOfScalarPos(u8, line, sp1 + 1, ' ') orelse
        return error.MalformedRequestLine;
    const target = line[sp1 + 1 .. sp2];
    if (target.len == 0 or !validCharset(target, &targetchar_table)) {
        return error.BadTarget;
    }

    const version_raw = line[sp2 + 1 ..];
    // Exactly one SP between parts: a third SP would land in version_raw and
    // fail the exact-match below.
    const version: Version = if (std.mem.eql(u8, version_raw, "HTTP/1.1"))
        .@"HTTP/1.1"
    else if (std.mem.eql(u8, version_raw, "HTTP/1.0"))
        .@"HTTP/1.0"
    else
        return error.BadVersion;

    return .{
        .method = methodFromToken(method_raw),
        .method_raw = method_raw,
        .target = target,
        .version = version,
        .headers = &.{},
        .host = null,
        .content_length = null,
        .transfer_chunked = false,
        .keep_alive = false,
        .expect_continue = false,
    };
}

fn methodFromToken(token: []const u8) Method {
    inline for (comptime std.meta.tags(Method)) |m| {
        if (m == .other) continue;
        if (std.mem.eql(u8, token, @tagName(m))) return m;
    }
    return .other;
}

/// Strict Content-Length: ASCII digits only — no sign, no whitespace, no
/// hex. Overflow is rejected.
fn parseContentLength(value: []const u8) error{Invalid}!u64 {
    if (value.len == 0 or value.len > 19) return error.Invalid;
    var result: u64 = 0;
    for (value) |c| {
        if (c < '0' or c > '9') return error.Invalid;
        result = result * 10 + (c - '0');
    }
    return result;
}

// ── Tests ────────────────────────────────────────────────────────────────

fn expectReject(expected: ParseError, bytes: []const u8) !void {
    var storage: [max_headers]Header = undefined;
    try std.testing.expectError(expected, parse(bytes, &storage));
}

test "parse: minimal GET" {
    var storage: [max_headers]Header = undefined;
    const head = try parse("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n", &storage);
    try std.testing.expectEqual(Method.GET, head.method);
    try std.testing.expectEqualStrings("/", head.target);
    try std.testing.expectEqual(Version.@"HTTP/1.1", head.version);
    try std.testing.expectEqualStrings("example.com", head.host.?);
    try std.testing.expect(head.keep_alive);
    try std.testing.expectEqual(null, head.content_length);
    try std.testing.expect(!head.transfer_chunked);
    try std.testing.expectEqual(1, head.headers.len);
}

test "parse: headers, OWS trim, lookup, custom method" {
    var storage: [max_headers]Header = undefined;
    const head = try parse(
        "PURGE /cache/x?y=1 HTTP/1.1\r\n" ++
            "Host: h\r\n" ++
            "X-Custom:   spaced value\t \r\n" ++
            "Content-Length: 12\r\n" ++
            "\r\n",
        &storage,
    );
    try std.testing.expectEqual(Method.other, head.method);
    try std.testing.expectEqualStrings("PURGE", head.method_raw);
    try std.testing.expectEqualStrings("/cache/x?y=1", head.target);
    try std.testing.expectEqualStrings("spaced value", head.header("x-custom").?);
    try std.testing.expectEqual(12, head.content_length.?);
}

test "parse: chunked transfer-encoding" {
    var storage: [max_headers]Header = undefined;
    const head = try parse(
        "POST /up HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n",
        &storage,
    );
    try std.testing.expect(head.transfer_chunked);
    try std.testing.expectEqual(null, head.content_length);
}

test "parse: keep-alive semantics across versions" {
    var storage: [max_headers]Header = undefined;

    const h11 = try parse("GET / HTTP/1.1\r\nHost: h\r\n\r\n", &storage);
    try std.testing.expect(h11.keep_alive);

    const h11c = try parse("GET / HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n", &storage);
    try std.testing.expect(!h11c.keep_alive);

    const h10 = try parse("GET / HTTP/1.0\r\n\r\n", &storage);
    try std.testing.expect(!h10.keep_alive);

    const h10k = try parse("GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n", &storage);
    try std.testing.expect(h10k.keep_alive);
}

test "parse: expect 100-continue" {
    var storage: [max_headers]Header = undefined;
    const head = try parse(
        "PUT /f HTTP/1.1\r\nHost: h\r\nExpect: 100-continue\r\nContent-Length: 4\r\n\r\n",
        &storage,
    );
    try std.testing.expect(head.expect_continue);
}

test "smuggling: Content-Length + Transfer-Encoding rejected" {
    try expectReject(error.ConflictingFraming, "POST / HTTP/1.1\r\nHost: h\r\n" ++
        "Content-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n");
    // Order reversed.
    try expectReject(error.ConflictingFraming, "POST / HTTP/1.1\r\nHost: h\r\n" ++
        "Transfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n");
}

test "smuggling: duplicate Content-Length rejected (same and different)" {
    try expectReject(error.ConflictingFraming, "POST / HTTP/1.1\r\nHost: h\r\n" ++
        "Content-Length: 5\r\nContent-Length: 5\r\n\r\n");
    try expectReject(error.ConflictingFraming, "POST / HTTP/1.1\r\nHost: h\r\n" ++
        "Content-Length: 5\r\nContent-Length: 6\r\n\r\n");
}

test "smuggling: malformed Content-Length values rejected" {
    try expectReject(error.BadContentLength, "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: +5\r\n\r\n");
    try expectReject(error.BadContentLength, "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5x\r\n\r\n");
    try expectReject(error.BadContentLength, "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 0x5\r\n\r\n");
    try expectReject(error.BadContentLength, "POST / HTTP/1.1\r\nHost: h\r\nContent-Length:\r\n\r\n");
    try expectReject(error.BadContentLength, "POST / HTTP/1.1\r\nHost: h\r\n" ++
        "Content-Length: 99999999999999999999999\r\n\r\n");
}

test "smuggling: transfer-encoding chains and unknown codings rejected" {
    try expectReject(error.UnsupportedTransferEncoding, "POST / HTTP/1.1\r\nHost: h\r\n" ++
        "Transfer-Encoding: gzip, chunked\r\n\r\n");
    try expectReject(error.UnsupportedTransferEncoding, "POST / HTTP/1.1\r\nHost: h\r\n" ++
        "Transfer-Encoding: chunked, identity\r\n\r\n");
    try expectReject(error.UnsupportedTransferEncoding, "POST / HTTP/1.1\r\nHost: h\r\n" ++
        "Transfer-Encoding: xchunked\r\n\r\n");
    // TE on HTTP/1.0 (no Host requirement there, so TE is the tripwire).
    try expectReject(error.UnsupportedTransferEncoding, "POST / HTTP/1.0\r\n" ++
        "Transfer-Encoding: chunked\r\n\r\n");
    // Duplicate TE headers.
    try expectReject(error.ConflictingFraming, "POST / HTTP/1.1\r\nHost: h\r\n" ++
        "Transfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n");
}

test "smuggling: header field-line whitespace games rejected" {
    // Space before colon.
    try expectReject(error.MalformedHeader, "GET / HTTP/1.1\r\nHost : h\r\n\r\n");
    // obs-fold continuation.
    try expectReject(error.MalformedHeader, "GET / HTTP/1.1\r\nHost: h\r\n X-Fold: y\r\n\r\n");
    try expectReject(error.MalformedHeader, "GET / HTTP/1.1\r\nHost: h\r\n\tfolded\r\n\r\n");
    // Empty header name / missing colon.
    try expectReject(error.MalformedHeader, "GET / HTTP/1.1\r\n: v\r\nHost: h\r\n\r\n");
    try expectReject(error.MalformedHeader, "GET / HTTP/1.1\r\nNoColon\r\nHost: h\r\n\r\n");
}

test "smuggling: line termination strictness" {
    // Bare LF line endings.
    try expectReject(error.MalformedRequestLine, "GET / HTTP/1.1\nHost: h\r\n\r\n");
    try expectReject(error.MalformedHeader, "GET / HTTP/1.1\r\nHost: h\n\r\n");
    // Bare CR inside a value is a control character.
    try expectReject(error.MalformedHeader, "GET / HTTP/1.1\r\nHost: h\rX: y\r\n\r\n");
    // NUL in value.
    try expectReject(error.MalformedHeader, "GET / HTTP/1.1\r\nHost: h\x00x\r\n\r\n");
}

test "smuggling: request line strictness" {
    // Double space between method and target → empty target.
    try expectReject(error.BadTarget, "GET  / HTTP/1.1\r\nHost: h\r\n\r\n");
    // Tab as separator is part of the method token → not a token char.
    try expectReject(error.MalformedRequestLine, "GET\t/ HTTP/1.1\r\nHost: h\r\n\r\n");
    // Trailing space lands in the version literal.
    try expectReject(error.BadVersion, "GET / HTTP/1.1 \r\nHost: h\r\n\r\n");
    try expectReject(error.BadVersion, "GET / http/1.1\r\nHost: h\r\n\r\n");
    try expectReject(error.BadVersion, "GET / HTTP/2.0\r\nHost: h\r\n\r\n");
    try expectReject(error.MalformedRequestLine, "/ HTTP/1.1\r\nHost: h\r\n\r\n");
    try expectReject(error.MalformedRequestLine, "\r\nHost: h\r\n\r\n");
}

test "parse: host discipline" {
    try expectReject(error.MissingHost, "GET / HTTP/1.1\r\nX: y\r\n\r\n");
    try expectReject(error.ConflictingFraming, "GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n");
    // HTTP/1.0 does not require Host.
    var storage: [max_headers]Header = undefined;
    _ = try parse("GET / HTTP/1.0\r\n\r\n", &storage);
}

test "parse: too many headers" {
    var buf: [8192]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try w.writeAll("GET / HTTP/1.1\r\nHost: h\r\n");
    for (0..max_headers) |i| {
        try w.print("X-{d}: v\r\n", .{i});
    }
    try w.writeAll("\r\n");
    var storage: [max_headers]Header = undefined;
    try std.testing.expectError(error.TooManyHeaders, parse(w.buffered(), &storage));
}

//! HTTP/1.1 response head parser (client side).
//!
//! Sibling of `request_parser` with the same strict discipline: exact-CRLF
//! lines (bare CR/LF rejected), token-only header names, obs-fold rejected,
//! Content-Length digits-only with any-duplicate rejected, Transfer-Encoding
//! only exactly "chunked", CL+TE rejected. Pure function: bytes in, struct
//! out; returned slices borrow `bytes`.
//!
//! Reuses request_parser's lexical primitives (LineIterator, isToken,
//! validFieldValue, parseContentLength) so both directions enforce one
//! grammar. The status line and the response-specific header set are the
//! only differences from the request side.

const std = @import("std");
const rp = @import("request_parser.zig");
const codec = @import("codec.zig");

pub const max_headers = codec.max_headers;
pub const Header = codec.Header;
pub const Version = codec.Version;

pub const ResponseHead = struct {
    version: Version,
    status: u16,
    reason: []const u8,
    /// Sub-slice of the caller-provided storage.
    headers: []const Header,
    content_length: ?u64,
    transfer_chunked: bool,
    keep_alive: bool,

    /// Case-insensitive single-header lookup.
    pub fn header(self: *const ResponseHead, name: []const u8) ?[]const u8 {
        for (self.headers) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
        }
        return null;
    }
};

pub const ParseError = error{
    MalformedStatusLine,
    BadVersion,
    BadStatus,
    MalformedHeader,
    TooManyHeaders,
    BadContentLength,
    /// Duplicate Content-Length, CL+TE combination, or duplicate TE.
    ConflictingFraming,
    UnsupportedTransferEncoding,
};

/// Parses a complete response head: status line up to and including the
/// blank-line CRLF. `headers_storage` must hold at least `max_headers`.
pub fn parse(bytes: []const u8, headers_storage: []Header) ParseError!ResponseHead {
    var it = rp.LineIterator{ .bytes = bytes };

    const status_line = (it.next() catch return error.MalformedStatusLine) orelse
        return error.MalformedStatusLine;
    var head = try parseStatusLine(status_line);

    var header_count: usize = 0;
    var seen_content_length = false;
    var seen_transfer_encoding = false;
    var connection_close = false;
    var connection_keep_alive = false;

    while (true) {
        const line = (it.next() catch return error.MalformedHeader) orelse
            return error.MalformedHeader; // missing blank-line terminator
        if (line.len == 0) break;

        // obs-fold / leading whitespace: reject (RFC 9112 §5.2).
        if (line[0] == ' ' or line[0] == '\t') return error.MalformedHeader;

        const colon = std.mem.indexOfScalar(u8, line, ':') orelse
            return error.MalformedHeader;
        const name = line[0..colon];
        if (!rp.isToken(name)) return error.MalformedHeader;
        const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
        if (!rp.validFieldValue(value)) return error.MalformedHeader;

        if (header_count >= headers_storage.len) return error.TooManyHeaders;
        headers_storage[header_count] = .{ .name = name, .value = value };
        header_count += 1;

        switch (name.len) {
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
                head.content_length = rp.parseContentLength(value) catch
                    return error.BadContentLength;
            },
            17 => if (std.ascii.eqlIgnoreCase(name, "transfer-encoding")) {
                if (seen_transfer_encoding) return error.ConflictingFraming;
                seen_transfer_encoding = true;
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
    if (head.transfer_chunked and head.content_length != null) {
        return error.ConflictingFraming;
    }

    head.keep_alive = switch (head.version) {
        .@"HTTP/1.1" => !connection_close,
        .@"HTTP/1.0" => connection_keep_alive and !connection_close,
    };
    head.headers = headers_storage[0..header_count];
    return head;
}

/// "HTTP/1.1 200 OK" — version SP 3-digit-status [SP reason]. Reason may be
/// empty; exactly one SP separates each part.
fn parseStatusLine(line: []const u8) ParseError!ResponseHead {
    const sp1 = std.mem.indexOfScalar(u8, line, ' ') orelse
        return error.MalformedStatusLine;
    const version_raw = line[0..sp1];
    const version: Version = if (std.mem.eql(u8, version_raw, "HTTP/1.1"))
        .@"HTTP/1.1"
    else if (std.mem.eql(u8, version_raw, "HTTP/1.0"))
        .@"HTTP/1.0"
    else
        return error.BadVersion;

    // Exactly three status digits must follow the single SP.
    if (line.len < sp1 + 1 + 3) return error.MalformedStatusLine;
    const code_raw = line[sp1 + 1 .. sp1 + 1 + 3];
    var status: u16 = 0;
    for (code_raw) |c| {
        if (c < '0' or c > '9') return error.BadStatus;
        status = status * 10 + (c - '0');
    }
    // RFC 9110: status codes are 100–599. Reject out-of-range (e.g. "000",
    // "099") rather than silently mis-frame body presence downstream.
    if (status < 100 or status > 599) return error.BadStatus;

    var reason: []const u8 = "";
    const after = line[sp1 + 1 + 3 ..];
    if (after.len != 0) {
        // A reason (possibly empty) must be introduced by exactly one SP.
        if (after[0] != ' ') return error.MalformedStatusLine;
        reason = after[1..];
        if (!rp.validFieldValue(reason)) return error.MalformedStatusLine;
    }

    return .{
        .version = version,
        .status = status,
        .reason = reason,
        .headers = &.{},
        .content_length = null,
        .transfer_chunked = false,
        .keep_alive = false,
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

fn expectReject(expected: ParseError, bytes: []const u8) !void {
    var storage: [max_headers]Header = undefined;
    try std.testing.expectError(expected, parse(bytes, &storage));
}

test "parse: minimal 200" {
    var storage: [max_headers]Header = undefined;
    const head = try parse("HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\n", &storage);
    try std.testing.expectEqual(@as(u16, 200), head.status);
    try std.testing.expectEqualStrings("OK", head.reason);
    try std.testing.expectEqual(@as(u64, 5), head.content_length.?);
    try std.testing.expect(head.keep_alive);
    try std.testing.expect(!head.transfer_chunked);
}

test "parse: empty reason and header lookup" {
    var storage: [max_headers]Header = undefined;
    const head = try parse("HTTP/1.1 204\r\nx-a:  v \r\n\r\n", &storage);
    try std.testing.expectEqual(@as(u16, 204), head.status);
    try std.testing.expectEqualStrings("", head.reason);
    try std.testing.expectEqualStrings("v", head.header("X-A").?);
}

test "parse: chunked, and keep-alive across versions" {
    var storage: [max_headers]Header = undefined;
    const ch = try parse("HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n", &storage);
    try std.testing.expect(ch.transfer_chunked);
    try std.testing.expectEqual(@as(?u64, null), ch.content_length);

    const c10 = try parse("HTTP/1.0 200 OK\r\n\r\n", &storage);
    try std.testing.expect(!c10.keep_alive);
    const c10k = try parse("HTTP/1.0 200 OK\r\nconnection: keep-alive\r\n\r\n", &storage);
    try std.testing.expect(c10k.keep_alive);
    const c11c = try parse("HTTP/1.1 200 OK\r\nconnection: close\r\n\r\n", &storage);
    try std.testing.expect(!c11c.keep_alive);
}

test "reject: framing and status-line games" {
    try expectReject(error.ConflictingFraming, "HTTP/1.1 200 OK\r\ncontent-length: 5\r\ntransfer-encoding: chunked\r\n\r\n");
    try expectReject(error.ConflictingFraming, "HTTP/1.1 200 OK\r\ncontent-length: 1\r\ncontent-length: 2\r\n\r\n");
    try expectReject(error.UnsupportedTransferEncoding, "HTTP/1.1 200 OK\r\ntransfer-encoding: gzip, chunked\r\n\r\n");
    try expectReject(error.BadVersion, "HTTP/2 200 OK\r\n\r\n");
    try expectReject(error.BadStatus, "HTTP/1.1 2x0 OK\r\n\r\n");
    try expectReject(error.BadStatus, "HTTP/1.1  200 OK\r\n\r\n"); // double SP → code " 20"
    try expectReject(error.MalformedStatusLine, "HTTP/1.1\r\n\r\n"); // no status
    // Bare LF in status line.
    try expectReject(error.MalformedStatusLine, "HTTP/1.1 200 OK\n\r\n");
}

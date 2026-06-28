//! HTTP/1.1 request head encoder (client side).
//!
//! Sibling of `response_encode`. Prints the request line + headers into the
//! connection's buffered writer; a fixed-length body then rides the same
//! flush, which zio's Writer drains in one vectored syscall — the outbound
//! application of response_encode's write-path philosophy.

const std = @import("std");
const rp = @import("request_parser.zig");
const codec = @import("codec.zig");

pub const Header = codec.Header;
pub const Method = codec.Method;

pub const HeadOptions = struct {
    method: Method = .GET,
    /// Raw method token, required only when `method == .other`.
    method_raw: ?[]const u8 = null,
    /// origin-form request target, e.g. "/path?query" (default "/").
    target: []const u8 = "/",
    /// Host header value: "host" or "host:port".
    host: []const u8,
    extra_headers: []const Header = &.{},
    content_length: ?u64 = null,
    chunked: bool = false,
    /// false → emit `connection: close`.
    keep_alive: bool = true,
    /// Default request headers emitted (when non-null) after Host, before the
    /// framing headers. The client fills these with its policy defaults
    /// (User-Agent, Accept, Accept-Encoding) unless the caller overrode them
    /// via `extra_headers` (then the client leaves them null and the caller's
    /// version rides `extra_headers`). null = omit.
    user_agent: ?[]const u8 = null,
    accept: ?[]const u8 = null,
    accept_encoding: ?[]const u8 = null,
};

pub const EncodeError = error{
    /// A method token, target, host, or header field contained bytes that
    /// would break framing (CR/LF, SP in the wrong place, control chars).
    /// The encoder is a safety boundary: it refuses to emit a request that a
    /// strict parser would reject — closing the client-side CRLF/header
    /// injection vector (CWE-93/113) symmetrically with request_parser.
    InvalidRequestField,
};

/// Prints the request line + Host + framing + extra headers + blank line into
/// `w`. Does not flush. Caller writes the body (if any) afterward.
///
/// Validates every caller-supplied field for injection before writing: a value
/// containing CRLF must never reach the wire.
pub fn writeHead(w: *std.Io.Writer, options: HeadOptions) (std.Io.Writer.Error || EncodeError)!void {
    // `.other` requires an explicit raw token; falling back to GET would
    // silently send the wrong method and mask a caller bug.
    const method_str = if (options.method == .other)
        (options.method_raw orelse return error.InvalidRequestField)
    else
        @tagName(options.method);

    if (!rp.isToken(method_str)) return error.InvalidRequestField;
    if (!rp.validTarget(options.target)) return error.InvalidRequestField;
    if (!rp.validFieldValue(options.host)) return error.InvalidRequestField;
    for (options.extra_headers) |h| {
        if (!rp.isToken(h.name) or !rp.validFieldValue(h.value)) return error.InvalidRequestField;
        // Reject caller-supplied framing headers: the encoder emits Host and
        // the framing (content-length/transfer-encoding/connection) from its
        // typed options, so a duplicate here is a smuggling vector (CL+TE,
        // second Content-Length, ambiguous Host). Symmetric with the parser's
        // ConflictingFraming rejection (CWE-113).
        if (rp.isReservedFramingHeader(h.name) or std.ascii.eqlIgnoreCase(h.name, "host"))
            return error.InvalidRequestField;
    }

    try w.print("{s} {s} HTTP/1.1\r\nhost: {s}\r\n", .{
        method_str, options.target, options.host,
    });
    // Policy default headers (client-supplied static values; validated like any
    // field value so the encoder stays a strict injection boundary).
    if (options.user_agent) |v| {
        if (!rp.validFieldValue(v)) return error.InvalidRequestField;
        try w.print("user-agent: {s}\r\n", .{v});
    }
    if (options.accept) |v| {
        if (!rp.validFieldValue(v)) return error.InvalidRequestField;
        try w.print("accept: {s}\r\n", .{v});
    }
    if (options.accept_encoding) |v| {
        if (!rp.validFieldValue(v)) return error.InvalidRequestField;
        try w.print("accept-encoding: {s}\r\n", .{v});
    }
    if (options.content_length) |cl| {
        try w.print("content-length: {d}\r\n", .{cl});
    } else if (options.chunked) {
        try w.writeAll("transfer-encoding: chunked\r\n");
    }
    if (!options.keep_alive) {
        try w.writeAll("connection: close\r\n");
    }
    for (options.extra_headers) |h| {
        try w.print("{s}: {s}\r\n", .{ h.name, h.value });
    }
    try w.writeAll("\r\n");
}

// ── Tests ────────────────────────────────────────────────────────────────

test "writeHead: GET request line + host, no body" {
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeHead(&w, .{ .host = "example.com", .keep_alive = false });
    try std.testing.expectEqualStrings(
        "GET / HTTP/1.1\r\nhost: example.com\r\nconnection: close\r\n\r\n",
        w.buffered(),
    );
}

test "writeHead: POST with content-length + extra header" {
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeHead(&w, .{
        .method = .POST,
        .target = "/echo",
        .host = "t",
        .content_length = 11,
        .extra_headers = &.{.{ .name = "x-a", .value = "b" }},
    });
    try std.testing.expectEqualStrings(
        "POST /echo HTTP/1.1\r\nhost: t\r\ncontent-length: 11\r\nx-a: b\r\n\r\n",
        w.buffered(),
    );
}

test "writeHead: rejects CRLF / injection in caller fields" {
    var buf: [256]u8 = undefined;
    // Header value with embedded CRLF (request smuggling vector).
    {
        var w: std.Io.Writer = .fixed(&buf);
        try std.testing.expectError(error.InvalidRequestField, writeHead(&w, .{
            .host = "h",
            .extra_headers = &.{.{ .name = "x", .value = "a\r\nContent-Length: 0" }},
        }));
    }
    // Target with a space would split the request line.
    {
        var w: std.Io.Writer = .fixed(&buf);
        try std.testing.expectError(error.InvalidRequestField, writeHead(&w, .{
            .target = "/a HTTP/1.1\r\nHost: evil",
            .host = "h",
        }));
    }
    // Non-token header name.
    {
        var w: std.Io.Writer = .fixed(&buf);
        try std.testing.expectError(error.InvalidRequestField, writeHead(&w, .{
            .host = "h",
            .extra_headers = &.{.{ .name = "bad name", .value = "v" }},
        }));
    }
    // CRLF in host.
    {
        var w: std.Io.Writer = .fixed(&buf);
        try std.testing.expectError(error.InvalidRequestField, writeHead(&w, .{
            .host = "h\r\nX: y",
        }));
    }
    // `.other` method with no raw token must fail, not silently send GET.
    {
        var w: std.Io.Writer = .fixed(&buf);
        try std.testing.expectError(error.InvalidRequestField, writeHead(&w, .{
            .method = .other,
            .host = "h",
        }));
    }
}

test "writeHead: rejects caller-supplied framing headers (smuggling)" {
    var buf: [256]u8 = undefined;
    const reserved = [_][]const u8{ "content-length", "Transfer-Encoding", "Connection", "Host" };
    for (reserved) |name| {
        var w: std.Io.Writer = .fixed(&buf);
        try std.testing.expectError(error.InvalidRequestField, writeHead(&w, .{
            .host = "h",
            .content_length = 0,
            .extra_headers = &.{.{ .name = name, .value = "0" }},
        }));
    }
}

test "writeHead: emits policy default headers after host" {
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeHead(&w, .{
        .host = "h",
        .user_agent = "talon-http-client/1.0",
        .accept = "*/*",
        .accept_encoding = "gzip, deflate",
    });
    try std.testing.expectEqualStrings(
        "GET / HTTP/1.1\r\nhost: h\r\n" ++
            "user-agent: talon-http-client/1.0\r\naccept: */*\r\naccept-encoding: gzip, deflate\r\n\r\n",
        w.buffered(),
    );
}

test "writeHead: custom method via method_raw" {
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeHead(&w, .{ .method = .other, .method_raw = "PURGE", .host = "h", .keep_alive = false });
    try std.testing.expectEqualStrings(
        "PURGE / HTTP/1.1\r\nhost: h\r\nconnection: close\r\n\r\n",
        w.buffered(),
    );
}

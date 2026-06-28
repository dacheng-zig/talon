//! talon.http.codec: direction-neutral HTTP/1.1 wire codec.
//!
//! Both the server (talon.http server layer) and the client (talon.http.client)
//! consume this package. The split is by direction:
//!   - request_parser  / response_parser : heads in  (server reads requests,
//!                                          client reads responses)
//!   - request_encode  / response_encode : heads out
//!   - body            : BodyReader (in) + ChunkedBodyWriter (out), both
//!                       direction-neutral
//!
//! If the client is ever extracted into its own repo (deferred until the API
//! stabilizes), THIS module is the public semver contract boundary — the
//! migration surface is already collapsed here.

const std = @import("std");

pub const request_parser = @import("request_parser.zig");
pub const response_parser = @import("response_parser.zig");
pub const request_encode = @import("request_encode.zig");
pub const response_encode = @import("response_encode.zig");
const body = @import("body.zig");
const head = @import("head.zig");

pub const BodyReader = body.BodyReader;
pub const BodyError = body.BodyError;
pub const ChunkedBodyWriter = response_encode.ChunkedBodyWriter;
pub const DateCache = response_encode.DateCache;
/// Shared head-end scanner (server request loop + client response).
pub const findHeadEnd = head.findHeadEnd;
pub const HeadEndError = head.HeadEndError;

// Shared, direction-neutral wire vocabulary. This module is the codec's
// contract boundary, so the types both directions agree on are defined
// here directly; the parser/encoder leaves re-export these names for local use.
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
    /// Any other token method; see request_parser's `Head.method_raw`.
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

/// std.http.Status, used by the server response encoder.
pub const Status = std.http.Status;

test {
    @import("std").testing.refAllDecls(@This());
    _ = request_parser;
    _ = response_parser;
    _ = request_encode;
    _ = response_encode;
    _ = body;
    _ = head;
}

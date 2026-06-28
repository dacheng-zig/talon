//! Request type.
//!
//! Header/target slices borrow the per-request arena copy of the head;
//! their lifetime is the current request — dupe with `req.arena` to escape.

const std = @import("std");
const parser = @import("codec/request_parser.zig");
const body_mod = @import("codec/body.zig");
const BodyReader = body_mod.BodyReader;
const BodyError = body_mod.BodyError;

pub const Request = struct {
    head: parser.Head,
    /// Per-request arena: reset between requests on the same connection.
    arena: std.mem.Allocator,
    body: *BodyReader,

    pub fn method(self: *const Request) parser.Method {
        return self.head.method;
    }

    pub fn target(self: *const Request) []const u8 {
        return self.head.target;
    }

    /// Case-insensitive header lookup.
    pub fn header(self: *const Request, name: []const u8) ?[]const u8 {
        return self.head.header(name);
    }

    /// Streaming body access (`std.Io.Reader`; CL-bounded or chunk-decoded).
    pub fn bodyReader(self: *Request) *std.Io.Reader {
        return self.body.reader();
    }

    /// The framing/limit failure recorded by the last body read, or null if
    /// none has failed. A body read returns the `error.ReadFailed` sentinel
    /// and stashes the real cause here; `BodyTooLarge` (chunked body past
    /// `limits.max_body_size`) lets a caller answer 413 instead of 400.
    /// Oversized Content-Length bodies are rejected before the handler runs.
    pub fn bodyError(self: *const Request) ?BodyError {
        return self.body.err;
    }
};

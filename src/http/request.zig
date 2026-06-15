//! Request type (§2 public contract item 2).
//!
//! Header/target slices borrow the per-request arena copy of the head;
//! their lifetime is the current request — dupe with `req.arena` to escape.

const std = @import("std");
const parser = @import("parser.zig");
const BodyReader = @import("body.zig").BodyReader;

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
};

//! Request type.
//!
//! Header/target slices borrow the per-request arena copy of the head;
//! their lifetime is the current request. To retain them after the request,
//! dupe with a longer-lived allocator and free through that allocator.

const std = @import("std");
const zio = @import("zio");
const parser = @import("../codec/request_parser.zig");
const encode = @import("../codec/response_encode.zig");
const body_mod = @import("../codec/body.zig");
const BodyReader = body_mod.BodyReader;
const BodyError = body_mod.BodyError;
const Header = parser.Header;

/// Connection-upgrade handle, present on every request. A handler that speaks
/// a post-HTTP protocol (WebSocket, …) calls `accept` to send the 101 and take
/// over the connection's reader/writer for the rest of the coroutine's life;
/// the HTTP loop then exits instead of reading another request.
pub const Upgrade = struct {
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    /// Set true by `accept` so the protocol loop stops after the handler
    /// returns. Points at a flag owned by the connection loop.
    taken: *bool,
    /// Type-erased read-timeout setter for the underlying transport (the only
    /// connection method not already reachable through the io interfaces).
    ctx: *anyopaque,
    set_read_timeout_fn: *const fn (*anyopaque, zio.Timeout) void,

    pub const AcceptOptions = struct {
        /// Upgrade-header protocol token, e.g. "websocket".
        protocol: []const u8,
        /// Response headers for the 101 (e.g. sec-websocket-accept).
        extra_headers: []const Header = &.{},
        /// Read deadline for the upgraded protocol. Default none: reads block
        /// until data or peer close (keep-alive idle timeouts no longer apply).
        read_timeout: zio.Timeout = .none,
    };

    /// Sends `101 Switching Protocols` and hands the connection to the caller.
    /// After this returns, use `reader`/`writer` for the upgraded protocol.
    pub fn accept(self: *Upgrade, options: AcceptOptions) !void {
        try encode.writeUpgrade(self.writer, .{
            .protocol = options.protocol,
            .extra_headers = options.extra_headers,
        });
        try self.writer.flush();
        self.set_read_timeout_fn(self.ctx, options.read_timeout);
        self.taken.* = true;
    }

    pub fn setReadTimeout(self: *Upgrade, timeout: zio.Timeout) void {
        self.set_read_timeout_fn(self.ctx, timeout);
    }
};

pub const Request = struct {
    head: parser.Head,
    /// Per-request arena: reset between requests on the same connection.
    arena: std.mem.Allocator,
    body: *BodyReader,
    /// Connection-upgrade handle (WebSocket and other post-HTTP protocols).
    upgrade: *Upgrade,

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

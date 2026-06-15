//! Response type (§2 public contract item 2).

const std = @import("std");
const parser = @import("parser.zig");
const encode = @import("encode.zig");

pub const Response = struct {
    out: *std.Io.Writer,
    date: *encode.DateCache,
    /// Negotiated keep-alive; respond() may turn it off, never back on.
    keep_alive: bool,
    /// HEAD request: head is written normally, the body is suppressed.
    suppress_body: bool = false,
    written: bool = false,

    pub const RespondOptions = struct {
        status: encode.Status = .ok,
        extra_headers: []const parser.Header = &.{},
        /// Set false to close the connection after this response.
        keep_alive: bool = true,
    };

    /// Fixed-length response: head + body leave in one buffered flush
    /// (vectored into a single syscall by the zio writer).
    pub fn respond(self: *Response, body: []const u8, options: RespondOptions) !void {
        std.debug.assert(!self.written);
        self.written = true;
        if (!options.keep_alive) self.keep_alive = false;
        try encode.writeHead(self.out, self.date, .{
            .status = options.status,
            .extra_headers = options.extra_headers,
            .content_length = body.len,
            .keep_alive = self.keep_alive,
        });
        if (!self.suppress_body) {
            try self.out.writeAll(body);
        }
    }

    /// Streaming chunked response. Write through the returned writer's
    /// `.interface`, then call `.finish()`. `buffer` sizes the chunks
    /// (allocate from `req.arena`).
    pub fn startChunked(
        self: *Response,
        options: RespondOptions,
        buffer: []u8,
    ) !encode.ChunkedBodyWriter {
        std.debug.assert(!self.written);
        self.written = true;
        if (!options.keep_alive) self.keep_alive = false;
        try encode.writeHead(self.out, self.date, .{
            .status = options.status,
            .extra_headers = options.extra_headers,
            .chunked = true,
            .keep_alive = self.keep_alive,
        });
        return encode.ChunkedBodyWriter.init(self.out, buffer);
    }
};

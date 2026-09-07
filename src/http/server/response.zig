//! Response type.

const std = @import("std");
const parser = @import("../codec/request_parser.zig");
const encode = @import("../codec/response_encode.zig");
const event_stream = @import("event_stream.zig");

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
        // Informational responses cannot terminate a handler's response.
        // Protocol upgrades use Request.upgrade instead.
        if (@intFromEnum(options.status) < 200) return error.InvalidStatus;
        if (!options.keep_alive) self.keep_alive = false;
        encode.writeHead(self.out, self.date, .{
            .status = options.status,
            .extra_headers = options.extra_headers,
            .content_length = if (encode.statusAllowsBody(options.status)) body.len else null,
            .keep_alive = self.keep_alive,
        }) catch |err| {
            // Validation happens before output. A transport failure can leave
            // a partial head, so it must never be followed by a fallback 500.
            if (err != error.InvalidHeader) self.written = true;
            return err;
        };
        self.written = true;
        if (!self.suppress_body and encode.statusAllowsBody(options.status)) {
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
        if (@intFromEnum(options.status) < 200) return error.InvalidStatus;
        if (!options.keep_alive) self.keep_alive = false;
        encode.writeHead(self.out, self.date, .{
            .status = options.status,
            .extra_headers = options.extra_headers,
            .chunked = true,
            .keep_alive = self.keep_alive,
        }) catch |err| {
            // Validation happens before output. A transport failure can leave
            // a partial head, so it must never be followed by a fallback 500.
            if (err != error.InvalidHeader) self.written = true;
            return err;
        };
        self.written = true;
        var body_writer = encode.ChunkedBodyWriter.init(self.out, buffer);
        body_writer.suppress_body = self.suppress_body or !encode.statusAllowsBody(options.status);
        return body_writer;
    }

    /// Opens a Server-Sent Events stream: a chunked response carrying the SSE
    /// default headers. Write events through the returned `EventStream`; keep
    /// it at a stable address (the chunked writer resolves its parent by
    /// pointer). `buffer` (from `req.arena`) sizes the per-event staging.
    pub fn startEventStream(self: *Response, buffer: []u8) !event_stream.EventStream {
        const cw = try self.startChunked(.{ .extra_headers = &event_stream.default_headers }, buffer);
        return .{ .chunked = cw };
    }
};

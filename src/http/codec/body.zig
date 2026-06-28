//! HTTP body reader (direction-neutral): a `std.Io.Reader` over the
//! connection reader, decoding Content-Length-bounded, chunked, or (response
//! only) close-delimited framing. Streaming — never buffers the whole body.
//!
//! Strictness (smuggling defense, parser.zig's counterpart on the body
//! side): chunk sizes are bare hex (no extensions, no 0x prefix), lines end
//! with exact CRLF, trailers are rejected.

const std = @import("std");
const parser = @import("request_parser.zig");

pub const BodyError = error{
    /// Stream ended before the declared body length.
    TruncatedBody,
    MalformedChunk,
    /// Chunk extensions and trailer fields are rejected by policy.
    UnsupportedChunkFeature,
    /// Accumulated body exceeds limits.max_body_size.
    BodyTooLarge,
    ReadFailed,
};

pub const BodyReader = struct {
    upstream: *std.Io.Reader,
    state: State,
    /// Total body bytes produced so far (for max_body enforcement on
    /// chunked bodies; CL bodies are pre-checked by the protocol).
    produced: u64 = 0,
    max_body: ?u64,
    interface: std.Io.Reader,
    err: ?BodyError = null,
    chunk_remaining: u64 = 0,

    const State = union(enum) {
        none,
        /// Remaining bytes of a Content-Length body.
        content_length: u64,
        chunked: ChunkState,
        /// Close-delimited response body: no Content-Length, no chunked.
        /// Reads until upstream EOF, which is the clean terminator (not
        /// truncation). Response-only — the request `init` never produces it.
        until_close,
    };

    const ChunkState = enum {
        size_line,
        /// `data` carries remaining-in-chunk via `chunk_remaining`.
        data,
        data_crlf,
        done,
    };

    /// `buffer` enables peek/takeDelimiter-style use on the body; size it
    /// from the request arena.
    pub fn init(
        upstream: *std.Io.Reader,
        head: *const parser.Head,
        max_body: ?u64,
        buffer: []u8,
    ) BodyReader {
        // Request framing: chunked → CL → none. A request with neither is
        // bodyless (never close-delimited — that is a response-only mode).
        const state: State = if (head.transfer_chunked)
            .{ .chunked = .size_line }
        else if (head.content_length) |cl|
            (if (cl == 0) .none else .{ .content_length = cl })
        else
            .none;

        return fromState(upstream, state, max_body, buffer);
    }

    /// Response body framing, derived by the client from status + method.
    pub const ResponseFraming = struct {
        transfer_chunked: bool,
        content_length: ?u64,
        /// False for HEAD responses and 1xx/204/304: no body regardless of
        /// any framing headers present (the client decides this).
        has_body: bool,
    };

    /// Response-side constructor (client). Unlike requests, a body with no
    /// Content-Length and no chunked coding is close-delimited (read to EOF).
    pub fn initResponse(
        upstream: *std.Io.Reader,
        framing: ResponseFraming,
        max_body: ?u64,
        buffer: []u8,
    ) BodyReader {
        const state: State = if (!framing.has_body)
            .none
        else if (framing.transfer_chunked)
            .{ .chunked = .size_line }
        else if (framing.content_length) |cl|
            (if (cl == 0) .none else .{ .content_length = cl })
        else
            .until_close;

        return fromState(upstream, state, max_body, buffer);
    }

    fn fromState(upstream: *std.Io.Reader, state: State, max_body: ?u64, buffer: []u8) BodyReader {
        return .{
            .upstream = upstream,
            .state = state,
            .max_body = max_body,
            .interface = .{
                .vtable = &.{ .stream = streamImpl },
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    pub fn reader(self: *BodyReader) *std.Io.Reader {
        return &self.interface;
    }

    /// Consumes whatever the handler left unread so the connection can be
    /// reused for the next request.
    pub fn discard(self: *BodyReader) BodyError!void {
        _ = self.interface.discardRemaining() catch {
            return self.err orelse error.ReadFailed;
        };
    }

    fn fail(self: *BodyReader, err: BodyError) error{ReadFailed} {
        self.err = err;
        return error.ReadFailed;
    }

    fn streamImpl(io_r: *std.Io.Reader, io_w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *BodyReader = @alignCast(@fieldParentPtr("interface", io_r));
        switch (self.state) {
            .none => return error.EndOfStream,
            .content_length => |remaining| {
                if (remaining == 0) return error.EndOfStream;
                const n = try self.streamFromUpstream(io_w, limit, remaining);
                self.state = .{ .content_length = remaining - n };
                return n;
            },
            .until_close => {
                // Close-delimited: copy whatever upstream has; EOF is the
                // clean terminator here (not TruncatedBody).
                const up = self.upstream;
                if (up.bufferedLen() == 0) {
                    up.fillMore() catch |err| switch (err) {
                        error.EndOfStream => return error.EndOfStream,
                        error.ReadFailed => return self.fail(error.ReadFailed),
                    };
                }
                const window = up.buffered();
                const n = limit.minInt(window.len);
                if (n == 0) return 0;
                const dest = io_w.writableSliceGreedy(1) catch return error.WriteFailed;
                const copied = @min(n, dest.len);
                if (self.max_body) |max| {
                    if (self.produced + copied > max) return self.fail(error.BodyTooLarge);
                }
                @memcpy(dest[0..copied], window[0..copied]);
                io_w.advance(copied);
                up.toss(copied);
                self.produced += copied;
                return copied;
            },
            .chunked => |*chunk_state| {
                while (true) {
                    switch (chunk_state.*) {
                        .done => return error.EndOfStream,
                        .size_line => {
                            try self.readChunkSizeLine();
                            if (self.chunk_remaining == 0) {
                                // Last chunk: require immediate final CRLF
                                // (no trailer fields).
                                try self.expectCrlf(error.UnsupportedChunkFeature);
                                chunk_state.* = .done;
                                return error.EndOfStream;
                            }
                            chunk_state.* = .data;
                        },
                        .data => {
                            if (self.chunk_remaining == 0) {
                                chunk_state.* = .data_crlf;
                                continue;
                            }
                            const n = try self.streamFromUpstream(io_w, limit, self.chunk_remaining);
                            self.chunk_remaining -= n;
                            if (self.chunk_remaining == 0) chunk_state.* = .data_crlf;
                            return n;
                        },
                        .data_crlf => {
                            try self.expectCrlf(error.MalformedChunk);
                            chunk_state.* = .size_line;
                        },
                    }
                }
            },
        }
    }

    /// Copies up to min(limit, cap, upstream-buffered) bytes into io_w.
    fn streamFromUpstream(self: *BodyReader, io_w: *std.Io.Writer, limit: std.Io.Limit, cap: u64) std.Io.Reader.StreamError!usize {
        const up = self.upstream;
        if (up.bufferedLen() == 0) {
            up.fillMore() catch |err| switch (err) {
                error.EndOfStream => return self.fail(error.TruncatedBody),
                error.ReadFailed => return self.fail(error.ReadFailed),
            };
        }
        const window = up.buffered();
        var n: usize = @intCast(@min(window.len, cap));
        n = limit.minInt(n);
        if (n == 0) return 0;

        const dest = io_w.writableSliceGreedy(1) catch return error.WriteFailed;
        const copied = @min(n, dest.len);
        // Enforce on bytes actually produced (copied), not the larger window
        // (n): a destination smaller than the window must not trip the limit
        // early. `produced` is accumulated by `copied`, so the check matches.
        if (self.max_body) |max| {
            if (self.produced + copied > max) return self.fail(error.BodyTooLarge);
        }
        @memcpy(dest[0..copied], window[0..copied]);
        io_w.advance(copied);
        up.toss(copied);
        self.produced += copied;
        return copied;
    }

    /// Reads a strict chunk-size line: bare hex digits + CRLF.
    fn readChunkSizeLine(self: *BodyReader) std.Io.Reader.StreamError!void {
        const line = self.takeLine() catch |err| return err;
        if (line.len == 0) return self.fail(error.MalformedChunk);
        var size: u64 = 0;
        for (line) |c| {
            const digit = std.fmt.charToDigit(c, 16) catch {
                // ';' here would be a chunk extension: rejected by policy.
                if (c == ';') return self.fail(error.UnsupportedChunkFeature);
                return self.fail(error.MalformedChunk);
            };
            // 16 hex digits max — overflow guard.
            if (size > (std.math.maxInt(u64) >> 4)) return self.fail(error.MalformedChunk);
            size = (size << 4) | digit;
        }
        self.chunk_remaining = size;
    }

    fn expectCrlf(self: *BodyReader, on_garbage: BodyError) std.Io.Reader.StreamError!void {
        const pair = self.upstream.take(2) catch |err| switch (err) {
            error.EndOfStream => return self.fail(error.TruncatedBody),
            error.ReadFailed => return self.fail(error.ReadFailed),
        };
        if (!std.mem.eql(u8, pair, "\r\n")) return self.fail(on_garbage);
    }

    /// Takes one CRLF-terminated line from upstream (bounded; chunk size
    /// lines are short).
    fn takeLine(self: *BodyReader) std.Io.Reader.StreamError![]const u8 {
        const max_line = 18; // 16 hex digits + slack
        const up = self.upstream;
        while (true) {
            const window = up.buffered();
            if (std.mem.indexOfScalar(u8, window, '\n')) |nl| {
                if (nl == 0 or window[nl - 1] != '\r') return self.fail(error.MalformedChunk);
                const line = window[0 .. nl - 1];
                if (line.len > max_line) return self.fail(error.MalformedChunk);
                up.toss(nl + 1);
                return line;
            }
            if (window.len > max_line + 2) return self.fail(error.MalformedChunk);
            up.fillMore() catch |err| switch (err) {
                error.EndOfStream => return self.fail(error.TruncatedBody),
                error.ReadFailed => return self.fail(error.ReadFailed),
            };
        }
    }
};

//! ClientConnection: a single outbound HTTP/1.1 connection state machine.
//! One request/response round-trip — the dual of the
//! server's per-connection loop, gluing the reused codec pieces together:
//!
//!   request_encode (head) → write body → response_parser (head)
//!     → BodyReader.initResponse (streaming response body)
//!
//! NOTE: must not be moved after `create` — the buffered reader/writer resolve
//! their parent via @fieldParentPtr (same constraint as the server's
//! Connection). It is therefore heap-allocated with a stable address.

const std = @import("std");
const zio = @import("zio");
const codec = @import("../codec/codec.zig");
const flate = std.compress.flate;
const zstd = std.compress.zstd;

const read_buf_size = 16 * 1024;
const write_buf_size = 4 * 1024;
const body_buf_size = 4 * 1024;
// Strictly below read_buf_size so the head scanner trips its size guard
// before the read buffer fills (codec/head.zig precondition).
const max_head_size = read_buf_size - 1024;

/// Request body source for `sendRequest`. `bytes` is the in-memory fast path;
/// `reader`/`chunked` stream the body without buffering it — the large-file
/// upload path. `reader` carries a known length (Content-Length framed, the
/// common file-upload case where size comes from a stat); `chunked` is for an
/// unknown-length stream (Transfer-Encoding: chunked). Streaming bodies are
/// single-use: they cannot be replayed for a transparent retry or a
/// body-preserving redirect (the client guards against both).
pub const Body = union(enum) {
    none,
    bytes: []const u8,
    reader: struct { reader: *std.Io.Reader, len: u64 },
    chunked: *std.Io.Reader,

    /// Whether this body can be re-sent (retry / body-preserving redirect). A
    /// consumed stream cannot, so only the empty and in-memory bodies qualify.
    pub fn replayable(self: Body) bool {
        return switch (self) {
            .none, .bytes => true,
            .reader, .chunked => false,
        };
    }
};

pub fn ClientConnection(comptime Raw: type) type {
    return struct {
        /// Transport I/O storage + lifecycle, abstracted over whether the
        /// transport hands back value-type readers (plain TCP/memory) or owns
        /// pinned reader/writer interfaces (TLS). See `Transport`.
        transport: Transport(Raw),
        body_buf: []u8,
        gpa: std.mem.Allocator,
        /// Pins the response head for the round-trip's lifetime (header slices
        /// borrow this; valid until `destroy`).
        arena: std.heap.ArenaAllocator,
        headers_storage: [codec.response_parser.max_headers]codec.Header = undefined,
        head: codec.response_parser.ResponseHead = undefined,
        body: codec.BodyReader = undefined,
        request_method: codec.Method = .GET,
        /// Lazily-allocated transparent decoders, reused across round-trips on
        /// this connection; each is null until a response first needs it, and
        /// freed on `destroy`. `flate` covers gzip/deflate; `zstd` covers zstd.
        flate_decoder: ?*flate.Decompress = null,
        zstd_decoder: ?*zstd.Decompress = null,
        /// zstd needs an explicit output window (flate manages its history
        /// internally). Sized `default_window_len + block_size_max` so any
        /// spec-conformant `Content-Encoding: zstd` stream (≤ 8 MiB window)
        /// decodes; lazily allocated on the first zstd response.
        zstd_window: ?[]u8 = null,
        /// The decoded body reader for the current response (points into one of
        /// the decoders above), or null when the body is served raw.
        decoded_reader: ?*std.Io.Reader = null,
        /// True when the last response body is close-delimited (no
        /// Content-Length, not chunked): the server signals end-of-body by
        /// closing, so the connection cannot be reused even if it claimed
        /// keep-alive (the §14.1 pooling precondition).
        close_delimited: bool = false,
        // ── Pool bookkeeping (owned/managed by the connection pool) ──────────
        /// Monotonic-ns timestamp this transport was established; drives the
        /// connection-lifetime knob. 0 until the pool stamps it.
        created_ns: u64 = 0,
        /// Owned copy of this connection's origin key (e.g. "http://h:80"),
        /// so the pool can re-bucket it on checkin. Empty until the pool sets it.
        origin_key: []const u8 = &.{},

        const Self = @This();

        /// Allocates and wires the connection in place (stable address). Takes
        /// ownership of `raw` on entry: every error path here closes it, so the
        /// caller must not close `raw` after calling `create` (success or
        /// failure). Until `Transport.init` succeeds the raw is closed directly;
        /// after, ownership passes to `transport` and its `deinit` closes it —
        /// exactly one close on every path (the alternative, a caller-side
        /// `raw.close()` on failure, double-closed the fd on a mid-create OOM).
        pub fn create(gpa: std.mem.Allocator, raw: Raw) !*Self {
            const self = gpa.create(Self) catch |err| {
                raw.close();
                return err;
            };
            errdefer gpa.destroy(self);

            var transport = Transport(Raw).init(gpa, raw) catch |err| {
                raw.close(); // init does not take ownership of raw on failure
                return err;
            };
            errdefer transport.deinit(gpa); // transport now owns (and closes) raw
            const body_buf = try gpa.alloc(u8, body_buf_size);
            errdefer gpa.free(body_buf);

            self.* = .{
                .transport = transport,
                .body_buf = body_buf,
                .gpa = gpa,
                .arena = std.heap.ArenaAllocator.init(gpa),
            };
            return self;
        }

        /// Closes the transport and frees all owned memory.
        pub fn destroy(self: *Self) void {
            self.transport.deinit(self.gpa);
            self.arena.deinit();
            if (self.flate_decoder) |d| self.gpa.destroy(d);
            if (self.zstd_decoder) |d| self.gpa.destroy(d);
            if (self.zstd_window) |w| self.gpa.free(w);
            if (self.origin_key.len != 0) self.gpa.free(self.origin_key);
            self.gpa.free(self.body_buf);
            self.gpa.destroy(self);
        }

        /// Whether this connection may be returned to the pool after its body
        /// has been drained. Three conditions, all required (the missing one is
        /// the classic pool-poisoning bug): the response declared keep-alive,
        /// the body was not close-delimited, and no framing error occurred
        /// while reading/draining it (a poisoned body leaves stray bytes that
        /// would corrupt the next request on the wire).
        pub fn reusableAfterDrain(self: *const Self) bool {
            return self.head.keep_alive and !self.close_delimited and self.body.err == null;
        }

        pub fn reader(self: *Self) *std.Io.Reader {
            return self.transport.reader();
        }

        pub fn writer(self: *Self) *std.Io.Writer {
            return self.transport.writer();
        }

        /// Per-read deadline (kernel-level zio Timeout), the dual of the
        /// server's Connection.setReadTimeout. It stays in effect until changed,
        /// so a deadline set before reading the response head also bounds the
        /// streamed body reads that follow. No-op on transports without timeout
        /// support (memory pipes).
        pub fn setReadTimeout(self: *Self, timeout: zio.Timeout) void {
            self.transport.setReadTimeout(timeout);
        }

        /// Per-write deadline; bounds sending the request head and body.
        pub fn setWriteTimeout(self: *Self, timeout: zio.Timeout) void {
            self.transport.setWriteTimeout(timeout);
        }

        /// Best-effort liveness probe for an *idle* pooled connection: a short
        /// bounded read that distinguishes a still-open keep-alive socket from
        /// one the peer closed (or left unexpected bytes on) while idle — the
        /// classic stale-connection defense. The mechanics live in the
        /// `Transport` seam (they differ for TLS vs plain). Transports without
        /// timeout support (memory pipes) are assumed live.
        ///
        /// Must only be called on a fully-drained idle connection (no in-flight
        /// round-trip): any buffered or readable byte then means the connection
        /// is unusable, not that a response arrived.
        pub fn isLikelyLive(self: *Self) bool {
            return self.transport.isLikelyLive();
        }

        /// Encodes the request head, writes the body, and flushes. A `.bytes`
        /// body rides the head's flush — both leave in one vectored syscall when
        /// they fit the write buffer. `.reader`/`.chunked` stream the body
        /// without buffering it whole (large-file upload). The caller must have
        /// set `opts.content_length`/`opts.chunked` to match `body` (the client
        /// does — see `roundtrip`).
        pub fn sendRequest(self: *Self, opts: codec.request_encode.HeadOptions, body: Body) !void {
            self.request_method = opts.method;
            const w = self.writer();
            try codec.request_encode.writeHead(w, opts);
            switch (body) {
                .none => {},
                .bytes => |b| try w.writeAll(b),
                .reader => |src| {
                    // Stream exactly `src.len` bytes: Content-Length is already
                    // on the wire, so sending fewer would desync the connection.
                    // A short reader is a caller contract violation → fail (the
                    // connection is then closed, not pooled).
                    var left = src.len;
                    while (left > 0) {
                        const n = src.reader.stream(w, std.Io.Limit.limited(left)) catch |err| switch (err) {
                            error.EndOfStream => return error.UploadBodyTruncated,
                            error.ReadFailed => return error.ReadFailed,
                            error.WriteFailed => return error.WriteFailed,
                        };
                        left -= n;
                    }
                },
                .chunked => |r| {
                    // `body_buf` is free here: it only backs the *response*
                    // BodyReader, set up later in readResponse. Chunk it now.
                    var cw = codec.response_encode.ChunkedBodyWriter.init(w, self.body_buf);
                    _ = r.streamRemaining(&cw.interface) catch |err| switch (err) {
                        error.ReadFailed => return error.ReadFailed,
                        error.WriteFailed => return error.WriteFailed,
                    };
                    try cw.finish();
                },
            }
            // Flush the whole write stack. For TLS this also pushes the encrypted
            // record onto the socket (the TLS writer's own flush only encrypts
            // into the underlying buffer) — see Transport/TlsTransport.flush.
            try self.transport.flush();
        }

        /// Reads and parses the response head (skipping interim 1xx responses),
        /// then sets up the streaming response BodyReader. The body is consumed
        /// afterward via `bodyReader()`. `max_body` caps the *raw* response body
        /// (DoS guard; null = unbounded); returns `error.ResponseBodyTooLarge`
        /// when the declared Content-Length already exceeds it. When `decompress`
        /// and the response carries a supported `Content-Encoding`, the body is
        /// transparently decoded — see `bodyReader`.
        pub fn readResponse(self: *Self, max_body: ?u64, decompress: bool) !void {
            const r = self.reader();
            _ = self.arena.reset(.retain_capacity);
            self.head = try readFinalHead(r, self.arena.allocator(), &self.headers_storage);

            if (max_body) |max| {
                if (self.head.content_length) |cl| {
                    if (cl > max) return error.ResponseBodyTooLarge;
                }
            }

            const has_body = responseHasBody(self.request_method, self.head.status);
            // Close-delimited: a body present but framed by neither
            // Content-Length nor chunked — the transport close is the only
            // terminator, so this connection is single-use (see reusableAfterDrain).
            self.close_delimited = has_body and !self.head.transfer_chunked and self.head.content_length == null;
            self.body = codec.BodyReader.initResponse(r, .{
                .transfer_chunked = self.head.transfer_chunked,
                .content_length = self.head.content_length,
                .has_body = has_body,
            }, max_body, self.body_buf);

            // Transparent decompression: decode on top of the (raw-capped)
            // BodyReader. `max_body` still bounds the *compressed* input; the
            // decoder is streaming (no full-output buffering), but a malicious
            // server can still amplify — callers reading untrusted compressed
            // bodies should bound their own reads (e.g. `Response.readAllAlloc`).
            self.decoded_reader = null;
            if (decompress and has_body) {
                if (self.head.header("content-encoding")) |ce|
                    self.decoded_reader = try self.initDecoder(ce);
            }
        }

        /// Sets up the transparent decoder for `content_encoding` on top of the
        /// raw body and returns its decoded reader; null when the encoding is
        /// not transparently handled (br/identity/multi-encoding → caller gets
        /// the raw body and may decode it itself). Decoders are lazily allocated
        /// and reused across round-trips.
        fn initDecoder(self: *Self, content_encoding: []const u8) !?*std.Io.Reader {
            if (flateContainerFor(content_encoding)) |container| {
                if (self.flate_decoder == null) self.flate_decoder = try self.gpa.create(flate.Decompress);
                self.flate_decoder.?.* = flate.Decompress.init(self.body.reader(), container, &.{});
                return &self.flate_decoder.?.reader;
            }
            if (std.ascii.eqlIgnoreCase(content_encoding, "zstd")) {
                if (self.zstd_window == null)
                    self.zstd_window = try self.gpa.alloc(u8, zstd.default_window_len + zstd.block_size_max);
                if (self.zstd_decoder == null) self.zstd_decoder = try self.gpa.create(zstd.Decompress);
                // Non-empty buffer → the decoder owns its window (it does not
                // push the capacity requirement onto the caller's writer).
                self.zstd_decoder.?.* = zstd.Decompress.init(self.body.reader(), self.zstd_window.?, .{});
                return &self.zstd_decoder.?.reader;
            }
            return null;
        }

        /// The response body stream: the transparently-decoded stream when a
        /// supported Content-Encoding was decoded, otherwise the raw body.
        pub fn bodyReader(self: *Self) *std.Io.Reader {
            return self.decoded_reader orelse self.body.reader();
        }
    };
}

/// Idle-connection liveness probe window. A short, bounded read that fires
/// reliably (a zero-duration zio timer is not guaranteed to fire, which would
/// hang the probe on a healthy idle socket). Small enough to be cheap, large
/// enough to never false-evict a live connection.
const liveness_probe_timeout: zio.Timeout = .{ .duration = zio.Duration.fromMilliseconds(1) };

/// The transport seam `ClientConnection` reads/writes/times-out through. Two
/// shapes, selected at comptime:
///
///   - *value* transports (plain TCP, in-memory pipes) hand back value-type
///     `Raw.Reader`/`Raw.Writer` bound to caller-owned buffers — the original
///     contract; `ClientConnection` owns those buffers.
///   - *pinned* transports (TLS) own their own record buffers and expose ready
///     `*std.Io.Reader`/`*std.Io.Writer` (the decrypted streams live inside a
///     heap-pinned object and cannot be copied out). Marked by
///     `Raw.pinned_transport`.
fn Transport(comptime Raw: type) type {
    const pinned = @hasDecl(Raw, "pinned_transport") and Raw.pinned_transport;
    return if (pinned) PinnedTransport(Raw) else ValueTransport(Raw);
}

/// Value-type transport storage (plain TCP / memory): owns the read/write
/// buffers and the value-type reader/writer states whose `interface` field is
/// recovered via `@fieldParentPtr` — hence pinned inside the heap-stable
/// `ClientConnection`.
fn ValueTransport(comptime Raw: type) type {
    return struct {
        raw: Raw,
        reader_state: Raw.Reader,
        writer_state: Raw.Writer,
        read_buf: []u8,
        write_buf: []u8,

        const Self = @This();

        fn init(gpa: std.mem.Allocator, raw: Raw) !Self {
            const read_buf = try gpa.alloc(u8, read_buf_size);
            errdefer gpa.free(read_buf);
            const write_buf = try gpa.alloc(u8, write_buf_size);
            errdefer gpa.free(write_buf);
            return .{
                .raw = raw,
                .reader_state = raw.reader(read_buf),
                .writer_state = raw.writer(write_buf),
                .read_buf = read_buf,
                .write_buf = write_buf,
            };
        }

        fn deinit(self: *Self, gpa: std.mem.Allocator) void {
            self.raw.close();
            gpa.free(self.write_buf);
            gpa.free(self.read_buf);
        }

        fn reader(self: *Self) *std.Io.Reader {
            return &self.reader_state.interface;
        }

        fn writer(self: *Self) *std.Io.Writer {
            return &self.writer_state.interface;
        }

        fn flush(self: *Self) !void {
            try self.writer_state.interface.flush();
        }

        fn setReadTimeout(self: *Self, timeout: zio.Timeout) void {
            if (comptime std.meta.hasMethod(Raw.Reader, "setTimeout"))
                self.reader_state.setTimeout(timeout);
        }

        fn setWriteTimeout(self: *Self, timeout: zio.Timeout) void {
            if (comptime std.meta.hasMethod(Raw.Writer, "setTimeout"))
                self.writer_state.setTimeout(timeout);
        }

        fn isLikelyLive(self: *Self) bool {
            if (comptime !std.meta.hasMethod(Raw.Reader, "setTimeout")) return true;
            const r = &self.reader_state.interface;
            if (r.bufferedLen() != 0) return false; // unexpected leftover bytes
            self.reader_state.setTimeout(liveness_probe_timeout);
            defer self.reader_state.setTimeout(.none);
            _ = r.fill(1) catch |err| switch (err) {
                error.EndOfStream => return false, // peer closed
                error.ReadFailed => {
                    const timed_out = if (self.reader_state.err) |e| e == error.Timeout else false;
                    if (timed_out) {
                        self.reader_state.err = null;
                        return true; // no data, socket still open
                    }
                    return false; // a real read error
                },
            };
            return false; // unsolicited data on an idle connection → unusable
        }
    };
}

/// Pinned transport storage (TLS): the `Raw` owns its buffers and pinned
/// interfaces, so this is a thin pass-through. The `Raw` value is freely
/// movable (only its heap-pinned internals must stay put).
fn PinnedTransport(comptime Raw: type) type {
    return struct {
        raw: Raw,

        const Self = @This();

        fn init(gpa: std.mem.Allocator, raw: Raw) !Self {
            _ = gpa; // the raw already owns its buffers (allocated when dialed)
            return .{ .raw = raw };
        }

        fn deinit(self: *Self, gpa: std.mem.Allocator) void {
            _ = gpa; // raw.close frees the raw's own pinned state
            self.raw.close();
        }

        fn reader(self: *Self) *std.Io.Reader {
            return self.raw.ioReader();
        }

        fn writer(self: *Self) *std.Io.Writer {
            return self.raw.ioWriter();
        }

        fn flush(self: *Self) !void {
            try self.raw.flush();
        }

        fn setReadTimeout(self: *Self, timeout: zio.Timeout) void {
            self.raw.setReadTimeout(timeout);
        }

        fn setWriteTimeout(self: *Self, timeout: zio.Timeout) void {
            self.raw.setWriteTimeout(timeout);
        }

        fn isLikelyLive(self: *Self) bool {
            return self.raw.isLikelyLive();
        }
    };
}

/// Maps an HTTP Content-Encoding token to a flate container; null if not a
/// flate encoding (zstd is handled separately; br/identity/multi-encoding →
/// caller gets the raw body and may decode it itself). HTTP "deflate" is
/// zlib-wrapped per RFC 9110 (raw-deflate senders exist but are non-conformant).
fn flateContainerFor(content_encoding: []const u8) ?flate.Container {
    if (std.ascii.eqlIgnoreCase(content_encoding, "gzip")) return .gzip;
    if (std.ascii.eqlIgnoreCase(content_encoding, "deflate")) return .zlib;
    return null;
}

/// Reads response heads from `r`, discarding interim 1xx informational
/// responses, and returns the final head (slices borrow `arena`). Leaves the
/// reader positioned at the first byte of the final response body.
///
/// RFC 9110 §15.2 / RFC 9112: a client MUST be able to parse one or more 1xx
/// responses (e.g. 100 Continue, 103 Early Hints) that precede the final
/// response; they carry no body. 101 (Switching Protocols) is returned as the
/// final head — this client never requests an upgrade, so a 101 cannot
/// legitimately arrive, and returning it avoids blocking on a phantom next head.
fn readFinalHead(
    r: *std.Io.Reader,
    arena: std.mem.Allocator,
    headers_storage: []codec.Header,
) !codec.response_parser.ResponseHead {
    while (true) {
        const head_len = try codec.findHeadEnd(r, max_head_size, null);
        // Pin the head: BodyReader reads through the same reader whose buffer
        // rebases on refill (same reason as the server, protocol.zig).
        const head_bytes = try arena.dupe(u8, r.buffered()[0..head_len]);
        r.toss(head_len);
        const head = try codec.response_parser.parse(head_bytes, headers_storage);
        if (head.status >= 100 and head.status < 200 and head.status != 101) continue;
        return head;
    }
}

test "readFinalHead: skips interim 1xx and returns the final head" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var storage: [codec.response_parser.max_headers]codec.Header = undefined;
    var r: std.Io.Reader = .fixed(
        "HTTP/1.1 100 Continue\r\n\r\n" ++
            "HTTP/1.1 103 Early Hints\r\nlink: </s.css>\r\n\r\n" ++
            "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nhi",
    );
    const head = try readFinalHead(&r, arena.allocator(), &storage);
    try std.testing.expectEqual(@as(u16, 200), head.status);
    try std.testing.expectEqual(@as(?u64, 2), head.content_length);
    // Reader is left at the body — the interim heads were consumed.
    try std.testing.expectEqualStrings("hi", r.buffered());
}

test "readFinalHead: returns 101 as final (no phantom read)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var storage: [codec.response_parser.max_headers]codec.Header = undefined;
    var r: std.Io.Reader = .fixed("HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\r\n");
    const head = try readFinalHead(&r, arena.allocator(), &storage);
    try std.testing.expectEqual(@as(u16, 101), head.status);
}

/// Response body presence, decided by request method + status code (RFC 9112
/// §6.3): HEAD and 2xx-CONNECT have no body; 1xx/204/304 have no body; every
/// other response is framed by Content-Length, chunked, or close-delimited.
fn responseHasBody(method: codec.Method, status: u16) bool {
    if (method == .HEAD) return false;
    if (method == .CONNECT and status >= 200 and status < 300) return false;
    if (status < 200) return false; // 1xx informational
    if (status == 204 or status == 304) return false;
    return true;
}

test "responseHasBody: method and status rules" {
    try std.testing.expect(responseHasBody(.GET, 200));
    try std.testing.expect(!responseHasBody(.HEAD, 200));
    try std.testing.expect(!responseHasBody(.GET, 204));
    try std.testing.expect(!responseHasBody(.GET, 304));
    try std.testing.expect(!responseHasBody(.GET, 100));
    try std.testing.expect(!responseHasBody(.CONNECT, 200));
    try std.testing.expect(responseHasBody(.POST, 201));
}

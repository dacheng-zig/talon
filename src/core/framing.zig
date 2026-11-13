//! Framing toolbox — the Zig answer to Netty's codec framework.
//!
//! Three comptime components over `std.Io.Reader`'s buffered window, so a
//! custom `Proto` author writes the protocol state machine, never buffer
//! management:
//!
//!   - `LengthPrefixed` — RPC / private binary protocols
//!   - `Delimited`      — line protocols (RESP, SMTP, memcached text)
//!   - `Accumulator`    — incremental decoder template (ByteToMessageDecoder
//!                        equivalent) for custom state machines
//!
//! The three window-based components return borrowed slices: valid until the
//! next call on the reader. OwnedLengthPrefixed instead allocates each payload
//! independently, so large frames need not enlarge the connection window.
//! All components carry a `max_frame` defense; a borrowed frame
//! must also fit the reader's buffer capacity (size the buffer ≥ max_frame +
//! frame header). Timeouts ride on the underlying reader (zio per-op
//! Timeout). Stream side only — datagrams are already frames.

const std = @import("std");

pub const Error = error{
    /// Frame exceeds max_frame (or the reader's buffer capacity).
    FrameTooLarge,
    /// Stream ended in the middle of a frame.
    PartialFrame,
    /// Decoder rejected the bytes (Accumulator only surfaces decoder errors
    /// as-is; this is for built-in decoders).
    InvalidFrame,
    ReadFailed,
};

/// Length-prefixed framing: `[len][payload]` where `len` is a fixed-width
/// integer of `LengthType` in `endian` byte order.
pub fn LengthPrefixed(comptime options: LengthPrefixedOptions) type {
    const len_size = lengthHeaderSize(options);

    return struct {
        reader: *std.Io.Reader,

        const Self = @This();

        pub fn init(reader: *std.Io.Reader) Self {
            return .{ .reader = reader };
        }

        /// Returns the next frame payload (without the length header), or
        /// null on clean end-of-stream. The slice is borrowed: valid until
        /// the next call on this reader.
        pub fn next(self: *Self) Error!?[]const u8 {
            const r = self.reader;
            const header = r.peek(len_size) catch |err| switch (err) {
                error.EndOfStream => {
                    // EOF exactly on a frame boundary is a clean end.
                    if (r.bufferedLen() == 0) return null;
                    return error.PartialFrame;
                },
                error.ReadFailed => return error.ReadFailed,
            };
            const payload_len = try payloadLength(options, header[0..len_size]);

            if (payload_len > r.buffer.len -| len_size) return error.FrameTooLarge;
            const total = len_size + payload_len;
            const frame = r.take(total) catch |err| switch (err) {
                error.EndOfStream => return error.PartialFrame,
                error.ReadFailed => return error.ReadFailed,
            };
            return frame[len_size..];
        }
    };
}

fn lengthHeaderSize(comptime options: LengthPrefixedOptions) usize {
    const info = @typeInfo(options.length_type);
    if (info != .int or info.int.signedness != .unsigned or
        info.int.bits == 0 or info.int.bits % 8 != 0)
        @compileError("talon.framing: length_type must be a nonzero, byte-aligned unsigned integer");
    return info.int.bits / 8;
}

fn payloadLength(comptime options: LengthPrefixedOptions, header: *const [lengthHeaderSize(options)]u8) Error!usize {
    var raw_len = std.mem.readInt(options.length_type, header, options.endian);
    if (options.includes_header) {
        if (raw_len < header.len) return error.InvalidFrame;
        raw_len -= header.len;
    }
    const len = std.math.cast(usize, raw_len) orelse return error.FrameTooLarge;
    if (len > options.max_frame) return error.FrameTooLarge;
    return len;
}

/// A single owner of its payload, independent of the reader's buffer. Do not
/// copy ownership; deinit exactly once, while the allocating allocator is live.
pub const OwnedFrame = struct {
    payload: []u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *OwnedFrame) void {
        self.allocator.free(self.payload);
        self.* = undefined;
    }
};

pub const OwnedFrameError = Error || std.mem.Allocator.Error || error{
    /// A previous next() failed after potentially consuming stream bytes.
    FrameReaderFailed,
};

/// Length-prefixed payloads allocated only after validating max_frame. Neither
/// header nor payload must fit the read window. Retained frames are caller-owned;
/// max_frame bounds one payload, not the sum of retained frames/connections.
pub fn OwnedLengthPrefixed(comptime options: LengthPrefixedOptions) type {
    const len_size = lengthHeaderSize(options);
    return struct {
        reader: *std.Io.Reader,
        allocator: std.mem.Allocator,
        failed: bool = false,
        ended: bool = false,

        const Self = @This();

        pub fn init(reader: *std.Io.Reader, allocator: std.mem.Allocator) Self {
            return .{ .reader = reader, .allocator = allocator };
        }

        /// null means clean EOF. On any error partial allocations are freed,
        /// and subsequent calls fail without reading again. End the connection;
        /// this decoder cannot resynchronize or resume after timeout/cancellation.
        pub fn next(self: *Self) OwnedFrameError!?OwnedFrame {
            if (self.failed) return error.FrameReaderFailed;
            if (self.ended) return null;
            errdefer self.failed = true;

            var header: [len_size]u8 = undefined;
            const n = try self.reader.readSliceShort(&header);
            if (n == 0) {
                self.ended = true;
                return null;
            }
            if (n != len_size) return error.PartialFrame;
            const len = try payloadLength(options, &header);
            const payload = try self.allocator.alloc(u8, len);
            errdefer self.allocator.free(payload);
            self.reader.readSliceAll(payload) catch |err| switch (err) {
                error.EndOfStream => return error.PartialFrame,
                error.ReadFailed => return error.ReadFailed,
            };
            return .{ .payload = payload, .allocator = self.allocator };
        }
    };
}

pub const LengthPrefixedOptions = struct {
    length_type: type = u32,
    endian: std.builtin.Endian = .big,
    max_frame: usize,
    /// Whether the length value counts the header itself.
    includes_header: bool = false,
};

/// Delimiter-separated framing for line protocols. The delimiter may be
/// multi-byte (e.g. "\r\n").
pub fn Delimited(comptime options: DelimitedOptions) type {
    comptime {
        if (options.delimiter.len == 0) {
            @compileError("talon.framing.Delimited: delimiter must not be empty");
        }
    }

    return struct {
        reader: *std.Io.Reader,

        const Self = @This();

        pub fn init(reader: *std.Io.Reader) Self {
            return .{ .reader = reader };
        }

        /// Returns the next frame (delimiter stripped), or null on clean
        /// end-of-stream. Borrowed slice: valid until the next call.
        pub fn next(self: *Self) Error!?[]const u8 {
            const r = self.reader;
            var search_start: usize = 0;
            while (true) {
                const window = r.buffered();
                if (std.mem.indexOfPos(u8, window, search_start, options.delimiter)) |idx| {
                    const frame = window[0..idx];
                    if (frame.len > options.max_frame) return error.FrameTooLarge;
                    r.toss(idx + options.delimiter.len);
                    return frame;
                }
                if (window.len > options.max_frame) {
                    const required_prefix = window.len - options.max_frame;
                    var prefix_len = @min(window.len, options.delimiter.len - 1);
                    // Only a real, unfinished delimiter prefix is framing
                    // overhead; arbitrary excess bytes remain payload.
                    while (prefix_len >= required_prefix) : (prefix_len -= 1) {
                        if (std.mem.endsWith(u8, window, options.delimiter[0..prefix_len])) break;
                    }
                    if (prefix_len < required_prefix) return error.FrameTooLarge;
                }
                // Re-scan only the tail that could still contain the
                // delimiter once more bytes arrive.
                search_start = window.len -| (options.delimiter.len - 1);
                r.fillMore() catch |err| switch (err) {
                    error.EndOfStream => {
                        if (r.bufferedLen() == 0) return null;
                        return error.PartialFrame;
                    },
                    error.ReadFailed => return error.ReadFailed,
                };
            }
        }
    };
}

pub const DelimitedOptions = struct {
    delimiter: []const u8,
    max_frame: usize,
};

/// Result of one decode attempt over the buffered window.
pub const DecodeResult = union(enum) {
    /// Not enough bytes yet; accumulate more.
    need_more,
    /// A complete frame: `payload` must be a sub-slice of the input window;
    /// `consumed` bytes are removed from the stream (payload + any framing
    /// overhead).
    frame: struct {
        payload: []const u8,
        consumed: usize,
    },
};

/// Incremental accumulation template: the fallback component for custom
/// protocol state machines. `Decoder` contract:
///
///   fn decode(window: []const u8) !DecodeResult
///
/// decode() is re-invoked with a strictly longer window after `need_more`.
/// Decoder errors propagate to the caller as-is.
/// max_frame bounds the encoded bytes consumed by one frame, including framing
/// overhead; following pipelined bytes do not count toward that frame.
pub fn Accumulator(comptime Decoder: type) type {
    comptime {
        if (!std.meta.hasFn(Decoder, "decode")) {
            @compileError("talon.framing.Accumulator: Decoder type '" ++ @typeName(Decoder) ++
                "' must declare 'pub fn decode(window: []const u8) !DecodeResult'");
        }
    }

    return struct {
        reader: *std.Io.Reader,
        max_frame: usize,

        const Self = @This();

        pub fn init(reader: *std.Io.Reader, max_frame: usize) Self {
            return .{ .reader = reader, .max_frame = max_frame };
        }

        /// Returns the next decoded frame payload, or null on clean
        /// end-of-stream. Borrowed slice: valid until the next call.
        pub fn next(self: *Self) anyerror!?[]const u8 {
            const r = self.reader;
            while (true) {
                const window = r.buffered();
                switch (try Decoder.decode(window)) {
                    .frame => |f| {
                        std.debug.assert(f.consumed <= window.len);
                        if (f.consumed > self.max_frame) return error.FrameTooLarge;
                        // toss() only moves the seek cursor; the payload
                        // bytes stay intact until the next fill/rebase,
                        // which is exactly the borrow contract.
                        r.toss(f.consumed);
                        return f.payload;
                    },
                    .need_more => {
                        if (window.len > self.max_frame) return error.FrameTooLarge;
                        r.fillMore() catch |err| switch (err) {
                            error.EndOfStream => {
                                if (r.bufferedLen() == 0) return null;
                                return error.PartialFrame;
                            },
                            error.ReadFailed => return error.ReadFailed,
                        };
                    },
                }
            }
        }
    };
}

// ── Tests ────────────────────────────────────────────────────────────────

const FragmentedReader = struct {
    interface: std.Io.Reader,
    source: []const u8,
    position: usize = 0,
    fail_at: ?usize = null,
    chunk_size: usize = 3,

    fn init(source: []const u8, buffer: []u8) FragmentedReader {
        return .{ .interface = .{ .vtable = &.{ .stream = stream }, .buffer = buffer, .seek = 0, .end = 0 }, .source = source };
    }

    fn stream(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *FragmentedReader = @fieldParentPtr("interface", r);
        if (self.fail_at) |end| if (self.position >= end) return error.ReadFailed;
        if (self.position == self.source.len) return error.EndOfStream;
        const n = limit.minInt(@min(self.chunk_size, self.source.len - self.position));
        try w.writeAll(self.source[self.position..][0..n]);
        self.position += n;
        return n;
    }
};

test "OwnedLengthPrefixed: fragmented large payloads outlive subsequent reads through tiny windows" {
    const data = "\x00\x00\x10\x00" ++ (&@as([4096]u8, @splat('x'))) ++ "\x00\x00\x00\x03end" ++ "\x00\x00\x00\x00";
    for ([_]usize{ 0, 1, 7 }) |capacity| {
        var buffer: [7]u8 = undefined;
        var r = FragmentedReader.init(data, buffer[0..capacity]);
        var counted = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        var frames = OwnedLengthPrefixed(.{ .max_frame = 4096 }).init(&r.interface, counted.allocator());
        var first = (try frames.next()).?;
        defer first.deinit();
        var second = (try frames.next()).?;
        defer second.deinit();
        var empty = (try frames.next()).?;
        defer empty.deinit();
        try std.testing.expectEqualStrings(&@as([4096]u8, @splat('x')), first.payload);
        try std.testing.expectEqualStrings("end", second.payload);
        try std.testing.expectEqual(@as(usize, 0), empty.payload.len);
        try std.testing.expectEqual(@as(usize, 4099), counted.allocated_bytes);
        try std.testing.expectEqual(null, try frames.next());
        try std.testing.expectEqual(null, try frames.next());
    }
}

test "OwnedLengthPrefixed: bounds and invalid lengths are rejected before allocation" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var over = std.Io.Reader.fixed("\x00\x05abcde");
    var frames = OwnedLengthPrefixed(.{ .length_type = u16, .max_frame = 4 }).init(&over, failing.allocator());
    try std.testing.expectError(error.FrameTooLarge, frames.next());
    const seek = over.seek;
    try std.testing.expectError(error.FrameReaderFailed, frames.next());
    try std.testing.expectEqual(seek, over.seek);

    var invalid = std.Io.Reader.fixed("\x01\x00");
    var inclusive = OwnedLengthPrefixed(.{ .length_type = u16, .endian = .little, .includes_header = true, .max_frame = 4 }).init(&invalid, failing.allocator());
    try std.testing.expectError(error.InvalidFrame, inclusive.next());
    var huge_header: [16]u8 = undefined;
    std.mem.writeInt(u128, &huge_header, @as(u128, std.math.maxInt(usize)) + 1, .big);
    var huge = std.Io.Reader.fixed(&huge_header);
    var wide = OwnedLengthPrefixed(.{ .length_type = u128, .max_frame = std.math.maxInt(usize) }).init(&huge, failing.allocator());
    try std.testing.expectError(error.FrameTooLarge, wide.next());
    // The borrowed entry shares the overflow check rather than trapping on cast.
    var huge2 = std.Io.Reader.fixed(&huge_header);
    var borrowed = LengthPrefixed(.{ .length_type = u128, .max_frame = std.math.maxInt(usize) }).init(&huge2);
    try std.testing.expectError(error.FrameTooLarge, borrowed.next());
    // A representable payload length must also be checked before adding header.
    std.mem.writeInt(u128, &huge_header, std.math.maxInt(usize), .big);
    var max_reader = std.Io.Reader.fixed(&huge_header);
    var max_frame = LengthPrefixed(.{ .length_type = u128, .max_frame = std.math.maxInt(usize) }).init(&max_reader);
    try std.testing.expectError(error.FrameTooLarge, max_frame.next());
    try std.testing.expect(!failing.has_induced_failure);
}

test "OwnedLengthPrefixed: includes-header, endian and exact payload limits" {
    var r = std.Io.Reader.fixed("\x06\x00test\x02\x00");
    var frames = OwnedLengthPrefixed(.{ .length_type = u16, .endian = .little, .includes_header = true, .max_frame = 4 }).init(&r, std.testing.allocator);
    var frame = (try frames.next()).?;
    defer frame.deinit();
    try std.testing.expectEqualStrings("test", frame.payload);
    var empty = (try frames.next()).?;
    defer empty.deinit();
    try std.testing.expectEqual(@as(usize, 0), empty.payload.len);
    try std.testing.expectEqual(null, try frames.next());
    var zero_reader = std.Io.Reader.fixed("\x00\x00\x00\x01x");
    var zero = OwnedLengthPrefixed(.{ .length_type = u16, .max_frame = 0 }).init(&zero_reader, std.testing.allocator);
    var zero_frame = (try zero.next()).?;
    defer zero_frame.deinit();
    try std.testing.expectError(error.FrameTooLarge, zero.next());
}

test "OwnedLengthPrefixed: truncated header/payload, OOM and read failure are terminal and free allocations" {
    for ([_][]const u8{ "\x00", "\x00\x05hel" }) |data| {
        var r = std.Io.Reader.fixed(data);
        var counted = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        var frames = OwnedLengthPrefixed(.{ .length_type = u16, .max_frame = 5 }).init(&r, counted.allocator());
        try std.testing.expectError(error.PartialFrame, frames.next());
        try std.testing.expectEqual(counted.allocated_bytes, counted.freed_bytes);
        try std.testing.expectError(error.FrameReaderFailed, frames.next());
    }
    var r = std.Io.Reader.fixed("\x00\x05hello\x00\x01x");
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var frames = OwnedLengthPrefixed(.{ .length_type = u16, .max_frame = 5 }).init(&r, failing.allocator());
    try std.testing.expectError(error.OutOfMemory, frames.next());
    const seek = r.seek;
    try std.testing.expectError(error.FrameReaderFailed, frames.next());
    try std.testing.expectEqual(seek, r.seek);

    for ([_]usize{ 0, 1, 4 }) |fail_at| {
        var fault = FragmentedReader.init("\x00\x05hello", &.{});
        fault.fail_at = fail_at;
        var counted = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        var decoder = OwnedLengthPrefixed(.{ .length_type = u16, .max_frame = 5 }).init(&fault.interface, counted.allocator());
        try std.testing.expectError(error.ReadFailed, decoder.next());
        try std.testing.expectEqual(counted.allocated_bytes, counted.freed_bytes);
        const position = fault.position;
        try std.testing.expectError(error.FrameReaderFailed, decoder.next());
        try std.testing.expectEqual(position, fault.position);
    }
}

test "LengthPrefixed: parses consecutive frames and clean EOF" {
    const data = "\x00\x03abc" ++ "\x00\x00" ++ "\x00\x05hello";
    var r: std.Io.Reader = .fixed(data);
    var framed = LengthPrefixed(.{ .length_type = u16, .max_frame = 64 }).init(&r);

    try std.testing.expectEqualStrings("abc", (try framed.next()).?);
    try std.testing.expectEqualStrings("", (try framed.next()).?);
    try std.testing.expectEqualStrings("hello", (try framed.next()).?);
    try std.testing.expectEqual(null, try framed.next());
}

test "LengthPrefixed: little-endian and includes_header" {
    // length 7 includes the 2-byte header => 5-byte payload
    const data = "\x07\x00world";
    var r: std.Io.Reader = .fixed(data);
    var framed = LengthPrefixed(.{
        .length_type = u16,
        .endian = .little,
        .max_frame = 64,
        .includes_header = true,
    }).init(&r);

    try std.testing.expectEqualStrings("world", (try framed.next()).?);
    try std.testing.expectEqual(null, try framed.next());
}

test "LengthPrefixed: rejects oversized frame" {
    const data = "\x00\xff" ++ (&@as([255]u8, @splat('x')));
    var r: std.Io.Reader = .fixed(data);
    var framed = LengthPrefixed(.{ .length_type = u16, .max_frame = 16 }).init(&r);
    try std.testing.expectError(error.FrameTooLarge, framed.next());
}

test "LengthPrefixed: truncated stream is a PartialFrame" {
    // Reader with spare capacity but a stream that ends early: promises 5
    // payload bytes, delivers 3. (A plain .fixed reader can't model this —
    // its capacity equals the data, so the capacity check fires first.)
    const data = "\x00\x05hel";
    var backing: [64]u8 = undefined;
    @memcpy(backing[0..data.len], data);
    var r: std.Io.Reader = .fixed(&backing);
    r.end = data.len;
    var framed = LengthPrefixed(.{ .length_type = u16, .max_frame = 64 }).init(&r);
    try std.testing.expectError(error.PartialFrame, framed.next());

    var r2: std.Io.Reader = .fixed("\x00"); // truncated header
    var framed2 = LengthPrefixed(.{ .length_type = u16, .max_frame = 64 }).init(&r2);
    try std.testing.expectError(error.PartialFrame, framed2.next());
}

test "Delimited: splits lines on multi-byte delimiter, strips it" {
    const data = "PING\r\nECHO hi\r\n\r\nlast\r\n";
    var r: std.Io.Reader = .fixed(data);
    var lines = Delimited(.{ .delimiter = "\r\n", .max_frame = 64 }).init(&r);

    try std.testing.expectEqualStrings("PING", (try lines.next()).?);
    try std.testing.expectEqualStrings("ECHO hi", (try lines.next()).?);
    try std.testing.expectEqualStrings("", (try lines.next()).?);
    try std.testing.expectEqualStrings("last", (try lines.next()).?);
    try std.testing.expectEqual(null, try lines.next());
}

test "Delimited: trailing bytes without delimiter are a PartialFrame" {
    var r: std.Io.Reader = .fixed("complete\r\ndangling");
    var lines = Delimited(.{ .delimiter = "\r\n", .max_frame = 64 }).init(&r);
    try std.testing.expectEqualStrings("complete", (try lines.next()).?);
    try std.testing.expectError(error.PartialFrame, lines.next());
}

test "Delimited: line over max_frame is rejected" {
    const data = (&@as([100]u8, @splat('y'))) ++ "\r\n";
    var r: std.Io.Reader = .fixed(data);
    var lines = Delimited(.{ .delimiter = "\r\n", .max_frame = 32 }).init(&r);
    try std.testing.expectError(error.FrameTooLarge, lines.next());
}

test "Delimited: exact payload limit survives fragmented delimiter and next frame" {
    for (1..5) |chunk_size| {
        var buffer: [16]u8 = undefined;
        var reader = FragmentedReader.init("abcde\r\nxy\r\n", &buffer);
        reader.chunk_size = chunk_size;
        var frames = Delimited(.{ .delimiter = "\r\n", .max_frame = 5 }).init(&reader.interface);
        try std.testing.expectEqualStrings("abcde", (try frames.next()).?);
        try std.testing.expectEqualStrings("xy", (try frames.next()).?);
        try std.testing.expectEqual(null, try frames.next());
    }
}

test "Delimited: overlapping delimiter prefix stays outside the payload limit" {
    for (1..5) |chunk_size| {
        var buffer: [16]u8 = undefined;
        var reader = FragmentedReader.init("xxxxxabababab", &buffer);
        reader.chunk_size = chunk_size;
        var frames = Delimited(.{ .delimiter = "abab", .max_frame = 5 }).init(&reader.interface);
        try std.testing.expectEqualStrings("xxxxx", (try frames.next()).?);
        try std.testing.expectEqualStrings("", (try frames.next()).?);
        try std.testing.expectEqual(null, try frames.next());
    }
}

test "Delimited: excess payload and invalid delimiter suffix are rejected" {
    for (1..5) |chunk_size| {
        for ([_][]const u8{ "abcdef\r\n", "abcdeX", "abcde\rX" }) |source| {
            var buffer: [16]u8 = undefined;
            var reader = FragmentedReader.init(source, &buffer);
            reader.chunk_size = chunk_size;
            var frames = Delimited(.{ .delimiter = "\r\n", .max_frame = 5 }).init(&reader.interface);
            try std.testing.expectError(error.FrameTooLarge, frames.next());
        }
    }
}

test "Delimited: pending delimiter preserves EOF and read failure" {
    for (1..5) |chunk_size| {
        var buffer: [16]u8 = undefined;
        var reader = FragmentedReader.init("abcde\r", &buffer);
        reader.chunk_size = chunk_size;
        var frames = Delimited(.{ .delimiter = "\r\n", .max_frame = 5 }).init(&reader.interface);
        try std.testing.expectError(error.PartialFrame, frames.next());
    }
    var buffer: [16]u8 = undefined;
    var reader = FragmentedReader.init("abcde\r\n", &buffer);
    reader.fail_at = 6;
    var frames = Delimited(.{ .delimiter = "\r\n", .max_frame = 5 }).init(&reader.interface);
    try std.testing.expectError(error.ReadFailed, frames.next());
}

test "Delimited: zero payload limit accepts only an empty delimited frame" {
    var buffer: [4]u8 = undefined;
    var reader = FragmentedReader.init("\r\nX\r\n", &buffer);
    reader.chunk_size = 1;
    var frames = Delimited(.{ .delimiter = "\r\n", .max_frame = 0 }).init(&reader.interface);
    try std.testing.expectEqualStrings("", (try frames.next()).?);
    try std.testing.expectError(error.FrameTooLarge, frames.next());
}

// Toy decoder for Accumulator: frames are `<digit>:<payload>` where the
// single digit is the payload length (e.g. "3:abc").
const ToyDecoder = struct {
    pub fn decode(window: []const u8) !DecodeResult {
        if (window.len < 2) return .need_more;
        if (window[1] != ':') return error.BadToyFrame;
        const len = std.fmt.charToDigit(window[0], 10) catch return error.BadToyFrame;
        const total = 2 + len;
        if (window.len < total) return .need_more;
        return .{ .frame = .{ .payload = window[2..total], .consumed = total } };
    }
};

test "Accumulator: drives a custom decoder to completion" {
    var r: std.Io.Reader = .fixed("3:abc0:5:hello");
    var acc = Accumulator(ToyDecoder).init(&r, 64);

    try std.testing.expectEqualStrings("abc", (try acc.next()).?);
    try std.testing.expectEqualStrings("", (try acc.next()).?);
    try std.testing.expectEqualStrings("hello", (try acc.next()).?);
    try std.testing.expectEqual(null, try acc.next());
}

test "Accumulator: decoder errors propagate; truncated input is PartialFrame" {
    var r: std.Io.Reader = .fixed("3;abc");
    var acc = Accumulator(ToyDecoder).init(&r, 64);
    try std.testing.expectError(error.BadToyFrame, acc.next());

    var r2: std.Io.Reader = .fixed("5:ab");
    var acc2 = Accumulator(ToyDecoder).init(&r2, 64);
    try std.testing.expectError(error.PartialFrame, acc2.next());
}

test "Accumulator: complete oversized frame is rejected before consuming" {
    var reader: std.Io.Reader = .fixed("5:hello");
    var frames = Accumulator(ToyDecoder).init(&reader, 4);
    try std.testing.expectError(error.FrameTooLarge, frames.next());
    try std.testing.expectEqual(@as(usize, 0), reader.seek);
}

test "Accumulator: encoded frame limit is independent of fragmentation" {
    for (1..8) |chunk_size| {
        var buffer: [16]u8 = undefined;
        var reader = FragmentedReader.init("3:abc", &buffer);
        reader.chunk_size = chunk_size;
        var frames = Accumulator(ToyDecoder).init(&reader.interface, 4);
        try std.testing.expectError(error.FrameTooLarge, frames.next());
        try std.testing.expectEqual(@as(usize, 0), reader.interface.seek);
    }
}

test "Accumulator: exact encoded limit permits pipelined frames in a larger window" {
    var reader: std.Io.Reader = .fixed("3:abc0:1:z");
    var frames = Accumulator(ToyDecoder).init(&reader, 5);
    try std.testing.expectEqualStrings("abc", (try frames.next()).?);
    try std.testing.expectEqualStrings("", (try frames.next()).?);
    try std.testing.expectEqualStrings("z", (try frames.next()).?);
    try std.testing.expectEqual(null, try frames.next());
}

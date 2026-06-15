//! Framing toolbox (design doc §8) — the Zig answer to Netty's codec framework.
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
//! All frames are borrowed slices into the reader's buffer: valid until the
//! next `next()` call. All components carry a `max_frame` defense; a frame
//! must also fit the reader's buffer capacity (size the buffer ≥ max_frame +
//! frame header). Timeouts ride on the underlying reader (zio per-op
//! Timeout). Stream side only — datagrams are already frames (§9).

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
    const len_size = @divExact(@typeInfo(options.length_type).int.bits, 8);
    comptime {
        if (@typeInfo(options.length_type).int.signedness != .unsigned) {
            @compileError("talon.framing.LengthPrefixed: length_type must be an unsigned integer");
        }
    }

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
            const raw_len = std.mem.readInt(options.length_type, header[0..len_size], options.endian);
            var payload_len: usize = @intCast(raw_len);
            if (options.includes_header) {
                if (payload_len < len_size) return error.InvalidFrame;
                payload_len -= len_size;
            }
            if (payload_len > options.max_frame) return error.FrameTooLarge;

            const total = len_size + payload_len;
            if (total > r.buffer.len) return error.FrameTooLarge;
            const frame = r.take(total) catch |err| switch (err) {
                error.EndOfStream => return error.PartialFrame,
                error.ReadFailed => return error.ReadFailed,
            };
            return frame[len_size..];
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
                if (window.len > options.max_frame) return error.FrameTooLarge;
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
    const data = "\x00\xff" ++ ("x" ** 255);
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
    const data = ("y" ** 100) ++ "\r\n";
    var r: std.Io.Reader = .fixed(data);
    var lines = Delimited(.{ .delimiter = "\r\n", .max_frame = 32 }).init(&r);
    try std.testing.expectError(error.FrameTooLarge, lines.next());
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

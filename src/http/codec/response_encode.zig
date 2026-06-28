//! Response encoding (write path).
//!
//! The head is printed into the connection's buffered writer; a fixed-length
//! body then rides the same flush, which zio's Writer drains with a single
//! vectored syscall (buffered head + body slices via writeSplatHeader) — the
//! coalesced writeVec path without explicit iovec plumbing here.

const std = @import("std");
const zio = @import("zio");
const parser = @import("request_parser.zig");
const codec = @import("codec.zig");

pub const Status = codec.Status;

/// RFC 9110 Date header value, cached per second. One instance per
/// connection: no synchronization, and keep-alive loops amortize the
/// formatting.
pub const DateCache = struct {
    second: i64 = -1,
    buf: [29]u8 = undefined,

    pub fn get(self: *DateCache) []const u8 {
        const now_s: i64 = @intCast(@divFloor(zio.Timestamp.now(.realtime).toNanoseconds(), std.time.ns_per_s));
        if (now_s != self.second) {
            self.second = now_s;
            formatHttpDate(@intCast(now_s), &self.buf);
        }
        return &self.buf;
    }
};

const weekdays = [7][]const u8{ "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
const months = [12][]const u8{ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

/// Formats `secs` (Unix epoch) as an IMF-fixdate, e.g.
/// "Thu, 11 Jun 2026 08:30:00 GMT" — always exactly 29 bytes.
pub fn formatHttpDate(secs: u64, out: *[29]u8) void {
    const epoch_secs: std.time.epoch.EpochSeconds = .{ .secs = secs };
    const day = epoch_secs.getEpochDay();
    const year_day = day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_secs = epoch_secs.getDaySeconds();
    // 1970-01-01 was a Thursday.
    const weekday = (day.day + 4) % 7;

    var w: std.Io.Writer = .fixed(out);
    w.print("{s}, {d:0>2} {s} {d} {d:0>2}:{d:0>2}:{d:0>2} GMT", .{
        weekdays[weekday],
        month_day.day_index + 1,
        months[@intFromEnum(month_day.month) - 1],
        year_day.year,
        day_secs.getHoursIntoDay(),
        day_secs.getMinutesIntoHour(),
        day_secs.getSecondsIntoMinute(),
    }) catch unreachable; // 29 bytes by construction for years 1000-9999
}

pub const HeadOptions = struct {
    status: Status = .ok,
    extra_headers: []const parser.Header = &.{},
    content_length: ?u64 = null,
    chunked: bool = false,
    keep_alive: bool = true,
};

pub const EncodeError = error{
    /// An app-supplied header name/value contained bytes that would break
    /// framing (CR/LF, control chars). The encoder refuses to emit it,
    /// closing the response-splitting vector (CWE-113) at the framework level
    /// instead of trusting every handler to sanitize.
    InvalidHeader,
};

/// Prints the status line + standard headers + extra headers + blank line
/// into `w` (the connection's buffered writer). Does not flush.
///
/// App-supplied `extra_headers` are validated for injection first; standard
/// headers (status phrase, date) are framework-generated and trusted.
pub fn writeHead(w: *std.Io.Writer, date: *DateCache, options: HeadOptions) (std.Io.Writer.Error || EncodeError)!void {
    for (options.extra_headers) |h| {
        if (!parser.isToken(h.name) or !parser.validFieldValue(h.value)) return error.InvalidHeader;
        // Reject app-supplied framing headers: the encoder emits the framing
        // (content-length/transfer-encoding/connection) and Date from its typed
        // options/standard headers, so a duplicate here is a response-splitting
        // vector (CL+TE, second Content-Length). Symmetric with the parser's
        // ConflictingFraming rejection (CWE-113).
        if (parser.isReservedFramingHeader(h.name) or std.ascii.eqlIgnoreCase(h.name, "date"))
            return error.InvalidHeader;
    }
    const code = @intFromEnum(options.status);
    const phrase = options.status.phrase() orelse "";
    try w.print("HTTP/1.1 {d} {s}\r\ndate: {s}\r\n", .{ code, phrase, date.get() });
    if (options.content_length) |cl| {
        try w.print("content-length: {d}\r\n", .{cl});
    } else if (options.chunked) {
        try w.writeAll("transfer-encoding: chunked\r\n");
    }
    if (!options.keep_alive) {
        try w.writeAll("connection: close\r\n");
    }
    for (options.extra_headers) |h| {
        try w.print("{s}: {s}\r\n", .{ h.name, h.value });
    }
    try w.writeAll("\r\n");
}

/// Streaming chunked body writer: every drain emits one chunk
/// (`<hex>\r\n<payload>\r\n`); `finish()` emits the last-chunk terminator.
pub const ChunkedBodyWriter = struct {
    out: *std.Io.Writer,
    interface: std.Io.Writer,
    finished: bool = false,

    pub fn init(out: *std.Io.Writer, buffer: []u8) ChunkedBodyWriter {
        return .{
            .out = out,
            .interface = .{
                .vtable = &.{ .drain = drainImpl },
                .buffer = buffer,
            },
        };
    }

    /// Flushes pending bytes as a final data chunk, then writes the
    /// zero-length terminating chunk. No trailers.
    pub fn finish(self: *ChunkedBodyWriter) !void {
        std.debug.assert(!self.finished);
        try self.interface.flush();
        self.finished = true;
        try self.out.writeAll("0\r\n\r\n");
    }

    fn drainImpl(io_w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *ChunkedBodyWriter = @alignCast(@fieldParentPtr("interface", io_w));
        const buffered = io_w.buffered();

        var total: usize = buffered.len;
        for (data[0..data.len -| 1]) |slice| total += slice.len;
        if (data.len > 0) total += data[data.len - 1].len * splat;
        if (total == 0) return 0;

        try self.out.print("{x}\r\n", .{total});
        try self.out.writeAll(buffered);
        for (data[0..data.len -| 1]) |slice| try self.out.writeAll(slice);
        if (data.len > 0) {
            const last = data[data.len - 1];
            for (0..splat) |_| try self.out.writeAll(last);
        }
        try self.out.writeAll("\r\n");
        return io_w.consume(total);
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

test "formatHttpDate: known timestamps" {
    var buf: [29]u8 = undefined;
    formatHttpDate(0, &buf);
    try std.testing.expectEqualStrings("Thu, 01 Jan 1970 00:00:00 GMT", &buf);
    // 2026-06-11 08:30:00 UTC
    formatHttpDate(1781166600, &buf);
    try std.testing.expectEqualStrings("Thu, 11 Jun 2026 08:30:00 GMT", &buf);
}

test "writeHead: rejects CRLF injection in app headers" {
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    var date: DateCache = .{};
    try std.testing.expectError(error.InvalidHeader, writeHead(&w, &date, .{
        .content_length = 0,
        .extra_headers = &.{.{ .name = "x", .value = "a\r\nSet-Cookie: evil=1" }},
    }));
}

test "writeHead: rejects app-supplied framing headers (response splitting)" {
    var buf: [256]u8 = undefined;
    var date: DateCache = .{};
    const reserved = [_][]const u8{ "Content-Length", "transfer-encoding", "connection", "Date" };
    for (reserved) |name| {
        var w: std.Io.Writer = .fixed(&buf);
        try std.testing.expectError(error.InvalidHeader, writeHead(&w, &date, .{
            .content_length = 0,
            .extra_headers = &.{.{ .name = name, .value = "0" }},
        }));
    }
}

test "writeHead: fixed-length keep-alive response" {
    var buf: [512]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    var date: DateCache = .{};
    try writeHead(&w, &date, .{
        .content_length = 5,
        .extra_headers = &.{.{ .name = "x-a", .value = "b" }},
    });
    const out = w.buffered();
    try std.testing.expect(std.mem.startsWith(u8, out, "HTTP/1.1 200 OK\r\ndate: "));
    try std.testing.expect(std.mem.indexOf(u8, out, "content-length: 5\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "x-a: b\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "connection: close") == null);
    try std.testing.expect(std.mem.endsWith(u8, out, "\r\n\r\n"));
}

test "ChunkedBodyWriter: chunks and terminator" {
    var out_buf: [256]u8 = undefined;
    var out: std.Io.Writer = .fixed(&out_buf);

    var chunk_buf: [8]u8 = undefined;
    var cw = ChunkedBodyWriter.init(&out, &chunk_buf);
    try cw.interface.writeAll("hello world"); // exceeds buffer → drains as chunks
    try cw.finish();

    const written = out.buffered();
    // Exact chunking depends on buffer fill points; decode it back instead.
    var r: std.Io.Reader = .fixed(written);
    const head = makeChunkedHead();
    var bbuf: [64]u8 = undefined;
    var body = @import("body.zig").BodyReader.init(&r, &head, null, &bbuf);
    var collected: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer collected.deinit();
    _ = try body.interface.streamRemaining(&collected.writer);
    try std.testing.expectEqualStrings("hello world", collected.written());
}

fn makeChunkedHead() parser.Head {
    return .{
        .method = .POST,
        .method_raw = "POST",
        .target = "/",
        .version = .@"HTTP/1.1",
        .headers = &.{},
        .host = "h",
        .content_length = null,
        .transfer_chunked = true,
        .keep_alive = true,
        .expect_continue = false,
    };
}

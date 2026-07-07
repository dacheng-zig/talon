//! Server-Sent Events wire codec — direction-neutral, sans-io.
//!
//! `serialize` turns an `Event` into `text/event-stream` bytes (server side).
//! `EventDecoder` pulls events from a `std.Io.Reader` incrementally (client
//! side), tolerating LF, CR, and CRLF line endings, payloads split across
//! reads, a leading UTF-8 BOM, and lone meta events.
//!
//! Wire format: WHATWG HTML "Server-sent events". Only `event`/`data`/`id`/
//! `retry` fields are recognized; other fields and `:`-leading comment lines
//! are ignored. A blank line dispatches the event accumulated so far, but only
//! if it carried data.

const std = @import("std");

/// One SSE event. On the encode side every field is an input; on the decode
/// side `data` is always set (possibly empty), `event`/`id` are the sticky
/// last-seen values, and `comment`/`retry_ms` are not produced (comments are
/// ignored, retry drives reconnect state, not the event).
pub const Event = struct {
    /// Event type name; null serializes nothing (client defaults to "message").
    event: ?[]const u8 = null,
    /// Event payload. Embedded '\n' splits into multiple `data:` lines on
    /// encode and rejoins with '\n' on decode.
    data: []const u8 = "",
    /// Sticky event id (carried into `Last-Event-ID` on reconnect).
    id: ?[]const u8 = null,
    /// Reconnect hint in milliseconds.
    retry_ms: ?u32 = null,
    /// Comment / heartbeat line (`: ...`), encode side only.
    comment: ?[]const u8 = null,
};

pub const SerializeError = error{
    /// A field carried a byte that would corrupt framing: CR/LF in
    /// `event`/`id`/`comment`, CR in `data`, or NUL in `id`.
    InvalidEventField,
} || std.Io.Writer.Error;

/// Writes `ev` as one event block terminated by a blank line.
pub fn serialize(w: *std.Io.Writer, ev: Event) SerializeError!void {
    if (ev.comment) |c| {
        if (hasByte(c, '\r') or hasByte(c, '\n')) return error.InvalidEventField;
        try w.writeAll(": ");
        try w.writeAll(c);
        try w.writeByte('\n');
    }
    if (ev.event) |e| {
        if (hasByte(e, '\r') or hasByte(e, '\n')) return error.InvalidEventField;
        try w.writeAll("event: ");
        try w.writeAll(e);
        try w.writeByte('\n');
    }
    // Emit a data line whenever any field is present: the receiver drops an
    // event whose data buffer stayed empty, so an event-only or id-only block
    // needs one (possibly empty) data line to actually dispatch.
    if (ev.data.len != 0 or ev.event != null or ev.id != null) {
        if (hasByte(ev.data, '\r')) return error.InvalidEventField;
        var it = std.mem.splitScalar(u8, ev.data, '\n');
        while (it.next()) |line| {
            try w.writeAll("data: ");
            try w.writeAll(line);
            try w.writeByte('\n');
        }
    }
    if (ev.id) |id| {
        if (hasByte(id, '\r') or hasByte(id, '\n') or hasByte(id, 0)) return error.InvalidEventField;
        try w.writeAll("id: ");
        try w.writeAll(id);
        try w.writeByte('\n');
    }
    if (ev.retry_ms) |ms| try w.print("retry: {d}\n", .{ms});
    try w.writeByte('\n');
}

fn hasByte(bytes: []const u8, byte: u8) bool {
    return std.mem.indexOfScalar(u8, bytes, byte) != null;
}

fn allDigits(bytes: []const u8) bool {
    for (bytes) |c| {
        if (c < '0' or c > '9') return false;
    }
    return true;
}

pub const DecodeError = error{
    /// A single line, or the accumulated event, exceeded the working buffer.
    EventTooLarge,
    /// The stream ended mid-line (no terminator before EOF on a partial line).
    PartialEvent,
    ReadFailed,
};

/// Incremental event reader over a `std.Io.Reader`.
///
/// `scratch` accumulates one event's `data` (front) plus its `event`/`id`
/// (back); size it to the largest event you expect. Returned slices borrow
/// `scratch` and are valid until the next `next()` call. The reader's own
/// buffer must be large enough to hold a single line.
pub const EventDecoder = struct {
    reader: *std.Io.Reader,
    scratch: []u8,
    /// Sticky id: persists across events until a new `id:` overrides it, so a
    /// reconnect always carries the most recent id.
    last_id: ?[]const u8 = null,
    /// Most recent `retry:` value in milliseconds, persistent across events —
    /// the reconnect delay a client should honor. Set even by a meta-only block.
    reconnect_ms: ?u32 = null,
    bom_checked: bool = false,

    // Per-event accumulation into `scratch`: data grows up from 0, the current
    // event name grows down from the end.
    data_len: usize = 0,
    event_name: ?[]const u8 = null,
    saw_field: bool = false,

    pub fn init(reader: *std.Io.Reader, scratch: []u8) EventDecoder {
        return .{ .reader = reader, .scratch = scratch };
    }

    /// Returns the next event, or null on clean end-of-stream.
    pub fn next(self: *EventDecoder) DecodeError!?Event {
        while (true) {
            const line = (try self.nextLine()) orelse {
                // Clean EOF drops any half-accumulated event: without a
                // terminating blank line it never dispatches.
                return null;
            };

            if (line.len == 0) {
                if (!self.saw_field) continue; // stray blank line
                // A block with no data dispatches nothing, even if it carried an
                // event name; any id in it was already recorded as sticky.
                if (self.data_len == 0) {
                    self.resetEvent();
                    continue;
                }
                return self.dispatch();
            }
            if (line[0] == ':') continue; // comment

            const colon = std.mem.indexOfScalar(u8, line, ':');
            const name = if (colon) |c| line[0..c] else line;
            var value = if (colon) |c| line[c + 1 ..] else line[line.len..];
            if (value.len != 0 and value[0] == ' ') value = value[1..];

            try self.field(name, value);
        }
    }

    fn field(self: *EventDecoder, name: []const u8, value: []const u8) DecodeError!void {
        if (std.mem.eql(u8, name, "data")) {
            self.saw_field = true;
            try self.appendData(value);
        } else if (std.mem.eql(u8, name, "event")) {
            self.saw_field = true;
            self.event_name = try self.stashBack(value);
        } else if (std.mem.eql(u8, name, "id")) {
            self.saw_field = true;
            if (hasByte(value, 0)) return; // NUL in id: ignore the field
            self.last_id = try self.stashBack(value);
        } else if (std.mem.eql(u8, name, "retry")) {
            self.saw_field = true;
            // ASCII digits only (no sign); a malformed value or overflow is
            // ignored rather than saturated.
            if (value.len != 0 and allDigits(value)) {
                if (std.fmt.parseInt(u32, value, 10)) |ms| self.reconnect_ms = ms else |_| {}
            }
        }
        // Unknown field: ignore.
    }

    fn dispatch(self: *EventDecoder) Event {
        // Drop the trailing '\n' the last data line contributed.
        const data = if (self.data_len > 0) self.scratch[0 .. self.data_len - 1] else "";
        const ev: Event = .{ .data = data, .event = self.event_name, .id = self.last_id };
        self.resetEvent();
        return ev;
    }

    fn resetEvent(self: *EventDecoder) void {
        self.data_len = 0;
        self.event_name = null;
        self.saw_field = false;
        // last_id is intentionally not reset — it is sticky.
    }

    fn appendData(self: *EventDecoder, value: []const u8) DecodeError!void {
        const back = self.backUsed();
        if (self.data_len + value.len + 1 > self.scratch.len - back) return error.EventTooLarge;
        @memcpy(self.scratch[self.data_len..][0..value.len], value);
        self.data_len += value.len;
        self.scratch[self.data_len] = '\n';
        self.data_len += 1;
    }

    // Copies a single-line value to the tail of scratch and returns a borrow.
    // Only `event` and the sticky `id` live here; each new value bumps a fresh
    // copy, so `event` + `id` coexist while an older overwritten copy is wasted
    // until the next event reset.
    fn stashBack(self: *EventDecoder, value: []const u8) DecodeError![]const u8 {
        const back = self.backUsed();
        if (self.data_len + back + value.len > self.scratch.len) return error.EventTooLarge;
        const start = self.scratch.len - back - value.len;
        @memcpy(self.scratch[start..][0..value.len], value);
        return self.scratch[start..][0..value.len];
    }

    fn backUsed(self: *const EventDecoder) usize {
        var used: usize = 0;
        if (self.event_name) |e| used += e.len;
        if (self.last_id) |id| used += id.len;
        return used;
    }

    // Reads one logical line (terminator stripped), handling LF, CR, and CRLF,
    // including a CR at the end of the buffer whose LF has not arrived yet.
    // Returns a slice borrowed from the reader buffer, valid until the caller
    // copies out and advances.
    fn nextLine(self: *EventDecoder) DecodeError!?[]const u8 {
        const r = self.reader;
        try self.stripBom();
        var scan: usize = 0;
        while (true) {
            const window = r.buffered();
            var i = scan;
            while (i < window.len) : (i += 1) {
                const c = window[i];
                if (c == '\n') {
                    const line = window[0..i];
                    r.toss(i + 1);
                    return line;
                }
                if (c == '\r') {
                    if (i + 1 < window.len) {
                        const skip: usize = if (window[i + 1] == '\n') 2 else 1;
                        const line = window[0..i];
                        r.toss(i + skip);
                        return line;
                    }
                    // CR at buffer end: need one more byte to resolve CRLF.
                    break;
                }
            }
            if (window.len > self.scratch.len) return error.EventTooLarge;
            scan = window.len -| 1; // keep a trailing CR in view for CRLF join
            r.fillMore() catch |err| switch (err) {
                error.EndOfStream => {
                    if (r.bufferedLen() == 0) return null;
                    // Trailing CR with no following byte terminates the line.
                    const rest = r.buffered();
                    if (rest[rest.len - 1] == '\r') {
                        const line = rest[0 .. rest.len - 1];
                        r.toss(rest.len);
                        return line;
                    }
                    return error.PartialEvent;
                },
                error.ReadFailed => return error.ReadFailed,
            };
        }
    }

    fn stripBom(self: *EventDecoder) DecodeError!void {
        if (self.bom_checked) return;
        const r = self.reader;
        const bom = "\xEF\xBB\xBF";
        const head = r.peek(bom.len) catch |err| switch (err) {
            error.EndOfStream => {
                self.bom_checked = true;
                return;
            },
            error.ReadFailed => return error.ReadFailed,
        };
        if (std.mem.eql(u8, head, bom)) r.toss(bom.len);
        self.bom_checked = true;
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

fn serializeAlloc(ev: Event) ![]u8 {
    var buf: std.Io.Writer.Allocating = .init(testing.allocator);
    errdefer buf.deinit();
    try serialize(&buf.writer, ev);
    return buf.toOwnedSlice();
}

test "serialize: full event" {
    const out = try serializeAlloc(.{ .event = "ticker", .id = "42", .data = "hi", .retry_ms = 3000 });
    defer testing.allocator.free(out);
    try testing.expectEqualStrings("event: ticker\ndata: hi\nid: 42\nretry: 3000\n\n", out);
}

test "serialize: multi-line data splits into data: lines" {
    const out = try serializeAlloc(.{ .data = "line one\nline two" });
    defer testing.allocator.free(out);
    try testing.expectEqualStrings("data: line one\ndata: line two\n\n", out);
}

test "serialize: comment-only heartbeat" {
    const out = try serializeAlloc(.{ .comment = "ping" });
    defer testing.allocator.free(out);
    try testing.expectEqualStrings(": ping\n\n", out);
}

test "serialize: rejects framing-breaking fields" {
    try testing.expectError(error.InvalidEventField, serializeAlloc(.{ .event = "a\nb" }));
    try testing.expectError(error.InvalidEventField, serializeAlloc(.{ .id = "a\rb" }));
    try testing.expectError(error.InvalidEventField, serializeAlloc(.{ .id = "a\x00b" }));
    try testing.expectError(error.InvalidEventField, serializeAlloc(.{ .data = "a\rb" }));
}

fn expectEvent(ev: Event, event: ?[]const u8, data: []const u8, id: ?[]const u8) !void {
    try testing.expectEqualStrings(data, ev.data);
    if (event) |e| try testing.expectEqualStrings(e, ev.event.?) else try testing.expectEqual(null, ev.event);
    if (id) |i| try testing.expectEqualStrings(i, ev.id.?) else try testing.expectEqual(null, ev.id);
}

test "decode: two events, sticky id, default type" {
    var r: std.Io.Reader = .fixed("event: ticker\nid: 42\ndata: {\"p\":1}\n\ndata: line one\ndata: line two\n\n");
    var scratch: [256]u8 = undefined;
    var dec = EventDecoder.init(&r, &scratch);

    try expectEvent((try dec.next()).?, "ticker", "{\"p\":1}", "42");
    // Second event: no id/event of its own → default type, but id stays sticky.
    try expectEvent((try dec.next()).?, null, "line one\nline two", "42");
    try testing.expectEqual(null, try dec.next());
}

test "decode: CRLF and CR line endings" {
    var r: std.Io.Reader = .fixed("data: a\r\ndata: b\r\n\r\ndata: c\rdata: d\r\r");
    var scratch: [256]u8 = undefined;
    var dec = EventDecoder.init(&r, &scratch);
    try expectEvent((try dec.next()).?, null, "a\nb", null);
    try expectEvent((try dec.next()).?, null, "c\nd", null);
    try testing.expectEqual(null, try dec.next());
}

test "decode: comments, BOM, and value space stripping" {
    var r: std.Io.Reader = .fixed("\xEF\xBB\xBF: comment\ndata:no-space\ndata:  two-space\n\n");
    var scratch: [256]u8 = undefined;
    var dec = EventDecoder.init(&r, &scratch);
    // First data line has no leading space (kept), second strips exactly one.
    try expectEvent((try dec.next()).?, null, "no-space\n two-space", null);
    try testing.expectEqual(null, try dec.next());
}

test "decode: lone meta event does not dispatch; NUL id ignored" {
    var r: std.Io.Reader = .fixed("id: 1\x00bad\n\ndata: real\n\n");
    var scratch: [256]u8 = undefined;
    var dec = EventDecoder.init(&r, &scratch);
    // id has NUL → ignored; the block has no data → no dispatch; next real event
    // arrives with no sticky id set.
    try expectEvent((try dec.next()).?, null, "real", null);
    try testing.expectEqual(null, try dec.next());
}

test "decode: field name without colon has empty value" {
    var r: std.Io.Reader = .fixed("data\ndata: x\n\n");
    var scratch: [256]u8 = undefined;
    var dec = EventDecoder.init(&r, &scratch);
    try expectEvent((try dec.next()).?, null, "\nx", null);
    try testing.expectEqual(null, try dec.next());
}

test "decode: event without data does not dispatch" {
    var r: std.Io.Reader = .fixed("event: ping\n\ndata: real\n\n");
    var scratch: [64]u8 = undefined;
    var dec = EventDecoder.init(&r, &scratch);
    // The event-only block carries no data → dropped; only the data block emits.
    try expectEvent((try dec.next()).?, null, "real", null);
    try testing.expectEqual(null, try dec.next());
}

test "decode: retry surfaces as reconnect_ms, even meta-only; malformed ignored" {
    var r: std.Io.Reader = .fixed("retry: 10000\n\nretry: abc\ndata: x\n\n");
    var scratch: [64]u8 = undefined;
    var dec = EventDecoder.init(&r, &scratch);
    // Lone `retry:` block dispatches no event but sets the reconnect delay.
    try expectEvent((try dec.next()).?, null, "x", null);
    try testing.expectEqual(@as(?u32, 10000), dec.reconnect_ms); // "abc" left it unchanged
    try testing.expectEqual(null, try dec.next());
}

test "decode: event too large for scratch" {
    var r: std.Io.Reader = .fixed("data: aaaaaaaaaaaaaaaa\n\n");
    var scratch: [8]u8 = undefined;
    var dec = EventDecoder.init(&r, &scratch);
    try testing.expectError(error.EventTooLarge, dec.next());
}

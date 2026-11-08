//! Application-owned WS stop notification and engine drain cancellation.
const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");
const ws = talon.http.codec.ws;
const payload_size = 8192;
const Outcome = enum { pending, sent, closed, canceled, write_failed, unexpected };

const App = struct {
    frame_size: ?usize,
    entered: zio.ResetEvent = .init,
    stop: zio.ResetEvent = .init,
    outcomes: [2]Outcome = @splat(.pending),
    payload_alive: [2]bool = @splat(false),
    cleaned: bool = false,
    remaining_sends: usize = 99,
    remaining_bytes: usize = 99,
    peak_sends: usize = 0,

    fn send(self: *App, socket: *talon.http.ws.WebSocket, payload: []const u8, id: usize) void {
        defer self.payload_alive[id] = payload.len == payload_size and
            payload[0] == 'A' + id and payload[payload.len - 1] == 'A' + id;
        socket.writeBinary(payload) catch |err| {
            self.outcomes[id] = switch (err) {
                error.Closed => .closed,
                error.Canceled => .canceled,
                error.WriteFailed => .write_failed,
                else => .unexpected,
            };
            return;
        };
        self.outcomes[id] = .sent;
    }

    pub fn handle(self: *App, req: *talon.http.Request, _: *talon.http.Response) !void {
        var socket = try talon.http.ws.upgrade(req, .{
            .max_message_size = 128,
            .send_options = .{ .max_frame_payload = self.frame_size },
        });
        var children: zio.Group = .init;
        // Group.cancel joins under a cancellation shield. Both the stack
        // Session and request-arena payload must outlive every child.
        defer {
            children.cancel();
            const snapshot = socket.sendStats();
            self.remaining_sends = snapshot.in_flight_data_sends;
            self.remaining_bytes = snapshot.in_flight_data_bytes;
            self.peak_sends = snapshot.peak_data_sends;
            self.cleaned = true;
        }
        for (0..2) |id| {
            const payload = try req.arena.alloc(u8, payload_size);
            @memset(payload, @as(u8, @intCast('A' + id)));
            try children.spawn(send, .{ self, &socket, payload, id });
        }
        const deadline = zio.Timeout.fromSeconds(2).toDeadline();
        while (socket.sendStats().in_flight_data_sends != 2) {
            if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline.deadline.toNanoseconds()) return error.SendersDidNotEnter;
            try zio.sleep(.fromMilliseconds(1));
        }
        self.entered.set();
        try self.stop.wait(); // No implicit WS stop notification from Server.
        try socket.close(.going_away, "shutdown");
        try children.wait();
        if (try socket.read() != null) return error.UnexpectedMessage;
    }
};

const Srv = talon.http.Server(App);
const Helpers = struct {
    fn forcedPrefix(reader: *std.Io.Reader, frame_size: ?usize) !void {
        var captured: [4096]u8 = undefined;
        var length: usize = 0;
        while (true) {
            const byte = reader.takeByte() catch |err| {
                if (err == error.EndOfStream) break;
                return err;
            };
            try std.testing.expect(length < captured.len);
            captured[length] = byte;
            length += 1;
        }
        // No peer read happened after pipe-full until all writers joined.
        // Validate even a final partial header, rather than treating any
        // header EOF as a valid prefix that could hide fallback bytes.
        var offset: usize = 0;
        var total: usize = 0;
        var identity: ?u8 = null;
        while (offset < length) {
            var expected: []const u8 = "\x82\x7e\x20\x00";
            if (frame_size != null) expected = if (total == 0) "\x02\x40" else "\x00\x40";
            const head_len = @min(expected.len, length - offset);
            try std.testing.expectEqualSlices(u8, expected[0..head_len], captured[offset..][0..head_len]);
            offset += head_len;
            if (head_len != expected.len) break;
            const body_len = @min(frame_size orelse payload_size, length - offset);
            for (captured[offset..][0..body_len]) |byte| {
                if (identity == null) {
                    try std.testing.expect(byte == 'A' or byte == 'B');
                    identity = byte;
                }
                try std.testing.expectEqual(identity.?, byte);
                total += 1;
            }
            offset += body_len;
        }
        try std.testing.expect(total > 0 and total < payload_size);
    }

    fn serve(server: *Srv, listener: *talon.MemoryListener, done: *zio.ResetEvent) !void {
        defer done.set();
        try server.serve(listener);
    }

    fn client(listener: *talon.MemoryListener, server: *Srv, app: *App, done: *zio.ResetEvent, notify: bool) !void {
        defer server.shutdown();
        const conn = try listener.connect();
        defer conn.close();
        var write_buffer: [256]u8 = undefined;
        var writer = conn.writer(&write_buffer);
        var read_buffer: [128]u8 = undefined;
        var reader = conn.reader(&read_buffer);
        const r = &reader.interface;
        try writer.interface.writeAll("GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" ++
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n");
        try writer.interface.flush();
        try std.testing.expectEqualStrings("HTTP/1.1 101 Switching Protocols\r\n", try r.takeDelimiterInclusive('\n'));
        var accepted = false;
        while (true) {
            const line = try r.takeDelimiterInclusive('\n');
            if (std.mem.eql(u8, line, "\r\n")) break;
            if (std.mem.indexOf(u8, line, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") != null) accepted = true;
        }
        try std.testing.expect(accepted);
        try app.entered.timedWait(.fromSeconds(2));
        const deadline = zio.Timeout.fromSeconds(2).toDeadline();
        while (true) {
            conn.recv.mutex.lockUncancelable();
            const full = conn.recv.count == conn.recv.buffer.len;
            conn.recv.mutex.unlock();
            if (full) break;
            if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline.deadline.toNanoseconds()) return error.WriterDidNotBlock;
            try zio.sleep(.fromMilliseconds(1));
        }
        server.shutdown();
        if (notify) {
            app.stop.set(); // Explicit application notification, independent of HTTP.
        } else {
            // Keep the peer open and paused: only the engine drain can stop
            // the parent and its two children. Resume after all tasks ended.
            try done.timedWait(.fromSeconds(2));
            return forcedPrefix(r, app.frame_size);
        }
        var current: ?u8 = null;
        var message_bytes: usize = 0;
        var completed: [2]usize = @splat(0);
        while (true) {
            const header = try ws.readFrameHeader(r);
            try std.testing.expect(!header.masked and header.payload_len > 0);
            if (header.opcode == .close) {
                try std.testing.expect(header.fin and header.payload_len == 10);
                var close_payload: [10]u8 = undefined;
                try r.readSliceAll(&close_payload);
                try std.testing.expectEqualSlices(u8, "\x03\xe9shutdown", &close_payload);
                try ws.writeFrame(&writer.interface, .close, true, "\x03\xe9", .{ 1, 2, 3, 4 });
                try writer.interface.flush();
                try std.testing.expectError(error.EndOfStream, r.takeByte());
                break;
            }
            try std.testing.expectEqual(if (current == null) ws.Opcode.binary else .continuation, header.opcode);
            try std.testing.expect(header.payload_len <= (app.frame_size orelse payload_size));
            var offset: usize = 0;
            while (offset < header.payload_len) : (offset += 1) {
                const byte = try r.takeByte();
                if (current == null) {
                    try std.testing.expect(byte == 'A' or byte == 'B');
                    current = byte;
                }
                try std.testing.expectEqual(current.?, byte);
                message_bytes += 1;
                try std.testing.expect(message_bytes <= payload_size);
            }
            try std.testing.expectEqual(message_bytes == payload_size, header.fin);
            if (header.fin) {
                completed[current.? - 'A'] += 1;
                try std.testing.expect(completed[current.? - 'A'] <= 1);
                current = null;
                message_bytes = 0;
            }
        }
        try done.timedWait(.fromSeconds(2));
        for (completed, app.outcomes) |count, outcome| {
            try std.testing.expectEqual(@as(usize, if (outcome == .sent) 1 else 0), count);
        }
    }
};

fn runShutdown(notify: bool) !void {
    for ([_]u8{ 1, 2 }) |executors| {
        const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
        defer rt.deinit();
        for ([_]?usize{ null, 64 }) |frame_size| {
            var budget = talon.core.AllocationBudget.init(std.testing.allocator, 1024 * 1024);
            defer budget.deinit();
            {
                var listener = try talon.MemoryListener.init(budget.allocator(), .{ .pipe_buffer_size = 256, .backlog = 1 });
                defer listener.deinit();
                var app: App = .{ .frame_size = frame_size };
                var server = try Srv.init(budget.allocator(), &app, .{
                    .limits = .{ .drain_timeout = if (notify) .fromSeconds(2) else .fromMilliseconds(50), .max_connections = 1 },
                });
                defer server.deinit();
                var done: zio.ResetEvent = .init;
                var tasks: zio.Group = .init;
                defer tasks.cancel();
                try tasks.spawn(Helpers.serve, .{ &server, &listener, &done });
                try tasks.spawn(Helpers.client, .{ &listener, &server, &app, &done, notify });
                try tasks.wait();
                try std.testing.expect(!tasks.hasFailed());
                try std.testing.expect(app.cleaned and app.payload_alive[0] and app.payload_alive[1]);
                try std.testing.expectEqual(@as(usize, 0), app.remaining_sends);
                try std.testing.expectEqual(@as(usize, 0), app.remaining_bytes);
                try std.testing.expectEqual(@as(usize, 2), app.peak_sends);
                for (app.outcomes) |outcome| {
                    if (notify) {
                        try std.testing.expect(outcome == .sent or outcome == .closed);
                    } else {
                        try std.testing.expect(outcome == .canceled or outcome == .write_failed);
                    }
                }
                const stats = server.stats();
                try std.testing.expectEqual(@as(u64, 1), stats.accepted);
                try std.testing.expectEqual(stats.accepted, stats.finished_tasks);
                try std.testing.expectEqual(@as(usize, 0), stats.active_tasks);
                try std.testing.expectEqual(@as(u64, if (notify) 0 else 1), stats.canceled_returns);
                try std.testing.expectEqual(@as(u64, 0), stats.connection_errors);
                try std.testing.expectEqual(@as(u64, if (notify) 0 else 1), stats.forced_drains);
                try std.testing.expectEqual(stats.forced_drains, stats.drain_timeouts);
            }
            try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
            try std.testing.expectEqual(@as(usize, 0), budget.stats().denied_requests);
        }
    }
}

test "WS shutdown: application notification closes and joins active senders before arena release" {
    try runShutdown(true);
}

test "WS shutdown: engine drain cancels parent and children without HTTP fallback or leaked sends" {
    try runShutdown(false);
}

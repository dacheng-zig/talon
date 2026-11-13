const std = @import("std");
const zio = @import("zio");
const core = @import("talon").core;
const ws = @import("talon").http.codec.ws;

fn expectFrame(reader: *std.Io.Reader, role: ws.Role, opcode: ws.Opcode, payload: []const u8) !void {
    const h = try ws.readFrameHeader(reader);
    try std.testing.expect(h.fin and h.masked == (role == .client));
    try std.testing.expectEqual(opcode, h.opcode);
    try std.testing.expectEqual(payload.len, h.payload_len);
    var buf: [2500]u8 = undefined;
    try reader.readSliceAll(buf[0..payload.len]);
    if (h.masked) ws.unmask(buf[0..payload.len], h.mask_key);
    try std.testing.expectEqualSlices(u8, payload, buf[0..payload.len]);
}

fn awaitFull(pipe: *core.Pipe) !void {
    for (0..1000) |_| {
        pipe.mutex.lockUncancelable();
        const full = pipe.count == pipe.buffer.len;
        pipe.mutex.unlock();
        if (full) return;
        try zio.sleep(.fromMilliseconds(1));
    }
    return error.WriterDidNotBlock;
}

test "WS: outbound arguments are rejected without output and valid boundary payloads survive in both roles" {
    inline for (.{ ws.Role.server, ws.Role.client }) |role| {
        var out: [1024]u8 = undefined;
        var writer: std.Io.Writer = .fixed(&out);
        var reader: std.Io.Reader = .fixed("");
        var msg_buf: [16]u8 = undefined;
        var s: ws.Session(role) = .{ .reader = &reader, .writer = &writer, .msg_buf = &msg_buf };
        const invalid_texts = [_][]const u8{ "\xff", "\xc0\xaf", "\xed\xa0\x80", "\xf4\x90\x80\x80", "\xe4\xb8" };
        for (invalid_texts) |text| {
            try std.testing.expectError(error.InvalidUtf8, s.writeText(text));
            try std.testing.expectError(error.InvalidUtf8, s.close(.normal, text));
        }
        var ping: [126]u8 = @splat(0xff);
        try std.testing.expectError(error.ControlPayloadTooLarge, s.writePing(&ping));
        for ([_]u16{ 0, 999, 1004, 1005, 1006, 1015, 1016, 2999, 5000, 65535 }) |code| {
            try std.testing.expectError(error.InvalidCloseCode, s.close(@fromBackingInt(@intCast(code)), ""));
        }
        var reason: [124]u8 = @splat('a');
        try std.testing.expectError(error.CloseReasonTooLarge, s.close(.normal, &reason));
        // A valid UTF-8 reason whose last character would be split by the old truncation.
        @memcpy(reason[121..], "中");
        try std.testing.expectError(error.CloseReasonTooLarge, s.close(.normal, &reason));
        try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
        try std.testing.expect(!s.write_failed and !s.close_sent);

        try s.writeText("中文🙂");
        try s.writeBinary("\xff");
        try s.writePing(ping[0..125]);
        @memset(reason[0..120], 'a');
        @memcpy(reason[120..123], "中");
        try s.close(@fromBackingInt(@intCast(3000)), reason[0..123]);
        const written = writer.buffered().len;
        try s.close(.normal, "");
        try std.testing.expectError(error.InvalidUtf8, s.writeText("\xff"));
        try std.testing.expectError(error.ControlPayloadTooLarge, s.writePing(&ping));
        try std.testing.expectError(error.InvalidCloseCode, s.close(@fromBackingInt(@intCast(1005)), ""));
        try std.testing.expectError(error.Closed, s.writeText("valid"));
        try std.testing.expectEqual(written, writer.buffered().len);
        var wire: std.Io.Reader = .fixed(writer.buffered());
        try expectFrame(&wire, role, .text, "中文🙂");
        try expectFrame(&wire, role, .binary, "\xff");
        try expectFrame(&wire, role, .ping, ping[0..125]);
        var close_payload: [125]u8 = undefined;
        std.mem.writeInt(u16, close_payload[0..2], 3000, .big);
        @memcpy(close_payload[2..], reason[0..123]);
        try expectFrame(&wire, role, .close, &close_payload);
        try std.testing.expectEqual(@as(usize, 0), wire.buffered().len);
    }
}

test "WS: valid close code boundaries and argument errors before failed writer state" {
    inline for (.{ ws.Role.server, ws.Role.client }) |role| {
        for ([_]u16{ 1000, 1003, 1007, 1011, 1012, 1013, 1014, 3000, 4999 }) |code| {
            var out: [32]u8 = undefined;
            var writer: std.Io.Writer = .fixed(&out);
            var reader: std.Io.Reader = .fixed("");
            var msg_buf: [1]u8 = undefined;
            var s: ws.Session(role) = .{ .reader = &reader, .writer = &writer, .msg_buf = &msg_buf };
            try s.close(@fromBackingInt(@intCast(code)), "");
            var wire: std.Io.Reader = .fixed(writer.buffered());
            var payload: [2]u8 = undefined;
            std.mem.writeInt(u16, &payload, code, .big);
            try expectFrame(&wire, role, .close, &payload);
        }
        var writer: std.Io.Writer = .fixed(&.{});
        var reader: std.Io.Reader = .fixed("");
        var msg_buf: [1]u8 = undefined;
        var s: ws.Session(role) = .{ .reader = &reader, .writer = &writer, .msg_buf = &msg_buf };
        try std.testing.expectError(error.WriteFailed, s.writeBinary("payload"));
        try std.testing.expectError(error.InvalidUtf8, s.writeText("\xff"));
        try std.testing.expectError(error.InvalidCloseCode, s.close(@fromBackingInt(@intCast(1006)), ""));
        try std.testing.expectError(error.WriteFailed, s.close(.normal, ""));
    }
}

test "WS: concurrent data, automatic pong and close preserve frames under backpressure in both roles" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    inline for (.{ ws.Role.server, ws.Role.client }) |role| {
        const S = ws.Session(role);
        var storage: [16]u8 = undefined;
        var pipe = core.Pipe.init(&storage);
        var wb: [8]u8 = undefined;
        var rb: [64]u8 = undefined;
        var writer = core.PipeWriter.init(&pipe, &wb);
        writer.setTimeout(.fromSeconds(2));
        var peer = core.PipeReader.init(&pipe, &rb);
        peer.setTimeout(.fromSeconds(2));
        var input: [64]u8 = undefined;
        var wire: std.Io.Writer = .fixed(&input);
        const mask: ?[4]u8 = if (role == .server) .{ 1, 2, 3, 4 } else null;
        try ws.writeFrame(&wire, .ping, true, "p", mask);
        try ws.writeFrame(&wire, .text, true, "in", mask);
        var reader: std.Io.Reader = .fixed(wire.buffered());
        var msg_buf: [32]u8 = undefined;
        var session: S = .{ .reader = &reader, .writer = &writer.interface, .msg_buf = &msg_buf };
        var done_a: zio.ResetEvent = .init;
        var done_b: zio.ResetEvent = .init;
        var done_read: zio.ResetEvent = .init;
        const F = struct {
            fn send(s: *S, char: u8, done: *zio.ResetEvent) !void {
                defer done.set();
                var payload: [2500]u8 = undefined;
                @memset(&payload, char);
                try s.writeBinary(&payload);
            }
            fn read(s: *S, done: *zio.ResetEvent) !void {
                defer done.set();
                const msg = (try s.read()).?;
                try std.testing.expectEqualStrings("in", msg.text);
            }
            fn close(s: *S, da: *zio.ResetEvent, db: *zio.ResetEvent, dr: *zio.ResetEvent) !void {
                try da.wait();
                try db.wait();
                try dr.wait();
                try s.close(.normal, "");
            }
            fn consume(r: *std.Io.Reader) !void {
                var seen_a = false;
                var seen_b = false;
                var seen_pong = false;
                for (0..3) |_| {
                    const h = try ws.readFrameHeader(r);
                    try std.testing.expect(h.fin and h.masked == (role == .client));
                    var payload: [2500]u8 = undefined;
                    const n: usize = @intCast(h.payload_len);
                    try std.testing.expect(n <= payload.len);
                    try r.readSliceAll(payload[0..n]);
                    if (h.masked) ws.unmask(payload[0..n], h.mask_key);
                    if (h.opcode == .pong) {
                        try std.testing.expect(!seen_pong);
                        seen_pong = true;
                        try std.testing.expectEqualStrings("p", payload[0..n]);
                    } else {
                        try std.testing.expectEqual(ws.Opcode.binary, h.opcode);
                        try std.testing.expectEqual(@as(usize, 2500), n);
                        const char = payload[0];
                        try std.testing.expect(char == 'a' or char == 'b');
                        for (payload) |byte| try std.testing.expectEqual(char, byte);
                        if (char == 'a') {
                            try std.testing.expect(!seen_a);
                            seen_a = true;
                        } else {
                            try std.testing.expect(!seen_b);
                            seen_b = true;
                        }
                    }
                }
                try std.testing.expect(seen_a and seen_b and seen_pong);
                try expectFrame(r, role, .close, &.{ 0x03, 0xe8 });
            }
        };
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.send, .{ &session, 'a', &done_a });
        try awaitFull(&pipe); // first writer is parked inside a partial frame
        try group.spawn(F.send, .{ &session, 'b', &done_b });
        try group.spawn(F.read, .{ &session, &done_read });
        try group.spawn(F.close, .{ &session, &done_a, &done_b, &done_read });
        try group.spawn(F.close, .{ &session, &done_a, &done_b, &done_read });
        try group.spawn(F.consume, .{&peer.interface});
        try group.wait();
        try std.testing.expect(!group.hasFailed());
        try std.testing.expectEqual(@as(usize, 0), pipe.count);
        try std.testing.expectError(error.Closed, session.writeText("late"));
        try std.testing.expectError(error.Closed, session.writePing("late"));
    }
}

test "WS: canceling a queued sender leaves the active frame and later sends intact" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    var storage: [16]u8 = undefined;
    var pipe = core.Pipe.init(&storage);
    var writer = core.PipeWriter.init(&pipe, &.{});
    writer.setTimeout(.fromSeconds(2));
    var rb: [64]u8 = undefined;
    var peer = core.PipeReader.init(&pipe, &rb);
    peer.setTimeout(.fromSeconds(2));
    var reader: std.Io.Reader = .fixed("");
    var session: ws.Session(.server) = .{ .reader = &reader, .writer = &writer.interface, .msg_buf = &.{} };
    const F = struct {
        fn first(s: *ws.Session(.server)) !void {
            try s.writeBinary(&(@as([2500]u8, @splat('a'))));
        }
        fn canceled(s: *ws.Session(.server)) !void {
            try std.testing.expectError(error.Canceled, s.writeText("must not appear"));
        }
        fn consume(r: *std.Io.Reader) !void {
            try expectFrame(r, .server, .binary, &(@as([2500]u8, @splat('a'))));
            try expectFrame(r, .server, .text, "later");
        }
    };
    var first = try zio.spawn(F.first, .{&session});
    defer first.cancel();
    try awaitFull(&pipe);
    var queued = try zio.spawn(F.canceled, .{&session});
    defer queued.cancel();
    var waiting = false;
    for (0..1000) |_| {
        waiting = session.write_mutex.queue.hasWaiters();
        if (waiting) break;
        try zio.sleep(.fromMilliseconds(1));
    }
    try std.testing.expect(waiting);
    queued.cancel();
    try queued.join();
    var consumer = try zio.spawn(F.consume, .{&peer.interface});
    defer consumer.cancel();
    try first.join();
    try session.writeText("later");
    try consumer.join();
}

test "WS: cancellation during frame output prevents all subsequent frame writes" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var storage: [16]u8 = undefined;
    var pipe = core.Pipe.init(&storage);
    var writer = core.PipeWriter.init(&pipe, &.{});
    var reader: std.Io.Reader = .fixed("");
    var session: ws.Session(.server) = .{ .reader = &reader, .writer = &writer.interface, .msg_buf = &.{} };
    const F = struct {
        fn send(s: *ws.Session(.server)) !void {
            try std.testing.expectError(error.WriteFailed, s.writeBinary(&(@as([2500]u8, @splat('x')))));
        }
    };
    var send = try zio.spawn(F.send, .{&session});
    defer send.cancel();
    try awaitFull(&pipe);
    send.cancel();
    try send.join();
    try std.testing.expectError(error.WriteFailed, session.writeText("later"));
    try std.testing.expectError(error.WriteFailed, session.writePing("later"));
    try std.testing.expectError(error.WriteFailed, session.close(.normal, ""));
    try std.testing.expectEqual(storage.len, pipe.count);
}

test "WS: local close still allows automatic pong and peer close without another close frame" {
    var input: [32]u8 = undefined;
    var wire: std.Io.Writer = .fixed(&input);
    try ws.writeFrame(&wire, .ping, true, "p", .{ 1, 2, 3, 4 });
    try ws.writeFrame(&wire, .close, true, &.{ 0x03, 0xe8 }, .{ 1, 2, 3, 4 });
    var reader: std.Io.Reader = .fixed(wire.buffered());
    var output: [32]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&output);
    var session: ws.Session(.server) = .{ .reader = &reader, .writer = &writer, .msg_buf = &.{} };
    try session.close(.normal, "");
    try std.testing.expectEqual(null, try session.read());
    try std.testing.expectEqual(null, try session.read());
    var peer: std.Io.Reader = .fixed(writer.buffered());
    try expectFrame(&peer, .server, .close, &.{ 0x03, 0xe8 });
    try expectFrame(&peer, .server, .pong, "p");
    try std.testing.expectEqual(@as(usize, 0), peer.bufferedLen());
}

test "WS: concurrent read rejects before consuming input and a failed read cannot be retried" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    var storage: [16]u8 = undefined;
    var pipe = core.Pipe.init(&storage);
    var rb: [16]u8 = undefined;
    var reader = core.PipeReader.init(&pipe, &rb);
    var out: [32]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&out);
    var session: ws.Session(.server) = .{ .reader = &reader.interface, .writer = &writer, .msg_buf = &.{} };
    const F = struct {
        fn read(s: *ws.Session(.server)) !void {
            try std.testing.expectError(error.ReadFailed, s.read());
        }
    };
    var read = try zio.spawn(F.read, .{&session});
    defer read.cancel();
    for (0..1000) |_| {
        if (session.read_busy.load(.acquire)) break;
        try zio.sleep(.fromMilliseconds(1));
    }
    try std.testing.expectError(error.ConcurrentRead, session.read());
    read.cancel();
    try read.join();
    try std.testing.expectEqual(null, try session.read());
    try std.testing.expectEqual(@as(usize, 0), pipe.count);
}

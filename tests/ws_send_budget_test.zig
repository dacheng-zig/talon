const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");
const ws = talon.http.codec.ws;

fn awaitFull(pipe: *talon.core.Pipe) !void {
    const deadline = zio.Timeout.fromSeconds(2).toDeadline();
    while (true) {
        pipe.mutex.lockUncancelable();
        const full = pipe.count == pipe.buffer.len;
        pipe.mutex.unlock();
        if (full) return;
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline.deadline.toNanoseconds()) return error.WriterDidNotBlock;
        try zio.sleep(.fromMilliseconds(1));
    }
}

fn awaitSends(session: anytype, count: usize) !void {
    const deadline = zio.Timeout.fromSeconds(2).toDeadline();
    while (session.sendStats().in_flight_data_sends != count) {
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline.deadline.toNanoseconds()) return error.SendersDidNotEnter;
        try zio.sleep(.fromMilliseconds(1));
    }
}

fn takeFrame(reader: *std.Io.Reader, role: ws.Role, payload: []u8) !ws.FrameHeader {
    const h = try ws.readFrameHeader(reader);
    try std.testing.expect(h.fin and h.masked == (role == .client));
    try std.testing.expect(h.payload_len <= payload.len);
    const n: usize = @intCast(h.payload_len);
    try reader.readSliceAll(payload[0..n]);
    if (h.masked) ws.unmask(payload[0..n], h.mask_key);
    return h;
}

test "WS send admission: independent limits, canceled waiters and automatic pong under slow output" {
    inline for (.{ ws.Role.server, ws.Role.client }) |role| {
        const S = ws.Session(role);
        for ([_]u8{ 1, 2 }) |executors| {
            for ([_]ws.SendLimits{
                .{ .max_data_sends = 2 },
                .{ .max_data_bytes = 2510 },
            }) |limits| {
                const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
                defer rt.deinit();
                var storage: [16]u8 = undefined;
                var pipe = talon.core.Pipe.init(&storage);
                var writer = talon.core.PipeWriter.init(&pipe, &.{});
                writer.setTimeout(.fromSeconds(3));
                var rb: [64]u8 = undefined;
                var peer = talon.core.PipeReader.init(&pipe, &rb);
                peer.setTimeout(.fromSeconds(3));
                var input: [32]u8 = undefined;
                var wire: std.Io.Writer = .fixed(&input);
                const mask: ?[4]u8 = if (role == .server) .{ 1, 2, 3, 4 } else null;
                try ws.writeFrame(&wire, .ping, true, "p", mask);
                try ws.writeFrame(&wire, .text, true, "in", mask);
                var reader: std.Io.Reader = .fixed(wire.buffered());
                var msg_buf: [8]u8 = undefined;
                var session: S = .{ .reader = &reader, .writer = &writer.interface, .msg_buf = &msg_buf, .send_limits = limits };
                const F = struct {
                    fn first(s: *S) !void {
                        try s.writeBinary(&(@as([2500]u8, @splat('a'))));
                    }
                    fn canceled(s: *S) !void {
                        try std.testing.expectError(error.Canceled, s.writeText("abcdefghij"));
                    }
                    fn replacement(s: *S) !void {
                        try s.writeText("0123456789");
                    }
                    fn read(s: *S) !void {
                        try std.testing.expectEqualStrings("in", (try s.read()).?.text);
                    }
                    fn consume(r: *std.Io.Reader) !void {
                        var payload: [2500]u8 = undefined;
                        const first_frame = try takeFrame(r, role, &payload);
                        try std.testing.expectEqual(ws.Opcode.binary, first_frame.opcode);
                        try std.testing.expectEqualSlices(u8, &(@as([2500]u8, @splat('a'))), &payload);
                        var text = false;
                        var pong = false;
                        for (0..2) |_| {
                            const h = try takeFrame(r, role, &payload);
                            if (h.opcode == .text) {
                                try std.testing.expect(!text);
                                text = true;
                                try std.testing.expectEqualStrings("0123456789", payload[0..@intCast(h.payload_len)]);
                            } else {
                                try std.testing.expectEqual(ws.Opcode.pong, h.opcode);
                                try std.testing.expect(!pong);
                                pong = true;
                                try std.testing.expectEqualStrings("p", payload[0..@intCast(h.payload_len)]);
                            }
                        }
                        try std.testing.expect(text and pong);
                    }
                };
                var first = try zio.spawn(F.first, .{&session});
                defer first.cancel();
                try awaitFull(&pipe);
                var queued = try zio.spawn(F.canceled, .{&session});
                defer queued.cancel();
                try awaitSends(&session, 2);
                const before = session.sendStats();
                try std.testing.expectEqual(@as(usize, 2510), before.in_flight_data_bytes);
                try std.testing.expectError(error.SendLimitExceeded, session.writeBinary("x"));
                try std.testing.expectError(error.InvalidUtf8, session.writeText("\xff"));
                try std.testing.expectEqual(@as(u64, 1), session.sendStats().rejected_data_sends);
                queued.cancel();
                try queued.join();
                const canceled = session.sendStats();
                try std.testing.expectEqual(@as(usize, 1), canceled.in_flight_data_sends);
                try std.testing.expectEqual(@as(usize, 2500), canceled.in_flight_data_bytes);
                var replacement = try zio.spawn(F.replacement, .{&session});
                defer replacement.cancel();
                try awaitSends(&session, 2);
                var read = try zio.spawn(F.read, .{&session});
                defer read.cancel();
                var consumer = try zio.spawn(F.consume, .{&peer.interface});
                defer consumer.cancel();
                try first.join();
                try replacement.join();
                try read.join();
                try consumer.join();
                const done = session.sendStats();
                try std.testing.expectEqual(@as(usize, 0), done.in_flight_data_sends);
                try std.testing.expectEqual(@as(usize, 0), done.in_flight_data_bytes);
                try std.testing.expectEqual(@as(usize, 2), done.peak_data_sends);
                try std.testing.expectEqual(@as(usize, 2510), done.peak_data_bytes);
                try std.testing.expectEqual(@as(usize, 0), pipe.count);
            }
        }
    }
}

test "WS send admission: racing producers cannot exceed either reservation and join to zero" {
    inline for (.{ ws.Role.server, ws.Role.client }) |role| {
        const S = ws.Session(role);
        for ([_]u8{ 1, 2 }) |executors| {
            const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
            defer rt.deinit();
            var storage: [16]u8 = undefined;
            var pipe = talon.core.Pipe.init(&storage);
            var writer = talon.core.PipeWriter.init(&pipe, &.{});
            writer.setTimeout(.fromSeconds(3));
            var rb: [32]u8 = undefined;
            var peer = talon.core.PipeReader.init(&pipe, &rb);
            peer.setTimeout(.fromSeconds(3));
            var reader: std.Io.Reader = .fixed("");
            var session: S = .{ .reader = &reader, .writer = &writer.interface, .msg_buf = &.{}, .send_limits = .{ .max_data_sends = 4, .max_data_bytes = 400 } };
            const F = struct {
                fn first(s: *S) !void {
                    try s.writeBinary(&(@as([100]u8, @splat('z'))));
                }
                fn attempt(s: *S) !void {
                    s.writeBinary(&(@as([100]u8, @splat('z')))) catch |err| switch (err) {
                        error.SendLimitExceeded => return,
                        else => return err,
                    };
                }
                fn consume(r: *std.Io.Reader) !void {
                    var payload: [100]u8 = undefined;
                    for (0..4) |_| {
                        const h = try takeFrame(r, role, &payload);
                        try std.testing.expectEqual(ws.Opcode.binary, h.opcode);
                        try std.testing.expectEqual(@as(u64, 100), h.payload_len);
                        try std.testing.expectEqualSlices(u8, &(@as([100]u8, @splat('z'))), &payload);
                    }
                }
            };
            var group: zio.Group = .init;
            defer group.cancel();
            try group.spawn(F.first, .{&session});
            try awaitFull(&pipe);
            for (0..20) |_| try group.spawn(F.attempt, .{&session});
            const deadline = zio.Timeout.fromSeconds(2).toDeadline();
            while (session.sendStats().rejected_data_sends != 17) {
                if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline.deadline.toNanoseconds()) return error.RejectionsDidNotFinish;
                try zio.sleep(.fromMilliseconds(1));
            }
            for (0..100) |_| {
                const s = session.sendStats();
                try std.testing.expectEqual(@as(usize, 4), s.in_flight_data_sends);
                try std.testing.expectEqual(@as(usize, 400), s.in_flight_data_bytes);
                try std.testing.expectEqual(@as(usize, 4), s.peak_data_sends);
                try std.testing.expectEqual(@as(usize, 400), s.peak_data_bytes);
            }
            try group.spawn(F.consume, .{&peer.interface});
            try group.wait();
            try std.testing.expect(!group.hasFailed());
            try std.testing.expectEqual(@as(usize, 0), session.sendStats().in_flight_data_sends);
            try std.testing.expectEqual(@as(usize, 0), session.sendStats().in_flight_data_bytes);
            try std.testing.expectEqual(@as(usize, 0), pipe.count);
        }
    }
}

test "WS send admission: partial output cancellation releases budget but preserves terminal write failure" {
    for ([_]u8{ 1, 2 }) |executors| {
        const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
        defer rt.deinit();
        var storage: [16]u8 = undefined;
        var pipe = talon.core.Pipe.init(&storage);
        var writer = talon.core.PipeWriter.init(&pipe, &.{});
        var reader: std.Io.Reader = .fixed("");
        var s: ws.Session(.server) = .{ .reader = &reader, .writer = &writer.interface, .msg_buf = &.{}, .send_limits = .{ .max_data_sends = 1, .max_data_bytes = 2500 } };
        const F = struct {
            fn send(session: *ws.Session(.server)) !void {
                try std.testing.expectError(error.WriteFailed, session.writeBinary(&(@as([2500]u8, @splat('x')))));
            }
        };
        var sender = try zio.spawn(F.send, .{&s});
        defer sender.cancel();
        try awaitFull(&pipe);
        try std.testing.expectEqual(@as(usize, 2500), s.sendStats().in_flight_data_bytes);
        sender.cancel();
        try sender.join();
        try std.testing.expectEqual(@as(usize, 0), s.sendStats().in_flight_data_sends);
        try std.testing.expectEqual(@as(usize, 0), s.sendStats().in_flight_data_bytes);
        try std.testing.expectError(error.WriteFailed, s.writeText("later"));
        try std.testing.expectEqual(@as(usize, 0), s.sendStats().in_flight_data_sends);
        try std.testing.expectEqual(@as(usize, 0), s.sendStats().in_flight_data_bytes);
        try std.testing.expectEqual(@as(usize, 16), pipe.count);
    }
}

test "WS send admission: zero bytes allows empty data while zero count still allows controls" {
    inline for (.{ ws.Role.server, ws.Role.client }) |role| {
        const rt = try zio.Runtime.init(std.testing.allocator, .{});
        defer rt.deinit();
        var reader: std.Io.Reader = .fixed("");
        var output: [32]u8 = undefined;
        var writer: std.Io.Writer = .fixed(&output);
        var s: ws.Session(role) = .{ .reader = &reader, .writer = &writer, .msg_buf = &.{}, .send_limits = .{ .max_data_bytes = 0 } };
        try std.testing.expectError(error.SendLimitExceeded, s.writeText("x"));
        try s.writeBinary("");
        s.send_limits.max_data_sends = 0; // no operations are running
        try std.testing.expectError(error.SendLimitExceeded, s.writeBinary(""));
        try s.writePing("p");
        try s.close(.normal, "");
        var wire: std.Io.Reader = .fixed(writer.buffered());
        var payload: [4]u8 = undefined;
        try std.testing.expectEqual(ws.Opcode.binary, (try takeFrame(&wire, role, &payload)).opcode);
        try std.testing.expectEqual(ws.Opcode.ping, (try takeFrame(&wire, role, &payload)).opcode);
        try std.testing.expectEqual(ws.Opcode.close, (try takeFrame(&wire, role, &payload)).opcode);
        try std.testing.expectEqual(@as(usize, 0), wire.bufferedLen());
        try std.testing.expectEqual(@as(usize, 1), s.sendStats().peak_data_sends);
        try std.testing.expectEqual(@as(usize, 0), s.sendStats().peak_data_bytes);
        try std.testing.expectEqual(@as(u64, 2), s.sendStats().rejected_data_sends);
        // Admission precedes the writer state; a permitted empty call reaches
        // Closed and must still give its slot back.
        try std.testing.expectError(error.SendLimitExceeded, s.writeBinary(""));
        s.send_limits.max_data_sends = null;
        try std.testing.expectError(error.Closed, s.writeBinary(""));
        try std.testing.expectEqual(@as(usize, 0), s.sendStats().in_flight_data_sends);
        try std.testing.expectEqual(@as(usize, 0), s.sendStats().in_flight_data_bytes);
    }
}

test "WS send admission: invalid arguments precede cancellation and canceled calls do not reserve" {
    const S = ws.Session(.server);
    const F = struct {
        fn attempt(s: *S, entered: *zio.ResetEvent, hold: *zio.ResetEvent) !void {
            entered.set();
            try std.testing.expectError(error.Canceled, hold.wait());
            // wait acknowledged the cancellation; restore it so the Session
            // call actually has a pending cancellation to check.
            zio.recancel();
            try std.testing.expectError(error.InvalidUtf8, s.writeText("\xff"));
            try std.testing.expectError(error.Canceled, s.writeBinary("x"));
            const snapshot = s.sendStats();
            try std.testing.expectEqual(@as(usize, 0), snapshot.peak_data_sends);
            try std.testing.expectEqual(@as(u64, 0), snapshot.rejected_data_sends);
        }
    };
    for ([_]u8{ 1, 2 }) |executors| {
        const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
        defer rt.deinit();
        var reader: std.Io.Reader = .fixed("");
        var output: [32]u8 = undefined;
        var writer: std.Io.Writer = .fixed(&output);
        var s: S = .{ .reader = &reader, .writer = &writer, .msg_buf = &.{}, .send_limits = .{ .max_data_sends = 0 } };
        var entered: zio.ResetEvent = .init;
        var hold: zio.ResetEvent = .init;
        var task = try zio.spawn(F.attempt, .{ &s, &entered, &hold });
        defer task.cancel();
        try entered.wait();
        task.cancel();
        try task.join();
        try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
    }
}

test "WS send admission: server upgrade and client spec enforce the same limits through complete echo" {
    const App = struct {
        ended: zio.ResetEvent = .init,
        final: ws.SendStats = .{},
        pub fn handle(self: *@This(), req: *talon.http.Request, _: *talon.http.Response) !void {
            var socket = try talon.http.ws.upgrade(req, .{ .send_limits = .{ .max_data_sends = 1, .max_data_bytes = 3 } });
            defer self.ended.set();
            try std.testing.expectError(error.SendLimitExceeded, socket.writeBinary("forbidden"));
            while (try socket.read()) |message| try socket.writeText(message.text);
            self.final = socket.sendStats();
        }
    };
    const Server = talon.http.Server(App);
    const F = struct {
        fn serve(server: *Server, listener: *talon.MemoryListener) !void {
            try server.serve(listener);
        }
        fn client(server: *Server, listener: *talon.MemoryListener, app: *App) !void {
            defer server.shutdown();
            const Client = talon.http.client.Client(talon.http.client.MemoryConnector);
            var c = Client.init(std.testing.allocator, .{ .listener = listener }, .{});
            defer c.deinit();
            var socket = try c.webSocket(.{
                .origin = .{ .host = "memory", .port = 80 },
                .send_limits = .{ .max_data_sends = 1, .max_data_bytes = 3 },
            });
            defer socket.deinit();
            try std.testing.expectError(error.SendLimitExceeded, socket.writeText("abcd"));
            try socket.writeText("abc");
            try std.testing.expectEqualStrings("abc", (try socket.read()).?.text);
            try socket.close(.normal, "");
            try std.testing.expectEqual(null, try socket.read());
            try app.ended.timedWait(.fromSeconds(2));
            for ([_]ws.SendStats{ app.final, socket.sendStats() }) |s| {
                try std.testing.expectEqual(@as(usize, 0), s.in_flight_data_sends);
                try std.testing.expectEqual(@as(usize, 0), s.in_flight_data_bytes);
                try std.testing.expectEqual(@as(usize, 1), s.peak_data_sends);
                try std.testing.expectEqual(@as(usize, 3), s.peak_data_bytes);
                try std.testing.expectEqual(@as(u64, 1), s.rejected_data_sends);
            }
        }
    };
    for ([_]u8{ 1, 2 }) |executors| {
        const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
        defer rt.deinit();
        var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
        defer listener.deinit();
        var app: App = .{};
        var server = try Server.init(std.testing.allocator, &app, .{});
        defer server.deinit();
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.serve, .{ &server, &listener });
        try group.spawn(F.client, .{ &server, &listener, &app });
        try group.wait();
        try std.testing.expect(!group.hasFailed());
    }
}

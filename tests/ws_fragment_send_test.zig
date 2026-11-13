const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");
const ws = talon.http.codec.ws;

fn takeFrame(reader: *std.Io.Reader, role: ws.Role, payload: []u8) !ws.FrameHeader {
    const h = try ws.readFrameHeader(reader);
    try std.testing.expect(h.masked == (role == .client));
    try std.testing.expect(h.payload_len <= payload.len);
    const n: usize = @intCast(h.payload_len);
    try reader.readSliceAll(payload[0..n]);
    if (h.masked) ws.unmask(payload[0..n], h.mask_key);
    return h;
}

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

test "WS fragments: exact FIN/opcode/mask, empty and UTF-8 split across frame sizes" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    inline for (.{ ws.Role.server, ws.Role.client }) |role| {
        for ([_]?usize{ null, 1, 3, 10, 125, 126, std.math.maxInt(usize) }) |size| {
            var output: [256]u8 = undefined;
            var writer: std.Io.Writer = .fixed(&output);
            var input: std.Io.Reader = .fixed("");
            var session: ws.Session(role) = .{ .reader = &input, .writer = &writer, .msg_buf = &.{}, .send_options = .{ .max_frame_payload = size } };
            const text = "中文🙂";
            try session.writeText(text);
            try session.writeBinary("");
            try session.writePing("control"); // may exceed the data frame size
            var reader: std.Io.Reader = .fixed(writer.buffered());
            var payload: [32]u8 = undefined;
            var offset: usize = 0;
            while (offset < text.len) {
                const h = try takeFrame(&reader, role, &payload);
                const n = @min(size orelse text.len, text.len - offset);
                try std.testing.expectEqual(@as(u64, n), h.payload_len);
                try std.testing.expectEqual(if (offset == 0) ws.Opcode.text else .continuation, h.opcode);
                try std.testing.expectEqual(offset + n == text.len, h.fin);
                try std.testing.expectEqualSlices(u8, text[offset..][0..n], payload[0..n]);
                offset += n;
            }
            const empty = try takeFrame(&reader, role, &payload);
            try std.testing.expect(empty.fin and empty.opcode == .binary and empty.payload_len == 0);
            const control = try takeFrame(&reader, role, &payload);
            try std.testing.expect(control.fin and control.opcode == .ping);
            try std.testing.expectEqualStrings("control", payload[0..@intCast(control.payload_len)]);
            try std.testing.expectEqual(@as(usize, 0), reader.bufferedLen());
            try std.testing.expectEqual(@as(usize, 0), session.sendStats().in_flight_data_sends);
        }
    }
}

test "WS fragments: slow output lets ping and automatic pong interrupt ordered data messages" {
    inline for (.{ ws.Role.server, ws.Role.client }) |role| {
        const S = ws.Session(role);
        for ([_]u8{ 1, 2 }) |executors| {
            const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
            defer rt.deinit();
            var storage: [16]u8 = undefined;
            var pipe = talon.core.Pipe.init(&storage);
            var wb: [8]u8 = undefined;
            var writer = talon.core.PipeWriter.init(&pipe, &wb);
            writer.setTimeout(.fromSeconds(3));
            var rb: [32]u8 = undefined;
            var peer = talon.core.PipeReader.init(&pipe, &rb);
            peer.setTimeout(.fromSeconds(3));
            var inbound: [32]u8 = undefined;
            var iw: std.Io.Writer = .fixed(&inbound);
            const mask: ?[4]u8 = if (role == .server) .{ 1, 2, 3, 4 } else null;
            try ws.writeFrame(&iw, .ping, true, "auto", mask);
            try ws.writeFrame(&iw, .text, true, "in", mask);
            var reader: std.Io.Reader = .fixed(iw.buffered());
            var msg_buf: [8]u8 = undefined;
            var session: S = .{ .reader = &reader, .writer = &writer.interface, .msg_buf = &msg_buf, .send_options = .{ .max_frame_payload = 64 }, .send_limits = .{ .max_data_sends = 2 } };
            var entered_ping: zio.ResetEvent = .init;
            var entered_read: zio.ResetEvent = .init;
            const F = struct {
                fn first(s: *S) !void {
                    try s.writeBinary(&(@as([2500]u8, @splat('a'))));
                }
                fn canceled(s: *S) !void {
                    try std.testing.expectError(error.Canceled, s.writeText("canceled"));
                }
                fn second(s: *S) !void {
                    try s.writeText("中文🙂");
                }
                fn ping(s: *S, entered: *zio.ResetEvent) !void {
                    entered.set();
                    try s.writePing("explicit");
                }
                fn read(s: *S, entered: *zio.ResetEvent) !void {
                    entered.set();
                    try std.testing.expectEqualStrings("in", (try s.read()).?.text);
                }
                fn consume(r: *std.Io.Reader) !void {
                    var payload: [64]u8 = undefined;
                    var assembled: usize = 0;
                    var messages: usize = 0;
                    var saw_ping = false;
                    var saw_pong = false;
                    while (messages < 2 or !saw_ping or !saw_pong) {
                        const h = try takeFrame(r, role, &payload);
                        const n: usize = @intCast(h.payload_len);
                        if (h.opcode.isControl()) {
                            // Both controls arrive before the large message ends.
                            try std.testing.expect(messages == 0 and assembled < 2500 and h.fin);
                            if (h.opcode == .ping) {
                                try std.testing.expect(!saw_ping);
                                saw_ping = true;
                                try std.testing.expectEqualStrings("explicit", payload[0..n]);
                            } else {
                                try std.testing.expect(h.opcode == .pong and !saw_pong);
                                saw_pong = true;
                                try std.testing.expectEqualStrings("auto", payload[0..n]);
                            }
                        } else if (messages == 0) {
                            try std.testing.expectEqual(if (assembled == 0) ws.Opcode.binary else .continuation, h.opcode);
                            try std.testing.expect(n <= 64);
                            for (payload[0..n]) |b| try std.testing.expectEqual(@as(u8, 'a'), b);
                            assembled += n;
                            if (h.fin) {
                                try std.testing.expectEqual(@as(usize, 2500), assembled);
                                messages += 1;
                            }
                        } else {
                            try std.testing.expect(h.fin and h.opcode == .text);
                            try std.testing.expectEqualStrings("中文🙂", payload[0..n]);
                            messages += 1;
                        }
                    }
                }
            };
            var first = try zio.spawn(F.first, .{&session});
            defer first.cancel();
            try awaitFull(&pipe);
            var canceled = try zio.spawn(F.canceled, .{&session});
            defer canceled.cancel();
            try awaitSends(&session, 2);
            canceled.cancel();
            try canceled.join(); // no bytes and no poisoned unfinished message
            var second = try zio.spawn(F.second, .{&session});
            defer second.cancel();
            try awaitSends(&session, 2);
            var ping = try zio.spawn(F.ping, .{ &session, &entered_ping });
            defer ping.cancel();
            var read = try zio.spawn(F.read, .{ &session, &entered_read });
            defer read.cancel();
            try entered_ping.wait();
            try entered_read.wait();
            var consumer = try zio.spawn(F.consume, .{&peer.interface});
            defer consumer.cancel();
            try first.join();
            try second.join();
            try ping.join();
            try read.join();
            try consumer.join();
            try std.testing.expectEqual(@as(usize, 0), pipe.count);
            try std.testing.expectEqual(@as(usize, 0), session.sendStats().in_flight_data_sends);
            try std.testing.expectEqual(@as(usize, 0), session.sendStats().in_flight_data_bytes);
        }
    }
}

test "WS fragments: cancellation immediately after a complete non-final flush is terminal" {
    // Deliver a real pending cancellation after flush has succeeded. This
    // distinguishes between-frame cancellation from a partial-frame I/O error.
    const Recorder = struct {
        interface: std.Io.Writer,
        output: [256]u8 = undefined,
        used: usize = 0,
        flushed: zio.ResetEvent = .init,
        hold: zio.ResetEvent = .init,
        fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *@This() = @alignCast(@fieldParentPtr("interface", w));
            std.debug.assert(data.len == 0 or (data.len == 1 and data[0].len == 0 and splat == 1));
            const n = w.buffered().len;
            @memcpy(self.output[self.used..][0..n], w.buffered());
            self.used += n;
            w.end = 0;
            self.flushed.set();
            self.hold.wait() catch {
                // Output has succeeded. Restore the cancellation acknowledged
                // here so the next between-frame yield must observe it.
                zio.recancel();
            };
            return 0;
        }
    };
    inline for (.{ ws.Role.server, ws.Role.client }) |role| {
        for ([_]u8{ 1, 2 }) |executors| {
            const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
            defer rt.deinit();
            var buffer: [128]u8 = undefined;
            var output: Recorder = .{ .interface = .{ .vtable = &.{ .drain = Recorder.drain }, .buffer = &buffer } };
            var input: std.Io.Reader = .fixed("");
            const S = ws.Session(role);
            var session: S = .{ .reader = &input, .writer = &output.interface, .msg_buf = &.{}, .send_options = .{ .max_frame_payload = 64 } };
            const F = struct {
                fn send(s: *S) !void {
                    try std.testing.expectError(error.Canceled, s.writeBinary(&(@as([140]u8, @splat('x')))));
                }
            };
            var sender = try zio.spawn(F.send, .{&session});
            defer sender.cancel();
            try output.flushed.timedWait(.fromSeconds(2));
            sender.cancel();
            try sender.join();
            var wire: std.Io.Reader = .fixed(output.output[0..output.used]);
            var payload: [64]u8 = undefined;
            const first = try takeFrame(&wire, role, &payload);
            try std.testing.expect(first.opcode == .binary and !first.fin and first.payload_len == 64);
            try std.testing.expectEqual(@as(usize, 0), wire.bufferedLen());
            try std.testing.expectError(error.WriteFailed, session.writeText("later"));
            try std.testing.expectError(error.WriteFailed, session.close(.normal, ""));
            try std.testing.expectEqual(@as(usize, 0), session.sendStats().in_flight_data_bytes);
        }
    }
}

test "WS fragments: a partial first frame fails terminally and releases admission" {
    for ([_]u8{ 1, 2 }) |executors| {
        const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
        defer rt.deinit();
        var storage: [16]u8 = undefined;
        var pipe = talon.core.Pipe.init(&storage);
        var writer = talon.core.PipeWriter.init(&pipe, &.{});
        var input: std.Io.Reader = .fixed("");
        const S = ws.Session(.server);
        var session: S = .{ .reader = &input, .writer = &writer.interface, .msg_buf = &.{}, .send_options = .{ .max_frame_payload = 64 } };
        const F = struct {
            fn send(s: *S) !void {
                try std.testing.expectError(error.WriteFailed, s.writeBinary(&(@as([140]u8, @splat('a')))));
            }
        };
        var sender = try zio.spawn(F.send, .{&session});
        defer sender.cancel();
        try awaitFull(&pipe);
        sender.cancel();
        try sender.join();
        try std.testing.expectError(error.WriteFailed, session.writePing("p"));
        try std.testing.expectEqual(@as(usize, 16), pipe.count);
        try std.testing.expectEqual(@as(usize, 0), session.sendStats().in_flight_data_bytes);
    }
}

test "WS fragments: invalid configuration emits nothing and does not reserve" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var output: [32]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&output);
    var reader: std.Io.Reader = .fixed("");
    var session: ws.Session(.server) = .{ .reader = &reader, .writer = &writer, .msg_buf = &.{}, .send_options = .{ .max_frame_payload = 0 } };
    try std.testing.expectError(error.InvalidUtf8, session.writeText("\xff"));
    try std.testing.expectError(error.InvalidFrameSize, session.writeBinary(""));
    try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
    try std.testing.expectEqual(@as(usize, 0), session.sendStats().peak_data_sends);
    session.send_options.max_frame_payload = 1;
    try session.writeBinary("x");
}

test "WS fragments: close interrupts at a frame boundary without poisoning automatic pong" {
    inline for (.{ ws.Role.server, ws.Role.client }) |role| {
        const S = ws.Session(role);
        for ([_]u8{ 1, 2 }) |executors| {
            const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
            defer rt.deinit();
            var storage: [16]u8 = undefined;
            var pipe = talon.core.Pipe.init(&storage);
            var writer = talon.core.PipeWriter.init(&pipe, &.{});
            writer.setTimeout(.fromSeconds(3));
            var rb: [64]u8 = undefined;
            var peer = talon.core.PipeReader.init(&pipe, &rb);
            peer.setTimeout(.fromSeconds(3));
            var inbound: [32]u8 = undefined;
            var iw: std.Io.Writer = .fixed(&inbound);
            const mask: ?[4]u8 = if (role == .server) .{ 1, 2, 3, 4 } else null;
            try ws.writeFrame(&iw, .ping, true, "p", mask);
            try ws.writeFrame(&iw, .text, true, "in", mask);
            var reader: std.Io.Reader = .fixed(iw.buffered());
            var msg_buf: [4]u8 = undefined;
            var session: S = .{ .reader = &reader, .writer = &writer.interface, .msg_buf = &msg_buf, .send_options = .{ .max_frame_payload = 64 } };
            var entered: zio.ResetEvent = .init;
            const F = struct {
                fn send(s: *S) !void {
                    try std.testing.expectError(error.Closed, s.writeBinary(&(@as([2500]u8, @splat('a')))));
                }
                fn close(s: *S, ready: *zio.ResetEvent) !void {
                    ready.set();
                    try s.close(.normal, "");
                }
                fn consume(r: *std.Io.Reader) !void {
                    var payload: [64]u8 = undefined;
                    var bytes: usize = 0;
                    while (true) {
                        const h = try takeFrame(r, role, &payload);
                        if (h.opcode == .close) {
                            try std.testing.expect(h.fin and h.payload_len == 2);
                            try std.testing.expectEqual(@as(u16, 1000), std.mem.readInt(u16, payload[0..2], .big));
                            try std.testing.expect(bytes > 0 and bytes < 2500);
                            return;
                        }
                        try std.testing.expectEqual(if (bytes == 0) ws.Opcode.binary else .continuation, h.opcode);
                        try std.testing.expect(!h.fin);
                        bytes += @intCast(h.payload_len);
                    }
                }
            };
            var sender = try zio.spawn(F.send, .{&session});
            defer sender.cancel();
            try awaitFull(&pipe);
            var closer = try zio.spawn(F.close, .{ &session, &entered });
            defer closer.cancel();
            try entered.wait();
            var consumer = try zio.spawn(F.consume, .{&peer.interface});
            defer consumer.cancel();
            try sender.join();
            try closer.join();
            try consumer.join();
            try std.testing.expectEqual(@as(usize, 0), pipe.count);
            try std.testing.expectError(error.Closed, session.writeBinary("later"));
            try std.testing.expectError(error.Closed, session.writePing("later"));
            try session.close(.normal, "");
            try std.testing.expectEqualStrings("in", (try session.read()).?.text);
            var payload: [4]u8 = undefined;
            const pong = try takeFrame(&peer.interface, role, &payload);
            try std.testing.expect(pong.fin and pong.opcode == .pong and pong.payload_len == 1);
            try std.testing.expectEqual(@as(u8, 'p'), payload[0]);
            try std.testing.expectEqual(@as(usize, 0), pipe.count);
            try std.testing.expectEqual(@as(usize, 0), session.sendStats().in_flight_data_sends);
        }
    }
}

test "WS fragments: client and upgrade validate before side effects and round-trip fragmented UTF-8" {
    const App = struct {
        ended: zio.ResetEvent = .init,
        pub fn handle(self: *@This(), req: *talon.http.Request, _: *talon.http.Response) !void {
            defer self.ended.set();
            try std.testing.expectError(error.InvalidFrameSize, talon.http.ws.upgrade(req, .{ .send_options = .{ .max_frame_payload = 0 } }));
            try std.testing.expect(!req.upgrade.taken.*);
            var socket = try talon.http.ws.upgrade(req, .{ .send_options = .{ .max_frame_payload = 3 } });
            try std.testing.expectEqual(@as(?usize, 3), socket.send_options.max_frame_payload);
            while (try socket.read()) |message| try socket.writeText(message.text);
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
            const origin: talon.http.client.Origin = .{ .host = "memory", .port = 80 };
            try std.testing.expectError(error.InvalidFrameSize, c.webSocket(.{ .origin = origin, .send_options = .{ .max_frame_payload = 0 } }));
            try std.testing.expectEqual(@as(u64, 0), c.pool.stats().created);
            var socket = try c.webSocket(.{ .origin = origin, .send_options = .{ .max_frame_payload = 1 } });
            defer socket.deinit();
            try std.testing.expectEqual(@as(?usize, 1), socket.session.send_options.max_frame_payload);
            try socket.writeText("中文🙂");
            try std.testing.expectEqualStrings("中文🙂", (try socket.read()).?.text);
            try socket.close(.normal, "");
            try std.testing.expectEqual(null, try socket.read());
            try app.ended.timedWait(.fromSeconds(2));
            try std.testing.expectEqual(@as(usize, 0), socket.sendStats().in_flight_data_bytes);
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

test "WS fragments: controlled slow-output comparison measures data preceding a queued ping" {
    inline for (.{ ws.Role.server, ws.Role.client }) |role| {
        const S = ws.Session(role);
        for ([_]u8{ 1, 2 }) |executors| {
            for ([_]?usize{ null, 64 }) |size| {
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
                var session: S = .{ .reader = &reader, .writer = &writer.interface, .msg_buf = &.{}, .send_options = .{ .max_frame_payload = size } };
                var entered: zio.ResetEvent = .init;
                var preceding: usize = 0;
                const F = struct {
                    fn send(s: *S) !void {
                        try s.writeBinary(&(@as([2500]u8, @splat('a'))));
                    }
                    fn ping(s: *S, ready: *zio.ResetEvent) !void {
                        ready.set();
                        try s.writePing("p");
                    }
                    fn consume(r: *std.Io.Reader, observed: *usize) !void {
                        var bytes: usize = 0;
                        var final = false;
                        var saw_ping = false;
                        var payload: [2500]u8 = undefined;
                        while (!final or !saw_ping) {
                            const h = try takeFrame(r, role, &payload);
                            if (h.opcode == .ping) {
                                try std.testing.expect(!saw_ping and h.fin and h.payload_len == 1);
                                try std.testing.expectEqual(@as(u8, 'p'), payload[0]);
                                saw_ping = true;
                                observed.* = bytes;
                            } else {
                                try std.testing.expect(!final);
                                try std.testing.expectEqual(if (bytes == 0) ws.Opcode.binary else .continuation, h.opcode);
                                for (payload[0..@intCast(h.payload_len)]) |b| try std.testing.expectEqual(@as(u8, 'a'), b);
                                bytes += @intCast(h.payload_len);
                                final = h.fin;
                            }
                        }
                        try std.testing.expectEqual(@as(usize, 2500), bytes);
                    }
                };
                var sender = try zio.spawn(F.send, .{&session});
                defer sender.cancel();
                try awaitFull(&pipe);
                var ping = try zio.spawn(F.ping, .{ &session, &entered });
                defer ping.cancel();
                try entered.wait();
                var consumer = try zio.spawn(F.consume, .{ &peer.interface, &preceding });
                defer consumer.cancel();
                try sender.join();
                try ping.join();
                try consumer.join();
                if (size != null) {
                    try std.testing.expect(preceding >= 64 and preceding < 2500);
                } else {
                    try std.testing.expectEqual(@as(usize, 2500), preceding);
                }
                try std.testing.expectEqual(@as(usize, 0), pipe.count);
            }
        }
    }
}

test "WS fragments: loopback TCP upgrades and echoes masked fragments across extended lengths" {
    const App = struct {
        pub fn handle(_: *@This(), req: *talon.http.Request, _: *talon.http.Response) !void {
            var socket = try talon.http.ws.upgrade(req, .{ .send_options = .{ .max_frame_payload = 125 }, .read_timeout = .fromSeconds(3) });
            while (try socket.read()) |message| try socket.writeBinary(message.binary);
        }
    };
    const Server = talon.http.Server(App);
    const F = struct {
        fn serve(server: *Server, listener: *talon.TcpListener) !void {
            try server.serve(listener);
        }
        fn client(server: *Server, port: u16) !void {
            defer server.shutdown();
            const Client = talon.http.client.Client(talon.http.client.TcpConnector);
            var c = Client.init(std.testing.allocator, .{}, .{});
            defer c.deinit();
            var socket = try c.webSocket(.{
                .origin = .{ .host = "127.0.0.1", .port = port },
                .send_options = .{ .max_frame_payload = 126 },
                .read_timeout = .fromSeconds(3),
            });
            defer socket.deinit();
            var payload: [4097]u8 = undefined;
            var prng = std.Random.DefaultPrng.init(0x12345678);
            prng.random().bytes(&payload);
            try socket.writeBinary(&payload);
            try std.testing.expectEqualSlices(u8, &payload, (try socket.read()).?.binary);
            try socket.close(.normal, "");
            try std.testing.expectEqual(null, try socket.read());
            try std.testing.expectEqual(@as(usize, 0), socket.sendStats().in_flight_data_bytes);
        }
    };
    for ([_]u8{ 1, 2 }) |executors| {
        const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
        defer rt.deinit();
        var app: App = .{};
        var server = try Server.init(std.testing.allocator, &app, .{});
        defer server.deinit();
        var listener = try talon.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.serve, .{ &server, &listener });
        try group.spawn(F.client, .{ &server, listener.server.socket.address.ip.getPort() });
        try group.wait();
        try std.testing.expect(!group.hasFailed());
        try std.testing.expectEqual(@as(u64, 1), server.stats().finished_tasks);
        try std.testing.expectEqual(@as(u32, 0), server.stats().active_tasks);
    }
}

test "WS fragments: cancellation waiting behind a control frame joins cleanup before releasing the message" {
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
            var input: std.Io.Reader = .fixed("");
            var session: S = .{ .reader = &input, .writer = &writer.interface, .msg_buf = &.{}, .send_options = .{ .max_frame_payload = 64 } };
            var entered: zio.ResetEvent = .init;
            var done: zio.ResetEvent = .init;
            const F = struct {
                fn send(s: *S, ended: *zio.ResetEvent) anyerror!void {
                    defer ended.set();
                    try std.testing.expectError(error.Canceled, s.writeBinary(&(@as([2500]u8, @splat('a')))));
                }
                fn ping(s: *S, ready: *zio.ResetEvent) !void {
                    ready.set();
                    try s.writePing(&(@as([125]u8, @splat('p'))));
                }
                fn cancel(sender: *zio.JoinHandle(anyerror!void), requested: *zio.ResetEvent) void {
                    requested.set();
                    sender.cancel(); // requests cancellation and also joins
                }
            };
            var sender = try zio.spawn(F.send, .{ &session, &done });
            defer sender.cancel();
            try awaitFull(&pipe);
            var ping = try zio.spawn(F.ping, .{ &session, &entered });
            defer ping.cancel();
            try entered.wait();
            // Stop after the control header, keeping its large body blocked.
            var bytes: usize = 0;
            var control: ws.FrameHeader = undefined;
            var payload: [125]u8 = undefined;
            while (true) {
                const h = try ws.readFrameHeader(&peer.interface);
                try std.testing.expect(h.masked == (role == .client));
                if (h.opcode == .ping) {
                    control = h;
                    break;
                }
                try std.testing.expect(!h.fin and h.payload_len <= 64);
                try peer.interface.readSliceAll(payload[0..@intCast(h.payload_len)]);
                bytes += @intCast(h.payload_len);
            }
            try std.testing.expect(bytes > 0 and bytes < 2500);
            try awaitFull(&pipe);
            var requested: zio.ResetEvent = .init;
            var canceller = try zio.spawn(F.cancel, .{ &sender, &requested });
            defer canceller.cancel();
            try requested.wait();
            // Cleanup must acquire the frame lock before ending the message.
            try std.testing.expectError(error.Timeout, done.timedWait(.fromMilliseconds(10)));
            try std.testing.expectEqual(@as(usize, 2500), session.sendStats().in_flight_data_bytes);
            try std.testing.expect(control.fin and control.payload_len == 125);
            try peer.interface.readSliceAll(&payload);
            if (control.masked) ws.unmask(&payload, control.mask_key);
            try std.testing.expectEqualSlices(u8, &(@as([125]u8, @splat('p'))), &payload);
            try ping.join();
            canceller.join();
            try sender.join();
            try std.testing.expectError(error.WriteFailed, session.writeBinary("later"));
            try std.testing.expectEqual(@as(usize, 0), session.sendStats().in_flight_data_bytes);
            try std.testing.expectEqual(@as(usize, 0), pipe.count);
        }
    }
}

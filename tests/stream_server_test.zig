//! Integration tests for talon.StreamServer: the protocol-agnostic engine
//! serving real connections over MemoryListener, exercising graceful shutdown,
//! drain cancellation, and connection hijacking through the public API.

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const StreamServer = talon.StreamServer;
const MemoryListener = talon.MemoryListener;

const TestApp = struct {
    requests: u32 = 0,
};

test "StreamServer: custom framing uses independent small and large buffer capacities" {
    try std.testing.expect(!@hasField(talon.core.Limits, "max_header_size"));
    try std.testing.expect(!@hasField(talon.core.Connection(talon.core.MemoryConnection), "arena"));
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    const Frames = talon.core.framing.LengthPrefixed(.{ .max_frame = 32 * 1024 });
    const Proto = struct {
        pub fn serve(conn: anytype, app: *TestApp) !void {
            var frames = Frames.init(conn.reader());
            const payload = (try frames.next()).?;
            app.requests += 1;
            try conn.writer().writeInt(u32, @intCast(payload.len), .big);
            try conn.writer().writeAll(payload);
            try conn.writer().flush();
        }
    };
    const Srv = StreamServer(Proto, TestApp);
    for ([_]usize{ 256, 32 * 1024 + 4 }) |read_size| {
        const write_size: usize = if (read_size == 256) 64 * 1024 else 37;
        var listener = try MemoryListener.init(std.testing.allocator, .{});
        defer listener.deinit();
        var app: TestApp = .{};
        var server = try Srv.init(std.testing.allocator, &app, .{ .limits = .{
            .read_buffer_size = read_size,
            .write_buffer_size = write_size,
        } });
        defer server.deinit();
        const F = struct {
            fn serve(s: *Srv, l: *MemoryListener) !void {
                try s.serve(l);
            }
            fn client(l: *MemoryListener, s: *Srv, payload_len: usize) !void {
                defer s.shutdown();
                const raw = try l.connect();
                defer raw.close();
                var writer = raw.writer(&.{});
                writer.setTimeout(.fromSeconds(2));
                try writer.interface.writeInt(u32, @intCast(payload_len), .big);
                const fragment = [_]u8{'x'} ** 997;
                var left = payload_len;
                while (left != 0) {
                    const n = @min(left, fragment.len);
                    try writer.interface.writeAll(fragment[0..n]);
                    left -= n;
                }
                var buf: [1024]u8 = undefined;
                var reader = raw.reader(&buf);
                reader.setTimeout(.fromSeconds(2));
                try std.testing.expectEqual(payload_len, try reader.interface.takeInt(u32, .big));
                for (0..payload_len) |_| try std.testing.expectEqual(@as(u8, 'x'), try reader.interface.takeByte());
            }
        };
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.serve, .{ &server, &listener });
        try group.spawn(F.client, .{ &listener, &server, read_size - 4 });
        try group.wait();
        try std.testing.expect(!group.hasFailed());
        try std.testing.expectEqual(@as(u32, 1), app.requests);
    }
}

test "StreamServer: invalid and changed resource limits fail before accepting" {
    var app: TestApp = .{};
    const Srv = StreamServer(LineEchoProto, TestApp);
    for ([_]talon.core.Limits{
        .{ .read_buffer_size = 0 },
        .{ .write_buffer_size = 0 },
    }) |limits| try std.testing.expectError(error.InvalidBufferSize, Srv.init(std.testing.allocator, &app, .{ .limits = limits }));
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    server.limits.read_buffer_size += 1;
    try std.testing.expectError(error.ResourceLimitsChanged, server.serve(&listener));
    server.limits.read_buffer_size -= 1;
    server.limits.write_buffer_size += 1;
    try std.testing.expectError(error.ResourceLimitsChanged, server.serve(&listener));
    server.limits.write_buffer_size -= 1;
    server.limits.max_connections += 1;
    try std.testing.expectError(error.ResourceLimitsChanged, server.serve(&listener));
}

test "StreamServer: enabled handshake budget rejects a transport without deadlines" {
    const Raw = struct {
        pub const Reader = struct { interface: std.Io.Reader };
        pub const Writer = struct { interface: std.Io.Writer };
        pub fn reader(_: @This(), _: []u8) Reader {
            return .{ .interface = .fixed("") };
        }
        pub fn writer(_: @This(), buf: []u8) Writer {
            return .{ .interface = .fixed(buf) };
        }
        pub fn close(_: @This()) void {}
        pub fn shutdown(_: @This()) void {}
        pub fn remoteInfo(_: @This()) talon.core.RemoteInfo {
            return .memory;
        }
    };
    const Listener = struct {
        pub const RawConnection = Raw;
        pub fn accept(_: *@This()) !Raw {
            return error.Closed;
        }
        pub fn close(_: *@This()) void {}
    };
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener: Listener = .{};
    var app: TestApp = .{};
    const Srv = StreamServer(LineEchoProto, TestApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();
    var read_buf: [16]u8 = undefined;
    var write_buf: [16]u8 = undefined;
    var conn = talon.core.Connection(Raw).init(.{}, &read_buf, &write_buf, &server.engine_limits, &server.shutting_down, std.testing.allocator);
    try std.testing.expectError(error.UnsupportedReadTimeout, conn.setReadTimeout(.fromSeconds(1)));
    try std.testing.expectError(error.UnsupportedWriteTimeout, conn.setWriteTimeout(.fromSeconds(1)));
    try conn.setReadTimeout(.none);
    try conn.setWriteTimeout(.none);
    try std.testing.expectError(error.UnsupportedReadTimeout, conn.waitReadable(.fromSeconds(1)));
    try std.testing.expectError(error.EndOfStream, conn.waitReadable(.none));
    try std.testing.expectError(error.HandshakeTimeoutUnsupported, server.serve(&listener));
    server.limits.handshake_timeout = .none;
    try std.testing.expectError(error.Closed, server.serve(&listener));

    const App = struct {
        pub fn handle(_: *@This(), _: *talon.http.Request, _: *talon.http.Response) !void {}
    };
    var http_app: App = .{};
    var http_server = try talon.http.Server(App).init(std.testing.allocator, &http_app, .{ .limits = .{ .handshake_timeout = .none } });
    defer http_server.deinit();
    try std.testing.expectError(error.UnsupportedReadTimeout, http_server.serve(&listener));
    http_server.limits.header_read_timeout = .none;
    http_server.limits.keep_alive_timeout = .none;
    http_server.limits.min_body_data_rate = null;
    try std.testing.expectError(error.UnsupportedWriteTimeout, http_server.serve(&listener));
    http_server.limits.write_timeout = .none;
    try std.testing.expectError(error.Closed, http_server.serve(&listener));
}

test "HTTP: PROXY and first request head share the establishment budget" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    const App = struct {
        hits: usize = 0,
        pub fn handle(self: *@This(), _: *talon.http.Request, res: *talon.http.Response) !void {
            self.hits += 1;
            try res.respond("ok", .{});
        }
    };
    const Srv = talon.http.ServerWith(App, .{talon.middleware.proxy_protocol});
    var app: App = .{};
    var server = try Srv.init(std.testing.allocator, &app, .{ .limits = .{
        .handshake_timeout = .fromMilliseconds(80),
        .header_read_timeout = .fromSeconds(1),
    } });
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn run(l: *MemoryListener, s: *Srv) !void {
            defer s.shutdown();
            const conn = try l.connect();
            defer conn.close();
            try zio.sleep(.fromMilliseconds(30));
            var writer = conn.writer(&.{});
            try writer.interface.writeAll("\r\n\r\n\x00\r\nQUIT\n\x20\x00\x00\x00GET / HTTP/1.1\r\nHost:");
            var buf: [64]u8 = undefined;
            var reader = conn.reader(&buf);
            reader.setTimeout(.fromMilliseconds(300));
            // A fresh header budget would stall for one second instead.
            try std.testing.expectError(error.EndOfStream, reader.interface.takeByte());
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener });
    try group.spawn(F.run, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(@as(usize, 0), app.hits);
}

test "StreamServer: stalled PROXY preamble releases the only connection permit" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    var app: TestApp = .{};
    const Srv = talon.StreamServerWith(LineEchoProto, TestApp, .{talon.middleware.proxy_protocol});
    var server = try Srv.init(std.testing.allocator, &app, .{ .limits = .{ .max_connections = 1, .handshake_timeout = .fromMilliseconds(50) } });
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn run(l: *MemoryListener, s: *Srv) !void {
            const stalled = try l.connect();
            defer stalled.close();
            var sw = stalled.writer(&.{});
            try sw.interface.writeAll("\r\n\r\n");
            var sbuf: [64]u8 = undefined;
            var sr = stalled.reader(&sbuf);
            sr.setTimeout(.fromSeconds(1));
            try std.testing.expectError(error.EndOfStream, sr.interface.takeByte());
            const healthy = try l.connect();
            defer healthy.close();
            var hw = healthy.writer(&.{});
            try hw.interface.writeAll("\r\n\r\n\x00\r\nQUIT\n\x20\x00\x00\x00hello\n");
            var hbuf: [64]u8 = undefined;
            var hr = healthy.reader(&hbuf);
            hr.setTimeout(.fromSeconds(1));
            try std.testing.expectEqualStrings("> hello\n", try hr.interface.takeDelimiterInclusive('\n'));
            s.shutdown();
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener });
    try group.spawn(F.run, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(@as(u32, 1), app.requests);
}

test "StreamServer: middleware and protocol handshake share a deadline and release it for the session" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    const P = struct {
        pub fn handshake(conn: anytype, _: *TestApp) !void {
            try std.testing.expectError(error.EstablishmentInProgress, conn.detachRaw());
            try std.testing.expect(!conn.raw_detached);
            const before = conn.reader_state.timeout.deadline.toNanoseconds();
            try conn.setReadTimeout(.fromSeconds(60));
            try conn.setWriteTimeout(.none);
            try std.testing.expectEqual(before, conn.reader_state.timeout.deadline.toNanoseconds());
            try std.testing.expectEqual(before, conn.writer_state.timeout.deadline.toNanoseconds());
            _ = try conn.reader().takeByte();
        }
        pub fn serve(conn: anytype, app: *TestApp) !void {
            try std.testing.expect(conn.reader_state.timeout == .none);
            try std.testing.expect(conn.writer_state.timeout == .none);
            try LineEchoProto.serve(conn, app);
        }
    };
    const M = struct {
        pub fn run(conn: anytype, next: anytype) !void {
            const before = conn.reader_state.timeout.deadline.toNanoseconds();
            _ = try conn.reader().takeByte();
            try conn.setReadTimeout(.none);
            try std.testing.expectEqual(before, conn.reader_state.timeout.deadline.toNanoseconds());
            try next.call(conn);
        }
    };
    var app: TestApp = .{};
    const Srv = talon.StreamServerWith(P, TestApp, .{M});
    var server = try Srv.init(std.testing.allocator, &app, .{ .limits = .{ .handshake_timeout = .fromMilliseconds(50) } });
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn run(l: *MemoryListener, s: *Srv) !void {
            const stalled = try l.connect();
            defer stalled.close();
            var sw = stalled.writer(&.{});
            try sw.interface.writeAll("P");
            var sbuf: [64]u8 = undefined;
            var sr = stalled.reader(&sbuf);
            sr.setTimeout(.fromSeconds(1));
            try std.testing.expectError(error.EndOfStream, sr.interface.takeByte());
            const healthy = try l.connect();
            defer healthy.close();
            var hw = healthy.writer(&.{});
            try hw.interface.writeAll("PH");
            // A session remains usable past the establishment deadline.
            try zio.sleep(.fromMilliseconds(80));
            try hw.interface.writeAll("hello\n");
            var hbuf: [64]u8 = undefined;
            var hr = healthy.reader(&hbuf);
            hr.setTimeout(.fromSeconds(1));
            try std.testing.expectEqualStrings("> hello\n", try hr.interface.takeDelimiterInclusive('\n'));
            s.shutdown();
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener });
    try group.spawn(F.run, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

/// Line-echo test protocol: one reply line per request line.
const LineEchoProto = struct {
    pub fn serve(conn: anytype, app: *TestApp) anyerror!void {
        const r = conn.reader();
        const w = conn.writer();
        while (true) {
            const line = r.takeDelimiterInclusive('\n') catch |err| switch (err) {
                error.EndOfStream => return,
                else => return err,
            };
            app.requests += 1;
            try w.writeAll("> ");
            try w.writeAll(line);
            try w.flush();
            if (conn.isShuttingDown()) return;
        }
    }
};

test "StreamServer: serves connections over MemoryListener and shuts down gracefully" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: TestApp = .{};
    const Server = StreamServer(LineEchoProto, TestApp);
    var server = try Server.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Server, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn client(l: *MemoryListener, s: *Server) !void {
            const conn = try l.connect();
            defer conn.close();

            var wbuf: [64]u8 = undefined;
            var w = conn.writer(&wbuf);
            var rbuf: [64]u8 = undefined;
            var r = conn.reader(&rbuf);

            try w.interface.writeAll("hello\n");
            try w.interface.flush();
            const reply1 = try r.interface.takeDelimiterInclusive('\n');
            try std.testing.expectEqualStrings("> hello\n", reply1);

            try w.interface.writeAll("again\n");
            try w.interface.flush();
            const reply2 = try r.interface.takeDelimiterInclusive('\n');
            try std.testing.expectEqualStrings("> again\n", reply2);

            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.client, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(2, app.requests);
}

test "StreamServer: drain cancels connections that ignore shutdown" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    // Protocol that never returns on its own: blocks reading a line that
    // never arrives. Shutdown must hard-cancel it after drain_timeout.
    const StuckProto = struct {
        pub fn serve(conn: anytype, app: *TestApp) anyerror!void {
            _ = app;
            _ = try conn.reader().takeDelimiterInclusive('\n');
        }
    };

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: TestApp = .{};
    const Server = StreamServer(StuckProto, TestApp);
    var server = try Server.init(std.testing.allocator, &app, .{
        .limits = .{ .drain_timeout = .fromMilliseconds(50) },
    });
    defer server.deinit();

    var served: zio.ResetEvent = .init;

    const Fns = struct {
        fn runServer(s: *Server, l: *MemoryListener, done: *zio.ResetEvent) !void {
            try s.serve(l);
            done.set();
        }
        fn client(l: *MemoryListener, s: *Server, done: *zio.ResetEvent) !void {
            const conn = try l.connect();
            // Keep the connection open across shutdown so the stuck handler
            // can only exit via drain-timeout cancellation.
            defer conn.close();
            try zio.sleep(.fromMilliseconds(10));
            s.shutdown();
            try done.wait();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener, &served });
    try group.spawn(Fns.client, .{ &listener, &server, &served });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    const snapshot = server.stats();
    try std.testing.expectEqual(@as(u64, 1), snapshot.accepted);
    try std.testing.expectEqual(snapshot.accepted, snapshot.finished_tasks);
    try std.testing.expectEqual(@as(usize, 0), snapshot.active_tasks);
    try std.testing.expectEqual(@as(u64, 0), snapshot.canceled_returns);
    try std.testing.expectEqual(@as(u64, 1), snapshot.connection_errors);
    try std.testing.expectEqual(@as(u64, 1), snapshot.forced_drains);
    try std.testing.expectEqual(@as(u64, 1), snapshot.drain_timeouts);
}

test "StreamServer: protocol switching and checked detachment preserve data and close ownership" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    const Counts = struct { closes: usize = 0, shutdowns: usize = 0 };
    const Raw = struct {
        inner: talon.core.MemoryConnection,
        counts: *Counts,
        pub const Reader = talon.core.PipeReader;
        pub const Writer = talon.core.PipeWriter;
        pub fn reader(self: @This(), buf: []u8) Reader {
            return self.inner.reader(buf);
        }
        pub fn writer(self: @This(), buf: []u8) Writer {
            return self.inner.writer(buf);
        }
        pub fn remoteInfo(_: @This()) talon.core.RemoteInfo {
            return .memory;
        }
        pub fn close(self: @This()) void {
            self.counts.closes += 1;
            self.inner.close();
        }
        pub fn shutdown(self: @This()) void {
            self.counts.shutdowns += 1;
            self.inner.shutdown();
        }
    };
    const Listener = struct {
        inner: *MemoryListener,
        counts: *Counts,
        pub const RawConnection = Raw;
        pub fn accept(self: *@This()) !Raw {
            return .{ .inner = try self.inner.accept(), .counts = self.counts };
        }
        pub fn close(self: *@This()) void {
            self.inner.close();
        }
    };
    const Mode = enum { detach, buffered, write_failure, in_task };
    const App = struct { mode: Mode };
    const P = struct {
        pub fn serve(conn: anytype, app: *App) !void {
            _ = try conn.reader().takeByte();
            try conn.writer().writeAll("pending");
            switch (app.mode) {
                .detach => {
                    const raw = try conn.detachRaw();
                    defer raw.close();
                    try std.testing.expectError(error.AlreadyDetached, conn.detachRaw());
                    var writer = raw.writer(&.{});
                    try writer.interface.writeAll("raw");
                },
                .buffered => {
                    try std.testing.expectEqualStrings("bc", conn.reader().buffered());
                    try std.testing.expectError(error.BufferedInput, conn.detachRaw());
                    try std.testing.expect(!conn.raw_detached);
                    try std.testing.expectEqualStrings("pending", conn.writer().buffered());
                    try std.testing.expectEqualStrings("bc", try conn.reader().take(2));
                    try conn.writer().flush();
                },
                .write_failure => {
                    try std.testing.expectError(error.WriteFailed, conn.detachRaw());
                    try std.testing.expect(!conn.raw_detached);
                },
                .in_task => {
                    // Protocol switching needs no raw extraction: retain the
                    // original reader and its already-prefetched bytes.
                    try std.testing.expectEqualStrings("bc", try conn.reader().take(2));
                    try conn.writer().flush();
                },
            }
        }
    };
    const Srv = StreamServer(P, App);
    const F = struct {
        fn serve(s: *Srv, l: *Listener) !void {
            try s.serve(l);
        }
        fn run(l: *MemoryListener, s: *Srv, mode: Mode) !void {
            defer s.shutdown();
            const peer = try l.connect();
            defer peer.close();
            if (mode == .write_failure) peer.recv.closeRead();
            var writer = peer.writer(&.{});
            try writer.interface.writeAll(if (mode == .buffered or mode == .in_task) "abc" else "a");
            var buf: [64]u8 = undefined;
            var reader = peer.reader(&buf);
            reader.setTimeout(.fromSeconds(1));
            var out: [16]u8 = undefined;
            const n = try reader.interface.readSliceShort(&out);
            try std.testing.expectEqualStrings(switch (mode) {
                .detach => "pendingraw",
                .buffered, .in_task => "pending",
                .write_failure => "",
            }, out[0..n]);
        }
    };
    for ([_]Mode{ .detach, .buffered, .write_failure, .in_task }) |mode| {
        var listener = try MemoryListener.init(std.testing.allocator, .{});
        defer listener.deinit();
        var counts: Counts = .{};
        var tracked: Listener = .{ .inner = &listener, .counts = &counts };
        var app: App = .{ .mode = mode };
        var server = try Srv.init(std.testing.allocator, &app, .{ .limits = .{ .max_connections = 1 } });
        defer server.deinit();
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.serve, .{ &server, &tracked });
        try group.spawn(F.run, .{ &listener, &server, mode });
        try group.wait();
        try std.testing.expect(!group.hasFailed());
        try std.testing.expectEqual(@as(usize, 1), counts.closes);
        try std.testing.expectEqual(@as(usize, if (mode == .detach) 0 else 1), counts.shutdowns);
        const snapshot = server.stats();
        try std.testing.expectEqual(@as(u64, 1), snapshot.accepted);
        try std.testing.expectEqual(snapshot.accepted, snapshot.finished_tasks);
        try std.testing.expectEqual(@as(usize, 0), snapshot.active_tasks);
        try std.testing.expectEqual(@as(u64, if (mode == .detach) 1 else 0), snapshot.detached_connections);
    }
}

test "StreamServer: detached connection is not closed by the server" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const HijackProto = struct {
        pub fn serve(conn: anytype, app: *TestApp) anyerror!void {
            _ = app;
            const raw = try conn.detachRaw();
            // Hijacker owns the connection: write a farewell and close it.
            var wbuf: [32]u8 = undefined;
            var w = raw.writer(&wbuf);
            try w.interface.writeAll("bye\n");
            try w.interface.flush();
            raw.close();
        }
    };

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: TestApp = .{};
    const Server = StreamServer(HijackProto, TestApp);
    var server = try Server.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Server, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn client(l: *MemoryListener, s: *Server) !void {
            const conn = try l.connect();
            defer conn.close();
            var rbuf: [32]u8 = undefined;
            var r = conn.reader(&rbuf);
            const line = try r.interface.takeDelimiterInclusive('\n');
            try std.testing.expectEqualStrings("bye\n", line);
            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.client, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

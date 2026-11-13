const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");
const core = talon.core;

test "allocation budget: pinned TLS state denial closes an already-dialed socket" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var budget = core.AllocationBudget.init(std.testing.allocator, 0);
    defer budget.deinit();
    var listener = try core.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    defer listener.close();
    const F = struct {
        fn peer(l: *core.TcpListener) !void {
            const raw = try l.accept();
            defer raw.close();
            var buffer: [1]u8 = undefined;
            var reader = raw.reader(&buffer);
            reader.setTimeout(.fromSeconds(2));
            try std.testing.expectError(error.EndOfStream, reader.interface.takeByte());
        }
        fn connect(io: std.Io, port: u16, a: std.mem.Allocator) !void {
            const connector: talon.http.client.TlsConnector = .{ .gpa = a, .io = io, .verification = .insecure_no_verification };
            // The http branch still allocates the pinned TLS-capable State.
            try std.testing.expectError(error.OutOfMemory, connector.connect(.{ .host = "127.0.0.1", .port = port }, .fromSeconds(2)));
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.peer, .{&listener});
    try group.spawn(F.connect, .{ rt.io(), listener.server.socket.address.ip.getPort(), budget.allocator() });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
    try std.testing.expectEqual(@as(usize, 1), budget.stats().denied_requests);
}

test "allocation budget: idle pool storage and retained frames consume the same cumulative cap" {
    var budget = core.AllocationBudget.init(std.testing.allocator, 4096);
    defer budget.deinit();
    const a = budget.allocator();
    {
        var pool = try core.BufferPool.init(a, .{ .buffer_size = 512, .max_idle = 1 });
        defer pool.deinit();
        const block = try pool.rent();
        const live_pool = budget.stats().reserved_bytes;
        pool.give(block);
        try std.testing.expectEqual(live_pool, budget.stats().reserved_bytes);
        const Frames = core.framing.OwnedLengthPrefixed(.{ .length_type = u16, .max_frame = 2000 });
        const data = "\x07\xd0" ++ (&@as([2000]u8, @splat('x')));
        var first_reader = std.Io.Reader.fixed(data);
        var first_decoder = Frames.init(&first_reader, a);
        var first = (try first_decoder.next()).?;
        try std.testing.expectEqual(live_pool + 2000, budget.stats().reserved_bytes);
        var second_reader = std.Io.Reader.fixed(data);
        var second_decoder = Frames.init(&second_reader, a);
        try std.testing.expectError(error.OutOfMemory, second_decoder.next());
        try std.testing.expectEqual(live_pool + 2000, budget.stats().reserved_bytes);
        first.deinit();
        var later_reader = std.Io.Reader.fixed(data);
        var later_decoder = Frames.init(&later_reader, a);
        var later = (try later_decoder.next()).?;
        later.deinit();
        try std.testing.expectEqual(live_pool, budget.stats().reserved_bytes);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
    try std.testing.expectEqual(@as(usize, 1), budget.stats().denied_requests);
}

test "allocation budget: concurrent server frames reject excess, release admission and recover" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    var budget = core.AllocationBudget.init(std.testing.allocator, 64 * 1024);
    defer budget.deinit();
    const App = struct {
        held: zio.ResetEvent = .init,
        release: zio.ResetEvent = .init,
        served: std.atomic.Value(usize) = .init(0),
    };
    const Proto = struct {
        pub fn serve(conn: anytype, app: *App) !void {
            var frames = core.framing.OwnedLengthPrefixed(.{ .max_frame = 48 * 1024 }).init(conn.reader(), conn.gpa);
            var frame = (try frames.next()).?;
            defer frame.deinit();
            if (frame.payload.len == 48 * 1024) {
                app.held.set();
                try app.release.wait();
            }
            try conn.writer().writeAll("ok");
            try conn.writer().flush();
            _ = app.served.fetchAdd(1, .monotonic);
        }
    };
    const Server = core.StreamServer(Proto, App);
    {
        var listener = try core.MemoryListener.init(std.testing.allocator, .{});
        defer listener.deinit();
        var app: App = .{};
        var server = try Server.init(budget.allocator(), &app, .{ .limits = .{ .max_connections = 2, .read_buffer_size = 128, .write_buffer_size = 128 } });
        defer server.deinit();
        const F = struct {
            fn serve(s: *Server, l: *core.MemoryListener) !void {
                try s.serve(l);
            }
            fn client(s: *Server, l: *core.MemoryListener, app_state: *App) !void {
                defer s.shutdown();
                defer app_state.release.set();
                const first = try l.connect();
                defer first.close();
                var first_writer = first.writer(&.{});
                first_writer.setTimeout(.fromSeconds(2));
                try first_writer.interface.writeInt(u32, 48 * 1024, .big);
                try first_writer.interface.writeAll(&@as([(48 * 1024)]u8, @splat('x')));
                try app_state.held.timedWait(.fromSeconds(2));
                const excess = try l.connect();
                defer excess.close();
                var excess_writer = excess.writer(&.{});
                excess_writer.setTimeout(.fromSeconds(2));
                try excess_writer.interface.writeInt(u32, 48 * 1024, .big);
                var excess_buffer: [1]u8 = undefined;
                var excess_reader = excess.reader(&excess_buffer);
                excess_reader.setTimeout(.fromSeconds(2));
                try std.testing.expectError(error.EndOfStream, excess_reader.interface.takeByte());
                app_state.release.set();
                var first_buffer: [2]u8 = undefined;
                var first_reader = first.reader(&first_buffer);
                first_reader.setTimeout(.fromSeconds(2));
                try std.testing.expectEqualStrings("ok", try first_reader.interface.take(2));
                try std.testing.expectError(error.EndOfStream, first_reader.interface.takeByte());
                // EOF can precede the final buffer returns on another executor.
                // Wait for the owner to cache them before taking the snapshot.
                const deadline = zio.Timeout.fromSeconds(2).toDeadline();
                while (true) {
                    s.read_pool.mutex.lock();
                    const read_idle = s.read_pool.free.items.len;
                    s.read_pool.mutex.unlock();
                    s.write_pool.mutex.lock();
                    const write_idle = s.write_pool.free.items.len;
                    s.write_pool.mutex.unlock();
                    if (read_idle > 0 and write_idle > 0) break;
                    if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline.deadline.toNanoseconds())
                        return error.TestUnexpectedResult;
                    try zio.yield();
                }
                try std.testing.expect(s.trimIdleBuffers() >= 256);
                const later = try l.connect();
                defer later.close();
                var later_writer = later.writer(&.{});
                later_writer.setTimeout(.fromSeconds(2));
                try later_writer.interface.writeInt(u32, 2, .big);
                try later_writer.interface.writeAll("hi");
                var later_buffer: [2]u8 = undefined;
                var later_reader = later.reader(&later_buffer);
                later_reader.setTimeout(.fromSeconds(2));
                try std.testing.expectEqualStrings("ok", try later_reader.interface.take(2));
            }
        };
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.serve, .{ &server, &listener });
        try group.spawn(F.client, .{ &server, &listener, &app });
        try group.wait();
        try std.testing.expect(!group.hasFailed());
        try std.testing.expectEqual(@as(usize, 2), app.served.load(.acquire));
        try std.testing.expect(budget.stats().reserved_bytes > 0); // idle pool capacity
    }
    try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
    try std.testing.expect(budget.stats().denied_requests >= 1);
    try std.testing.expect(budget.stats().peak_reserved_bytes <= 64 * 1024);
}

test "allocation budget: HTTP server/client/listener share charges including request arena and idle reuse" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var budget = core.AllocationBudget.init(std.testing.allocator, 256 * 1024);
    defer budget.deinit();
    const App = struct {
        budget: *core.AllocationBudget,
        hits: usize = 0,
        pub fn handle(self: *@This(), req: *talon.http.Request, res: *talon.http.Response) !void {
            self.hits += 1;
            if (self.hits == 2) {
                // Deliberately propagate OOM so HTTP must produce a closing
                // 500 and release its arena; the next connection must recover.
                _ = try req.arena.alloc(u8, 1024 * 1024);
                return error.TestUnexpectedResult;
            }
            const bytes = try req.arena.alloc(u8, 64 * 1024);
            @memset(bytes, 'x');
            try std.testing.expect(self.budget.stats().reserved_bytes > bytes.len);
            try res.respond("ok", .{});
        }
    };
    const Server = talon.http.Server(App);
    {
        var listener = try core.MemoryListener.init(budget.allocator(), .{});
        defer listener.deinit();
        var app: App = .{ .budget = &budget };
        var server = try Server.init(budget.allocator(), &app, .{});
        defer server.deinit();
        const F = struct {
            fn serve(s: *Server, l: *core.MemoryListener) !void {
                try s.serve(l);
            }
            fn client(s: *Server, l: *core.MemoryListener, gpa: std.mem.Allocator) !void {
                defer s.shutdown();
                const Client = talon.http.client.Client(talon.http.client.MemoryConnector);
                var c = Client.init(gpa, .{ .listener = l }, .{});
                defer c.deinit();
                for ([_]u16{ 200, 500, 200 }) |status| {
                    var response = try c.get(.{ .host = "memory", .port = 80 }, "/");
                    defer response.deinit();
                    try std.testing.expectEqual(status, response.status());
                    var body: [128]u8 = undefined;
                    var sink = std.Io.Writer.fixed(&body);
                    _ = try response.bodyReader().streamRemaining(&sink);
                    if (status == 200) try std.testing.expectEqualStrings("ok", sink.buffered());
                }
                try std.testing.expectEqual(@as(u64, 1), c.poolStats().reused);
                try std.testing.expectEqual(@as(u64, 2), c.poolStats().created);
            }
        };
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.serve, .{ &server, &listener });
        try group.spawn(F.client, .{ &server, &listener, budget.allocator() });
        try group.wait();
        try std.testing.expect(!group.hasFailed());
        try std.testing.expectEqual(@as(usize, 3), app.hits);
        try std.testing.expect(budget.stats().reserved_bytes > 0);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
    // Arena allocation can attempt growth then a new node, so one logical
    // allocation may contribute more than one denied allocator request.
    try std.testing.expect(budget.stats().denied_requests >= 1);
    try std.testing.expect(budget.stats().peak_reserved_bytes <= 256 * 1024);
}

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

fn response(listener: *talon.MemoryListener, request: []const u8) ![]u8 {
    const raw = try listener.connect();
    defer raw.close();
    var writer = raw.writer(&.{});
    writer.setTimeout(.fromSeconds(2));
    try writer.interface.writeAll(request);
    try writer.interface.flush();
    var buffer: [1024]u8 = undefined;
    var reader = raw.reader(&buffer);
    reader.setTimeout(.fromSeconds(2));
    return reader.interface.allocRemaining(std.testing.allocator, .limited(8192));
}

test "HTTP arena budget: zero head body and handler denials preserve response commitment and recover" {
    const Mode = enum { zero, body, handler, committed };
    const App = struct {
        calls: usize = 0,
        pub fn handle(self: *@This(), req: *talon.http.Request, res: *talon.http.Response) !void {
            self.calls += 1;
            if (std.mem.eql(u8, req.target(), "/committed")) try res.respond("prefix", .{ .keep_alive = false });
            if (!std.mem.eql(u8, req.target(), "/")) _ = try req.arena.alloc(u8, 16 * 1024);
            try res.respond("ok", .{ .keep_alive = false });
        }
    };
    const Server = talon.http.Server(App);
    for ([_]Mode{ .zero, .body, .handler, .committed }) |mode| {
        const rt = try zio.Runtime.init(std.testing.allocator, .{});
        defer rt.deinit();
        var budget = talon.core.AllocationBudget.init(std.testing.allocator, 64 * 1024);
        defer budget.deinit();
        {
            var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
            defer listener.deinit();
            var app: App = .{};
            var server = try Server.init(budget.allocator(), &app, .{ .limits = .{
                .max_connections = 2,
                .max_header_size = 128,
                .read_buffer_size = 1024,
                .write_buffer_size = 256,
                .max_request_arena_bytes = switch (mode) {
                    .zero => 0,
                    .body => 1024,
                    else => 8192,
                },
            } });
            defer server.deinit();
            const F = struct {
                fn serve(s: *Server, l: *talon.MemoryListener) !void {
                    try s.serve(l);
                }
                fn client(s: *Server, l: *talon.MemoryListener, kind: Mode) !void {
                    defer s.shutdown();
                    const request = switch (kind) {
                        .zero => "GET / HTTP/1.1\r\nHost: h\r\n\r\n",
                        .body => "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\n\r\nx",
                        .handler => "GET /large HTTP/1.1\r\nHost: h\r\n\r\n",
                        .committed => "GET /committed HTTP/1.1\r\nHost: h\r\n\r\n",
                    };
                    const first = try response(l, request);
                    defer std.testing.allocator.free(first);
                    try std.testing.expect(std.mem.startsWith(u8, first, if (kind == .committed) "HTTP/1.1 200 " else "HTTP/1.1 500 "));
                    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, first, "HTTP/1.1 "));
                    if (kind == .committed) try std.testing.expect(std.mem.endsWith(u8, first, "prefix"));
                    const next = try response(l, "GET / HTTP/1.1\r\nHost: h\r\n\r\n");
                    defer std.testing.allocator.free(next);
                    try std.testing.expect(std.mem.startsWith(u8, next, if (kind == .zero) "HTTP/1.1 500 " else "HTTP/1.1 200 "));
                    if (kind != .zero) try std.testing.expect(std.mem.endsWith(u8, next, "ok"));
                }
            };
            var group: zio.Group = .init;
            defer group.cancel();
            try group.spawn(F.serve, .{ &server, &listener });
            try group.spawn(F.client, .{ &server, &listener, mode });
            try group.wait();
            try std.testing.expect(!group.hasFailed());
            try std.testing.expectEqual(@as(u64, 2), server.stats().accepted);
            try std.testing.expectEqual(server.stats().accepted, server.stats().finished_tasks);
            try std.testing.expectEqual(@as(usize, 0), server.stats().active_tasks);
            try std.testing.expectEqual(@as(u64, if (mode == .handler or mode == .committed) 1 else 0), server.stats().connection_errors);
            try std.testing.expectEqual(@as(usize, switch (mode) {
                .zero => 0,
                .body => 1,
                else => 2,
            }), app.calls);
            // Local denial happens before the outer shared budget is called.
            try std.testing.expectEqual(@as(usize, 0), budget.stats().denied_requests);
        }
        try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
    }
}

test "HTTP arena budget: keep-alive reset reuses bounded capacity without cumulative request charging" {
    const App = struct {
        calls: usize = 0,
        pub fn handle(self: *@This(), req: *talon.http.Request, res: *talon.http.Response) !void {
            _ = try req.arena.alloc(u8, 4096);
            self.calls += 1;
            try res.respond("", .{ .keep_alive = self.calls < 5 });
        }
    };
    const Server = talon.http.Server(App);
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    var app: App = .{};
    var server = try Server.init(std.testing.allocator, &app, .{ .limits = .{
        .max_request_arena_bytes = 8192,
        .max_retained_arena = null,
    } });
    defer server.deinit();
    const F = struct {
        fn serve(s: *Server, l: *talon.MemoryListener) !void {
            try s.serve(l);
        }
        fn client(s: *Server, l: *talon.MemoryListener) !void {
            defer s.shutdown();
            const raw = try l.connect();
            defer raw.close();
            var writer = raw.writer(&.{});
            writer.setTimeout(.fromSeconds(2));
            var buffer: [1024]u8 = undefined;
            var reader = raw.reader(&buffer);
            reader.setTimeout(.fromSeconds(2));
            for (0..5) |_| {
                try writer.interface.writeAll("GET / HTTP/1.1\r\nHost: h\r\n\r\n");
                try writer.interface.flush();
                try std.testing.expect(std.mem.startsWith(u8, try reader.interface.takeDelimiterInclusive('\n'), "HTTP/1.1 200 "));
                while (!std.mem.eql(u8, try reader.interface.takeDelimiterInclusive('\n'), "\r\n")) {}
            }
            try std.testing.expectError(error.EndOfStream, reader.interface.takeByte());
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener });
    try group.spawn(F.client, .{ &server, &listener });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(@as(usize, 5), app.calls);
    try std.testing.expectEqual(@as(u64, 0), server.stats().connection_errors);
}

test "HTTP arena budget: concurrent connections have independent caps above one shared budget" {
    const App = struct {
        ready: zio.ResetEvent = .init,
        release: zio.ResetEvent = .init,
        held: std.atomic.Value(usize) = .init(0),
        pub fn handle(self: *@This(), req: *talon.http.Request, res: *talon.http.Response) !void {
            const bytes = try req.arena.alloc(u8, 4096);
            @memset(bytes, 'x');
            if (self.held.fetchAdd(1, .acq_rel) == 1) self.ready.set();
            try self.release.wait();
            try std.testing.expectError(error.OutOfMemory, req.arena.alloc(u8, 16 * 1024));
            for (bytes) |byte| try std.testing.expectEqual(@as(u8, 'x'), byte);
            try res.respond("ok", .{ .keep_alive = false });
        }
    };
    const Server = talon.http.Server(App);
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    var budget = talon.core.AllocationBudget.init(std.testing.allocator, 64 * 1024);
    defer budget.deinit();
    {
        var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
        defer listener.deinit();
        var app: App = .{};
        var server = try Server.init(budget.allocator(), &app, .{ .limits = .{
            .max_connections = 2,
            .max_request_arena_bytes = 8192,
            .drain_timeout = .fromSeconds(2),
            .max_header_size = 128,
            .read_buffer_size = 1024,
            .write_buffer_size = 256,
        } });
        defer server.deinit();
        const F = struct {
            fn serve(s: *Server, l: *talon.MemoryListener) !void {
                try s.serve(l);
            }
            fn client(l: *talon.MemoryListener) !void {
                const wire = try response(l, "GET / HTTP/1.1\r\nHost: h\r\n\r\n");
                defer std.testing.allocator.free(wire);
                try std.testing.expect(std.mem.startsWith(u8, wire, "HTTP/1.1 200 ") and std.mem.endsWith(u8, wire, "ok"));
            }
            fn coordinate(s: *Server, a: *App, parent: *talon.core.AllocationBudget) !void {
                defer s.shutdown();
                defer a.release.set();
                try a.ready.timedWait(.fromSeconds(2));
                try std.testing.expectEqual(@as(usize, 2), s.stats().active_tasks);
                try std.testing.expect(parent.stats().reserved_bytes >= 2 * 4096);
            }
        };
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.serve, .{ &server, &listener });
        try group.spawn(F.client, .{&listener});
        try group.spawn(F.client, .{&listener});
        try group.spawn(F.coordinate, .{ &server, &app, &budget });
        try group.wait();
        try std.testing.expect(!group.hasFailed());
        try std.testing.expectEqual(@as(u64, 2), server.stats().accepted);
        try std.testing.expectEqual(server.stats().accepted, server.stats().finished_tasks);
        try std.testing.expectEqual(@as(u64, 0), server.stats().connection_errors);
        try std.testing.expectEqual(@as(usize, 0), budget.stats().denied_requests);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
}

test "HTTP arena budget: upgrade preparation is capped and committed WebSocket output has no fallback" {
    const App = struct {
        pub fn handle(_: *@This(), req: *talon.http.Request, _: *talon.http.Response) !void {
            const large = std.mem.eql(u8, req.target(), "/upgrade-deny");
            var socket = try talon.http.ws.upgrade(req, .{ .max_message_size = if (large) 16 * 1024 else 1024 });
            try socket.writeBinary("ok");
            _ = try req.arena.alloc(u8, 16 * 1024);
            return error.ExpectedAllocationDenial;
        }
    };
    const Server = talon.http.Server(App);
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var budget = talon.core.AllocationBudget.init(std.testing.allocator, 64 * 1024);
    defer budget.deinit();
    {
        var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
        defer listener.deinit();
        var app: App = .{};
        var server = try Server.init(budget.allocator(), &app, .{ .limits = .{
            .max_request_arena_bytes = 8192,
        } });
        defer server.deinit();
        const F = struct {
            fn serve(s: *Server, l: *talon.MemoryListener) !void {
                try s.serve(l);
            }
            fn client(s: *Server, l: *talon.MemoryListener) !void {
                defer s.shutdown();
                for ([_][]const u8{ "/upgrade-deny", "/upgrade" }) |target| {
                    var storage: [512]u8 = undefined;
                    const request = try std.fmt.bufPrint(&storage, "GET {s} HTTP/1.1\r\nHost: h\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n", .{target});
                    const wire = try response(l, request);
                    defer std.testing.allocator.free(wire);
                    const denied = std.mem.eql(u8, target, "/upgrade-deny");
                    try std.testing.expect(std.mem.startsWith(u8, wire, if (denied) "HTTP/1.1 500 " else "HTTP/1.1 101 "));
                    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, wire, "HTTP/1.1 "));
                    if (!denied) try std.testing.expect(std.mem.endsWith(u8, wire, "\x82\x02ok"));
                }
            }
        };
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.serve, .{ &server, &listener });
        try group.spawn(F.client, .{ &server, &listener });
        try group.wait();
        try std.testing.expect(!group.hasFailed());
        try std.testing.expectEqual(@as(u64, 2), server.stats().accepted);
        try std.testing.expectEqual(server.stats().accepted, server.stats().finished_tasks);
        try std.testing.expectEqual(@as(u64, 2), server.stats().connection_errors);
        try std.testing.expectEqual(@as(usize, 0), budget.stats().denied_requests);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
}

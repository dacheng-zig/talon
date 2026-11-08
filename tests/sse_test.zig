//! End-to-end Server-Sent Events: the client's SseSource drives the talon
//! server's EventStream in-process over MemoryConnector (no sockets). Proves
//! the whole loop — event serialize on the server, chunked framing, incremental
//! decode + sticky id on the client.

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const client = talon.http.client;
const Server = talon.http.Server;
const Request = talon.http.Request;
const Response = talon.http.Response;
const MemoryListener = talon.MemoryListener;

const SseApp = struct {
    pub fn handle(_: *SseApp, _: *Request, res: *Response) !void {
        var buf: [512]u8 = undefined;
        var stream = try res.startEventStream(&buf);
        try stream.send(.{ .event = "tick", .id = "1", .data = "one" });
        try stream.send(.{ .event = "tick", .id = "2", .data = "two\nlines" });
        try stream.comment("heartbeat");
        try stream.send(.{ .data = "three" }); // no id → client keeps sticky "2"
        try stream.finish();
    }
};

test "sse: client streams server events, sticky id, multi-line data" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: SseApp = .{};
    const Srv = Server(SseApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }

        fn runClient(l: *MemoryListener, s: *Srv) !void {
            const Client = client.Client(client.MemoryConnector);
            var c = Client.init(std.testing.allocator, .{ .listener = l }, .{
                .pool = .{ .max_active = 1, .max_waiters = 0 },
            });
            defer c.deinit();

            var src = try c.sseSource(.{
                .origin = .{ .host = "memory", .port = 80 },
                .target = "/events",
                // Stop after the server ends the stream instead of reconnecting.
                .reconnect = .{ .max_retries = 0 },
            });
            defer src.deinit();

            const e1 = (try src.next()).?;
            try std.testing.expectEqual(@as(usize, 1), c.poolStats().active);
            try std.testing.expectError(error.PoolQueueFull, c.get(.{ .host = "other", .port = 80 }, "/"));
            try std.testing.expectEqualStrings("tick", e1.event.?);
            try std.testing.expectEqualStrings("one", e1.data);
            try std.testing.expectEqualStrings("1", e1.id.?);

            const e2 = (try src.next()).?;
            try std.testing.expectEqualStrings("two\nlines", e2.data);
            try std.testing.expectEqualStrings("2", e2.id.?);

            // Comment (heartbeat) dispatches nothing; the next event has no id of
            // its own, so the sticky "2" carries over.
            const e3 = (try src.next()).?;
            try std.testing.expectEqual(null, e3.event);
            try std.testing.expectEqualStrings("three", e3.data);
            try std.testing.expectEqualStrings("2", e3.id.?);

            // Server called finish(): clean end, no reconnect → null.
            try std.testing.expectEqual(null, try src.next());
            try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);

            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.runClient, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

test "sse: reconnect keeps metadata-only ids and an empty id clears the resume header" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    const App = struct {
        hits: usize = 0,
        resumed: bool = false,
        cleared: bool = false,
        pub fn handle(self: *@This(), req: *Request, res: *Response) !void {
            self.hits += 1;
            switch (self.hits) {
                1 => try res.respond("id: old\ndata: first\n\nid: new\n\n", .{ .extra_headers = &.{.{ .name = "content-type", .value = "text/event-stream" }} }),
                2 => {
                    self.resumed = std.mem.eql(u8, req.header("last-event-id") orelse "", "new");
                    try res.respond("data: second\n\ndata: third\n\nid:\n\n", .{ .extra_headers = &.{.{ .name = "content-type", .value = "text/event-stream" }} });
                },
                else => {
                    self.cleared = req.header("last-event-id") == null;
                    try res.respond("", .{ .status = .no_content });
                },
            }
        }
    };
    const Srv = Server(App);
    const F = struct {
        fn serve(server: *Srv, listener: *MemoryListener) !void {
            try server.serve(listener);
        }
        fn request(server: *Srv, listener: *MemoryListener) !void {
            defer server.shutdown();
            var c = client.Client(client.MemoryConnector).init(std.testing.allocator, .{ .listener = listener }, .{});
            defer c.deinit();
            var source = try c.sseSource(.{
                .origin = .{ .host = "memory", .port = 80 },
                .reconnect = .{ .max_retries = 3, .initial_delay_ms = 1, .max_delay_ms = 1 },
            });
            defer source.deinit();
            try std.testing.expectEqualStrings("first", (try source.next()).?.data);
            for ([_][]const u8{ "second", "third" }) |expected| {
                const event = (try source.next()).?;
                try std.testing.expectEqualStrings(expected, event.data);
                try std.testing.expectEqualStrings("new", event.id.?);
            }
            try std.testing.expectEqual(null, try source.next());
            try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
        }
    };
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    var app: App = .{};
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener });
    try group.spawn(F.request, .{ &server, &listener });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(@as(usize, 3), app.hits);
    try std.testing.expect(app.resumed);
    try std.testing.expect(app.cleared);
}

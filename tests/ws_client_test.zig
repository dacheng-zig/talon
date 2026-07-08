//! End-to-end WebSocket: the client's `webSocket` drives the talon server's
//! `ws.upgrade` echo loop in-process over MemoryConnector (no sockets). Proves
//! the whole loop — handshake (key→accept), client-masked frames, server
//! fragment reassembly + echo, text/binary, and the close handshake.

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const client = talon.http.client;
const Server = talon.http.Server;
const Request = talon.http.Request;
const Response = talon.http.Response;
const MemoryListener = talon.MemoryListener;

const EchoApp = struct {
    pub fn handle(_: *EchoApp, req: *Request, res: *Response) !void {
        if (!std.mem.eql(u8, req.target(), "/ws")) {
            return res.respond("not here\n", .{ .status = .not_found });
        }
        var socket = talon.http.ws.upgrade(req, .{}) catch {
            return res.respond("expected upgrade\n", .{ .status = .bad_request });
        };
        while (try socket.read()) |msg| switch (msg) {
            .text => |t| try socket.writeText(t),
            .binary => |b| try socket.writeBinary(b),
        };
    }
};

test "ws client: handshake, echo text + binary, close" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: EchoApp = .{};
    const Srv = Server(EchoApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }

        fn runClient(l: *MemoryListener, s: *Srv) !void {
            const Client = client.Client(client.MemoryConnector);
            var c = Client.init(std.testing.allocator, .{ .listener = l }, .{});
            defer c.deinit();

            var sock = try c.webSocket(.{
                .origin = .{ .host = "memory", .port = 80 },
                .target = "/ws",
            });
            defer sock.deinit();

            // Text round-trip.
            try sock.writeText("hello");
            const m1 = (try sock.read()).?;
            try std.testing.expectEqualStrings("hello", m1.text);

            // Binary round-trip.
            try sock.writeBinary(&[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF });
            const m2 = (try sock.read()).?;
            try std.testing.expectEqualSlices(u8, &[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF }, m2.binary);

            // Close handshake: after we send close the server echoes it and ends
            // its loop, so the next read observes end-of-stream (null).
            try sock.close(.normal, "bye");
            try std.testing.expectEqual(@as(?@TypeOf(sock).Message, null), try sock.read());

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

test "ws client: rejects a bad handshake (no upgrade)" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: EchoApp = .{};
    const Srv = Server(EchoApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }

        fn runClient(l: *MemoryListener, s: *Srv) !void {
            const Client = client.Client(client.MemoryConnector);
            var c = Client.init(std.testing.allocator, .{ .listener = l }, .{});
            defer c.deinit();

            // "/other" returns 404, not a 101 — the handshake must fail and the
            // connection must be destroyed (no leak, not pooled).
            try std.testing.expectError(error.HandshakeFailed, c.webSocket(.{
                .origin = .{ .host = "memory", .port = 80 },
                .target = "/other",
            }));

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

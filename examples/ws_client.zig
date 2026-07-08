//! talon WebSocket client demo: spins up a talon ws echo server and drives it
//! with the talon WebSocket client in the same process, printing each echo.
//!
//! Run: zig build run-ws_client

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const client = talon.http.client;

const EchoApp = struct {
    pub fn handle(_: *EchoApp, req: *talon.http.Request, res: *talon.http.Response) !void {
        if (!std.mem.eql(u8, req.target(), "/ws")) {
            return res.respond("connect to /ws\n", .{});
        }
        var socket = talon.http.ws.upgrade(req, .{}) catch {
            return res.respond("expected a WebSocket upgrade\n", .{ .status = .bad_request });
        };
        while (try socket.read()) |msg| switch (msg) {
            .text => |t| try socket.writeText(t),
            .binary => |b| try socket.writeBinary(b),
        };
    }
};

const Srv = talon.http.Server(EchoApp);

fn runServer(server: *Srv, listener: *talon.TcpListener) !void {
    try server.serve(listener);
}

fn runClient(port: u16, server: *Srv) !void {
    var c = client.Client(client.TcpConnector).init(std.heap.page_allocator, .{}, .{});
    defer c.deinit();

    var sock = try c.webSocket(.{
        .origin = .{ .host = "127.0.0.1", .port = port },
        .target = "/ws",
    });
    defer sock.deinit();

    for ([_][]const u8{ "hello", "from", "talon ws client" }) |line| {
        try sock.writeText(line);
        if (try sock.read()) |msg| switch (msg) {
            .text => |t| std.log.info("echo: {s}", .{t}),
            .binary => |b| std.log.info("echo ({d} bytes binary)", .{b.len}),
        };
    }

    try sock.close(.normal, "done");
    server.shutdown();
}

pub fn main(init: std.process.Init) !void {
    const rt = try zio.Runtime.init(init.gpa, .{});
    defer rt.deinit();

    var listener = try talon.TcpListener.listen(
        try zio.net.IpAddress.parseIp4("127.0.0.1", 0),
        .{},
    );
    const port = listener.server.socket.address.ip.getPort();

    var app: EchoApp = .{};
    var server = try Srv.init(init.gpa, &app, .{});
    defer server.deinit();

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(runServer, .{ &server, &listener });
    try group.spawn(runClient, .{ port, &server });
    try group.wait();
}

//! talon HTTP client demo: spins up a talon server and
//! drives it with the talon client in the same process, printing the response.
//!
//! Run: zig build run-http_client

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const client = talon.http.client;

const App = struct {
    pub fn handle(self: *App, req: *talon.http.Request, res: *talon.http.Response) !void {
        _ = self;
        _ = req;
        try res.respond("Hello from talon server!\n", .{
            .extra_headers = &.{.{ .name = "content-type", .value = "text/plain" }},
        });
    }
};

const Srv = talon.http.Server(App);

fn runServer(server: *Srv, listener: *talon.TcpListener) !void {
    try server.serve(listener);
}

fn runClient(port: u16, server: *Srv) !void {
    var c = client.Client(client.TcpConnector).init(std.heap.page_allocator, .{}, .{});
    defer c.deinit();

    var resp = try c.get(.{ .host = "127.0.0.1", .port = port }, "/");
    defer resp.deinit();

    std.log.info("response: {d} {s}", .{ resp.status(), resp.reason() });
    if (resp.header("content-type")) |ct| std.log.info("content-type: {s}", .{ct});

    var body: std.Io.Writer.Allocating = .init(std.heap.page_allocator);
    defer body.deinit();
    _ = try resp.bodyReader().streamRemaining(&body.writer);
    std.log.info("body: {s}", .{body.written()});

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

    var app: App = .{};
    var server = try Srv.init(init.gpa, &app, .{});
    defer server.deinit();

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(runServer, .{ &server, &listener });
    try group.spawn(runClient, .{ port, &server });
    try group.wait();
}

//! WebSocket echo server on talon.
//!
//! Run: zig build run-ws
//! Try: websocat ws://127.0.0.1:8080/ws   (or any browser WebSocket client)

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const App = struct {
    pub fn handle(_: *App, req: *talon.http.Request, res: *talon.http.Response) !void {
        // Only /ws upgrades; anything else gets a plain 200.
        if (!std.mem.eql(u8, req.target(), "/ws")) {
            return res.respond("talon websocket echo — connect to /ws\n", .{});
        }
        var socket = talon.http.ws.upgrade(req, .{}) catch |err| switch (err) {
            error.NotWebSocket, error.MissingKey, error.UnsupportedVersion =>
                return res.respond("expected a WebSocket upgrade\n", .{ .status = .bad_request }),
            else => return err,
        };
        while (try socket.read()) |msg| switch (msg) {
            .text => |t| try socket.writeText(t),
            .binary => |b| try socket.writeBinary(b),
        };
    }
};

fn signalWatcher(server: *talon.http.Server(App)) !void {
    var sig = try zio.Signal.init(.interrupt);
    defer sig.deinit();
    try sig.wait();
    server.shutdown();
}

pub fn main(init: std.process.Init) !void {
    const rt = try zio.Runtime.init(init.gpa, .{
        .stack_pool = .{ .maximum_size = 8 * 1024 * 1024, .committed_size = 64 * 1024 },
    });
    defer rt.deinit();

    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 8080);
    var listener = try talon.TcpListener.listen(addr, .{});

    var app: App = .{};
    var server = try talon.http.Server(App).init(init.gpa, &app, .{});
    defer server.deinit();

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(signalWatcher, .{&server});

    std.log.info("talon ws echo on ws://{f}/ws (Ctrl+C to stop)", .{addr});
    try server.serve(&listener);
}

//! Server-Sent Events on talon: pushes a counter tick every second.
//!
//! Run: zig build run-sse
//! Try: curl -N http://127.0.0.1:8080/events

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const App = struct {
    pub fn handle(_: *App, req: *talon.http.Request, res: *talon.http.Response) !void {
        var buf: [256]u8 = undefined;
        var stream = try res.startEventStream(&buf);

        var idbuf: [16]u8 = undefined;
        var n: u32 = 0;
        while (n < 100) : (n += 1) {
            const id = std.fmt.bufPrint(&idbuf, "{d}", .{n}) catch unreachable;
            var databuf: [64]u8 = undefined;
            const data = std.fmt.bufPrint(&databuf, "tick {d}", .{n}) catch unreachable;
            // A write error means the client went away — stop.
            stream.send(.{ .event = "tick", .id = id, .data = data }) catch break;
            zio.sleep(.fromSeconds(1)) catch break;
        }
        _ = req;
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

    std.log.info("talon sse on http://{f}/events (curl -N; Ctrl+C to stop)", .{addr});
    try server.serve(&listener);
}

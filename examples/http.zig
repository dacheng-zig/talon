//! HTTP hello-world on talon (design doc §10 examples).
//!
//! Run: zig build run-http
//! Try: curl -v http://127.0.0.1:8080/

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

// Deliberately NOT using `zio.debug_io` for std.log: routing log writes
// through the zio loop panics when stderr is a regular file (positional
// write path hits Loop.add outside a task context). Default blocking
// stderr writes are fine for an example.

const App = struct {
    pub fn handle(self: *App, req: *talon.http.Request, res: *talon.http.Response) !void {
        _ = self;
        _ = req;
        try res.respond("Hello from talon!\n", .{
            .extra_headers = &.{
                .{ .name = "content-type", .value = "text/plain; charset=utf-8" },
            },
        });
    }
};

fn signalWatcher(server: *talon.http.Server(App)) !void {
    var sig = try zio.Signal.init(.interrupt);
    defer sig.deinit();
    try sig.wait();
    std.log.info("SIGINT received, draining connections...", .{});
    server.shutdown();
}

pub fn main(init: std.process.Init) !void {
    const rt = try zio.Runtime.init(init.gpa, .{
        // §5.4: 256KB default committed stacks are the largest hidden cost at
        // 10k connections; 64KB is the engine's working point.
        .stack_pool = .{
            .maximum_size = 8 * 1024 * 1024,
            .committed_size = 64 * 1024,
        },
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

    std.log.info("talon http listening on http://{f} (Ctrl+C to stop)", .{addr});
    try server.serve(&listener);
    std.log.info("bye", .{});
}

//! RESP echo server using only `talon.core` — a non-HTTP protocol on the bare
//! engine contract: listener, connection limits, graceful shutdown, framing
//! toolbox, with zero use of talon's HTTP layer (design doc §10). The
//! core-compiles-without-http guarantee is enforced by the isolated core test
//! build in build.zig.
//!
//! M1: line handling rides framing.Delimited (§8) instead of hand-rolled
//! delimiter scanning. Speaks RESP inline commands; try:
//!   zig build run-resp
//!   redis-cli -p 6380 ping
//!   redis-cli -p 6380 echo hello

const std = @import("std");
const zio = @import("zio");
const core = @import("talon").core;

// No `zio.debug_io` override here — see http.zig for why.

const App = struct {
    commands: std.atomic.Value(u64) = .init(0),
};

const Lines = core.framing.Delimited(.{ .delimiter = "\r\n", .max_frame = 512 });

const RespEchoProto = struct {
    pub fn serve(conn: anytype, app: *App) anyerror!void {
        const w = conn.writer();
        var lines = Lines.init(conn.reader());

        while (true) {
            // Request-boundary idle wait: lets shutdown interrupt idle
            // connections instead of waiting out the drain timeout.
            conn.waitReadable(conn.limits.keep_alive_timeout) catch return;

            const line = lines.next() catch |err| switch (err) {
                error.PartialFrame => return, // peer vanished mid-line
                else => return err,
            } orelse return; // clean EOF
            if (line.len == 0) continue;
            _ = app.commands.fetchAdd(1, .monotonic);

            if (std.ascii.eqlIgnoreCase(line, "ping")) {
                try w.writeAll("+PONG\r\n");
            } else if (std.ascii.startsWithIgnoreCase(line, "echo ")) {
                try w.print("+{s}\r\n", .{line["echo ".len..]});
            } else {
                try w.print("+{s}\r\n", .{line});
            }
            try w.flush();

            if (conn.isShuttingDown()) return;
        }
    }
};

const Server = core.StreamServer(RespEchoProto, App);

fn signalWatcher(server: *Server) !void {
    var sig = try zio.Signal.init(.interrupt);
    defer sig.deinit();
    try sig.wait();
    std.log.info("SIGINT received, draining connections...", .{});
    server.shutdown();
}

pub fn main(init: std.process.Init) !void {
    const rt = try zio.Runtime.init(init.gpa, .{
        .stack_pool = .{
            .maximum_size = 8 * 1024 * 1024,
            .committed_size = 64 * 1024,
        },
    });
    defer rt.deinit();

    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 6380);
    var listener = try core.TcpListener.listen(addr, .{});

    var app: App = .{};
    var server = try Server.init(init.gpa, &app, .{});
    defer server.deinit();

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(signalWatcher, .{&server});

    std.log.info("talon resp-echo listening on {f} (Ctrl+C to stop)", .{addr});
    try server.serve(&listener);
    std.log.info("served {d} commands, bye", .{app.commands.load(.monotonic)});
}

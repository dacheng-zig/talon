//! talon HTTP client micro-benchmark: in-process pool-hit throughput.
//!
//! Drives the talon server with the talon client over MemoryConnector — no
//! sockets, no kernel noise — to measure the pure protocol + pool-reuse cost
//! (the §8 "in-process ceiling" reference). Every request after the first
//! reuses the single pooled keep-alive connection, so this exercises the
//! zero-redial hot path.
//!
//! Run: zig build run-http_client_bench
//!
//! Note: a micro-benchmark, not a rigorous comparison. The headline number is
//! in-process round-trips/sec; treat it as a relative signal across changes,
//! not an absolute. A socket-based comparison vs std.http.Client belongs in a
//! dedicated harness (C1 acceptance), out of scope for this example.

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const client = talon.http.client;

const App = struct {
    pub fn handle(self: *App, req: *talon.http.Request, res: *talon.http.Response) !void {
        _ = self;
        _ = req;
        try res.respond("hello from talon\n", .{
            .extra_headers = &.{.{ .name = "content-type", .value = "text/plain" }},
        });
    }
};

const Srv = talon.http.Server(App);
const iterations: u64 = 200_000;

fn runServer(server: *Srv, listener: *talon.MemoryListener) !void {
    try server.serve(listener);
}

fn runClient(listener: *talon.MemoryListener, server: *Srv) !void {
    var c = client.Client(client.MemoryConnector).init(std.heap.page_allocator, .{ .listener = listener }, .{});
    defer c.deinit();
    const origin: client.Origin = .{ .host = "memory", .port = 80 };

    // Warm up: establish (and pool) the connection the loop will reuse.
    {
        var resp = try c.get(origin, "/");
        resp.deinit();
    }

    var sw = zio.Stopwatch.start();
    var i: u64 = 0;
    while (i < iterations) : (i += 1) {
        var resp = try c.get(origin, "/");
        resp.deinit(); // drains the body and returns the connection to the pool
    }
    const elapsed_ns = sw.read().toNanoseconds();

    const secs = @as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_s;
    const rps = @as(f64, @floatFromInt(iterations)) / secs;
    const stats = c.poolStats();
    std.log.info("pool-hit GET x{d}: {d:.1} ms total, {d:.0} req/s", .{
        iterations, secs * 1000.0, rps,
    });
    std.log.info("pool: created={d} reused={d} evicted={d} (1 dial, rest reused)", .{
        stats.created, stats.reused, stats.evicted,
    });

    server.shutdown();
}

pub fn main(init: std.process.Init) !void {
    const rt = try zio.Runtime.init(init.gpa, .{});
    defer rt.deinit();

    var listener = try talon.MemoryListener.init(init.gpa, .{});
    defer listener.deinit();

    var app: App = .{};
    var server = try Srv.init(init.gpa, &app, .{});
    defer server.deinit();

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(runServer, .{ &server, &listener });
    try group.spawn(runClient, .{ &listener, &server });
    try group.wait();
}

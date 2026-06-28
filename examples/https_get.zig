//! talon HTTPS client demo: fetch real https URLs over TLS with `getUrl`.
//!
//! The TLS counterpart of `http_get`: it builds a `TlsClient` (std
//! `crypto.tls.Client` over zio sockets) that dials `https` over TLS and `http`
//! plaintext through one pool, verifying the server certificate against the OS
//! trust store. Redirects, gzip/deflate decode, and keep-alive pooling work
//! exactly as for plain HTTP.
//!
//! Run:
//!   zig build run-https_get                          # defaults to https://example.com/
//!   zig build run-https_get -- https://example.com/ https://www.iana.org/
//!
//! Trust: certificates are verified against the system root store. To talk to a
//! server with a self-signed or otherwise unverifiable certificate, swap the
//! connector's `verification` to `.self_signed` or `.insecure_no_verification`
//! (test/dev only — the latter defeats TLS authentication).

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const client = talon.http.client;

const default_url = "https://example.com/";
const body_preview_limit = 1024;

fn fetchAll(gpa: std.mem.Allocator, io: std.Io, urls: []const []const u8) !void {
    // Scan the OS trust store once; the connector borrows it for every dial.
    var store: client.RootStore = .{};
    try store.load(gpa, io);
    defer store.deinit(gpa);

    // One long-lived TLS client shared across requests: same-origin requests
    // reuse the pooled keep-alive TLS connection (amortizing the handshake).
    var c = client.TlsClient.init(gpa, .{
        .gpa = gpa,
        .io = io,
        .verification = .{ .system = &store },
    }, .{});
    defer c.deinit();

    for (urls) |url| fetchOne(gpa, &c, url) catch |err|
        std.log.err("{s}: {s}", .{ url, @errorName(err) });

    const stats = c.poolStats();
    std.log.info("pool: created={d} reused={d} evicted={d}", .{
        stats.created, stats.reused, stats.evicted,
    });
}

fn fetchOne(gpa: std.mem.Allocator, c: *client.TlsClient, url: []const u8) !void {
    var resp = try c.getUrl(url);
    defer resp.deinit(); // drains the body, returns the connection to the pool

    std.log.info("GET {s} -> {d} {s}", .{ url, resp.status(), resp.reason() });
    if (resp.header("content-type")) |v| std.log.info("  content-type: {s}", .{v});
    if (resp.header("content-length")) |v| std.log.info("  content-length: {s}", .{v});
    if (resp.header("server")) |v| std.log.info("  server: {s}", .{v});

    var body: std.Io.Writer.Allocating = .init(gpa);
    defer body.deinit();
    _ = try resp.bodyReader().streamRemaining(&body.writer);

    const full = body.written();
    const shown = full[0..@min(full.len, body_preview_limit)];
    std.log.info("  body ({d} bytes):\n{s}{s}", .{
        full.len,
        shown,
        if (full.len > shown.len) "\n  … (truncated)" else "",
    });
}

pub fn main(init: std.process.Init) !void {
    const rt = try zio.Runtime.init(init.gpa, .{});
    defer rt.deinit();

    const argv = try init.minimal.args.toSlice(init.arena.allocator());
    const urls: []const []const u8 = if (argv.len > 1)
        @ptrCast(argv[1..])
    else
        &.{default_url};

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(fetchAll, .{ init.gpa, rt.io(), urls });
    try group.wait();
}

//! talon HTTP client demo: fetch real URLs over the network with `getUrl`.
//!
//! Unlike `http_client` (which drives an in-process talon server), this is the
//! everyday use of the client: point it at an absolute URL string and read the
//! response. The client follows redirects, decompresses gzip/deflate, and pools
//! keep-alive connections — so passing several URLs on one origin reuses a
//! single socket.
//!
//! Run:
//!   zig build run-http_get                       # defaults to http://example.com/
//!   zig build run-http_get -- http://info.cern.ch/ http://example.com/
//!
//! Note: this demo uses the plain `TcpConnector`, so `https://` URLs fail with
//! `error.TlsNotSupported`. Use a plain-HTTP endpoint that does not redirect to
//! HTTPS — e.g. http://example.com/ or http://info.cern.ch/ — or see the
//! `https_get` example (TlsClient) for TLS.

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const client = talon.http.client;

/// Default target when no URL is given on the command line.
const default_url = "http://example.com/";
/// Cap on printed body bytes — keep the demo output readable for large pages.
const body_preview_limit = 1024;

fn fetchAll(gpa: std.mem.Allocator, urls: []const []const u8) !void {
    // One long-lived client shared across every request: requests to the same
    // origin reuse the pooled keep-alive connection (defaults are sensible —
    // 30s read/write, 60s total, 10 redirects, gzip decode on).
    var c = client.Client(client.TcpConnector).init(gpa, .{}, .{});
    defer c.deinit();

    for (urls) |url| fetchOne(gpa, &c, url) catch |err| switch (err) {
        // Turn the one footgun into an actionable hint instead of a bare error.
        error.TlsNotSupported => std.log.err(
            "{s}: HTTPS is not supported yet — use a plain http:// URL",
            .{url},
        ),
        else => std.log.err("{s}: {s}", .{ url, @errorName(err) }),
    };

    // Surfaces connection reuse: created < total requests means the pool worked.
    const stats = c.poolStats();
    std.log.info("pool: created={d} reused={d} evicted={d}", .{
        stats.created, stats.reused, stats.evicted,
    });
}

fn fetchOne(gpa: std.mem.Allocator, c: *client.Client(client.TcpConnector), url: []const u8) !void {
    var resp = try c.getUrl(url);
    defer resp.deinit(); // drains the body, returns the connection to the pool

    std.log.info("GET {s} -> {d} {s}", .{ url, resp.status(), resp.reason() });
    if (resp.header("content-type")) |v| std.log.info("  content-type: {s}", .{v});
    if (resp.header("content-length")) |v| std.log.info("  content-length: {s}", .{v});
    if (resp.header("server")) |v| std.log.info("  server: {s}", .{v});

    // bodyReader yields the decoded stream when the server used gzip/deflate.
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

    // argv[1..] are URLs; fall back to the default when none are given. The
    // arena lives for the whole process, so these slices stay valid.
    const argv = try init.minimal.args.toSlice(init.arena.allocator());
    const urls: []const []const u8 = if (argv.len > 1)
        @ptrCast(argv[1..])
    else
        &.{default_url};

    // The client suspends on socket I/O, so it must run inside the zio runtime.
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(fetchAll, .{ init.gpa, urls });
    try group.wait();
}

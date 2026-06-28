//! Integration tests for talon.http.client (closed loop): the
//! client drives the talon server in-process over MemoryConnector (no sockets)
//! and over TCP. Exercises the full outbound path — request encode, response
//! parse, streaming body — against the real server.

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const client = talon.http.client;
const Server = talon.http.Server;
const Request = talon.http.Request;
const Response = talon.http.Response;
const MemoryListener = talon.MemoryListener;
const TcpListener = talon.TcpListener;

const HelloApp = struct {
    hits: u32 = 0,
    pub fn handle(self: *HelloApp, req: *Request, res: *Response) !void {
        _ = req;
        self.hits += 1;
        try res.respond("hello from talon\n", .{
            .extra_headers = &.{.{ .name = "content-type", .value = "text/plain" }},
        });
    }
};

const EchoApp = struct {
    pub fn handle(self: *EchoApp, req: *Request, res: *Response) !void {
        _ = self;
        var collected: std.Io.Writer.Allocating = .init(req.arena);
        _ = try req.bodyReader().streamRemaining(&collected.writer);
        try res.respond(collected.written(), .{});
    }
};

fn readBody(resp: anytype) ![]u8 {
    var collected: std.Io.Writer.Allocating = .init(std.testing.allocator);
    errdefer collected.deinit();
    _ = try resp.bodyReader().streamRemaining(&collected.writer);
    return collected.toOwnedSlice();
}

test "client: GET/POST/HEAD round-trips over MemoryConnector (in-process server)" {
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
            const origin: client.Origin = .{ .host = "memory", .port = 80 };

            // POST: the echo server returns the body verbatim.
            {
                var resp = try c.post(origin, "/echo", "hello talon");
                defer resp.deinit();
                try std.testing.expectEqual(@as(u16, 200), resp.status());
                const body = try readBody(resp);
                defer std.testing.allocator.free(body);
                try std.testing.expectEqualStrings("hello talon", body);
            }

            // GET with no body: echo server replies with an empty 200.
            {
                var resp = try c.get(origin, "/");
                defer resp.deinit();
                try std.testing.expectEqual(@as(u16, 200), resp.status());
                const body = try readBody(resp);
                defer std.testing.allocator.free(body);
                try std.testing.expectEqualStrings("", body);
            }

            // HEAD: head present, body suppressed by the server and by the
            // client's response framing (responseHasBody == false for HEAD).
            {
                var resp = try c.head(origin, "/echo");
                defer resp.deinit();
                try std.testing.expectEqual(@as(u16, 200), resp.status());
                try std.testing.expect(resp.header("content-length") != null);
                const body = try readBody(resp);
                defer std.testing.allocator.free(body);
                try std.testing.expectEqualStrings("", body);
            }

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

test "client: max_response_body caps an oversized response body (DoS guard)" {
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
            c.max_response_body = 8; // tiny cap; echo of a 64-byte body exceeds it

            const origin: client.Origin = .{ .host = "memory", .port = 80 };
            // The echo reply declares content-length: 64 > cap 8, so the client
            // rejects it in readResponse before streaming any body byte.
            try std.testing.expectError(
                error.ResponseBodyTooLarge,
                c.post(origin, "/echo", "0123456789" ** 6 ++ "0123"), // 64 bytes
            );

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

// Records what a request middleware did, observed after the chain runs.
var g_mw_order: [4]u8 = undefined;
var g_mw_order_n: usize = 0;
var g_mw_observed_status: u16 = 0;

fn mwMark(c: u8) void {
    g_mw_order[g_mw_order_n] = c;
    g_mw_order_n += 1;
}

// Mutates the outbound request before `next` (inbound) and reads the response
// after `next` (outbound) — exercises the around-style request middleware seam.
const InjectAndObserveMw = struct {
    pub fn run(ctx: anytype, next: anytype) !void {
        mwMark('>');
        ctx.spec.extra_headers = &.{.{ .name = "x-injected", .value = "yes" }};
        try next.call(ctx);
        mwMark('<');
        if (ctx.response) |r| g_mw_observed_status = r.status();
    }
};

const HeaderProbeApp = struct {
    saw_injected: bool = false,
    pub fn handle(self: *HeaderProbeApp, req: *Request, res: *Response) !void {
        if (req.header("x-injected") != null) self.saw_injected = true;
        try res.respond("ok", .{});
    }
};

test "client: request middleware mutates the outbound request and observes the response" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    g_mw_order_n = 0;
    g_mw_observed_status = 0;

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: HeaderProbeApp = .{};
    const Srv = Server(HeaderProbeApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn runClient(l: *MemoryListener, s: *Srv) !void {
            const Client = client.ClientWith(client.MemoryConnector, .{InjectAndObserveMw});
            var c = Client.init(std.testing.allocator, .{ .listener = l }, .{});
            defer c.deinit();

            var resp = try c.get(.{ .host = "memory", .port = 80 }, "/");
            defer resp.deinit();
            try std.testing.expectEqual(@as(u16, 200), resp.status());
            const body = try readBody(resp);
            defer std.testing.allocator.free(body);
            try std.testing.expectEqualStrings("ok", body);

            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.runClient, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());

    // Inbound mutation reached the server; outbound saw the final status;
    // around-ordering is inbound ('>') then outbound ('<').
    try std.testing.expect(app.saw_injected);
    try std.testing.expectEqual(@as(u16, 200), g_mw_observed_status);
    try std.testing.expectEqual(@as(usize, 2), g_mw_order_n);
    try std.testing.expectEqual(@as(u8, '>'), g_mw_order[0]);
    try std.testing.expectEqual(@as(u8, '<'), g_mw_order[1]);
}

// Precomputed payloads (generated with python gzip/zlib) so the test needs no
// runtime compressor (the std flate compressor wants a 64 KiB window buffer).
const gzip_plain = "hello from a gzipped talon response body";
const gzip_bytes = [_]u8{ 31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 203, 72, 205, 201, 201, 87, 72, 43, 202, 207, 85, 72, 84, 72, 175, 202, 44, 40, 72, 77, 81, 40, 73, 204, 201, 207, 83, 40, 74, 45, 46, 200, 207, 43, 78, 85, 72, 202, 79, 169, 4, 0, 174, 232, 138, 93, 40, 0, 0, 0 };
const deflate_plain = "deflated body";
const deflate_bytes = [_]u8{ 120, 156, 75, 73, 77, 203, 73, 44, 73, 77, 81, 72, 202, 79, 169, 4, 0, 35, 81, 5, 8 };
// zstd has no std compressor, only a decompressor — blob generated with the
// `zstd -19` CLI (std flate has the same one-way limit, hence the gzip/zlib
// blobs above).
const zstd_plain = "hello from a zstd-compressed talon response body";
const zstd_bytes = [_]u8{ 40, 181, 47, 253, 4, 104, 129, 1, 0, 104, 101, 108, 108, 111, 32, 102, 114, 111, 109, 32, 97, 32, 122, 115, 116, 100, 45, 99, 111, 109, 112, 114, 101, 115, 115, 101, 100, 32, 116, 97, 108, 111, 110, 32, 114, 101, 115, 112, 111, 110, 115, 101, 32, 98, 111, 100, 121, 199, 131, 122, 135 };

const CompressedApp = struct {
    pub fn handle(self: *CompressedApp, req: *Request, res: *Response) !void {
        _ = self;
        if (std.mem.eql(u8, req.target(), "/deflate")) {
            try res.respond(&deflate_bytes, .{ .extra_headers = &.{.{ .name = "content-encoding", .value = "deflate" }} });
        } else if (std.mem.eql(u8, req.target(), "/zstd")) {
            try res.respond(&zstd_bytes, .{ .extra_headers = &.{.{ .name = "content-encoding", .value = "zstd" }} });
        } else {
            try res.respond(&gzip_bytes, .{ .extra_headers = &.{.{ .name = "content-encoding", .value = "gzip" }} });
        }
    }
};

test "client: transparently decompresses gzip/deflate/zstd, and passes through when disabled" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: CompressedApp = .{};
    const Srv = Server(CompressedApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn runClient(l: *MemoryListener, s: *Srv) !void {
            const Client = client.Client(client.MemoryConnector);
            const origin: client.Origin = .{ .host = "memory", .port = 80 };

            // Default: decode gzip and deflate transparently.
            {
                var c = Client.init(std.testing.allocator, .{ .listener = l }, .{});
                defer c.deinit();
                {
                    var resp = try c.get(origin, "/gzip");
                    defer resp.deinit();
                    const body = try readBody(resp);
                    defer std.testing.allocator.free(body);
                    try std.testing.expectEqualStrings(gzip_plain, body);
                }
                {
                    var resp = try c.get(origin, "/deflate");
                    defer resp.deinit();
                    const body = try readBody(resp);
                    defer std.testing.allocator.free(body);
                    try std.testing.expectEqualStrings(deflate_plain, body);
                }
                {
                    var resp = try c.get(origin, "/zstd");
                    defer resp.deinit();
                    const body = try readBody(resp);
                    defer std.testing.allocator.free(body);
                    try std.testing.expectEqualStrings(zstd_plain, body);
                }
            }
            // Disabled: the caller receives the raw compressed bytes.
            {
                var c = Client.init(std.testing.allocator, .{ .listener = l }, .{ .decompress = false });
                defer c.deinit();
                var resp = try c.get(origin, "/gzip");
                defer resp.deinit();
                const body = try readBody(resp);
                defer std.testing.allocator.free(body);
                try std.testing.expectEqualSlices(u8, &gzip_bytes, body);
            }

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

const JsonApp = struct {
    pub fn handle(self: *JsonApp, req: *Request, res: *Response) !void {
        _ = self;
        _ = req;
        try res.respond(
            \\{"name":"talon","count":42}
        , .{ .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} });
    }
};

test "client: Response.json parses a typed body" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: JsonApp = .{};
    const Srv = Server(JsonApp);
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

            var resp = try c.get(.{ .host = "memory", .port = 80 }, "/");
            defer resp.deinit();
            const Payload = struct { name: []const u8, count: i64 };
            const parsed = try resp.json(Payload, std.testing.allocator, 1 << 20);
            defer parsed.deinit();
            try std.testing.expectEqualStrings("talon", parsed.value.name);
            try std.testing.expectEqual(@as(i64, 42), parsed.value.count);

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

const TargetEchoApp = struct {
    pub fn handle(self: *TargetEchoApp, req: *Request, res: *Response) !void {
        _ = self;
        try res.respond(req.target(), .{}); // echo the request target back as the body
    }
};

test "client: getUrl parses a full URL into origin + target" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: TargetEchoApp = .{};
    const Srv = Server(TargetEchoApp);
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

            // Path + query parsed out of the URL and sent as the target.
            {
                var resp = try c.getUrl("http://memory:80/items?limit=20");
                defer resp.deinit();
                const body = try readBody(resp);
                defer std.testing.allocator.free(body);
                try std.testing.expectEqualStrings("/items?limit=20", body);
            }
            // Query with no path normalizes to "/?...".
            {
                var resp = try c.getUrl("http://memory:80?q=1");
                defer resp.deinit();
                const body = try readBody(resp);
                defer std.testing.allocator.free(body);
                try std.testing.expectEqualStrings("/?q=1", body);
            }
            // A non-absolute URL is rejected rather than silently mishandled.
            try std.testing.expectError(error.InvalidUrl, c.getUrl("/relative/only"));

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

const CookieApp = struct {
    pub fn handle(self: *CookieApp, req: *Request, res: *Response) !void {
        _ = self;
        // Echo any received Cookie back as the body, and always set one.
        const echoed = req.header("cookie") orelse "none";
        try res.respond(echoed, .{
            .extra_headers = &.{.{ .name = "set-cookie", .value = "sid=abc123; Path=/" }},
        });
    }
};

test "client: cookies middleware stores Set-Cookie and resends it" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: CookieApp = .{};
    const Srv = Server(CookieApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn runClient(l: *MemoryListener, s: *Srv) !void {
            var jar = client.CookieJar.init(std.testing.allocator);
            defer jar.deinit();

            const Client = client.ClientWith(client.MemoryConnector, .{client.cookies});
            var c = Client.init(std.testing.allocator, .{ .listener = l }, .{ .cookie_jar = &jar });
            defer c.deinit();
            const origin: client.Origin = .{ .host = "memory", .port = 80 };

            // Request 1: no cookie yet; server sets sid=abc123.
            {
                var resp = try c.get(origin, "/");
                defer resp.deinit();
                const body = try readBody(resp);
                defer std.testing.allocator.free(body);
                try std.testing.expectEqualStrings("none", body);
            }
            // Request 2: the jar replays the stored cookie, which the server echoes.
            {
                var resp = try c.get(origin, "/");
                defer resp.deinit();
                const body = try readBody(resp);
                defer std.testing.allocator.free(body);
                try std.testing.expectEqualStrings("sid=abc123", body);
            }

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

const BearerProbeApp = struct {
    ok: bool = false,
    pub fn handle(self: *BearerProbeApp, req: *Request, res: *Response) !void {
        if (req.header("authorization")) |v| self.ok = std.mem.eql(u8, v, "Bearer s3cr3t");
        try res.respond("ok", .{});
    }
};

test "client: bearerAuth built-in middleware adds the Authorization header" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: BearerProbeApp = .{};
    const Srv = Server(BearerProbeApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn runClient(l: *MemoryListener, s: *Srv) !void {
            const Client = client.ClientWith(client.MemoryConnector, .{client.bearerAuth("s3cr3t")});
            var c = Client.init(std.testing.allocator, .{ .listener = l }, .{});
            defer c.deinit();

            var resp = try c.get(.{ .host = "memory", .port = 80 }, "/");
            defer resp.deinit();
            try std.testing.expectEqual(@as(u16, 200), resp.status());
            const body = try readBody(resp);
            defer std.testing.allocator.free(body);

            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.runClient, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expect(app.ok); // server received "Authorization: Bearer s3cr3t"
}

const RedirectApp = struct {
    pub fn handle(self: *RedirectApp, req: *Request, res: *Response) !void {
        _ = self;
        if (std.mem.eql(u8, req.target(), "/final")) {
            try res.respond("arrived", .{});
        } else {
            try res.respond("", .{
                .status = .found,
                .extra_headers = &.{.{ .name = "location", .value = "/final" }},
            });
        }
    }
};

const LoopRedirectApp = struct {
    pub fn handle(self: *LoopRedirectApp, req: *Request, res: *Response) !void {
        _ = self;
        _ = req;
        try res.respond("", .{
            .status = .found,
            .extra_headers = &.{.{ .name = "location", .value = "/loop" }},
        });
    }
};

test "client: follows a same-origin redirect to the final resource" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: RedirectApp = .{};
    const Srv = Server(RedirectApp);
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

            var resp = try c.get(.{ .host = "memory", .port = 80 }, "/start");
            defer resp.deinit();
            try std.testing.expectEqual(@as(u16, 200), resp.status());
            const body = try readBody(resp);
            defer std.testing.allocator.free(body);
            try std.testing.expectEqualStrings("arrived", body);

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

test "client: stops at redirect.max and returns the redirect response" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: LoopRedirectApp = .{};
    const Srv = Server(LoopRedirectApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn runClient(l: *MemoryListener, s: *Srv) !void {
            const Client = client.Client(client.MemoryConnector);
            var c = Client.init(std.testing.allocator, .{ .listener = l }, .{ .redirect = .{ .max = 2 } });
            defer c.deinit();

            // Server always redirects; after 2 hops the client gives up and
            // hands back the redirect response rather than looping forever.
            var resp = try c.get(.{ .host = "memory", .port = 80 }, "/start");
            defer resp.deinit();
            try std.testing.expectEqual(@as(u16, 302), resp.status());

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

const RedirectToPortApp = struct {
    port_b: u16,
    pub fn handle(self: *RedirectToPortApp, req: *Request, res: *Response) !void {
        _ = req;
        var buf: [64]u8 = undefined;
        const loc = try std.fmt.bufPrint(&buf, "http://127.0.0.1:{d}/", .{self.port_b});
        try res.respond("", .{
            .status = .found,
            .extra_headers = &.{.{ .name = "location", .value = loc }},
        });
    }
};

const AuthProbeApp = struct {
    saw_auth: bool = false,
    pub fn handle(self: *AuthProbeApp, req: *Request, res: *Response) !void {
        if (req.header("authorization") != null) self.saw_auth = true;
        try res.respond("at B", .{});
    }
};

test "client: cross-origin redirect strips Authorization (security default)" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener_a = try TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    var listener_b = try TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    const port_a = listener_a.server.socket.address.ip.getPort();
    const port_b = listener_b.server.socket.address.ip.getPort();

    var app_a: RedirectToPortApp = .{ .port_b = port_b };
    var app_b: AuthProbeApp = .{};
    const SrvA = Server(RedirectToPortApp);
    const SrvB = Server(AuthProbeApp);
    var server_a = try SrvA.init(std.testing.allocator, &app_a, .{});
    defer server_a.deinit();
    var server_b = try SrvB.init(std.testing.allocator, &app_b, .{});
    defer server_b.deinit();

    const Fns = struct {
        fn runA(s: *SrvA, l: *TcpListener) !void {
            try s.serve(l);
        }
        fn runB(s: *SrvB, l: *TcpListener) !void {
            try s.serve(l);
        }
        fn runClient(pa: u16, sa: *SrvA, sb: *SrvB) !void {
            const Client = client.Client(client.TcpConnector);
            var c = Client.init(std.testing.allocator, .{}, .{});
            defer c.deinit();

            // Server A (port a) redirects to server B (port b) — a different
            // origin — so the Authorization header must not be forwarded.
            var resp = try c.request(.{
                .method = .GET,
                .origin = .{ .host = "127.0.0.1", .port = pa },
                .target = "/",
                .extra_headers = &.{.{ .name = "authorization", .value = "Bearer secret" }},
            });
            defer resp.deinit();
            try std.testing.expectEqual(@as(u16, 200), resp.status());
            const body = try readBody(resp);
            defer std.testing.allocator.free(body);
            try std.testing.expectEqualStrings("at B", body);

            sa.shutdown();
            sb.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runA, .{ &server_a, &listener_a });
    try group.spawn(Fns.runB, .{ &server_b, &listener_b });
    try group.spawn(Fns.runClient, .{ port_a, &server_a, &server_b });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    // Server B must never have seen host A's credentials.
    try std.testing.expect(!app_b.saw_auth);
}

test "client: pooled connection is reused across sequential requests" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: HelloApp = .{};
    const Srv = Server(HelloApp);
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
            const origin: client.Origin = .{ .host = "memory", .port = 80 };

            const n: u32 = 4;
            var i: u32 = 0;
            while (i < n) : (i += 1) {
                var resp = try c.get(origin, "/");
                try std.testing.expectEqual(@as(u16, 200), resp.status());
                const body = try readBody(resp);
                std.testing.allocator.free(body);
                resp.deinit(); // returns the connection to the pool
            }

            // One dial, n-1 reuses; the server saw all n on a single keep-alive
            // connection (the pool reused it instead of redialing).
            const stats = c.poolStats();
            try std.testing.expectEqual(@as(u64, 1), stats.created);
            try std.testing.expectEqual(@as(u64, n - 1), stats.reused);
            try std.testing.expectEqual(@as(u64, 0), stats.evicted);

            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.runClient, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    // The server saw all 4 requests on the single reused keep-alive connection.
    try std.testing.expectEqual(@as(u32, 4), app.hits);
}

test "client: reapIdle closes idle connections past the idle timeout" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: HelloApp = .{};
    const Srv = Server(HelloApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn runClient(l: *MemoryListener, s: *Srv) !void {
            const Client = client.Client(client.MemoryConnector);
            var c = Client.init(std.testing.allocator, .{ .listener = l }, .{
                .pool = .{ .idle_timeout = .fromMilliseconds(20) },
            });
            defer c.deinit();
            const origin: client.Origin = .{ .host = "memory", .port = 80 };

            // Pool one connection, then let it age past the idle timeout.
            {
                var resp = try c.get(origin, "/");
                const body = try readBody(resp);
                std.testing.allocator.free(body);
                resp.deinit();
            }
            try std.testing.expectEqual(@as(usize, 0), c.reapIdle()); // not yet expired
            zio.sleep(.fromMilliseconds(60)) catch {};
            try std.testing.expectEqual(@as(usize, 1), c.reapIdle()); // now reaped
            try std.testing.expectEqual(@as(usize, 0), c.reapIdle()); // nothing left

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

test "client: max_per_origin throttles concurrent in-flight requests" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: HelloApp = .{};
    const Srv = Server(HelloApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn runClient(l: *MemoryListener, s: *Srv) !void {
            const Client = client.Client(client.MemoryConnector);
            var c = Client.init(std.testing.allocator, .{ .listener = l }, .{
                .pool = .{ .max_per_origin = 1, .pool_wait = .fromMilliseconds(50) },
            });
            defer c.deinit();
            const origin: client.Origin = .{ .host = "memory", .port = 80 };

            // Hold the single permit by keeping resp1 alive (its connection is
            // checked out).
            var resp1 = try c.get(origin, "/");
            // A second concurrent checkout finds no permit and times out.
            try std.testing.expectError(error.PoolWaitTimeout, c.get(origin, "/"));

            // Releasing resp1 frees the permit; the next request proceeds.
            resp1.deinit();
            var resp2 = try c.get(origin, "/");
            defer resp2.deinit();
            try std.testing.expectEqual(@as(u16, 200), resp2.status());
            const body = try readBody(resp2);
            defer std.testing.allocator.free(body);

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

test "client: connection:close response is not pooled (redials)" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: HelloApp = .{};
    const Srv = Server(HelloApp);
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
            const origin: client.Origin = .{ .host = "memory", .port = 80 };

            // Request with keep_alive=false: the server echoes `connection:
            // close`, so this connection must not be pooled.
            {
                var resp = try c.request(.{ .origin = origin, .keep_alive = false });
                try std.testing.expectEqual(@as(u16, 200), resp.status());
                const body = try readBody(resp);
                std.testing.allocator.free(body);
                resp.deinit();
            }
            // The next request finds no idle connection and dials a fresh one.
            {
                var resp = try c.get(origin, "/");
                const body = try readBody(resp);
                std.testing.allocator.free(body);
                resp.deinit();
            }

            const stats = c.poolStats();
            try std.testing.expectEqual(@as(u64, 2), stats.created);
            try std.testing.expectEqual(@as(u64, 0), stats.reused);

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

test "client: GET over TcpConnector with response headers" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    const port = listener.server.socket.address.ip.getPort();

    var app: HelloApp = .{};
    const Srv = Server(HelloApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *TcpListener) !void {
            try s.serve(l);
        }

        fn runClient(p: u16, s: *Srv) !void {
            const Client = client.Client(client.TcpConnector);
            var c = Client.init(std.testing.allocator, .{}, .{});
            defer c.deinit();

            var resp = try c.get(.{ .host = "127.0.0.1", .port = p }, "/");
            defer resp.deinit();
            try std.testing.expectEqual(@as(u16, 200), resp.status());
            try std.testing.expectEqualStrings("OK", resp.reason());
            try std.testing.expectEqualStrings("text/plain", resp.header("content-type").?);

            const body = try readBody(resp);
            defer std.testing.allocator.free(body);
            try std.testing.expectEqualStrings("hello from talon\n", body);

            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.runClient, .{ port, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(@as(u32, 1), app.hits);
}

test "client: idempotent request retries a server-closed pooled connection" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    const port = listener.server.socket.address.ip.getPort();

    var app: HelloApp = .{};
    const Srv = Server(HelloApp);
    // Short keep-alive so the server closes the idle pooled connection between
    // the two requests (the stale-connection case retry exists to absorb).
    var server = try Srv.init(std.testing.allocator, &app, .{
        .limits = .{ .keep_alive_timeout = .fromMilliseconds(50) },
    });
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *TcpListener) !void {
            try s.serve(l);
        }

        fn runClient(p: u16, s: *Srv) !void {
            const Client = client.Client(client.TcpConnector);
            var c = Client.init(std.testing.allocator, .{}, .{});
            defer c.deinit();
            const origin: client.Origin = .{ .host = "127.0.0.1", .port = p };

            // Request 1 dials and pools the connection on deinit.
            {
                var resp = try c.get(origin, "/");
                const body = try readBody(resp);
                std.testing.allocator.free(body);
                resp.deinit();
            }

            // Wait out the server's idle close. The server checks its keep-alive
            // budget on a ~1s poll tick, so sleep comfortably past that.
            zio.sleep(.fromMilliseconds(1300)) catch {};

            // Request 2 checks out the now-dead pooled connection; the first
            // attempt fails and the client transparently retries on a fresh one,
            // so the caller still gets a clean 200.
            {
                var resp = try c.get(origin, "/");
                try std.testing.expectEqual(@as(u16, 200), resp.status());
                const body = try readBody(resp);
                std.testing.allocator.free(body);
                resp.deinit();
            }

            const stats = c.poolStats();
            try std.testing.expectEqual(@as(u64, 2), stats.created); // req1 dial + retry dial
            try std.testing.expect(stats.reused >= 1); // the dead-connection reuse attempt

            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.runClient, .{ port, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    // Two requests reached a handler: req1, and req2's retry (its first attempt
    // hit the closed connection and never reached the server).
    try std.testing.expectEqual(@as(u32, 2), app.hits);
}

test "client: validate_on_checkout evicts a server-closed idle connection without reuse" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    const port = listener.server.socket.address.ip.getPort();

    var app: HelloApp = .{};
    const Srv = Server(HelloApp);
    var server = try Srv.init(std.testing.allocator, &app, .{
        .limits = .{ .keep_alive_timeout = .fromMilliseconds(50) },
    });
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *TcpListener) !void {
            try s.serve(l);
        }

        fn runClient(p: u16, s: *Srv) !void {
            const Client = client.Client(client.TcpConnector);
            var c = Client.init(std.testing.allocator, .{}, .{
                .pool = .{ .validate_on_checkout = true },
            });
            defer c.deinit();
            const origin: client.Origin = .{ .host = "127.0.0.1", .port = p };

            {
                var resp = try c.get(origin, "/");
                const body = try readBody(resp);
                std.testing.allocator.free(body);
                resp.deinit();
            }
            zio.sleep(.fromMilliseconds(1300)) catch {}; // let the server close the idle conn

            // The liveness probe detects the closed connection at checkout and
            // evicts it, so it is never reused (no failed attempt / retry).
            {
                var resp = try c.get(origin, "/");
                try std.testing.expectEqual(@as(u16, 200), resp.status());
                const body = try readBody(resp);
                std.testing.allocator.free(body);
                resp.deinit();
            }

            const stats = c.poolStats();
            try std.testing.expectEqual(@as(u64, 2), stats.created); // req1 + fresh dial
            try std.testing.expectEqual(@as(u64, 0), stats.reused); // dead conn never reused
            try std.testing.expect(stats.evicted >= 1); // probe evicted it

            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.runClient, .{ port, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

test "client: read timeout fires on a stalled server (no hang)" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    const port = listener.server.socket.address.ip.getPort();

    const Fns = struct {
        // Accepts the connection, reads the request, then stalls without ever
        // sending a response — the slow/stuck-server case the read timeout
        // exists to defend against.
        fn runStallServer(l: *TcpListener) !void {
            const conn = l.accept() catch return;
            defer conn.close();
            var buf: [1024]u8 = undefined;
            _ = conn.stream.read(&buf, .none) catch {};
            zio.sleep(.fromMilliseconds(300)) catch {};
        }

        fn runClient(p: u16, l: *TcpListener) !void {
            const Client = client.Client(client.TcpConnector);
            var c = Client.init(std.testing.allocator, .{}, .{
                .timeouts = .{ .read = .fromMilliseconds(50) },
            });
            defer c.deinit();

            // Must return an error (read deadline) rather than hang forever; a
            // hang would deadlock group.wait and fail the test by timeout.
            if (c.get(.{ .host = "127.0.0.1", .port = p }, "/")) |resp| {
                resp.deinit();
                return error.ExpectedReadTimeout;
            } else |_| {}

            l.close();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runStallServer, .{&listener});
    try group.spawn(Fns.runClient, .{ port, &listener });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

test "client: total deadline caps a generous per-read timeout" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    const port = listener.server.socket.address.ip.getPort();

    const Fns = struct {
        // Reads the request, then holds the connection open (a second blocking
        // read) until the client gives up and closes — never sending a byte.
        // No fixed sleep, so the only thing that can end the client's wait is
        // its own deadline: the test isolates `total`'s effect.
        fn runStallServer(l: *TcpListener) !void {
            const conn = l.accept() catch return;
            defer conn.close();
            var buf: [1024]u8 = undefined;
            _ = conn.stream.read(&buf, .none) catch {};
            _ = conn.stream.read(&buf, .none) catch {}; // returns on EOF when client closes
        }

        fn runClient(p: u16, l: *TcpListener) !void {
            const Client = client.Client(client.TcpConnector);
            // Per-read timeout is huge; the whole-request deadline is what must
            // fire, proving `total` caps the per-stage timeout. (Without total,
            // the client would block on the 30s read until the test timed out.)
            var c = Client.init(std.testing.allocator, .{}, .{
                .timeouts = .{ .read = .fromSeconds(30), .total = .fromMilliseconds(80) },
            });
            defer c.deinit();

            var sw = zio.Stopwatch.start();
            if (c.get(.{ .host = "127.0.0.1", .port = p }, "/")) |resp| {
                resp.deinit();
                return error.ExpectedTotalDeadline;
            } else |_| {}
            // Bounded by `total` (80ms), nowhere near the 30s read timeout.
            try std.testing.expect(sw.read().toNanoseconds() < 2 * std.time.ns_per_s);

            l.close();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runStallServer, .{&listener});
    try group.spawn(Fns.runClient, .{ port, &listener });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

// ── Default request headers + streaming upload ──────────────────────────────

const HeaderEchoApp = struct {
    pub fn handle(self: *HeaderEchoApp, req: *Request, res: *Response) !void {
        _ = self;
        const ua = req.header("user-agent") orelse "<none>";
        const acc = req.header("accept") orelse "<none>";
        const ae = req.header("accept-encoding") orelse "<none>";
        const body = try std.fmt.allocPrint(req.arena, "{s}|{s}|{s}", .{ ua, acc, ae });
        try res.respond(body, .{});
    }
};

test "client: sends policy default headers, caller overrides win" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: HeaderEchoApp = .{};
    const Srv = Server(HeaderEchoApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn runClient(l: *MemoryListener, s: *Srv) !void {
            const Client = client.Client(client.MemoryConnector);
            const origin: client.Origin = .{ .host = "memory", .port = 80 };

            // Defaults: User-Agent, Accept, and Accept-Encoding (decompress on).
            {
                var c = Client.init(std.testing.allocator, .{ .listener = l }, .{});
                defer c.deinit();
                var resp = try c.get(origin, "/");
                defer resp.deinit();
                const body = try readBody(resp);
                defer std.testing.allocator.free(body);
                try std.testing.expectEqualStrings("talon-http-client/1.0|*/*|gzip, deflate, zstd", body);
            }

            // decompress=off → no Accept-Encoding advertised.
            {
                var c = Client.init(std.testing.allocator, .{ .listener = l }, .{ .decompress = false });
                defer c.deinit();
                var resp = try c.get(origin, "/");
                defer resp.deinit();
                const body = try readBody(resp);
                defer std.testing.allocator.free(body);
                try std.testing.expectEqualStrings("talon-http-client/1.0|*/*|<none>", body);
            }

            // Caller-supplied headers override the defaults (no duplicates).
            {
                var c = Client.init(std.testing.allocator, .{ .listener = l }, .{});
                defer c.deinit();
                var resp = try c.request(.{ .origin = origin, .target = "/", .extra_headers = &.{
                    .{ .name = "user-agent", .value = "custom/9" },
                    .{ .name = "accept", .value = "application/json" },
                } });
                defer resp.deinit();
                const body = try readBody(resp);
                defer std.testing.allocator.free(body);
                try std.testing.expectEqualStrings("custom/9|application/json|gzip, deflate, zstd", body);
            }

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

test "client: streaming upload (Content-Length and chunked) round-trips" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: EchoApp = .{};
    const Srv = Server(EchoApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    // Larger than the connection write buffer (4 KiB) so the body streams
    // across multiple buffer fills rather than riding the head's flush.
    const payload = "talon-upload-" ** 1000;

    const Fns = struct {
        fn runServer(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn runClient(l: *MemoryListener, s: *Srv, body_payload: []const u8) !void {
            const Client = client.Client(client.MemoryConnector);
            var c = Client.init(std.testing.allocator, .{ .listener = l }, .{});
            defer c.deinit();
            const origin: client.Origin = .{ .host = "memory", .port = 80 };

            // Content-Length framed streaming (known length).
            {
                var src: std.Io.Reader = .fixed(body_payload);
                var resp = try c.request(.{
                    .method = .POST,
                    .origin = origin,
                    .target = "/echo",
                    .body = .{ .reader = .{ .reader = &src, .len = body_payload.len } },
                });
                defer resp.deinit();
                try std.testing.expectEqual(@as(u16, 200), resp.status());
                const echoed = try readBody(resp);
                defer std.testing.allocator.free(echoed);
                try std.testing.expectEqualStrings(body_payload, echoed);
            }

            // Chunked streaming (unknown length).
            {
                var src: std.Io.Reader = .fixed(body_payload);
                var resp = try c.request(.{
                    .method = .POST,
                    .origin = origin,
                    .target = "/echo",
                    .body = .{ .chunked = &src },
                });
                defer resp.deinit();
                try std.testing.expectEqual(@as(u16, 200), resp.status());
                const echoed = try readBody(resp);
                defer std.testing.allocator.free(echoed);
                try std.testing.expectEqualStrings(body_payload, echoed);
            }

            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.runClient, .{ &listener, &server, payload });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

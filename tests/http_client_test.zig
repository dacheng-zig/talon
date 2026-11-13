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

test "client: bodyless length metadata bypasses body cap and preserves pooled response boundaries" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    const F = struct {
        fn peer(l: *MemoryListener) !void {
            const raw = try l.accept();
            defer raw.close();
            var writer = raw.writer(&.{});
            writer.setTimeout(.fromSeconds(2));
            // Preload adjacent responses to expose any accidental body drain.
            try writer.interface.writeAll(
                "HTTP/1.1 200 OK\r\ncontent-length: 9999999999999999999\r\ncontent-encoding: gzip\r\n\r\n" ++
                    "HTTP/1.1 304 Not Modified\r\ncontent-length: 9999999999999999999\r\ncontent-encoding: gzip\r\n\r\n" ++
                    "HTTP/1.1 200 OK\r\ncontent-length: 1\r\n\r\nx",
            );
            try writer.interface.flush();
            var buffer: [1024]u8 = undefined;
            var reader = raw.reader(&buffer);
            reader.setTimeout(.fromSeconds(2));
            for ([_][]const u8{ "HEAD ", "GET ", "GET " }) |method| {
                const len = try talon.http.codec.findHeadEnd(&reader.interface, 512, null);
                try std.testing.expect(std.mem.startsWith(u8, reader.interface.buffered()[0..len], method));
                reader.interface.toss(len);
            }
        }
        fn request(l: *MemoryListener) !void {
            const Client = client.Client(client.MemoryConnector);
            var c = Client.init(std.testing.allocator, .{ .listener = l }, .{
                .max_response_body = 0,
                .pool = .{ .max_decoder_bytes = 0 },
                .timeouts = .{ .total = .fromSeconds(2) },
            });
            defer c.deinit();
            const origin: client.Origin = .{ .host = "memory", .port = 80 };
            for ([_]bool{ true, false }) |head| {
                var response = if (head) try c.head(origin, "/") else try c.get(origin, "/");
                defer response.deinit();
                try std.testing.expectEqual(@as(u16, if (head) 200 else 304), response.status());
                try std.testing.expectEqualStrings("9999999999999999999", response.header("content-length").?);
                const bytes = try response.readAllAlloc(std.testing.allocator, 0);
                defer std.testing.allocator.free(bytes);
                try std.testing.expectEqual(@as(usize, 0), bytes.len);
            }
            c.max_response_body = 1;
            var response = try c.get(origin, "/");
            defer response.deinit();
            const bytes = try response.readAllAlloc(std.testing.allocator, 1);
            defer std.testing.allocator.free(bytes);
            try std.testing.expectEqualStrings("x", bytes);
            try std.testing.expectEqual(@as(u64, 1), c.pool.stats().created);
            try std.testing.expectEqual(@as(u64, 2), c.pool.stats().reused);
            try std.testing.expectEqual(@as(usize, 0), c.pool.stats().decoder_bytes);
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.peer, .{&listener});
    try group.spawn(F.request, .{&listener});
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

test "client: switched protocols close on response release while rejected switches remain reusable" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    const Case = struct {
        method: talon.http.Method,
        upgrade: ?[]const u8 = null,
        status: u16,
        wire: []const u8,
        body: []const u8 = "",
        reusable: bool = false,
    };
    const cases = [_]Case{
        .{ .method = .GET, .upgrade = "other", .status = 101, .wire = "HTTP/1.1 101 Switching Protocols\r\nconnection: upgrade\r\nupgrade: other\r\n\r\nopaque" },
        .{ .method = .GET, .status = 101, .wire = "HTTP/1.1 101 Switching Protocols\r\nconnection: upgrade\r\nupgrade: other\r\n\r\nopaque" },
        .{ .method = .CONNECT, .status = 200, .wire = "HTTP/1.1 200 Connection Established\r\n\r\nopaque" },
        .{ .method = .CONNECT, .status = 299, .wire = "HTTP/1.1 299 Tunnel Ready\r\n\r\nopaque" },
        .{ .method = .CONNECT, .status = 407, .wire = "HTTP/1.1 407 Proxy Authentication Required\r\ncontent-length: 6\r\n\r\ndenied", .body = "denied", .reusable = true },
        .{ .method = .GET, .upgrade = "other", .status = 200, .wire = "HTTP/1.1 200 OK\r\ncontent-length: 3\r\n\r\nno!", .body = "no!", .reusable = true },
    };
    for (cases) |case| {
        var listener = try MemoryListener.init(std.testing.allocator, .{});
        defer listener.deinit();
        const F = struct {
            fn readRequest(raw: talon.core.MemoryConnection, buffer: []u8) !void {
                var reader = raw.reader(buffer);
                reader.setTimeout(.fromSeconds(2));
                const len = try talon.http.codec.findHeadEnd(&reader.interface, 512, null);
                reader.interface.toss(len);
            }
            fn peer(l: *MemoryListener, c: Case) !void {
                const first = try l.accept();
                defer first.close();
                var input_buffer: [1024]u8 = undefined;
                try readRequest(first, &input_buffer);
                var writer = first.writer(&.{});
                writer.setTimeout(.fromSeconds(2));
                try writer.interface.writeAll(c.wire);
                try writer.interface.flush();
                const next = if (c.reusable) first else blk: {
                    var reader = first.reader(&input_buffer);
                    reader.setTimeout(.fromSeconds(2));
                    try std.testing.expectError(error.EndOfStream, reader.interface.takeByte());
                    break :blk try l.accept();
                };
                defer if (!c.reusable) next.close();
                try readRequest(next, &input_buffer);
                var next_writer = next.writer(&.{});
                next_writer.setTimeout(.fromSeconds(2));
                try next_writer.interface.writeAll("HTTP/1.1 200 OK\r\ncontent-length: 1\r\n\r\nx");
                try next_writer.interface.flush();
            }
            fn request(l: *MemoryListener, c: Case) !void {
                defer l.close();
                const Client = client.Client(client.MemoryConnector);
                var http = Client.init(std.testing.allocator, .{ .listener = l }, .{
                    .pool = .{ .max_active = 1, .max_per_origin = 1 },
                    .timeouts = .{ .total = .fromSeconds(2) },
                });
                defer http.deinit();
                const origin: client.Origin = .{ .host = "memory", .port = 80 };
                {
                    var response = try http.request(.{ .method = c.method, .origin = origin, .upgrade = c.upgrade });
                    defer response.deinit();
                    try std.testing.expectEqual(c.status, response.status());
                    const bytes = try response.readAllAlloc(std.testing.allocator, 6);
                    defer std.testing.allocator.free(bytes);
                    try std.testing.expectEqualStrings(c.body, bytes);
                }
                try std.testing.expectEqual(@as(usize, if (c.reusable) 1 else 0), http.pool.idle_total);
                try std.testing.expectEqual(@as(usize, 0), http.poolStats().active);
                try std.testing.expectEqual(@as(usize, 0), http.poolStats().waiting);
                var response = try http.get(origin, "/");
                defer response.deinit();
                const bytes = try response.readAllAlloc(std.testing.allocator, 1);
                defer std.testing.allocator.free(bytes);
                try std.testing.expectEqualStrings("x", bytes);
                try std.testing.expectEqual(@as(u64, if (c.reusable) 1 else 2), http.poolStats().created);
                try std.testing.expectEqual(@as(u64, if (c.reusable) 1 else 0), http.poolStats().reused);
            }
        };
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.peer, .{ &listener, case });
        try group.spawn(F.request, .{ &listener, case });
        try group.wait();
        try std.testing.expect(!group.hasFailed());
    }
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
                c.post(origin, "/echo", "012345678901234567890123456789012345678901234567890123456789" ++ "0123"), // 64 bytes
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

            // The high-level request path must close and release admission on
            // workspace rejection rather than return compressed bytes or retry.
            {
                var c = Client.init(std.testing.allocator, .{ .listener = l }, .{ .pool = .{ .max_decoder_bytes = 0 } });
                defer c.deinit();
                try std.testing.expectError(error.DecoderBudgetExceeded, c.get(origin, "/gzip"));
                try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
                try std.testing.expectEqual(@as(usize, 0), c.poolStats().decoder_bytes);
                try std.testing.expectEqual(@as(usize, 0), c.pool.idle_total);
                try std.testing.expectEqual(@as(u64, 1), c.poolStats().created);
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

const BoundedClient = client.Client(client.MemoryConnector);

test "client: informational head budget rejection closes without retry and zero rejects before dial" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    const interim = "HTTP/1.1 103 Early Hints\r\nlink: </a>\r\n\r\n";
    const App = struct {
        pub fn handle(_: *@This(), _: *Request, res: *Response) !void {
            try res.out.writeAll(interim ++ interim ++ interim);
            try res.respond("", .{});
        }
    };
    var app: App = .{};
    const Srv = Server(App);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn run(l: *MemoryListener, s: *Srv) !void {
            defer s.shutdown();
            var c = BoundedClient.init(std.testing.allocator, .{ .listener = l }, .{ .max_response_head_bytes = 2 * interim.len });
            defer c.deinit();
            const origin: client.Origin = .{ .host = "head-budget", .port = 80 };
            try std.testing.expectError(error.ResponseHeadersTooLarge, c.get(origin, "/"));
            try std.testing.expectEqual(@as(u64, 1), c.poolStats().created);
            try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
            try std.testing.expectEqual(@as(usize, 0), c.pool.idle_total);
            c.max_response_head_bytes = 0;
            try std.testing.expectError(error.ResponseHeadersTooLarge, c.get(origin, "/"));
            try std.testing.expectEqual(@as(u64, 1), c.poolStats().created);
            c.max_response_head_bytes = null;
            const resp = try c.get(origin, "/");
            try std.testing.expectEqual(@as(u16, 200), resp.status());
            resp.deinit();
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener });
    try group.spawn(F.run, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

test "client: redirects and their informational heads share one exact byte allowance per request" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    const interim = "HTTP/1.1 103 Early Hints\r\nlink: </a>\r\n\r\n";
    const redirect = "HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\n\r\n";
    const final = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    const total = 2 * interim.len + redirect.len + final.len;
    const App = struct {
        pub fn handle(_: *@This(), req: *Request, res: *Response) !void {
            // Explicit heads make wire-byte boundaries independent of Date.
            try res.out.writeAll(interim);
            try res.out.writeAll(if (std.mem.eql(u8, req.target(), "/start")) redirect else final);
            res.written = true;
        }
    };
    var app: App = .{};
    const Srv = Server(App);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn run(l: *MemoryListener, s: *Srv) !void {
            defer s.shutdown();
            var c = BoundedClient.init(std.testing.allocator, .{ .listener = l }, .{ .max_response_head_bytes = total - 1 });
            defer c.deinit();
            const origin: client.Origin = .{ .host = "redirect-budget", .port = 80 };
            try std.testing.expectError(error.ResponseHeadersTooLarge, c.get(origin, "/start"));
            try std.testing.expectEqual(@as(u64, 1), c.poolStats().created);
            try std.testing.expectEqual(@as(u64, 1), c.poolStats().reused);
            try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
            try std.testing.expectEqual(@as(usize, 0), c.pool.idle_total);
            c.max_response_head_bytes = total;
            // A new application request gets a new allowance, including when
            // every redirect hop uses the same warm connection.
            for (0..2) |_| {
                const resp = try c.get(origin, "/start");
                try std.testing.expectEqual(@as(u16, 200), resp.status());
                resp.deinit();
            }
            try std.testing.expectEqual(@as(u64, 2), c.poolStats().created);
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener });
    try group.spawn(F.run, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

test "client: response head borrows survive body reads and idle arena retention is bounded" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{ .pipe_buffer_size = 64 * 1024 });
    defer listener.deinit();
    for ([_]?usize{ 0, 4096, null }) |retained| {
        var c = BoundedClient.init(std.testing.allocator, .{ .listener = &listener }, .{ .pool = .{ .max_retained_response_arena = retained } });
        defer c.deinit();
        const co = try c.pool.checkout(.{ .host = "head", .port = 80 }, .none);
        const peer = try listener.accept();
        defer peer.close();
        var writer = peer.writer(&.{});
        const value = &@as([8000]u8, @splat('v'));
        try writer.interface.print("HTTP/1.1 200 Saved\r\nContent-Length: 20000\r\nx-large: {s}\r\n\r\n", .{value});
        // Fits the memory pipe, but exceeds the client read buffer so body
        // consumption rebases the original head's backing Reader storage.
        try writer.interface.writeAll(&@as([20000]u8, @splat('b')));
        try writer.interface.flush();
        try co.conn.readResponse(null, true);
        const response: BoundedClient.Response = .{ .conn = co.conn, .pool = &c.pool };
        const capacity = co.conn.arena.queryCapacity();
        try std.testing.expect(capacity > 4096);
        try std.testing.expectEqual(@as(usize, 0), c.poolStats().idle_response_arena_bytes);
        const reason = response.reason();
        const header = response.header("x-large").?;
        const body = try response.readAllAlloc(std.testing.allocator, 20000);
        defer std.testing.allocator.free(body);
        try std.testing.expectEqual(@as(usize, 20000), body.len);
        try std.testing.expectEqualStrings("Saved", reason);
        try std.testing.expectEqualStrings(value, header);
        response.deinit();
        const idle_capacity = c.poolStats().idle_response_arena_bytes;
        if (retained) |max| {
            try std.testing.expect(idle_capacity <= max);
        } else try std.testing.expectEqual(capacity, idle_capacity);
        const next = try c.pool.checkout(.{ .host = "head", .port = 80 }, .none);
        try std.testing.expect(next.reused);
        try std.testing.expectEqual(@as(usize, 0), c.poolStats().idle_response_arena_bytes);
        try writer.interface.writeAll("HTTP/1.1 200 Small\r\nContent-Length: 0\r\n\r\n");
        try writer.interface.flush();
        try next.conn.readResponse(null, true);
        try std.testing.expectEqualStrings("Small", next.conn.head.reason);
        c.pool.checkin(next.conn, true);
        if (retained) |max| try std.testing.expect(c.poolStats().idle_response_arena_bytes <= max);
        c.pool.config.idle_timeout = .fromNanoseconds(0);
        try std.testing.expectEqual(@as(usize, 1), c.pool.reapExpired());
        try std.testing.expectEqual(@as(usize, 0), c.poolStats().idle_response_arena_bytes);
    }
}

test "client: failed arena shrink frees all storage without preventing transport reuse" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    var alloc = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var c = BoundedClient.init(alloc.allocator(), .{ .listener = &listener }, .{ .pool = .{ .max_retained_response_arena = 4096 } });
    defer c.deinit();
    // Warm the bucket first so the injected allocation failure is solely the
    // manual arena shrink, not best-effort pool bookkeeping.
    const first = try c.pool.checkout(.{ .host = "head", .port = 80 }, .none);
    const peer = try listener.accept();
    defer peer.close();
    c.pool.checkin(first.conn, true);
    const co = try c.pool.checkout(.{ .host = "head", .port = 80 }, .none);
    _ = try co.conn.arena.allocator().alloc(u8, 8000);
    alloc.fail_index = alloc.alloc_index;
    alloc.resize_fail_index = alloc.resize_index;
    c.pool.checkin(co.conn, true);
    try std.testing.expect(alloc.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().idle_response_arena_bytes);
    const next = try c.pool.checkout(.{ .host = "head", .port = 80 }, .none);
    try std.testing.expect(next.reused);
    c.pool.checkin(next.conn, false);
}

// A real framed response with the peer left open, so failed collection must
// close rather than wait for a declared but unsent remainder.
fn preparedResponse(c: *BoundedClient, l: *MemoryListener, bytes: []const u8, encoding: []const u8, declared_len: usize) !struct {
    response: BoundedClient.Response,
    peer: MemoryListener.RawConnection,
} {
    const co = try c.pool.checkout(.{ .host = "bounded", .port = 80 }, .none);
    errdefer c.pool.checkin(co.conn, false);
    const peer = try l.accept();
    errdefer peer.close();
    var writer = peer.writer(&.{});
    try writer.interface.print("HTTP/1.1 200 OK\r\nContent-Length: {d}\r\nContent-Encoding: {s}\r\n\r\n", .{ declared_len, encoding });
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
    try co.conn.readResponse(null, true);
    return .{ .response = .{ .conn = co.conn, .pool = &c.pool }, .peer = peer };
}

test "client: idle trim releases retained decoder and arena budgets and permits cold reuse" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var budget = talon.core.AllocationBudget.init(std.testing.allocator, 1024 * 1024);
    defer budget.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    var c = BoundedClient.init(budget.allocator(), .{ .listener = &listener }, .{});
    defer c.deinit();
    for (0..2) |_| {
        {
            const fixture = try preparedResponse(&c, &listener, &gzip_bytes, "gzip", gzip_bytes.len);
            defer fixture.peer.close();
            defer fixture.response.deinit();
            const body = try readBody(fixture.response);
            defer std.testing.allocator.free(body);
            try std.testing.expectEqualStrings(gzip_plain, body);
        }
        try std.testing.expect(c.poolStats().idle_decoder_bytes > 0);
        try std.testing.expect(c.poolStats().idle_response_arena_bytes > 0);
        try std.testing.expectEqual(@as(usize, 1), c.trimIdleConnections());
        try std.testing.expectEqual(@as(usize, 0), c.poolStats().decoder_bytes);
        try std.testing.expectEqual(@as(usize, 0), c.poolStats().idle_decoder_bytes);
        try std.testing.expectEqual(@as(usize, 0), c.poolStats().idle_response_arena_bytes);
        try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
    }
}

test "client: decoder byte budget rejects before allocation and releases on close across origins" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    const cost = std.compress.flate.max_window_len + @sizeOf(std.compress.flate.Decompress);
    for ([_]?usize{ 0, cost - 1, cost, null }) |limit| {
        var c = BoundedClient.init(std.testing.allocator, .{ .listener = &listener }, .{ .pool = .{
            .max_decoder_bytes = limit,
            .max_idle_decoder_bytes = 0,
        } });
        defer c.deinit();
        if (limit != null and limit.? < cost) {
            try std.testing.expectError(error.DecoderBudgetExceeded, preparedResponse(&c, &listener, &gzip_bytes, "gzip", gzip_bytes.len));
            try std.testing.expectEqual(@as(usize, 0), c.poolStats().decoder_bytes);
            try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
            continue;
        }
        const fixture = try preparedResponse(&c, &listener, &gzip_bytes, "gzip", gzip_bytes.len);
        defer fixture.peer.close();
        defer fixture.response.deinit();
        try std.testing.expectEqual(cost, c.poolStats().decoder_bytes);
        const other = try c.pool.checkout(.{ .host = "other", .port = 80 }, .none);
        const peer = try listener.accept();
        defer peer.close();
        var writer = peer.writer(&.{});
        try writer.interface.print("HTTP/1.1 200 OK\r\nContent-Length: {d}\r\nContent-Encoding: gzip\r\n\r\n", .{gzip_bytes.len});
        try writer.interface.writeAll(&gzip_bytes);
        try writer.interface.flush();
        if (limit == null) {
            try other.conn.readResponse(null, true);
            try std.testing.expectEqual(2 * cost, c.poolStats().decoder_bytes);
        } else {
            try std.testing.expectError(error.DecoderBudgetExceeded, other.conn.readResponse(null, true));
            try std.testing.expect(other.conn.flate_window == null and other.conn.flate_decoder == null);
            try std.testing.expectEqual(cost, c.poolStats().decoder_bytes);
        }
        c.pool.checkin(other.conn, false);
        try std.testing.expect(peer.send.read_closed);
        try std.testing.expectEqual(cost, c.poolStats().decoder_bytes);
    }
}

test "client: bounded idle decoder retention preserves transport and accounting on reuse and eviction" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    const cost = std.compress.flate.max_window_len + @sizeOf(std.compress.flate.Decompress);
    for ([_]usize{ 0, cost - 1, cost }) |idle_limit| {
        var c = BoundedClient.init(std.testing.allocator, .{ .listener = &listener }, .{ .pool = .{
            .max_decoder_bytes = cost,
            .max_idle_decoder_bytes = idle_limit,
        } });
        defer c.deinit();
        const fixture = try preparedResponse(&c, &listener, &gzip_bytes, "gzip", gzip_bytes.len);
        defer fixture.peer.close();
        const body = try readBody(fixture.response);
        defer std.testing.allocator.free(body);
        try std.testing.expectEqualStrings(gzip_plain, body);
        fixture.response.deinit();
        const retained: usize = if (idle_limit == cost) cost else 0;
        try std.testing.expectEqual(retained, c.poolStats().decoder_bytes);
        try std.testing.expectEqual(retained, c.poolStats().idle_decoder_bytes);
        const next = try c.pool.checkout(.{ .host = "bounded", .port = 80 }, .none);
        try std.testing.expect(next.reused);
        try std.testing.expectEqual(retained, c.poolStats().decoder_bytes);
        try std.testing.expectEqual(@as(usize, 0), c.poolStats().idle_decoder_bytes);
        c.pool.checkin(next.conn, true);
        c.pool.config.idle_timeout = .fromNanoseconds(0);
        try std.testing.expectEqual(@as(usize, 1), c.pool.reapExpired());
        try std.testing.expectEqual(@as(usize, 0), c.poolStats().decoder_bytes);
        try std.testing.expectEqual(@as(usize, 0), c.poolStats().idle_decoder_bytes);
    }
    // The default idle budget keeps small flate storage but frees an 8 MiB
    // zstd workspace while preserving the reusable transport.
    var c = BoundedClient.init(std.testing.allocator, .{ .listener = &listener }, .{});
    defer c.deinit();
    const fixture = try preparedResponse(&c, &listener, &zstd_bytes, "zstd", zstd_bytes.len);
    defer fixture.peer.close();
    try std.testing.expect(c.poolStats().decoder_bytes > 8 * 1024 * 1024);
    const body = try readBody(fixture.response);
    defer std.testing.allocator.free(body);
    try std.testing.expectEqualStrings(zstd_plain, body);
    fixture.response.deinit();
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().decoder_bytes);
    const next = try c.pool.checkout(.{ .host = "bounded", .port = 80 }, .none);
    try std.testing.expect(next.reused);
    c.pool.checkin(next.conn, false);
}

test "client: decoder object and window allocation failures roll back their complete reservation" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    inline for (.{ "gzip", "zstd" }) |encoding| {
        for (0..2) |allocation| {
            var alloc = std.testing.FailingAllocator.init(std.testing.allocator, .{});
            var c = BoundedClient.init(alloc.allocator(), .{ .listener = &listener }, .{});
            defer c.deinit();
            // Prime parsing/arena separately so the two injected failures land
            // on the decoder window and object, not unrelated header storage.
            const fixture = try preparedResponse(&c, &listener, "", "identity", 0);
            defer fixture.peer.close();
            var writer = fixture.peer.writer(&.{});
            try writer.interface.print("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Encoding: {s}\r\n\r\n", .{encoding});
            try writer.interface.flush();
            alloc.fail_index = alloc.alloc_index + allocation;
            // Initialization is selected by method/status and encoding, even
            // for a zero-length body; no compressed input is consumed here.
            try std.testing.expectError(error.OutOfMemory, fixture.response.conn.readResponse(null, true));
            try std.testing.expectEqual(@as(usize, 0), c.poolStats().decoder_bytes);
            try std.testing.expect(fixture.response.conn.flate_window == null and fixture.response.conn.zstd_window == null);
            c.pool.checkin(fixture.response.conn, false);
        }
    }
}

test "client: idle decoder sub-budget is shared and mixed encoding reservations accumulate" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    const cost = std.compress.flate.max_window_len + @sizeOf(std.compress.flate.Decompress);
    var c = BoundedClient.init(std.testing.allocator, .{ .listener = &listener }, .{ .pool = .{
        .max_decoder_bytes = 2 * cost,
        .max_idle_decoder_bytes = cost,
    } });
    defer c.deinit();
    const first = try preparedResponse(&c, &listener, &gzip_bytes, "gzip", gzip_bytes.len);
    defer first.peer.close();
    const second = try preparedResponse(&c, &listener, &gzip_bytes, "gzip", gzip_bytes.len);
    defer second.peer.close();
    try std.testing.expectEqual(2 * cost, c.poolStats().decoder_bytes);
    first.response.deinit();
    second.response.deinit();
    try std.testing.expectEqual(cost, c.poolStats().decoder_bytes);
    try std.testing.expectEqual(cost, c.poolStats().idle_decoder_bytes);
    c.pool.config.idle_timeout = .fromNanoseconds(0);
    try std.testing.expectEqual(@as(usize, 2), c.pool.reapExpired());
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().decoder_bytes);

    c.pool.config.idle_timeout = null;
    const fixture = try preparedResponse(&c, &listener, &gzip_bytes, "gzip", gzip_bytes.len);
    defer fixture.peer.close();
    const body = try readBody(fixture.response);
    defer std.testing.allocator.free(body);
    var writer = fixture.peer.writer(&.{});
    try writer.interface.writeAll("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Encoding: zstd\r\n\r\n");
    try writer.interface.flush();
    // A second encoding cannot overwrite or forget the existing reservation.
    try std.testing.expectError(error.DecoderBudgetExceeded, fixture.response.conn.readResponse(null, true));
    try std.testing.expectEqual(cost, c.poolStats().decoder_bytes);
    try std.testing.expect(fixture.response.conn.flate_decoder != null);
    try std.testing.expect(fixture.response.conn.zstd_decoder == null);
    c.pool.checkin(fixture.response.conn, false);
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().decoder_bytes);
}

test "client: canceled decoded collection closes and returns workspace reservation" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    var c = BoundedClient.init(std.testing.allocator, .{ .listener = &listener }, .{});
    defer c.deinit();
    // Only the gzip header arrives; decoding must wait for the absent body.
    const fixture = try preparedResponse(&c, &listener, gzip_bytes[0..10], "gzip", gzip_bytes.len);
    defer fixture.peer.close();
    try std.testing.expect(c.poolStats().decoder_bytes > 0);
    try fixture.response.conn.setReadTimeout(.fromSeconds(2));
    const F = struct {
        fn read(response: BoundedClient.Response, started: *zio.ResetEvent) !void {
            started.set();
            try std.testing.expectError(error.ReadFailed, response.readAllAlloc(std.testing.allocator, 1024));
            try std.testing.expect(response.conn.lastReadCanceled());
        }
    };
    var started: zio.ResetEvent = .init;
    var task = try zio.spawn(F.read, .{ fixture.response, &started });
    defer task.cancel();
    try started.wait();
    task.cancel();
    try task.join();
    fixture.response.deinit();
    try std.testing.expect(fixture.peer.send.read_closed);
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().decoder_bytes);
    try std.testing.expectEqual(@as(usize, 0), c.poolStats().idle_decoder_bytes);
}

test "client: concurrent decoder initialization shares one reservation across executors" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    const cost = std.compress.flate.max_window_len + @sizeOf(std.compress.flate.Decompress);
    var c = BoundedClient.init(std.testing.allocator, .{ .listener = &listener }, .{ .pool = .{ .max_decoder_bytes = cost } });
    defer c.deinit();
    const first = try c.pool.checkout(.{ .host = "first", .port = 80 }, .none);
    defer c.pool.checkin(first.conn, false);
    const second = try c.pool.checkout(.{ .host = "second", .port = 80 }, .none);
    defer c.pool.checkin(second.conn, false);
    const peer_a = try listener.accept();
    defer peer_a.close();
    const peer_b = try listener.accept();
    defer peer_b.close();
    for ([_]MemoryListener.RawConnection{ peer_a, peer_b }) |peer| {
        var writer = peer.writer(&.{});
        try writer.interface.writeAll("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Encoding: gzip\r\n\r\n");
        try writer.interface.flush();
    }
    const ConnPtr = @TypeOf(first.conn);
    const F = struct {
        fn decode(conn: ConnPtr, start: *zio.ResetEvent, success: *bool) !void {
            try start.wait();
            conn.readResponse(null, true) catch |err| {
                try std.testing.expectEqual(error.DecoderBudgetExceeded, err);
                success.* = false;
                return;
            };
            success.* = true;
        }
    };
    var start: zio.ResetEvent = .init;
    var results: [2]bool = undefined;
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.decode, .{ first.conn, &start, &results[0] });
    try group.spawn(F.decode, .{ second.conn, &start, &results[1] });
    start.set();
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expect(results[0] != results[1]);
    try std.testing.expectEqual(cost, c.poolStats().decoder_bytes);
}

test "client: decoded collection enforces zero, exact and exceeded limits across encodings" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    const cases = [_]struct { encoded: []const u8, decoded: []const u8, encoding: []const u8 }{
        .{ .encoded = "", .decoded = "", .encoding = "identity" },
        .{ .encoded = "plain", .decoded = "plain", .encoding = "identity" },
        .{ .encoded = &gzip_bytes, .decoded = gzip_plain, .encoding = "gzip" },
        .{ .encoded = &deflate_bytes, .decoded = deflate_plain, .encoding = "deflate" },
        .{ .encoded = &zstd_bytes, .decoded = zstd_plain, .encoding = "zstd" },
        .{ .encoded = &@as([10000]u8, @splat('x')), .decoded = &@as([10000]u8, @splat('x')), .encoding = "identity" },
    };
    for (cases) |case| {
        for ([_]usize{ 0, case.decoded.len -| 1, case.decoded.len, case.decoded.len + 1 }) |limit| {
            var c = BoundedClient.init(std.testing.allocator, .{ .listener = &listener }, .{ .pool = .{ .max_per_origin = 1 } });
            defer c.deinit();
            const fixture = try preparedResponse(&c, &listener, case.encoded, case.encoding, case.encoded.len);
            defer fixture.peer.close();
            var alloc = std.testing.FailingAllocator.init(std.testing.allocator, .{});
            if (limit < case.decoded.len) {
                try std.testing.expectError(error.ResponseTooLarge, fixture.response.readAllAlloc(alloc.allocator(), limit));
                try std.testing.expectEqual(alloc.allocated_bytes, alloc.freed_bytes);
                fixture.response.deinit();
                try std.testing.expect(fixture.peer.send.read_closed);
                try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
            } else {
                const body = try fixture.response.readAllAlloc(alloc.allocator(), limit);
                try std.testing.expectEqualStrings(case.decoded, body);
                alloc.allocator().free(body);
                fixture.response.deinit();
                const next = try c.pool.checkout(.{ .host = "bounded", .port = 80 }, .none);
                try std.testing.expect(next.reused);
                c.pool.checkin(next.conn, false);
            }
        }
    }
}

test "client: decompression amplification and JSON share the allocation bound" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    // Python gzip.compress(b'x' * 65536, mtime=0): 96 bytes expand to 64 KiB.
    const bomb = "\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\xed\xc1\x01\x01\x00\x00\x00\x80\x90\xdb\xcd\xef\x08\x0a" ++ &@as([63]u8, @splat('\x00')) ++ "\x6a\x9a\x58\xb4\x2c\x00\x00\x01\x00";
    for ([_]bool{ false, true }) |as_json| {
        var c = BoundedClient.init(std.testing.allocator, .{ .listener = &listener }, .{ .pool = .{ .max_per_origin = 1 } });
        defer c.deinit();
        const fixture = try preparedResponse(&c, &listener, bomb, "gzip", bomb.len);
        defer fixture.peer.close();
        var budget: [32]u8 = undefined;
        var fixed = std.heap.FixedBufferAllocator.init(&budget);
        var alloc = std.testing.FailingAllocator.init(fixed.allocator(), .{});
        if (as_json) {
            try std.testing.expectError(error.ResponseTooLarge, fixture.response.json(std.json.Value, alloc.allocator(), budget.len));
        } else {
            try std.testing.expectError(error.ResponseTooLarge, fixture.response.readAllAlloc(alloc.allocator(), budget.len));
        }
        // A post-read check would exhaust this allocator instead of reporting
        // ResponseTooLarge. Only the permitted output is ever allocated.
        try std.testing.expectEqual(budget.len, alloc.allocated_bytes);
        try std.testing.expectEqual(alloc.allocated_bytes, alloc.freed_bytes);
        fixture.response.deinit();
        try std.testing.expect(fixture.peer.send.read_closed);
        try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
    }
}

test "client: failed bounded reads close without draining a stalled remainder" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    for (0..3) |failure| {
        var c = BoundedClient.init(std.testing.allocator, .{ .listener = &listener }, .{ .pool = .{ .max_per_origin = 1 } });
        defer c.deinit();
        const fixture = try preparedResponse(&c, &listener, "abc", "identity", 100);
        defer fixture.peer.close();
        // Any attempted drain records Timeout, exposing an accidental read in
        // deinit without leaving the test hanging indefinitely.
        try fixture.response.conn.setReadTimeout(.fromMilliseconds(10));
        var alloc = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
        switch (failure) {
            0 => try std.testing.expectError(error.ResponseTooLarge, fixture.response.readAllAlloc(std.testing.allocator, 1)),
            1 => try std.testing.expectError(error.OutOfMemory, fixture.response.readAllAlloc(alloc.allocator(), 1)),
            2 => {
                fixture.peer.shutdown();
                try std.testing.expectError(error.ReadFailed, fixture.response.readAllAlloc(std.testing.allocator, 100));
            },
            else => unreachable,
        }
        try std.testing.expect(!fixture.response.conn.finishResponse());
        try std.testing.expect(!fixture.response.conn.lastReadTimedOut());
        fixture.response.deinit();
        try std.testing.expect(fixture.peer.send.read_closed);
        try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
    }
}

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

test "client: redirect method and body headers stay correct across a later origin change" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    const App = struct {
        status: talon.http.Status,
        cross_origin: bool,
        pub fn handle(self: *@This(), req: *Request, res: *Response) !void {
            if (std.mem.eql(u8, req.target(), "/start")) {
                try res.respond("", .{ .status = self.status, .extra_headers = &.{.{ .name = "location", .value = "/hop" }} });
                return;
            }
            if (std.mem.eql(u8, req.target(), "/hop")) {
                try res.respond("", .{ .status = .temporary_redirect, .extra_headers = &.{.{ .name = "location", .value = if (self.cross_origin) "http://other/final" else "/final" }} });
                return;
            }
            const has_body_headers = req.header("content-type") != null or req.header("content-encoding") != null or
                req.header("content-language") != null or req.header("content-location") != null;
            var body: std.Io.Writer.Allocating = .init(req.arena);
            _ = try req.bodyReader().streamRemaining(&body.writer);
            try res.respond(body.written(), .{ .extra_headers = &.{
                .{ .name = "x-method", .value = req.head.method_raw },
                .{ .name = "x-body-headers", .value = if (has_body_headers) "present" else "absent" },
                .{ .name = "x-auth", .value = if (req.header("authorization") != null) "present" else "absent" },
                .{ .name = "x-custom", .value = req.header("x-custom") orelse "missing" },
            } });
        }
    };
    const Srv = Server(App);
    const F = struct {
        fn serve(server: *Srv, listener: *MemoryListener) !void {
            try server.serve(listener);
        }
        fn request(listener: *MemoryListener, server: *Srv, method: client.Method, drops_body: bool, cross_origin: bool) !void {
            defer server.shutdown();
            var c = client.Client(client.MemoryConnector).init(std.testing.allocator, .{ .listener = listener }, .{});
            defer c.deinit();
            var response = try c.request(.{
                .origin = .{ .host = "memory", .port = 80 },
                .method = method,
                .target = "/start",
                .body = if (method == .HEAD) .none else .{ .bytes = "payload" },
                .extra_headers = &.{
                    .{ .name = "Content-Type", .value = "text/plain" },
                    .{ .name = "Content-Encoding", .value = "identity" },
                    .{ .name = "Content-Language", .value = "en" },
                    .{ .name = "Content-Location", .value = "/start" },
                    .{ .name = "Authorization", .value = "secret" },
                    .{ .name = "x-custom", .value = "preserved" },
                },
            });
            defer response.deinit();
            const final_method: client.Method = if (drops_body and method != .HEAD) .GET else method;
            try std.testing.expectEqualStrings(@tagName(final_method), response.header("x-method").?);
            try std.testing.expectEqualStrings(if (drops_body) "absent" else "present", response.header("x-body-headers").?);
            try std.testing.expectEqualStrings(if (cross_origin) "absent" else "present", response.header("x-auth").?);
            try std.testing.expectEqualStrings("preserved", response.header("x-custom").?);
            const body = try response.readAllAlloc(std.testing.allocator, 64);
            defer std.testing.allocator.free(body);
            try std.testing.expectEqualStrings(if (drops_body) "" else "payload", body);
        }
    };
    const Case = struct { method: client.Method, status: talon.http.Status, drops_body: bool, cross_origin: bool = true };
    for ([_]Case{
        .{ .method = .HEAD, .status = .see_other, .drops_body = true },
        .{ .method = .POST, .status = .see_other, .drops_body = true },
        .{ .method = .POST, .status = .moved_permanently, .drops_body = true },
        .{ .method = .POST, .status = .found, .drops_body = true },
        .{ .method = .PUT, .status = .temporary_redirect, .drops_body = false },
        .{ .method = .PUT, .status = .permanent_redirect, .drops_body = false },
        .{ .method = .POST, .status = .see_other, .drops_body = true, .cross_origin = false },
    }) |case| {
        var listener = try MemoryListener.init(std.testing.allocator, .{});
        defer listener.deinit();
        var app: App = .{ .status = case.status, .cross_origin = case.cross_origin };
        var server = try Srv.init(std.testing.allocator, &app, .{});
        defer server.deinit();
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.serve, .{ &server, &listener });
        try group.spawn(F.request, .{ &listener, &server, case.method, case.drops_body, case.cross_origin });
        try group.wait();
        try std.testing.expect(!group.hasFailed());
    }
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

test "client: request close intent forbids reuse when response omits close header" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    const F = struct {
        fn peer(l: *MemoryListener) !void {
            for (0..2) |connection| {
                const raw = try l.accept();
                defer raw.close();
                var buffer: [4096]u8 = undefined;
                var r = raw.reader(&buffer);
                r.setTimeout(.fromSeconds(2));
                var w = raw.writer(&.{});
                for (0..if (connection == 0) @as(usize, 1) else 2) |_| {
                    var close = false;
                    while (true) {
                        const line = try r.interface.takeDelimiterInclusive('\n');
                        if (std.mem.eql(u8, line, "\r\n")) break;
                        if (std.mem.eql(u8, line, "connection: close\r\n")) close = true;
                    }
                    try std.testing.expectEqual(connection == 0, close);
                    // HTTP/1.1 default permits reuse, but local close intent
                    // must still win. Leave raw open to verify client EOF.
                    try w.interface.writeAll("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
                    try w.interface.flush();
                }
                try std.testing.expectError(error.EndOfStream, r.interface.takeByte());
            }
        }
        fn run(l: *MemoryListener) !void {
            const C = client.Client(client.MemoryConnector);
            var c = C.init(std.testing.allocator, .{ .listener = l }, .{ .retry = .{ .max_retries = 0 } });
            defer c.deinit();
            for (0..3) |i| {
                const response = try c.request(.{ .origin = .{ .host = "close", .port = 80 }, .keep_alive = i != 0 });
                defer response.deinit();
                try std.testing.expect(response.conn.head.keep_alive);
                const body = try readBody(response);
                defer std.testing.allocator.free(body);
                try std.testing.expectEqualStrings("ok", body);
            }
            try std.testing.expectEqual(@as(u64, 2), c.poolStats().created);
            try std.testing.expectEqual(@as(u64, 1), c.poolStats().reused);
            try std.testing.expectEqual(@as(usize, 0), c.poolStats().active);
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.peer, .{&listener});
    try group.spawn(F.run, .{&listener});
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
        const body = try req.arena.print("{s}|{s}|{s}", .{ ua, acc, ae });
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

            // Disabling defaults preserves explicit caller headers.
            for ([_]bool{ false, true }) |explicit| {
                var c = Client.init(std.testing.allocator, .{ .listener = l }, .{ .default_headers = false });
                defer c.deinit();
                var resp = try c.request(.{ .origin = origin, .extra_headers = if (explicit) &.{
                    .{ .name = "User-Agent", .value = "custom/9" },
                    .{ .name = "Accept", .value = "application/json" },
                    .{ .name = "Accept-Encoding", .value = "identity" },
                } else &.{} });
                defer resp.deinit();
                const body = try readBody(resp);
                defer std.testing.allocator.free(body);
                try std.testing.expectEqualStrings(if (explicit) "custom/9|application/json|identity" else "<none>|<none>|<none>", body);
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
    const chunk = "talon-upload-";
    const payload = &@as([1000 * chunk.len]u8, @bitCast(@as([1000][chunk.len]u8, @splat(chunk.*))));

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

test "client: origin permits reclaim idle history and survive waiting cancellation" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    const C = client.Client(client.MemoryConnector);
    var c = C.init(std.testing.allocator, .{ .listener = &listener }, .{
        .pool = .{ .max_per_origin = 1, .pool_wait = .fromSeconds(2), .idle_timeout = .fromNanoseconds(1) },
    });
    defer c.deinit();
    for (0..40) |i| {
        var name: [32]u8 = undefined;
        const origin: client.Origin = .{ .host = try std.fmt.bufPrint(&name, "origin-{d}", .{i}), .port = 80 };
        const co = try c.pool.checkout(origin, .none);
        const peer = try listener.accept();
        c.pool.checkin(co.conn, true);
        peer.close();
        _ = c.pool.reapExpired();
        try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
    }
    const origin: client.Origin = .{ .host = "held", .port = 80 };
    const co = try c.pool.checkout(origin, .none);
    const peer = try listener.accept();
    defer peer.close();
    const F = struct {
        fn wait(cl: *C, o: client.Origin) !void {
            try std.testing.expectError(error.Canceled, cl.pool.checkout(o, .none));
        }
    };
    var waiter = try zio.spawn(F.wait, .{ &c, origin });
    // Observe the waiter reference under the same mutex as its mutation.
    var ready = false;
    for (0..1000) |_| {
        c.pool.mutex.lock();
        ready = co.conn.pool_permit.?.users == 2;
        c.pool.mutex.unlock();
        if (ready) break;
        try zio.sleep(.fromMilliseconds(1));
    }
    waiter.cancel();
    try waiter.join();
    try std.testing.expect(ready);
    c.pool.checkin(co.conn, false);
    try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
    // A holder returning while a waiter wakes must not free that waiter's state.
    const held = try c.pool.checkout(origin, .none);
    const held_peer = try listener.accept();
    defer held_peer.close();
    const Handoff = struct {
        fn run(cl: *C, l: *MemoryListener, o: client.Origin) !void {
            const next = try cl.pool.checkout(o, .none);
            const next_peer = try l.accept();
            next_peer.close();
            cl.pool.checkin(next.conn, false);
        }
    };
    var handoff = try zio.spawn(Handoff.run, .{ &c, &listener, origin });
    ready = false;
    for (0..1000) |_| {
        c.pool.mutex.lock();
        ready = held.conn.pool_permit.?.users == 2;
        c.pool.mutex.unlock();
        if (ready) break;
        try zio.sleep(.fromMilliseconds(1));
    }
    c.pool.checkin(held.conn, false);
    try handoff.join();
    try std.testing.expect(ready);
    try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
    listener.close();
    // Dial failures must also relinquish the last reference.
    for (0..10) |_| try std.testing.expectError(error.Closed, c.pool.checkout(origin, .none));
    try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
}

test "client: failed redial reclaims an expired origin bucket" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    for ([_]?usize{ null, 1 }) |max| {
        var listener = try MemoryListener.init(std.testing.allocator, .{});
        defer listener.deinit();
        const C = client.Client(client.MemoryConnector);
        var c = C.init(std.testing.allocator, .{ .listener = &listener }, .{
            .pool = .{ .max_per_origin = max, .idle_timeout = .fromSeconds(60) },
        });
        defer c.deinit();
        const origin: client.Origin = .{ .host = "expired", .port = 80 };
        const co = try c.pool.checkout(origin, .none);
        const peer = try listener.accept();
        peer.close();
        c.pool.checkin(co.conn, true);
        // Force expiry without relying on scheduler timing.
        c.pool.config.idle_timeout = .fromNanoseconds(1);
        try zio.sleep(.fromMilliseconds(1));
        listener.close();
        try std.testing.expectError(error.Closed, c.pool.checkout(origin, .none));
        try std.testing.expectEqual(@as(usize, 0), c.pool.buckets.count());
        try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
    }
}

test "client: pool allocation failures release ownership and permits" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    const F = struct {
        fn run(gpa: std.mem.Allocator) !void {
            var listener = try MemoryListener.init(std.testing.allocator, .{});
            defer listener.deinit();
            const C = client.Client(client.MemoryConnector);
            var c = C.init(gpa, .{ .listener = &listener }, .{ .pool = .{ .max_per_origin = 1, .pool_wait = .fromMilliseconds(1) } });
            defer c.deinit();
            const origin: client.Origin = .{ .host = "oom", .port = 80 };
            const co = try c.pool.checkout(origin, .none);
            const peer = try listener.accept();
            peer.close();
            // Even a failed origin-key allocation must keep the permit held.
            try std.testing.expectEqual(@as(usize, 1), co.conn.pool_permit.?.users);
            try std.testing.expectError(error.PoolWaitTimeout, c.pool.checkout(origin, .none));
            c.pool.checkin(co.conn, true);
            c.pool.config.idle_timeout = .fromNanoseconds(1);
            try zio.sleep(.fromMilliseconds(1));
            const next = c.pool.checkout(origin, .none) catch |err| {
                try std.testing.expectEqual(@as(usize, 0), c.pool.buckets.count());
                try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
                return err;
            };
            const next_peer = try listener.accept();
            next_peer.close();
            try std.testing.expectError(error.PoolWaitTimeout, c.pool.checkout(origin, .none));
            c.pool.checkin(next.conn, false);
            try std.testing.expectEqual(@as(usize, 0), c.pool.buckets.count());
            try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
        }
    };
    var baseline = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    try F.run(baseline.allocator());
    // Pooling is best effort: OOM may validly return an unpooled connection.
    // Enumerate failure points and verify ownership in both success/error cases.
    for (0..baseline.allocations) |fail_index| {
        var alloc = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
        F.run(alloc.allocator()) catch |err| if (err != error.OutOfMemory) return err;
        try std.testing.expect(alloc.has_induced_failure);
        try std.testing.expectEqual(alloc.allocated_bytes, alloc.freed_bytes);
    }
}

test "client: warm pool reuse performs no allocations" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    var alloc = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const C = client.Client(client.MemoryConnector);
    var c = C.init(alloc.allocator(), .{ .listener = &listener }, .{ .pool = .{ .max_per_origin = 1 } });
    defer c.deinit();
    const origin: client.Origin = .{ .host = "warm", .port = 80 };
    const first = try c.pool.checkout(origin, .none);
    const peer = try listener.accept();
    defer peer.close();
    var writer = peer.writer(&.{});
    const head = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nx-warm: value\r\n\r\n";
    try writer.interface.writeAll(head);
    try writer.interface.flush();
    try first.conn.readResponse(null, false);
    c.pool.checkin(first.conn, true);
    const allocations = alloc.allocations;
    for (0..20) |_| {
        const co = try c.pool.checkout(origin, .none);
        try std.testing.expect(co.reused);
        try writer.interface.writeAll(head);
        try writer.interface.flush();
        try co.conn.readResponse(null, false);
        try std.testing.expectEqualStrings("value", co.conn.head.header("x-warm").?);
        c.pool.checkin(co.conn, true);
    }
    try std.testing.expectEqual(allocations, alloc.allocations);
}

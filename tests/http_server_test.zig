//! Integration tests for talon.http.Server: full request/response cycles over
//! a transport (MemoryListener / TcpListener), driving the public API only.

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const Server = talon.http.Server;
const ServerWith = talon.http.ServerWith;
const Request = talon.http.Request;
const Response = talon.http.Response;
const MemoryListener = talon.MemoryListener;
const TcpListener = talon.TcpListener;
const middleware = talon.middleware;

const HelloApp = struct {
    hits: u32 = 0,

    pub fn handle(self: *HelloApp, req: *Request, res: *Response) !void {
        _ = req;
        self.hits += 1;
        try res.respond("hello from talon\n", .{
            .extra_headers = &.{
                .{ .name = "content-type", .value = "text/plain" },
            },
        });
    }
};

/// Reads one CRLF-terminated line, returning it without the CRLF.
fn readLine(reader: *std.Io.Reader, buf: []u8) ![]const u8 {
    const line = try reader.takeDelimiterInclusive('\n');
    const trimmed = std.mem.trimEnd(u8, line, "\r\n");
    @memcpy(buf[0..trimmed.len], trimmed);
    return buf[0..trimmed.len];
}

/// Reads response head, returns content-length; asserts the status line.
fn expectResponseHead(r: *std.Io.Reader, expected_status: []const u8) !usize {
    var line_buf: [256]u8 = undefined;
    const status_line = try readLine(r, &line_buf);
    try std.testing.expectEqualStrings(expected_status, status_line);
    var content_length: usize = 0;
    while (true) {
        const line = try readLine(r, &line_buf);
        if (line.len == 0) break;
        const prefix = "content-length: ";
        if (std.ascii.startsWithIgnoreCase(line, prefix)) {
            content_length = try std.fmt.parseInt(usize, line[prefix.len..], 10);
        }
    }
    return content_length;
}

test "talon.http.Server: HTTP/1.1 keep-alive request cycle over memory transport" {
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

        fn client(l: *MemoryListener, s: *Srv) !void {
            const conn = try l.connect();
            defer conn.close();

            var wbuf: [256]u8 = undefined;
            var w = conn.writer(&wbuf);
            var rbuf: [4096]u8 = undefined;
            var r = conn.reader(&rbuf);

            // Two requests on one connection: exercises keep-alive.
            for (0..2) |_| {
                try w.interface.writeAll("GET / HTTP/1.1\r\nhost: test\r\n\r\n");
                try w.interface.flush();

                const content_length = try expectResponseHead(&r.interface, "HTTP/1.1 200 OK");
                try std.testing.expect(content_length > 0);

                var body_buf: [256]u8 = undefined;
                try r.interface.readSliceAll(body_buf[0..content_length]);
                try std.testing.expectEqualStrings("hello from talon\n", body_buf[0..content_length]);
            }

            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.client, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(2, app.hits);
}

const EchoApp = struct {
    pub fn handle(self: *EchoApp, req: *Request, res: *Response) !void {
        _ = self;
        // Read entire body via the streaming reader into the request arena.
        var collected: std.Io.Writer.Allocating = .init(req.arena);
        _ = try req.bodyReader().streamRemaining(&collected.writer);
        try res.respond(collected.written(), .{
            .extra_headers = &.{.{ .name = "x-echo", .value = "1" }},
        });
    }
};

test "talon.http.Server: POST bodies — content-length and chunked — echo back" {
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

        fn client(l: *MemoryListener, s: *Srv) !void {
            const conn = try l.connect();
            defer conn.close();

            var wbuf: [256]u8 = undefined;
            var w = conn.writer(&wbuf);
            var rbuf: [4096]u8 = undefined;
            var r = conn.reader(&rbuf);

            // Content-Length body.
            try w.interface.writeAll("POST /echo HTTP/1.1\r\nhost: t\r\ncontent-length: 11\r\n\r\nhello talon");
            try w.interface.flush();
            {
                const cl = try expectResponseHead(&r.interface, "HTTP/1.1 200 OK");
                var buf: [64]u8 = undefined;
                try r.interface.readSliceAll(buf[0..cl]);
                try std.testing.expectEqualStrings("hello talon", buf[0..cl]);
            }

            // Chunked body on the same connection (keep-alive survived).
            try w.interface.writeAll("POST /echo HTTP/1.1\r\nhost: t\r\ntransfer-encoding: chunked\r\n\r\n" ++
                "4\r\nzig \r\n6\r\nrules!\r\n0\r\n\r\n");
            try w.interface.flush();
            {
                const cl = try expectResponseHead(&r.interface, "HTTP/1.1 200 OK");
                var buf: [64]u8 = undefined;
                try r.interface.readSliceAll(buf[0..cl]);
                try std.testing.expectEqualStrings("zig rules!", buf[0..cl]);
            }

            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.client, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

test "talon.http.Server: smuggling attempt is rejected with 400 and close" {
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

        fn client(l: *MemoryListener, s: *Srv) !void {
            const conn = try l.connect();
            defer conn.close();

            var wbuf: [256]u8 = undefined;
            var w = conn.writer(&wbuf);
            var rbuf: [4096]u8 = undefined;
            var r = conn.reader(&rbuf);

            try w.interface.writeAll("POST / HTTP/1.1\r\nhost: t\r\n" ++
                "content-length: 5\r\ntransfer-encoding: chunked\r\n\r\n0\r\n\r\n");
            try w.interface.flush();

            _ = try expectResponseHead(&r.interface, "HTTP/1.1 400 Bad Request");
            // Connection must be closed after a framing violation: the
            // smuggled follow-up must never be parsed as a request.
            var sink: [64]u8 = undefined;
            _ = r.interface.readSliceAll(sink[0..1]) catch |err| {
                try std.testing.expect(err == error.EndOfStream or err == error.ReadFailed);
            };

            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.client, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(0, app.hits); // handler never ran
}

test "talon.http.Server: shutdown interrupts an idle keep-alive connection promptly (tcp)" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    const port = listener.server.socket.address.ip.getPort();

    var app: HelloApp = .{};
    const Srv = Server(HelloApp);
    // Default drain_timeout is 30s: if shutdown still relied on the
    // drain-cancel path for idle connections, the elapsed assert below
    // would fail.
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    var served: zio.ResetEvent = .init;

    const Fns = struct {
        fn runServer(s: *Srv, l: *TcpListener, done: *zio.ResetEvent) !void {
            try s.serve(l);
            done.set();
        }

        fn client(p: u16, s: *Srv, done: *zio.ResetEvent) !void {
            const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", p);
            const stream = try addr.connect(.{});
            defer stream.close();

            try stream.writeAll("GET / HTTP/1.1\r\nhost: t\r\n\r\n", .none);
            var buf: [1024]u8 = undefined;
            const n = try stream.read(&buf, .fromSeconds(5));
            try std.testing.expect(std.mem.startsWith(u8, buf[0..n], "HTTP/1.1 200"));

            // Connection now idles in keep-alive; shutdown must not need the
            // 30s drain window to reclaim it.
            var stopwatch = zio.Stopwatch.start();
            s.shutdown();
            try done.wait();
            const elapsed_ms = stopwatch.read().toMilliseconds();
            try std.testing.expect(elapsed_ms < 5_000);
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener, &served });
    try group.spawn(Fns.client, .{ port, &server, &served });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(1, app.hits);
}

test "talon.http.ServerWith: proxy_protocol middleware rewrites remote identity" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    const RemoteCheckApp = struct {
        pub fn handle(self: *@This(), req: *Request, res: *Response) !void {
            _ = self;
            _ = req;
            try res.respond("ok", .{});
        }
    };

    // Capture middleware after proxy_protocol: assert the rewrite happened.
    const AssertProxied = struct {
        pub fn run(conn: anytype, next: anytype) anyerror!void {
            const info = conn.remoteInfo();
            try std.testing.expect(info == .net);
            try std.testing.expectEqual(12345, info.net.ip.getPort());
            try next.call(conn);
        }
    };

    var app: RemoteCheckApp = .{};
    const Srv = ServerWith(RemoteCheckApp, .{ middleware.proxy_protocol, AssertProxied });
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }

        fn client(l: *MemoryListener, s: *Srv) !void {
            const conn = try l.connect();
            defer conn.close();

            var wbuf: [256]u8 = undefined;
            var w = conn.writer(&wbuf);
            var rbuf: [1024]u8 = undefined;
            var r = conn.reader(&rbuf);

            // PROXY v2 preamble: src 10.1.2.3:12345 → dst 10.0.0.1:80.
            const preamble = "\x0d\x0a\x0d\x0a\x00\x0d\x0a\x51\x55\x49\x54\x0a" ++ // signature
                "\x21" ++ // v2, PROXY
                "\x11" ++ // AF_INET, STREAM
                "\x00\x0c" ++ // 12 bytes of addresses
                "\x0a\x01\x02\x03" ++ // src 10.1.2.3
                "\x0a\x00\x00\x01" ++ // dst 10.0.0.1
                "\x30\x39" ++ // src port 12345
                "\x00\x50"; // dst port 80
            try w.interface.writeAll(preamble);
            try w.interface.writeAll("GET / HTTP/1.1\r\nhost: t\r\n\r\n");
            try w.interface.flush();

            const cl = try expectResponseHead(&r.interface, "HTTP/1.1 200 OK");
            var buf: [16]u8 = undefined;
            try r.interface.readSliceAll(buf[0..cl]);
            try std.testing.expectEqualStrings("ok", buf[0..cl]);

            s.shutdown();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.runServer, .{ &server, &listener });
    try group.spawn(Fns.client, .{ &listener, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

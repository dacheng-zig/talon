//! Connection upgrade: a handler calls `req.upgrade.accept` to send 101 and
//! take over the connection, then speaks a post-HTTP protocol on the same
//! socket. Driven with a raw memory connection (the client speaks the upgrade
//! handshake by hand, since the HTTP client never requests one).

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const Server = talon.http.Server;
const Request = talon.http.Request;
const Response = talon.http.Response;
const MemoryListener = talon.MemoryListener;

// After 101, echoes one line back over the upgraded connection.
const EchoUpgradeApp = struct {
    pub fn handle(_: *EchoUpgradeApp, req: *Request, _: *Response) !void {
        try req.upgrade.accept(.{
            .protocol = "echo",
            .extra_headers = &.{.{ .name = "x-test", .value = "1" }},
        });
        const line = try req.upgrade.reader.takeDelimiterInclusive('\n');
        try req.upgrade.writer.writeAll(line);
        try req.upgrade.writer.flush();
    }
};

// Reads a response head, asserting the status line and returning nothing else.
fn expect101(r: *std.Io.Reader) !void {
    const status = try r.takeDelimiterInclusive('\n');
    try std.testing.expect(std.mem.indexOf(u8, status, "101 Switching Protocols") != null);
    var saw_upgrade = false;
    var saw_test = false;
    while (true) {
        const line = try r.takeDelimiterInclusive('\n');
        if (std.mem.eql(u8, line, "\r\n")) break; // end of head
        if (std.mem.indexOf(u8, line, "upgrade: echo") != null) saw_upgrade = true;
        if (std.mem.indexOf(u8, line, "x-test: 1") != null) saw_test = true;
    }
    try std.testing.expect(saw_upgrade and saw_test);
}

test "upgrade: handler sends 101 and takes over the connection" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: EchoUpgradeApp = .{};
    const Srv = Server(EchoUpgradeApp);
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
            var cw = conn.writer(&wbuf);
            var rbuf: [256]u8 = undefined;
            var cr = conn.reader(&rbuf);

            try cw.interface.writeAll("GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: echo\r\nConnection: upgrade\r\n\r\n");
            try cw.interface.flush();
            try expect101(&cr.interface);

            // Now the connection speaks the upgraded protocol: send a line, get
            // it echoed back.
            try cw.interface.writeAll("hello upgraded\n");
            try cw.interface.flush();
            const echo = try cr.interface.takeDelimiterInclusive('\n');
            try std.testing.expectEqualStrings("hello upgraded\n", echo);

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

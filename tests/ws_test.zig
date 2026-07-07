//! End-to-end WebSocket: a talon WS echo server upgraded via the HTTP handler,
//! driven by a hand-rolled client (raw memory connection) that performs the
//! real RFC 6455 handshake and speaks masked frames through the same codec.
//! Covers text/binary echo, fragmentation reassembly, auto ping→pong, and the
//! close handshake.

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const Server = talon.http.Server;
const Request = talon.http.Request;
const Response = talon.http.Response;
const MemoryListener = talon.MemoryListener;
const wscodec = talon.http.codec.ws;

const WsEchoApp = struct {
    pub fn handle(_: *WsEchoApp, req: *Request, _: *Response) !void {
        var socket = try talon.http.ws.upgrade(req, .{});
        while (try socket.read()) |msg| switch (msg) {
            .text => |t| try socket.writeText(t),
            .binary => |b| try socket.writeBinary(b),
        };
    }
};

const client_key = "dGhlIHNhbXBsZSBub25jZQ==";
const client_accept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
const client_mask = [4]u8{ 0xA1, 0xB2, 0xC3, 0xD4 };

// Consumes the 101 head, asserting the accept value.
fn readHandshake(r: *std.Io.Reader) !void {
    const status = try r.takeDelimiterInclusive('\n');
    try std.testing.expect(std.mem.indexOf(u8, status, "101") != null);
    var ok = false;
    while (true) {
        const line = try r.takeDelimiterInclusive('\n');
        if (std.mem.eql(u8, line, "\r\n")) break;
        if (std.mem.indexOf(u8, line, client_accept) != null) ok = true;
    }
    try std.testing.expect(ok);
}

// Reads one server frame (unmasked) into `buf`, asserting opcode and returning
// the payload slice.
fn readServerFrame(r: *std.Io.Reader, buf: []u8, expect_op: wscodec.Opcode) ![]u8 {
    const h = try wscodec.readFrameHeader(r);
    try std.testing.expectEqual(expect_op, h.opcode);
    try std.testing.expect(!h.masked); // server→client frames are never masked
    const len: usize = @intCast(h.payload_len);
    try r.readSliceAll(buf[0..len]);
    return buf[0..len];
}

test "ws: handshake, text/binary echo, fragmentation, ping/pong, close" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var listener = try MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();

    var app: WsEchoApp = .{};
    const Srv = Server(WsEchoApp);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();

    const Fns = struct {
        fn runServer(s: *Srv, l: *MemoryListener) !void {
            try s.serve(l);
        }
        fn client(l: *MemoryListener, s: *Srv) !void {
            const conn = try l.connect();
            defer conn.close();
            var wbuf: [512]u8 = undefined;
            var cw = conn.writer(&wbuf);
            const w = &cw.interface;
            var rbuf: [512]u8 = undefined;
            var cr = conn.reader(&rbuf);
            const r = &cr.interface;
            var payload: [256]u8 = undefined;

            try w.writeAll("GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n" ++
                "Connection: Upgrade\r\nSec-WebSocket-Key: " ++ client_key ++
                "\r\nSec-WebSocket-Version: 13\r\n\r\n");
            try w.flush();
            try readHandshake(r);

            // Text echo.
            try wscodec.writeFrame(w, .text, true, "hello", client_mask);
            try w.flush();
            try std.testing.expectEqualStrings("hello", try readServerFrame(r, &payload, .text));

            // Binary echo.
            try wscodec.writeFrame(w, .binary, true, &[_]u8{ 1, 2, 3, 0, 255 }, client_mask);
            try w.flush();
            try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 2, 3, 0, 255 }, try readServerFrame(r, &payload, .binary));

            // Fragmented text ("frag" + "ment") reassembled by the server.
            try wscodec.writeFrame(w, .text, false, "frag", client_mask);
            try wscodec.writeFrame(w, .continuation, true, "ment", client_mask);
            try w.flush();
            try std.testing.expectEqualStrings("fragment", try readServerFrame(r, &payload, .text));

            // Ping → auto pong (echoing the ping payload).
            try wscodec.writeFrame(w, .ping, true, "hi", client_mask);
            try w.flush();
            try std.testing.expectEqualStrings("hi", try readServerFrame(r, &payload, .pong));

            // Close handshake: client close → server echoes a close.
            var close_payload: [2]u8 = undefined;
            std.mem.writeInt(u16, &close_payload, 1000, .big);
            try wscodec.writeFrame(w, .close, true, &close_payload, client_mask);
            try w.flush();
            const h = try wscodec.readFrameHeader(r);
            try std.testing.expectEqual(wscodec.Opcode.close, h.opcode);

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

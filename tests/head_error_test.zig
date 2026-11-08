const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

test "HEAD: pre-handler errors suppress content without changing method case or error status" {
    const Kind = enum { arena, malformed, oversized, version };
    const App = struct {
        pub fn handle(_: *@This(), _: *talon.http.Request, _: *talon.http.Response) !void {
            return error.HandlerMustNotRun;
        }
    };
    const Server = talon.http.Server(App);
    for ([_]Kind{ .arena, .malformed, .oversized, .version }) |kind| {
        const rt = try zio.Runtime.init(std.testing.allocator, .{});
        defer rt.deinit();
        var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
        defer listener.deinit();
        var app: App = .{};
        var server = try Server.init(std.testing.allocator, &app, .{ .limits = .{
            .max_connections = 1,
            .max_header_size = 128,
            .read_buffer_size = 256,
            .write_buffer_size = 128,
            .max_request_arena_bytes = if (kind == .arena) 0 else null,
        } });
        defer server.deinit();
        const F = struct {
            fn serve(s: *Server, l: *talon.MemoryListener) !void {
                try s.serve(l);
            }
            fn client(s: *Server, l: *talon.MemoryListener, failure: Kind) !void {
                defer s.shutdown();
                for ([_][]const u8{ "HEAD", "GET", "head", "HEADX" }) |method| {
                    const raw = try l.connect();
                    defer raw.close();
                    var writer = raw.writer(&.{});
                    writer.setTimeout(.fromSeconds(2));
                    var request_storage: [512]u8 = undefined;
                    const tail = switch (failure) {
                        .arena => " / HTTP/1.1\r\nHost: h\r\n\r\n",
                        .malformed => " / HTTP/1.1\r\nHost: h\r\nBad-Header\r\n\r\n",
                        .oversized => " / HTTP/1.1\r\nHost: h\r\nX: " ++ "x" ** 160 ++ "\r\n\r\n",
                        .version => " / HTTP/9.9\r\nHost: h\r\n\r\n",
                    };
                    const request = try std.fmt.bufPrint(&request_storage, "{s}{s}", .{ method, tail });
                    // The first fragment does not contain a complete method token.
                    try writer.interface.writeAll(request[0..3]);
                    try writer.interface.flush();
                    try zio.sleep(.fromMilliseconds(5));
                    try writer.interface.writeAll(request[3..]);
                    try writer.interface.flush();
                    var buffer: [1024]u8 = undefined;
                    var reader = raw.reader(&buffer);
                    reader.setTimeout(.fromSeconds(2));
                    const wire = try reader.interface.allocRemaining(std.testing.allocator, .limited(8192));
                    defer std.testing.allocator.free(wire);
                    const status: []const u8 = switch (failure) {
                        .arena => "500 Internal Server Error",
                        .malformed => "400 Bad Request",
                        .oversized => "431 Request Header Fields Too Large",
                        .version => "505 HTTP Version Not Supported",
                    };
                    var status_storage: [128]u8 = undefined;
                    const expected_status = try std.fmt.bufPrint(&status_storage, "HTTP/1.1 {s}\r\n", .{status});
                    try std.testing.expect(std.mem.startsWith(u8, wire, expected_status));
                    const end = std.mem.indexOf(u8, wire, "\r\n\r\n") orelse return error.MissingResponseHead;
                    const body = wire[end + 4 ..];
                    const phrase = status[4..];
                    var length_storage: [64]u8 = undefined;
                    const length = try std.fmt.bufPrint(&length_storage, "content-length: {d}\r\n", .{phrase.len});
                    try std.testing.expect(std.mem.indexOf(u8, wire[0 .. end + 2], length) != null);
                    if (std.mem.eql(u8, method, "HEAD")) {
                        try std.testing.expectEqual(@as(usize, 0), body.len);
                    } else try std.testing.expectEqualStrings(phrase, body);
                }
            }
        };
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.serve, .{ &server, &listener });
        try group.spawn(F.client, .{ &server, &listener, kind });
        try group.wait();
        try std.testing.expect(!group.hasFailed());
        try std.testing.expectEqual(@as(u64, 4), server.stats().accepted);
        try std.testing.expectEqual(server.stats().accepted, server.stats().finished_tasks);
        try std.testing.expectEqual(@as(usize, 0), server.stats().active_tasks);
        try std.testing.expectEqual(@as(u64, 0), server.stats().connection_errors);
    }
}

test "HEAD: pipelined error suppression uses the next request at the reader seek offset" {
    const App = struct {
        calls: usize = 0,
        pub fn handle(self: *@This(), _: *talon.http.Request, res: *talon.http.Response) !void {
            self.calls += 1;
            try res.respond("ok", .{});
        }
    };
    const Server = talon.http.Server(App);
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try talon.MemoryListener.init(std.testing.allocator, .{});
    defer listener.deinit();
    var app: App = .{};
    var server = try Server.init(std.testing.allocator, &app, .{ .limits = .{
        .max_header_size = 128,
        .read_buffer_size = 256,
        .write_buffer_size = 512,
    } });
    defer server.deinit();
    const F = struct {
        fn serve(s: *Server, l: *talon.MemoryListener) !void {
            try s.serve(l);
        }
        fn client(s: *Server, l: *talon.MemoryListener) !void {
            defer s.shutdown();
            for ([_]bool{ false, true }) |first_head| {
                const raw = try l.connect();
                defer raw.close();
                var writer = raw.writer(&.{});
                writer.setTimeout(.fromSeconds(2));
                var request_storage: [256]u8 = undefined;
                const request = try std.fmt.bufPrint(&request_storage, "{s} / HTTP/1.1\r\nHost: h\r\n\r\n{s} / HTTP/1.1\r\nHost: h\r\nBad-Header\r\n\r\n", .{ if (first_head) "HEAD" else "GET", if (first_head) "GET" else "HEAD" });
                try writer.interface.writeAll(request);
                try writer.interface.flush();
                var buffer: [1024]u8 = undefined;
                var reader = raw.reader(&buffer);
                reader.setTimeout(.fromSeconds(2));
                const wire = try reader.interface.allocRemaining(std.testing.allocator, .limited(8192));
                defer std.testing.allocator.free(wire);
                try std.testing.expect(std.mem.startsWith(u8, wire, "HTTP/1.1 200 OK\r\n"));
                const first_end = (std.mem.indexOf(u8, wire, "\r\n\r\n") orelse return error.MissingResponseHead) + 4;
                if (!first_head) try std.testing.expectEqualStrings("ok", wire[first_end..][0..2]);
                const second = wire[first_end + @as(usize, if (first_head) 0 else 2) ..];
                try std.testing.expect(std.mem.startsWith(u8, second, "HTTP/1.1 400 Bad Request\r\n"));
                const second_end = (std.mem.indexOf(u8, second, "\r\n\r\n") orelse return error.MissingResponseHead) + 4;
                try std.testing.expectEqualStrings(if (first_head) "Bad Request" else "", second[second_end..]);
            }
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener });
    try group.spawn(F.client, .{ &server, &listener });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(@as(usize, 2), app.calls);
    try std.testing.expectEqual(@as(u64, 2), server.stats().accepted);
    try std.testing.expectEqual(server.stats().accepted, server.stats().finished_tasks);
    try std.testing.expectEqual(@as(usize, 0), server.stats().active_tasks);
    try std.testing.expectEqual(@as(u64, 0), server.stats().connection_errors);
}

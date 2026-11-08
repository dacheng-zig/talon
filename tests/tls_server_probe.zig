//! Finite real TCP/TLS fixture, with native task and allocation evidence.
const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");
const tls = @import("talon_tls");

const App = struct {
    fn send(socket: *talon.http.ws.WebSocket, id: u8) !void {
        var payload: [65536]u8 = undefined;
        for (0..3) |sequence| {
            @memset(&payload, id);
            payload[1] = @intCast(sequence);
            try socket.writeBinary(&payload);
        }
    }

    pub fn handle(_: *App, req: *talon.http.Request, res: *talon.http.Response) !void {
        if (std.mem.eql(u8, req.target(), "/duplex")) {
            var socket = try talon.http.ws.upgrade(req, .{ .max_message_size = 1024, .send_options = .{ .max_frame_payload = 4096 } });
            var children: zio.Group = .init;
            defer children.cancel();
            try children.spawn(send, .{ &socket, @as(u8, 'A') });
            try children.spawn(send, .{ &socket, @as(u8, 'B') });
            // Automatic Pong can overlap the two sender children.
            while (try socket.read()) |message| switch (message) {
                .text => |text| if (std.mem.eql(u8, text, "done")) break,
                .binary => {},
            };
            try children.wait();
            try socket.close(.normal, "");
        } else if (std.mem.eql(u8, req.target(), "/ws")) {
            var socket = try talon.http.ws.upgrade(req, .{ .max_message_size = 1024, .send_options = .{ .max_frame_payload = 4096 } });
            while (try socket.read()) |message| switch (message) {
                .text => |text| try socket.writeText(text),
                .binary => |bytes| try socket.writeBinary(bytes),
            };
        } else try res.respond("secure talon\n", .{});
    }
};

const NotifyProtocol = struct {
    pub fn serve(conn: anytype, _: *App) !void {
        try conn.writer().writeAll("notify");
        try conn.writer().flush();
        try conn.raw.closeNotify();
    }
};

const ExpiredProtocol = struct {
    pub fn serve(conn: anytype, _: *App) !void {
        try conn.writer().writeAll("expired");
        try conn.writer().flush();
        try conn.setWriteTimeout(.{ .deadline = .fromNanoseconds(0) });
    }
};

const AlpnProtocol = struct {
    const Http = talon.http.Http1Protocol(App);
    pub const Limits = Http.Limits;
    pub const engineLimits = Http.engineLimits;
    pub const validateTransport = Http.validateTransport;

    pub fn serve(conn: anytype, app: *App) !void {
        return serveWithConfig(conn, app, .{});
    }
    pub fn serveWithConfig(conn: anytype, app: *App, limits: Limits) !void {
        const protocol = conn.raw.negotiatedProtocol();
        std.log.info("tls_alpn_selected={s}", .{if (protocol.len == 0) "none" else protocol});
        std.log.info("tls_reader_window={d} max_header_size={d}", .{ conn.reader().buffer.len, limits.max_header_size });
        try Http.serveWithConfig(conn, app, limits);
    }
};

fn run(comptime Proto: type, gpa: std.mem.Allocator, context: *tls.Context, duration: usize, normal_notify: bool) !void {
    return runWithWindow(Proto, gpa, context, duration, normal_notify, 16 * 1024);
}

fn runWithWindow(comptime Proto: type, gpa: std.mem.Allocator, context: *tls.Context, duration: usize, normal_notify: bool, max_header_size: u32) !void {
    const Server = talon.StreamServer(Proto, App);
    const Stopper = struct {
        fn stop(server: *Server, milliseconds: usize) !void {
            try zio.sleep(.fromMilliseconds(milliseconds));
            server.shutdown();
        }
    };
    var budget = talon.core.AllocationBudget.init(gpa, 4 * 1024 * 1024);
    defer budget.deinit();
    {
        var app: App = .{};
        const Policy = if (@hasDecl(Proto, "Limits")) Proto.Limits else talon.core.Limits;
        var limits: Policy = .{ .handshake_timeout = .fromMilliseconds(300), .drain_timeout = .fromMilliseconds(300) };
        if (@hasField(Policy, "max_header_size")) limits.max_header_size = max_header_size;
        var server = try Server.init(budget.allocator(), &app, .{
            .limits = limits,
        });
        defer server.deinit();
        var listener = try talon.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
        var adapter: tls.Adapter = .{ .context = context, .close_notify_timeout = if (normal_notify) .fromMilliseconds(200) else null };
        var task = try zio.spawn(Stopper.stop, .{ &server, duration });
        defer task.cancel();
        std.log.info("tls_server_port={d}", .{listener.server.socket.address.ip.getPort()});
        try server.serveAdapted(&listener, &adapter);
        try task.join();
        const stats = server.stats();
        std.log.info("tls_server_done=1 accepted={d} finished={d} active={d} transport_failures={d} handshake_timeouts={d} forced_drains={d} connection_errors={d}", .{
            stats.accepted, stats.finished_tasks, stats.active_tasks, stats.transport_failures, stats.handshake_timeouts, stats.forced_drains, stats.connection_errors,
        });
        if (stats.accepted != stats.finished_tasks or stats.active_tasks != 0) return error.UnreleasedTask;
    }
    std.log.info("tls_server_budget={d}", .{budget.stats().reserved_bytes});
    if (budget.stats().reserved_bytes != 0) return error.UnreleasedAllocation;
}

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len != 4 and args.len != 5) return error.ExpectedCertificateKeyDuration;
    const cert = try init.arena.allocator().dupeZ(u8, args[1]);
    const key = try init.arena.allocator().dupeZ(u8, args[2]);
    const duration = try std.fmt.parseInt(usize, args[3], 10);
    if (duration < 1000 or duration > 10000) return error.InvalidDuration;
    const enable_alpn = args.len == 5 and std.mem.eql(u8, args[4], "alpn");
    const enable_sni = args.len == 5 and (std.mem.eql(u8, args[4], "sni") or std.mem.eql(u8, args[4], "sni-check"));
    const arena = init.arena.allocator();
    var alpha_name = "alpha.localhost".*;
    const identities: []const tls.Context.NamedIdentity = if (enable_sni) &.{
        .{ .server_name = &alpha_name, .certificate_chain = try std.fmt.allocPrintSentinel(arena, "{s}.alpha.pem", .{cert}, 0), .private_key = try std.fmt.allocPrintSentinel(arena, "{s}.alpha.pem", .{key}, 0) },
        .{ .server_name = "beta.localhost", .certificate_chain = try std.fmt.allocPrintSentinel(arena, "{s}.beta.pem", .{cert}, 0), .private_key = try std.fmt.allocPrintSentinel(arena, "{s}.beta.pem", .{key}, 0) },
    } else &.{};
    const options: tls.Context.Options = .{
        .alpn_protocols = if (enable_alpn or enable_sni) &.{"http/1.1"} else &.{},
        .server_identities = identities,
    };
    if (args.len == 5 and std.mem.eql(u8, args[4], "sni-check")) {
        const Check = struct {
            fn valid(gpa: std.mem.Allocator, certificate: [:0]const u8, private_key: [:0]const u8, config: tls.Context.Options) !void {
                var ctx = try tls.Context.initWithOptions(gpa, certificate, private_key, config);
                defer ctx.deinit();
            }
            fn invalid(gpa: std.mem.Allocator, certificate: [:0]const u8, private_key: [:0]const u8, config: tls.Context.Options) !void {
                var entries: [2]tls.Context.NamedIdentity = .{ config.server_identities[0], config.server_identities[1] };
                entries[1].private_key = ""; // First child has already loaded.
                var ctx = tls.Context.initWithOptions(gpa, certificate, private_key, .{ .alpn_protocols = config.alpn_protocols, .server_identities = &entries }) catch |err| {
                    if (err == error.TlsIdentityFailed) return;
                    return err;
                };
                ctx.deinit();
                return error.TestUnexpectedResult;
            }
        };
        try std.testing.checkAllAllocationFailures(init.gpa, Check.valid, .{ cert, key, options });
        try std.testing.checkAllAllocationFailures(init.gpa, Check.invalid, .{ cert, key, options });
        std.log.info("tls_sni_configuration_checked=1", .{});
        return;
    }
    var context = try tls.Context.initWithOptions(init.gpa, cert, key, options);
    @memset(&alpha_name, 'x'); // Context owns names after init, including moves.
    defer context.deinit();
    const rt = try zio.Runtime.init(init.gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    if (args.len == 5) {
        if (std.mem.eql(u8, args[4], "wide-head")) {
            try runWithWindow(AlpnProtocol, init.gpa, &context, duration, true, 64 * 1024);
        } else if (std.mem.eql(u8, args[4], "notify")) {
            try run(NotifyProtocol, init.gpa, &context, duration, false);
        } else if (std.mem.eql(u8, args[4], "finish")) {
            try run(talon.http.Http1Protocol(App), init.gpa, &context, duration, true);
        } else if (std.mem.eql(u8, args[4], "expired")) {
            try run(ExpiredProtocol, init.gpa, &context, duration, true);
        } else if (enable_alpn or enable_sni or std.mem.eql(u8, args[4], "alpn-disabled")) {
            try run(AlpnProtocol, init.gpa, &context, duration, true);
        } else return error.InvalidMode;
    } else try run(talon.http.Http1Protocol(App), init.gpa, &context, duration, false);
}

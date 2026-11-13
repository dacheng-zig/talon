//! Local certificate acceptance probe. See docs/design/tls.md.
const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");
const http = talon.http.client;

fn probe(gpa: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var store: http.RootStore = .{};
    defer store.deinit(gpa);
    try store.bundle.addCertsFromFilePath(gpa, io, std.Io.Timestamp.now(io, .real), .cwd(), args[1]);
    var budget = talon.core.AllocationBudget.init(gpa, 256 * 1024);
    defer budget.deinit();
    var result: ?anyerror = null;
    {
        var c = http.TlsClient.init(budget.allocator(), .{
            .gpa = budget.allocator(),
            .io = io,
            .verification = if (args.len == 4) .self_signed else .{ .system = &store },
        }, .{ .retry = .{ .max_retries = 0 }, .timeouts = .{ .connect = .fromSeconds(2), .read = .fromSeconds(2), .write = .fromSeconds(2), .total = .fromSeconds(5) } });
        defer c.deinit();
        fetch(&c, args[0]) catch |err| {
            result = err;
        };
        if (c.poolStats().active != 0 or c.poolStats().waiting != 0) return error.UnreleasedAdmission;
        if (result != null and c.poolStats().created != 0) return error.FailedHandshakeWasPooled;
    }
    if (budget.stats().reserved_bytes != 0) return error.UnreleasedTlsAllocation;
    if (result) |err| {
        if (!std.mem.eql(u8, @errorName(err), args[2])) {
            std.log.err("expected={s} actual={s}", .{ args[2], @errorName(err) });
            return error.UnexpectedTlsError;
        }
    } else if (!std.mem.eql(u8, args[2], "ok")) return error.ExpectedTlsRejection;
    std.log.info("result={s} requested_bytes_after_deinit=0 active=0 waiting=0", .{if (result) |err| @errorName(err) else "ok"});
}

fn fetch(c: *http.TlsClient, url: []const u8) !void {
    const response = try c.getUrl(url);
    defer response.deinit();
    if (response.status() != 200) return error.UnexpectedStatus;
    const body = try response.readAllAlloc(std.heap.page_allocator, 17);
    defer std.heap.page_allocator.free(body);
    if (!std.mem.eql(u8, body, &@as([17]u8, @splat('x')))) return error.UnexpectedBody;
}

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len < 4 or args.len > 5 or (args.len == 5 and !std.mem.eql(u8, args[4], "self-signed"))) return error.InvalidProbeArguments;
    const rt = try zio.Runtime.init(init.gpa, .{});
    defer rt.deinit();
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(probe, .{ init.gpa, rt.io(), @as([]const []const u8, @ptrCast(args[1..])) });
    try group.wait();
    if (group.hasFailed() or group.isCanceled()) return error.TlsProbeFailed;
}

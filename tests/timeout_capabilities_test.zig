const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

// An immediate, pinned fixture transport: deadlines are recorded without
// blocking I/O, so each capability combination can be exercised separately.
fn Fixture(comptime can_read: bool, comptime can_write: bool) type {
    return struct {
        const Self = @This();
        reader: std.Io.Reader = .fixed("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"),
        output: std.Io.Writer.Allocating,
        dials: usize = 0,
        closes: usize = 0,
        read_timeout: zio.Timeout = .none,
        write_timeout: zio.Timeout = .none,

        pub const Raw = struct {
            state: *Self,
            pub const pinned_transport = true;
            pub const setReadTimeout = if (can_read) readTimeout else {};
            pub const setWriteTimeout = if (can_write) writeTimeout else {};
            fn readTimeout(self: @This(), timeout: zio.Timeout) void {
                self.state.read_timeout = timeout;
            }
            fn writeTimeout(self: @This(), timeout: zio.Timeout) void {
                self.state.write_timeout = timeout;
            }
            pub fn ioReader(self: @This()) *std.Io.Reader {
                return &self.state.reader;
            }
            pub fn ioWriter(self: @This()) *std.Io.Writer {
                return &self.state.output.writer;
            }
            pub fn flush(_: @This()) !void {}
            pub fn close(self: @This()) void {
                self.state.closes += 1;
            }
            pub fn isLikelyLive(_: @This()) bool {
                return true;
            }
        };
        pub const Connector = struct {
            state: *Self,
            pub const RawConnection = Raw;
            pub fn connect(self: @This(), _: talon.http.client.Origin, _: zio.Timeout) !Raw {
                self.state.dials += 1;
                return .{ .state = self.state };
            }
        };
    };
}

test "client: unsupported request budgets fail before dial and output, explicit disabling remains usable" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    const none: talon.http.client.Timeouts = .{ .connect = .none, .read = .none, .write = .none, .total = .none };
    inline for (.{ false, true }) |read| {
        inline for (.{ false, true }) |write| {
            const F = Fixture(read, write);
            const C = talon.http.client.Client(F.Connector);
            for (0..4) |policy| {
                var state: F = .{ .output = .init(std.testing.allocator) };
                defer state.output.deinit();
                var c = C.init(std.testing.allocator, .{ .state = &state }, .{ .timeouts = none });
                defer c.deinit();
                var timeouts = none;
                switch (policy) {
                    0 => {},
                    1 => timeouts.read = .fromSeconds(1),
                    2 => timeouts.write = .fromSeconds(1),
                    3 => timeouts.total = .fromSeconds(1),
                    else => unreachable,
                }
                const expected: ?anyerror = if ((policy == 1 or policy == 3) and !read)
                    error.UnsupportedReadTimeout
                else if ((policy == 2 or policy == 3) and !write)
                    error.UnsupportedWriteTimeout
                else
                    null;
                const spec: C.RequestSpec = .{ .origin = .{ .host = "fixture", .port = 80 }, .timeouts = timeouts };
                if (expected) |err| {
                    try std.testing.expectError(err, c.request(spec));
                    try std.testing.expectEqual(@as(usize, 0), state.dials);
                    try std.testing.expectEqual(@as(usize, 0), state.output.written().len);
                    try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
                } else {
                    const resp = try c.request(spec);
                    try std.testing.expectEqual(@as(u16, 200), resp.status());
                    if (policy == 3) {
                        try std.testing.expect(state.read_timeout == .deadline and state.write_timeout == .deadline);
                        try std.testing.expectEqual(state.read_timeout.deadline.toNanoseconds(), state.write_timeout.deadline.toNanoseconds());
                    }
                    resp.deinit();
                    try std.testing.expectEqual(@as(usize, 1), state.dials);
                    try std.testing.expectEqual(@as(usize, 1), state.closes);
                }
            }
        }
    }
}

test "client: configured liveness validation requires read deadlines before pool acquisition" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    const F = Fixture(false, false);
    var state: F = .{ .output = .init(std.testing.allocator) };
    defer state.output.deinit();
    const C = talon.http.client.Client(F.Connector);
    var c = C.init(std.testing.allocator, .{ .state = &state }, .{
        .timeouts = .{ .read = .none, .write = .none, .total = .none },
        .pool = .{ .validate_on_checkout = true, .max_per_origin = 1 },
    });
    defer c.deinit();
    try std.testing.expectError(error.UnsupportedReadTimeout, c.get(.{ .host = "fixture", .port = 80 }, "/"));
    try std.testing.expectEqual(@as(usize, 0), state.dials);
    try std.testing.expectEqual(@as(usize, 0), c.pool.origins.count());
}

test "upgrade: unsupported read deadline cannot commit a 101 response" {
    var input: std.Io.Reader = .fixed("");
    var out: [512]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&out);
    var taken = false;
    var ctx: u8 = 0;
    const Setter = struct {
        fn set(_: *anyopaque, timeout: zio.Timeout) talon.core.TimeoutError!void {
            try (talon.core.TimeoutCapabilities{ .read = false, .write = true }).checkRead(timeout);
        }
    };
    const Upgrade = std.meta.Child(@FieldType(talon.http.Request, "upgrade"));
    var upgrade: Upgrade = .{
        .reader = &input,
        .writer = &writer,
        .taken = &taken,
        .ctx = &ctx,
        .set_read_timeout_fn = Setter.set,
    };
    try std.testing.expectError(error.UnsupportedReadTimeout, upgrade.accept(.{ .protocol = "echo", .read_timeout = .fromSeconds(1) }));
    try std.testing.expect(!taken);
    try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
    try upgrade.accept(.{ .protocol = "echo", .read_timeout = .none });
    try std.testing.expect(taken);
}

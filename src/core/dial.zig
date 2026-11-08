//! Protocol-neutral outbound transport establishment. Dialers declare Target,
//! RawConnection and dial(target, timeout); success transfers raw ownership.
//! A single absolute deadline covers resolution and all connection attempts.
const std = @import("std");
const zio = @import("zio");
const transport = @import("listener.zig");
const tcp_user_timeout = @import("tcp_user_timeout.zig");

pub const TcpEndpoint = struct {
    /// Borrowed for the duration of dial; no scheme or default-port policy.
    host: []const u8,
    port: u16,
};

pub const TcpDialer = struct {
    pub const Target = TcpEndpoint;
    pub const RawConnection = transport.TcpConnection;
    /// Optional source address. Port zero delegates port selection to the OS;
    /// fixed ports are exclusive, with no implicit reuse or fallback policy.
    local_address: ?zio.net.IpAddress = null,
    /// Linux TCP_USER_TIMEOUT on the established socket. Null preserves OS
    /// policy; 0 restores its default. Separate from the establishment deadline.
    tcp_user_timeout_ms: ?u31 = null,

    pub fn dial(self: TcpDialer, endpoint: Target, timeout: zio.Timeout) !RawConnection {
        try tcp_user_timeout.validate(self.tcp_user_timeout_ms);
        return dialWith(ZioBackend{ .local_address = self.local_address, .tcp_user_timeout_ms = self.tcp_user_timeout_ms }, endpoint, timeout);
    }
};

pub const MemoryDialer = struct {
    listener: *transport.MemoryListener,
    pub const Target = void;
    pub const RawConnection = transport.MemoryConnection;

    pub fn dial(self: MemoryDialer, _: Target, timeout: zio.Timeout) !RawConnection {
        const deadline = timeout.toDeadline();
        try checkDeadline(deadline);
        const raw = try zio.withTimeout(deadline, transport.MemoryListener.connect, .{self.listener});
        errdefer raw.close();
        try checkDeadline(deadline);
        return raw;
    }
};

fn checkDeadline(deadline: zio.Timeout) !void {
    try zio.checkCancel();
    if (deadline == .deadline and
        zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline.deadline.toNanoseconds())
        return error.Timeout;
}

const ZioBackend = struct {
    pub const RawConnection = transport.TcpConnection;
    local_address: ?zio.net.IpAddress,
    tcp_user_timeout_ms: ?u31,

    fn resolve(_: ZioBackend, endpoint: TcpEndpoint, storage: []zio.net.HostName.LookupResult) !usize {
        const host = try zio.net.HostName.init(endpoint.host);
        return host.lookup(storage, .{ .port = endpoint.port });
    }

    fn connect(self: ZioBackend, address: zio.net.IpAddress, deadline: zio.Timeout) !RawConnection {
        const stream = if (self.local_address) |local| blk: {
            if (local.getFamily() != address.getFamily()) return error.AddressFamilyMismatch;
            var socket = try zio.net.Socket.open(.stream, .fromPosix(address.any.family), .ip);
            errdefer socket.close();
            try socket.bind(.{ .ip = local });
            try checkDeadline(deadline);
            try socket.connect(.{ .ip = address }, .{ .timeout = deadline });
            break :blk zio.net.Stream{ .socket = socket };
        } else try zio.net.tcpConnectToAddress(address, .{ .timeout = deadline });
        errdefer stream.close();
        try tcp_user_timeout.apply(stream.socket, self.tcp_user_timeout_ms);
        stream.socket.setNoDelay(true) catch {};
        return .{ .stream = stream };
    }
};

fn numericAddress(endpoint: TcpEndpoint) ?zio.net.IpAddress {
    return zio.net.IpAddress.parseIp(endpoint.host, endpoint.port) catch null;
}

// Keep the resolver/attempt mechanics testable without Internet DNS or packets
// dropped by a firewall. Production has one backend and no runtime dispatch.
fn dialWith(backend: anytype, endpoint: TcpEndpoint, timeout: zio.Timeout) !@TypeOf(backend).RawConnection {
    const deadline = timeout.toDeadline();
    try checkDeadline(deadline);
    const Attempt = struct {
        fn run(b: @TypeOf(backend), target: TcpEndpoint, budget: zio.Timeout) !@TypeOf(backend).RawConnection {
            const raw = if (numericAddress(target)) |address|
                try b.connect(address, budget)
            else blk: {
                var storage: [32]zio.net.HostName.LookupResult = undefined;
                const count = try b.resolve(target, &storage);
                var last_error: ?anyerror = null;
                var incompatible = false;
                for (storage[0..count]) |entry| {
                    const address = switch (entry) {
                        .address => |address| address,
                        .canonical_name => continue,
                    };
                    try checkDeadline(budget);
                    const connected = b.connect(address, budget) catch |err| {
                        if (err == error.Canceled or err == error.Timeout or err == error.TcpUserTimeoutFailed) return err;
                        if (err == error.AddressFamilyMismatch) {
                            incompatible = true;
                            continue;
                        }
                        last_error = err;
                        continue;
                    };
                    break :blk connected;
                }
                try checkDeadline(budget);
                return last_error orelse (if (incompatible) error.AddressFamilyMismatch else error.UnknownHostName);
            };
            errdefer raw.close();
            try checkDeadline(budget);
            return raw;
        }
    };
    return zio.withTimeout(deadline, Attempt.run, .{ backend, endpoint, deadline });
}

test "TCP endpoints recognize IP literals without resolving hostnames" {
    const ipv4 = numericAddress(.{ .host = "127.0.0.1", .port = 19091 }).?;
    try std.testing.expectEqual(zio.net.IpAddress.Family.ipv4, ipv4.getFamily());
    try std.testing.expectEqual(@as(u16, 19091), ipv4.getPort());
    const ipv6 = numericAddress(.{ .host = "::1", .port = 443 }).?;
    try std.testing.expectEqual(zio.net.IpAddress.Family.ipv6, ipv6.getFamily());
    try std.testing.expectEqual(@as(u16, 443), ipv6.getPort());
    try std.testing.expect(numericAddress(.{ .host = "example.test", .port = 80 }) == null);
}

const Probe = struct {
    mode: enum { fallback, resolving, connecting, late_connection, no_addresses, refused, incompatible_last, incompatible_all, configuration_failed } = .fallback,
    resolve_calls: usize = 0,
    connect_calls: usize = 0,
    closes: usize = 0,
    deadlines: [2]zio.Timeout = undefined,
    entered: zio.ResetEvent = .init,
};

const TestBackend = struct {
    probe: *Probe,
    pub const RawConnection = struct {
        probe: *Probe,
        fn close(self: @This()) void {
            self.probe.closes += 1;
        }
    };

    fn resolve(self: TestBackend, endpoint: TcpEndpoint, storage: []zio.net.HostName.LookupResult) !usize {
        self.probe.resolve_calls += 1;
        if (self.probe.mode == .resolving) {
            self.probe.entered.set();
            try zio.sleep(.fromSeconds(30));
        }
        if (self.probe.mode == .no_addresses) return 0;
        storage[0] = .{ .address = try zio.net.IpAddress.parseIp4("127.0.0.1", endpoint.port) };
        storage[1] = storage[0];
        return 2;
    }

    fn connect(self: TestBackend, _: zio.net.IpAddress, deadline: zio.Timeout) !RawConnection {
        self.probe.deadlines[self.probe.connect_calls] = deadline;
        self.probe.connect_calls += 1;
        if (self.probe.mode == .configuration_failed) return error.TcpUserTimeoutFailed;
        if (self.probe.mode == .connecting or self.probe.mode == .late_connection) {
            self.probe.entered.set();
            zio.sleep(.fromSeconds(30)) catch |err| {
                if (self.probe.mode != .late_connection) return err;
                // Model a backend handing back an established socket despite
                // cancellation. Ownership must be closed before returning error.
                return .{ .probe = self.probe };
            };
        }
        if (self.probe.mode == .incompatible_all or
            (self.probe.mode == .incompatible_last and self.probe.connect_calls == 2))
            return error.AddressFamilyMismatch;
        if (self.probe.mode == .refused or self.probe.connect_calls == 1) return error.ConnectionRefused;
        return .{ .probe = self.probe };
    }
};

test "TCP dial: one deadline spans fallback attempts and success transfers ownership" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var probe: Probe = .{};
    const deadline = zio.Timeout.fromSeconds(1).toDeadline();
    const raw = try dialWith(TestBackend{ .probe = &probe }, .{ .host = "example.test", .port = 9000 }, deadline);
    try std.testing.expectEqual(@as(usize, 1), probe.resolve_calls);
    try std.testing.expectEqual(@as(usize, 2), probe.connect_calls);
    try std.testing.expectEqual(deadline, probe.deadlines[0]);
    try std.testing.expectEqual(deadline, probe.deadlines[1]);
    try std.testing.expectEqual(@as(usize, 0), probe.closes);
    raw.close();
    try std.testing.expectEqual(@as(usize, 1), probe.closes);
    for ([_]Probe{ .{ .mode = .no_addresses }, .{ .mode = .refused } }) |initial| {
        var failed = initial;
        const expected = if (failed.mode == .no_addresses) error.UnknownHostName else error.ConnectionRefused;
        try std.testing.expectError(expected, dialWith(TestBackend{ .probe = &failed }, .{ .host = "example.test", .port = 9000 }, .none));
    }
}

test "TCP dial: incompatible fallback preserves a compatible connection failure" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    for ([_]Probe{ .{ .mode = .incompatible_last }, .{ .mode = .incompatible_all } }) |initial| {
        var probe = initial;
        const expected = if (probe.mode == .incompatible_all) error.AddressFamilyMismatch else error.ConnectionRefused;
        try std.testing.expectError(expected, dialWith(TestBackend{ .probe = &probe }, .{ .host = "example.test", .port = 9000 }, .none));
        try std.testing.expectEqual(@as(usize, 2), probe.connect_calls);
    }
}

test "TCP dial: mandatory socket configuration failure stops DNS fallback" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var probe: Probe = .{ .mode = .configuration_failed };
    try std.testing.expectError(error.TcpUserTimeoutFailed, dialWith(TestBackend{ .probe = &probe }, .{ .host = "example.test", .port = 9000 }, .none));
    try std.testing.expectEqual(@as(usize, 1), probe.resolve_calls);
    try std.testing.expectEqual(@as(usize, 1), probe.connect_calls);
}

test "TCP dial: expired, DNS/attempt timeout and late success cannot leak or retry" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var expired: Probe = .{};
    try std.testing.expectError(error.Timeout, dialWith(TestBackend{ .probe = &expired }, .{ .host = "example.test", .port = 9000 }, .fromMilliseconds(0)));
    try std.testing.expectEqual(@as(usize, 0), expired.resolve_calls);
    for ([_]Probe{ .{ .mode = .resolving }, .{ .mode = .connecting }, .{ .mode = .late_connection } }) |initial| {
        var probe = initial;
        try std.testing.expectError(error.Timeout, dialWith(TestBackend{ .probe = &probe }, .{ .host = "example.test", .port = 9000 }, .fromMilliseconds(10)));
        try std.testing.expectEqual(@as(usize, 1), probe.resolve_calls);
        try std.testing.expectEqual(@as(usize, if (probe.mode == .resolving) 0 else 1), probe.connect_calls);
        try std.testing.expectEqual(@as(usize, if (probe.mode == .late_connection) 1 else 0), probe.closes);
    }
}

test "TCP dial: user cancellation of resolution/attempt propagates without retry" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    const F = struct {
        fn run(probe: *Probe) !void {
            try std.testing.expectError(error.Canceled, dialWith(TestBackend{ .probe = probe }, .{ .host = "example.test", .port = 9000 }, .fromSeconds(5)));
        }
    };
    for ([_]Probe{ .{ .mode = .resolving }, .{ .mode = .connecting } }) |initial| {
        var probe = initial;
        var task = try zio.spawn(F.run, .{&probe});
        defer task.cancel();
        try probe.entered.wait();
        task.cancel();
        try task.join();
        try std.testing.expectEqual(@as(usize, if (probe.mode == .resolving) 0 else 1), probe.connect_calls);
        try std.testing.expectEqual(@as(usize, 0), probe.closes);
    }
}

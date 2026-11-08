const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");
const core = talon.core;

const Counts = struct {
    allocated: std.atomic.Value(usize) = .init(0),
    freed: std.atomic.Value(usize) = .init(0),
    closed: std.atomic.Value(usize) = .init(0),
    flushed: std.atomic.Value(usize) = .init(0),
    remote_calls: std.atomic.Value(usize) = .init(0),
};

const Raw = struct {
    inner: core.MemoryConnection,
    counts: *Counts,
    pub const Reader = core.MemoryConnection.Reader;
    pub const Writer = core.MemoryConnection.Writer;
    pub fn reader(self: Raw, buf: []u8) Reader {
        return self.inner.reader(buf);
    }
    pub fn writer(self: Raw, buf: []u8) Writer {
        return self.inner.writer(buf);
    }
    pub fn close(self: Raw) void {
        _ = self.counts.closed.fetchAdd(1, .monotonic);
        self.inner.close();
    }
    pub fn shutdown(self: Raw) void {
        self.inner.shutdown();
    }
    pub fn remoteInfo(self: Raw) core.RemoteInfo {
        _ = self.counts.remote_calls.fetchAdd(1, .monotonic);
        return .memory;
    }
};

const Listener = struct {
    inner: *core.MemoryListener,
    counts: *Counts,
    accept_calls: usize = 0,
    close_calls: usize = 0,
    pub const RawConnection = Raw;
    pub fn accept(self: *Listener) !Raw {
        self.accept_calls += 1;
        return .{ .inner = try self.inner.accept(), .counts = self.counts };
    }
    pub fn close(self: *Listener) void {
        self.close_calls += 1;
        self.inner.close();
    }
};

fn Pinned(comptime R: type) type {
    return struct {
        pub const pinned_transport = true;
        state: *State,
        const State = struct {
            raw: R,
            gpa: std.mem.Allocator,
            reader: R.Reader,
            writer: R.Writer,
            clear: std.Io.Writer,
            read_buf: [32 * 1024]u8 = undefined,
            wire_buf: [64]u8 = undefined,
            clear_buf: [256]u8 = undefined,
            deadline: zio.Timeout,
        };
        pub fn ioReader(self: @This()) *std.Io.Reader {
            return &self.state.reader.interface;
        }
        pub fn ioWriter(self: @This()) *std.Io.Writer {
            return &self.state.clear;
        }
        // The clear writer's own flush is a no-op. Connection must dispatch
        // to the transport flush to move both buffered layers to the peer.
        pub fn flush(self: @This()) std.Io.Writer.Error!void {
            try self.state.writer.interface.writeAll(self.state.clear.buffered());
            self.state.clear.end = 0;
            try self.state.writer.interface.flush();
            _ = self.state.raw.counts.flushed.fetchAdd(1, .monotonic);
        }
        pub fn setReadTimeout(self: @This(), t: zio.Timeout) void {
            self.state.reader.setTimeout(t);
        }
        pub fn setWriteTimeout(self: @This(), t: zio.Timeout) void {
            self.state.writer.setTimeout(t);
        }
        pub fn lastReadTimedOut(self: @This()) bool {
            return if (self.state.reader.err) |err| err == error.Timeout else false;
        }
        pub fn clearReadError(self: @This()) void {
            self.state.reader.err = null;
        }
        pub fn remoteInfo(self: @This()) core.RemoteInfo {
            return self.state.raw.remoteInfo();
        }
        pub fn shutdown(self: @This()) void {
            self.state.raw.shutdown();
        }
        pub fn close(self: @This()) void {
            self.state.raw.close();
            _ = self.state.raw.counts.freed.fetchAdd(1, .monotonic);
            self.state.gpa.destroy(self.state);
        }
    };
}

const Adapter = struct {
    delay: zio.Duration = .fromNanoseconds(0),
    pub fn Connection(comptime R: type) type {
        return Pinned(R);
    }
    pub fn validateTransport(_: *Adapter, caps: core.TimeoutCapabilities, limits: core.Limits) !void {
        try caps.require(limits.handshake_timeout != .none, limits.handshake_timeout != .none);
    }
    pub fn wrap(self: *Adapter, raw: anytype, gpa: std.mem.Allocator, deadline: zio.Timeout) !Pinned(@TypeOf(raw)) {
        const T = Pinned(@TypeOf(raw));
        const state = try gpa.create(T.State);
        _ = raw.counts.allocated.fetchAdd(1, .monotonic);
        errdefer {
            _ = raw.counts.freed.fetchAdd(1, .monotonic);
            gpa.destroy(state); // raw remains engine-owned on failure
        }
        state.* = .{ .raw = raw, .gpa = gpa, .reader = undefined, .writer = undefined, .clear = undefined, .deadline = deadline };
        state.reader = raw.reader(&state.read_buf);
        state.writer = raw.writer(&state.wire_buf);
        state.clear = .fixed(&state.clear_buf);
        state.reader.setTimeout(deadline);
        state.writer.setTimeout(deadline);
        if (try state.reader.interface.takeByte() != 'A') return error.BadAdapterPreface;
        if (self.delay.toNanoseconds() != 0) try zio.sleep(self.delay);
        return .{ .state = state };
    }
};

const App = struct { served: usize = 0 };

test "configured adapter receives the engine window without legacy wrap" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    const Configured = struct {
        pub const Connection = Adapter.Connection;
        pub fn wrapWithOptions(_: *@This(), raw: anytype, options: core.WrapOptions) !Connection(@TypeOf(raw)) {
            try std.testing.expectEqual(@as(usize, 6144), options.read_buffer_size);
            try std.testing.expect(options.deadline == .deadline);
            var adapter: Adapter = .{};
            return adapter.wrap(raw, options.allocator, options.deadline);
        }
    };
    const P = struct {
        pub fn serve(conn: anytype, app: *App) !void {
            app.served += 1;
            try conn.writer().writeAll("ok");
            try conn.writer().flush();
        }
    };
    const Srv = core.StreamServer(P, App);
    var inner = try core.MemoryListener.init(std.testing.allocator, .{});
    defer inner.deinit();
    var counts: Counts = .{};
    var listener: Listener = .{ .inner = &inner, .counts = &counts };
    var app: App = .{};
    var adapter: Configured = .{};
    var server = try Srv.init(std.testing.allocator, &app, .{ .limits = .{ .read_buffer_size = 6144 } });
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *Listener, a: *Configured) !void {
            try s.serveAdapted(l, a);
        }
        fn client(l: *core.MemoryListener, s: *Srv) !void {
            defer s.shutdown();
            const peer = try l.connect();
            defer peer.close();
            var writer = peer.writer(&.{});
            try writer.interface.writeByte('A');
            var buffer: [8]u8 = undefined;
            var reader = peer.reader(&buffer);
            reader.setTimeout(.fromSeconds(2));
            try std.testing.expectEqualStrings("ok", try reader.interface.take(2));
            try std.testing.expectError(error.EndOfStream, reader.interface.takeByte());
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener, &adapter });
    try group.spawn(F.client, .{ &inner, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(@as(usize, 1), app.served);
    try std.testing.expectEqual(@as(usize, 1), counts.closed.load(.monotonic));
    try std.testing.expectEqual(counts.allocated.load(.monotonic), counts.freed.load(.monotonic));
}

const FinishMode = enum { normal, handler_error, middleware_error, detached, canceled_handler, swallowed_cancel, finish_error, canceled_finish };

fn checkFinish(comptime mode: FinishMode) !void {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    const State = struct {
        started: zio.ResetEvent = .init,
        finishing: zio.ResetEvent = .init,
        blocked: zio.ResetEvent = .init,
        finishes: usize = 0,
    };
    const Finishing = struct {
        state: *State,
        pub const Connection = Adapter.Connection;
        pub fn validateTransport(_: *@This(), caps: core.TimeoutCapabilities, limits: core.Limits) !void {
            try caps.require(limits.handshake_timeout != .none, limits.handshake_timeout != .none);
        }
        pub fn wrap(_: *@This(), raw: anytype, gpa: std.mem.Allocator, deadline: zio.Timeout) !Connection(@TypeOf(raw)) {
            var adapter: Adapter = .{};
            return adapter.wrap(raw, gpa, deadline);
        }
        pub fn finish(self: *@This(), raw: anytype) !void {
            // Finish must precede shutdown/free and remain task-tracked.
            try std.testing.expectEqual(@as(usize, 0), raw.state.raw.counts.closed.load(.monotonic));
            try std.testing.expectEqual(@as(usize, 0), raw.state.raw.counts.freed.load(.monotonic));
            self.state.finishes += 1;
            self.state.finishing.set();
            if (mode == .finish_error) return error.WriteFailed;
            if (mode == .canceled_finish) try self.state.blocked.wait();
            try raw.ioWriter().writeAll("finish");
            try raw.flush();
        }
    };
    const P = struct {
        pub fn serve(conn: anytype, state: *State) !void {
            state.started.set();
            if (mode == .handler_error) return error.ReadFailed;
            if (mode == .canceled_handler) try state.blocked.wait();
            if (mode == .swallowed_cancel) state.blocked.wait() catch {};
            if (mode == .detached) {
                const owned = try conn.detachRaw();
                owned.close();
            }
        }
    };
    const After = struct {
        pub fn run(conn: anytype, next: anytype) !void {
            try next.call(conn);
            if (mode == .middleware_error) return error.ReadFailed;
        }
    };
    const Srv = core.StreamServerWith(P, State, .{After});
    var inner = try core.MemoryListener.init(std.testing.allocator, .{});
    defer inner.deinit();
    var counts: Counts = .{};
    var listener: Listener = .{ .inner = &inner, .counts = &counts };
    var state: State = .{};
    var adapter: Finishing = .{ .state = &state };
    var server = try Srv.init(std.testing.allocator, &state, .{ .limits = .{ .drain_timeout = .fromMilliseconds(10) } });
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *Listener, a: *Finishing) !void {
            try s.serveAdapted(l, a);
        }
        fn client(l: *core.MemoryListener, s: *Srv, app: *State) !void {
            defer s.shutdown();
            const peer = try l.connect();
            defer peer.close();
            var writer = peer.writer(&.{});
            try writer.interface.writeByte('A');
            try app.started.wait();
            if (mode == .canceled_handler or mode == .swallowed_cancel or mode == .canceled_finish) {
                if (mode == .canceled_finish) {
                    try app.finishing.wait();
                    try std.testing.expectEqual(@as(usize, 1), s.stats().active_tasks);
                    try std.testing.expectEqual(@as(u64, 0), s.stats().finished_tasks);
                }
                s.shutdown();
            }
            var buf: [32]u8 = undefined;
            var reader = peer.reader(&buf);
            reader.setTimeout(.fromSeconds(2));
            if (mode == .normal) try std.testing.expectEqualStrings("finish", try reader.interface.take(6));
            try std.testing.expectError(error.EndOfStream, reader.interface.takeByte());
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener, &adapter });
    try group.spawn(F.client, .{ &inner, &server, &state });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    const finishes: usize = if (mode == .normal or mode == .finish_error or mode == .canceled_finish) 1 else 0;
    try std.testing.expectEqual(finishes, state.finishes);
    try std.testing.expectEqual(@as(usize, 1), counts.closed.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), counts.freed.load(.monotonic));
    const stats = server.stats();
    try std.testing.expectEqual(@as(u64, 1), stats.accepted);
    try std.testing.expectEqual(stats.accepted, stats.finished_tasks);
    try std.testing.expectEqual(@as(usize, 0), stats.active_tasks);
    try std.testing.expectEqual(@as(u64, if (mode == .handler_error or mode == .middleware_error or mode == .finish_error) 1 else 0), stats.connection_errors);
    try std.testing.expectEqual(@as(u64, if (mode == .canceled_handler or mode == .swallowed_cancel or mode == .canceled_finish) 1 else 0), stats.canceled_returns);
    try std.testing.expectEqual(@as(u64, if (mode == .detached) 1 else 0), stats.detached_connections);
}

test "adapter: normal finish respects errors, cancellation, detachment and tracked cleanup" {
    inline for (comptime std.meta.tags(FinishMode)) |mode| try checkFinish(mode);
}

const Proto = struct {
    pub fn handshake(conn: anytype, _: *App) !void {
        const deadline = conn.raw.state.deadline;
        try std.testing.expectEqual(deadline, conn.raw.state.reader.timeout);
        try conn.setReadTimeout(.fromSeconds(60));
        try std.testing.expectEqual(deadline, conn.raw.state.reader.timeout);
        try std.testing.expectError(error.EstablishmentInProgress, conn.detachRaw());
        if (try conn.reader().takeByte() != 'H') return error.BadProtocolPreface;
    }
    pub fn serve(conn: anytype, app: *App) !void {
        try std.testing.expect(conn.raw.state.reader.timeout == .none);
        try std.testing.expect(conn.raw.state.writer.timeout == .none);
        try std.testing.expectError(error.BufferedInput, conn.detachRaw());
        try std.testing.expectEqualStrings("S", try conn.reader().take(1));
        try conn.writer().writeAll("reply");
        try conn.writer().flush();
        app.served += 1;
    }
};

test "adapter: pinned I/O preserves pre-read data, shares establishment deadline and flushes the full stack" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var inner = try core.MemoryListener.init(std.testing.allocator, .{});
    defer inner.deinit();
    var counts: Counts = .{};
    var listener: Listener = .{ .inner = &inner, .counts = &counts };
    var adapter: Adapter = .{};
    var app: App = .{};
    const Middleware = struct {
        pub fn run(conn: anytype, next: anytype) !void {
            try std.testing.expectEqual(conn.raw.state.deadline, conn.raw.state.reader.timeout);
            try std.testing.expectEqual(@as(u8, 'M'), try conn.reader().takeByte());
            try next.call(conn);
        }
    };
    const Srv = core.StreamServerWith(Proto, App, .{Middleware});
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *Listener, a: *Adapter) !void {
            try s.serveAdapted(l, a);
        }
        fn client(l: *core.MemoryListener, s: *Srv) !void {
            defer s.shutdown();
            const peer = try l.connect();
            defer peer.close();
            var writer = peer.writer(&.{});
            try writer.interface.writeAll("AMHS");
            var buf: [32]u8 = undefined;
            var reader = peer.reader(&buf);
            reader.setTimeout(.fromSeconds(1));
            try std.testing.expectEqualStrings("reply", try reader.interface.take(5));
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener, &adapter });
    try group.spawn(F.client, .{ &inner, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(@as(usize, 1), app.served);
    try std.testing.expectEqual(@as(usize, 1), counts.closed.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), counts.freed.load(.monotonic));
    try std.testing.expect(counts.flushed.load(.monotonic) >= 1);
    try std.testing.expectEqual(@as(usize, 0), server.read_pool.free.items.len);
    try std.testing.expectEqual(@as(usize, 0), server.write_pool.free.items.len);
}

const Failure = enum { setup_timeout, protocol_timeout, canceled_setup, allocation, bad_preface, write_failure };

fn checkFailure(comptime failure: Failure) !void {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var inner = try core.MemoryListener.init(std.testing.allocator, .{});
    defer inner.deinit();
    var counts: Counts = .{};
    var listener: Listener = .{ .inner = &inner, .counts = &counts };
    var adapter: Adapter = .{ .delay = if (failure == .protocol_timeout) .fromMilliseconds(100) else .fromNanoseconds(0) };
    var app: App = .{};
    var alloc = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const Srv = core.StreamServer(Proto, App);
    var server = try Srv.init(alloc.allocator(), &app, .{ .limits = .{
        .max_connections = 1,
        .handshake_timeout = if (failure == .canceled_setup) .none else .fromMilliseconds(200),
        .drain_timeout = .fromMilliseconds(10),
    } });
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *Listener, a: *Adapter) !void {
            try s.serveAdapted(l, a);
        }
        fn client(l: *core.MemoryListener, s: *Srv, c: *Counts, allocator: *std.testing.FailingAllocator) !void {
            defer s.shutdown();
            if (failure == .allocation) allocator.fail_index = allocator.alloc_index;
            const stalled = try l.connect();
            defer stalled.close();
            if (failure == .bad_preface or failure == .protocol_timeout) {
                var writer = stalled.writer(&.{});
                try writer.interface.writeByte(if (failure == .bad_preface) 'B' else 'A');
            }
            if (failure == .write_failure) {
                var writer = stalled.writer(&.{});
                try writer.interface.writeAll("AHS");
                stalled.close();
            }
            if (failure == .canceled_setup) {
                for (0..1000) |_| {
                    if (c.allocated.load(.monotonic) == 1) break;
                    try zio.sleep(.fromMilliseconds(1));
                }
                try std.testing.expectEqual(@as(usize, 1), c.allocated.load(.monotonic));
                s.shutdown();
            }
            var buf: [32]u8 = undefined;
            var reader = stalled.reader(&buf);
            reader.setTimeout(.fromSeconds(2));
            try std.testing.expectError(error.EndOfStream, reader.interface.takeByte());
            if (failure == .canceled_setup) return;
            allocator.fail_index = std.math.maxInt(usize);
            const healthy = try l.connect();
            defer healthy.close();
            var writer = healthy.writer(&.{});
            try writer.interface.writeAll("AHS");
            var good_buf: [32]u8 = undefined;
            var good = healthy.reader(&good_buf);
            good.setTimeout(.fromSeconds(2));
            try std.testing.expectEqualStrings("reply", try good.interface.take(5));
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener, &adapter });
    try group.spawn(F.client, .{ &inner, &server, &counts, &alloc });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(@as(usize, if (failure == .canceled_setup) 1 else 2), counts.closed.load(.monotonic));
    try std.testing.expectEqual(counts.allocated.load(.monotonic), counts.freed.load(.monotonic));
    try std.testing.expectEqual(@as(usize, if (failure == .canceled_setup) 0 else 1), app.served);
    const snapshot = server.stats();
    try std.testing.expectEqual(@as(u64, if (failure == .canceled_setup) 1 else 2), snapshot.accepted);
    try std.testing.expectEqual(snapshot.accepted, snapshot.finished_tasks);
    try std.testing.expectEqual(@as(usize, 0), snapshot.active_tasks);
    try std.testing.expectEqual(@as(usize, 1), snapshot.peak_active_tasks);
    // Io.Reader wraps the canceled setup read as ReadFailed; it is not a
    // direct Canceled return. Drain counters preserve the shutdown cause.
    try std.testing.expectEqual(@as(u64, 0), snapshot.canceled_returns);
    try std.testing.expectEqual(@as(u64, if (failure == .write_failure or failure == .protocol_timeout) 0 else 1), snapshot.transport_failures);
    try std.testing.expectEqual(@as(u64, if (failure == .write_failure or failure == .protocol_timeout) 1 else 0), snapshot.connection_errors);
    try std.testing.expectEqual(@as(u64, if (failure == .canceled_setup) 1 else 0), snapshot.forced_drains);
    try std.testing.expectEqual(@as(u64, if (failure == .canceled_setup) 1 else 0), snapshot.drain_timeouts);
}

test "adapter: setup/protocol timeouts, rejection and OOM close once and release admission" {
    inline for (.{ Failure.setup_timeout, Failure.protocol_timeout, Failure.bad_preface, Failure.allocation, Failure.write_failure }) |failure|
        try checkFailure(failure);
}

test "adapter: shutdown cancels a stalled setup and frees its pinned state" {
    try checkFailure(.canceled_setup);
}

test "adapter: HTTP uses pinned buffers and complete flush after setup" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var inner = try core.MemoryListener.init(std.testing.allocator, .{});
    defer inner.deinit();
    var counts: Counts = .{};
    var listener: Listener = .{ .inner = &inner, .counts = &counts };
    var adapter: Adapter = .{};
    const Handler = struct {
        pub fn handle(_: *@This(), _: *talon.http.Request, res: *talon.http.Response) !void {
            try res.respond("hello", .{});
        }
    };
    var app: Handler = .{};
    const Srv = talon.http.Server(Handler);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *Listener, a: *Adapter) !void {
            try s.serveAdapted(l, a);
        }
        fn client(l: *core.MemoryListener, s: *Srv) !void {
            defer s.shutdown();
            const peer = try l.connect();
            defer peer.close();
            var writer = peer.writer(&.{});
            try writer.interface.writeAll("AGET / HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n");
            var buf: [512]u8 = undefined;
            var reader = peer.reader(&buf);
            reader.setTimeout(.fromSeconds(2));
            try std.testing.expectEqualStrings("HTTP/1.1 200 OK\r\n", try reader.interface.takeDelimiterInclusive('\n'));
            while (!std.mem.eql(u8, try reader.interface.takeDelimiterInclusive('\n'), "\r\n")) {}
            try std.testing.expectEqualStrings("hello", try reader.interface.take(5));
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener, &adapter });
    try group.spawn(F.client, .{ &inner, &server });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(@as(usize, 1), counts.closed.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), counts.freed.load(.monotonic));
}

test "adapter: pinned idle timeout can recover and detachment transfers every owned layer" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var inner = try core.MemoryListener.init(std.testing.allocator, .{});
    defer inner.deinit();
    var counts: Counts = .{};
    var listener: Listener = .{ .inner = &inner, .counts = &counts };
    var adapter: Adapter = .{};
    const State = struct { waiting: zio.ResetEvent = .init };
    var app: State = .{};
    const P = struct {
        pub fn serve(conn: anytype, state: *State) !void {
            state.waiting.set();
            try std.testing.expectError(error.Timeout, conn.waitReadable(.fromMilliseconds(10)));
            try conn.waitReadable(.fromSeconds(1));
            try std.testing.expectError(error.BufferedInput, conn.detachRaw());
            try std.testing.expectEqual(@as(u8, 'S'), try conn.reader().takeByte());
            try conn.writer().writeAll("reply");
            const owned = try conn.detachRaw(); // includes pinned State, not just the lower pipe
            defer owned.close();
            try std.testing.expectError(error.AlreadyDetached, conn.detachRaw());
        }
    };
    const After = struct {
        pub fn run(conn: anytype, next: anytype) !void {
            try next.call(conn);
            // The protocol has already closed/free'd the detached State.
            try std.testing.expect(conn.remoteInfo() == .memory);
        }
    };
    const Srv = core.StreamServerWith(P, State, .{After});
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();
    const F = struct {
        fn serve(s: *Srv, l: *Listener, a: *Adapter) !void {
            try s.serveAdapted(l, a);
        }
        fn client(l: *core.MemoryListener, s: *Srv, state: *State) !void {
            defer s.shutdown();
            const peer = try l.connect();
            defer peer.close();
            var writer = peer.writer(&.{});
            try writer.interface.writeByte('A');
            try state.waiting.wait();
            try zio.sleep(.fromMilliseconds(30));
            try writer.interface.writeByte('S');
            var buf: [32]u8 = undefined;
            var reader = peer.reader(&buf);
            reader.setTimeout(.fromSeconds(1));
            try std.testing.expectEqualStrings("reply", try reader.interface.take(5));
        }
    };
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(F.serve, .{ &server, &listener, &adapter });
    try group.spawn(F.client, .{ &inner, &server, &app });
    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(@as(usize, 1), counts.closed.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), counts.freed.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), counts.flushed.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), counts.remote_calls.load(.monotonic));
}

test "adapter: setup policy and final transport capabilities are checked before accept" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var inner = try core.MemoryListener.init(std.testing.allocator, .{});
    defer inner.deinit();
    var counts: Counts = .{};
    var listener: Listener = .{ .inner = &inner, .counts = &counts };
    const MissingTimeouts = struct {
        pub fn Connection(comptime R: type) type {
            return struct {
                pub const pinned_transport = true;
                inner: Pinned(R),
                pub fn ioReader(self: @This()) *std.Io.Reader {
                    return self.inner.ioReader();
                }
                pub fn ioWriter(self: @This()) *std.Io.Writer {
                    return self.inner.ioWriter();
                }
                pub fn flush(self: @This()) std.Io.Writer.Error!void {
                    try self.inner.flush();
                }
                pub fn remoteInfo(self: @This()) core.RemoteInfo {
                    return self.inner.remoteInfo();
                }
                pub fn close(self: @This()) void {
                    self.inner.close();
                }
                pub fn shutdown(self: @This()) void {
                    self.inner.shutdown();
                }
            };
        }
        pub fn wrap(_: *@This(), raw: anytype, gpa: std.mem.Allocator, deadline: zio.Timeout) !Connection(@TypeOf(raw)) {
            var adapter: Adapter = .{};
            return .{ .inner = try adapter.wrap(raw, gpa, deadline) };
        }
    };
    const BadConfig = struct {
        pub const Connection = Adapter.Connection;
        pub fn validateTransport(_: *@This(), _: core.TimeoutCapabilities, _: core.Limits) !void {
            return error.InvalidAdapterConfiguration;
        }
        pub fn wrap(_: *@This(), raw: anytype, gpa: std.mem.Allocator, deadline: zio.Timeout) !Connection(@TypeOf(raw)) {
            var adapter: Adapter = .{};
            return adapter.wrap(raw, gpa, deadline);
        }
    };
    // This protocol needs no timeout itself; the finite establishment budget
    // still checks the final type rather than the capable original pipe.
    const P = struct {
        pub fn serve(_: anytype, _: *App) !void {}
    };
    var app: App = .{};
    const Srv = core.StreamServer(P, App);
    var server = try Srv.init(std.testing.allocator, &app, .{});
    defer server.deinit();
    var missing: MissingTimeouts = .{};
    try std.testing.expectError(error.HandshakeTimeoutUnsupported, server.serveAdapted(&listener, &missing));
    var invalid: BadConfig = .{};
    try std.testing.expectError(error.InvalidAdapterConfiguration, server.serveAdapted(&listener, &invalid));
    try std.testing.expectEqual(@as(usize, 0), counts.allocated.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), listener.accept_calls);
    try std.testing.expectEqual(@as(usize, 0), listener.close_calls);
}

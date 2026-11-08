//! StreamServer: protocol-agnostic stream engine.
//!
//! `Proto` is a comptime contract: given a ready connection, it decides how to
//! speak. Implement one `Proto.serve` and inherit the engine's foundation —
//! listener abstraction, connection limits, graceful shutdown, plus
//! connection middleware, buffer pool, and the framing toolbox.
//!
//!   Proto.serve(conn: *Connection(Raw), app: *App) anyerror!void
//!   Proto.handshake(conn, app) !void   // optional; shares accept-time budget
//!   Proto.Limits / engineLimits(limits) !core.Limits // optional policy mapping
//!   Proto.serveWithConfig(conn, app, limits) !void    // optional configured entry
//!   Proto.validateTransport(capabilities, limits) !void // optional pre-accept check
//!
//! Without policy mapping, Options.limits is core.Limits. Capacities and the
//! connection limit are fixed at init; do not mutate options during serve.
//!
//! The engine runs middleware, optional handshake, then serve. Establishment
//! deadlines end before serve; a protocol may cap its first-message timeout
//! using conn.initialReadTimeout when negotiation occurs there instead.
//!
//! `Proto.serve` is a plain coroutine function: the protocol's connection loop
//! is linear code, not an async state machine.

const std = @import("std");
const zio = @import("zio");
const connection = @import("connection.zig");
const limits_mod = @import("limits.zig");
const chain_mod = @import("chain.zig");
const buffer_pool = @import("buffer_pool.zig");

pub const Limits = limits_mod.Limits;

/// Per-connection inputs for adapters that own their protocol reader window.
pub const WrapOptions = struct {
    allocator: std.mem.Allocator,
    deadline: zio.Timeout,
    read_buffer_size: usize,
};
pub const Connection = connection.Connection;
const BufferPool = buffer_pool.BufferPool;

const log = std.log.scoped(.talon);

/// Backoff after `accept()` hits an fd-exhaustion error. A slow-handler burst
/// can momentarily pin every fd; we pause rather than busy-spin retrying
/// `accept()` (which would just keep failing and peg the executor).
const accept_fd_backoff_ms = 10;

/// Coherent connection-task snapshot; not physical socket or HTTP request counts.
pub const ServerStats = struct {
    accepted: u64 = 0,
    active_tasks: usize = 0,
    peak_active_tasks: usize = 0,
    finished_tasks: u64 = 0,
    waiting_for_permit: bool = false,
    permit_wait_ns: u64 = 0,
    transport_failures: u64 = 0,
    buffer_failures: u64 = 0,
    connection_errors: u64 = 0,
    canceled_returns: u64 = 0,
    handshake_timeouts: u64 = 0,
    detached_connections: u64 = 0,
    accept_fd_backoffs: u64 = 0,
    accept_failures: u64 = 0,
    task_spawn_failures: u64 = 0,
    forced_drains: u64 = 0,
    drain_timeouts: u64 = 0,
};

const Metrics = struct {
    mutex: buffer_pool.SpinLock = .{},
    value: ServerStats = .{},

    fn snapshot(self: *Metrics) ServerStats {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.value;
    }
    fn start(self: *Metrics) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.value.accepted += 1;
        self.value.active_tasks += 1;
        self.value.peak_active_tasks = @max(self.value.peak_active_tasks, self.value.active_tasks);
    }
    fn finish(self: *Metrics, detached: bool) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.value.active_tasks != 0);
        self.value.active_tasks -= 1;
        self.value.finished_tasks += 1;
        if (detached) self.value.detached_connections += 1;
    }
    fn beginWait(self: *Metrics) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.value.waiting_for_permit = true;
    }
    fn endWait(self: *Metrics, elapsed: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.value.waiting_for_permit = false;
        self.value.permit_wait_ns +|= elapsed;
    }
    fn increment(self: *Metrics, comptime field: []const u8) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        @field(self.value, field) += 1;
    }
    fn taskError(self: *Metrics, comptime field: []const u8, err: anyerror) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (err == error.Canceled) {
            self.value.canceled_returns += 1;
        } else {
            @field(self.value, field) += 1;
            if (err == error.HandshakeTimeout) self.value.handshake_timeouts += 1;
        }
    }
};

const PlainTransport = struct {
    pub fn Connection(comptime Raw: type) type {
        return Raw;
    }
    pub fn wrap(_: *@This(), raw: anytype, _: std.mem.Allocator, _: zio.Timeout) !@TypeOf(raw) {
        return raw;
    }
};

/// StreamServer without connection middleware.
pub fn StreamServer(comptime Proto: type, comptime App: type) type {
    return StreamServerWith(Proto, App, .{});
}

/// StreamServer with a comptime connection middleware chain. Each
/// middleware is `fn (conn: *Connection, next: anytype) !void` (or a struct
/// type with `run`); it may inspect the connection, publish remote
/// identity, or reject by returning without calling next.
pub fn StreamServerWith(comptime Proto: type, comptime App: type, comptime middlewares: anytype) type {
    const Policy = if (@hasDecl(Proto, "Limits")) Proto.Limits else Limits;
    comptime {
        if (!@hasDecl(Proto, "serve")) {
            @compileError("talon.StreamServer: Proto type '" ++ @typeName(Proto) ++
                "' must declare 'pub fn serve(conn: anytype, app: *App) !void'");
        }
    }

    return struct {
        gpa: std.mem.Allocator,
        app: *App,
        limits: Policy,
        engine_limits: Limits,
        group: zio.Group = .init,
        conn_sem: zio.Semaphore,
        shutting_down: std.atomic.Value(bool) = .init(false),
        shutdown_requested: std.atomic.Value(bool) = .init(false),
        stop_event: zio.ResetEvent = .init,
        accept_error: ?anyerror = null,
        /// Independent pool capacities, chosen by the protocol or caller.
        read_pool: BufferPool,
        write_pool: BufferPool,
        metrics: Metrics = .{},

        const Self = @This();
        pub const Stats = ServerStats;

        /// Thread-safe, coherent lifecycle snapshot. No I/O or allocation.
        /// active_tasks includes raw cleanup; detached raw is externally owned.
        pub fn stats(self: *Self) Stats {
            return self.metrics.snapshot();
        }

        pub const Options = struct {
            limits: Policy = .{},
        };

        fn engineLimits(policy: Policy) !Limits {
            const engine = if (comptime @hasDecl(Proto, "engineLimits")) try Proto.engineLimits(policy) else policy;
            try engine.validate();
            return engine;
        }

        pub fn init(gpa: std.mem.Allocator, app: *App, options: Options) !Self {
            const engine = try engineLimits(options.limits);
            var read_pool = try BufferPool.init(gpa, .{
                .buffer_size = engine.read_buffer_size,
            });
            errdefer read_pool.deinit();
            const write_pool = try BufferPool.init(gpa, .{
                .buffer_size = engine.write_buffer_size,
            });
            return .{
                .gpa = gpa,
                .app = app,
                .limits = options.limits,
                .engine_limits = engine,
                .conn_sem = .{ .permits = engine.max_connections },
                .read_pool = read_pool,
                .write_pool = write_pool,
            };
        }

        /// Call after serve() has returned. In Debug builds this also
        /// reports any buffer rented but never returned.
        pub fn deinit(self: *Self) void {
            self.read_pool.deinit();
            self.write_pool.deinit();
        }

        /// Frees snapshots of idle read/write buffers without touching active
        /// connections. May run during serve(); not concurrently with deinit().
        /// Returns freed payload bytes; the two snapshots are independent.
        pub fn trimIdleBuffers(self: *Self) usize {
            const read_bytes = self.read_pool.trimIdle();
            return read_bytes +| self.write_pool.trimIdle();
        }

        /// Runs the server. Blocks the calling coroutine until shutdown() is
        /// requested (or the listener fails), then drains in-flight
        /// connections and returns. Closes the listener.
        ///
        /// `listener` is a pointer to any type satisfying the listener
        /// contract: accept() / close() / RawConnection.
        ///
        /// The accept loop runs as an internal task so that shutdown can
        /// interrupt a pending accept via zio task cancellation — closing the
        /// listener fd does NOT wake a parked accept on all platforms
        /// (observed on macOS/kqueue).
        pub fn serve(self: *Self, listener: anytype) !void {
            var adapter: PlainTransport = .{};
            return self.serveAdapted(listener, &adapter);
        }

        /// Adapter.Connection(Raw) chooses the final static transport type.
        /// wrapWithOptions(raw, options), or legacy wrap(raw, gpa, deadline),
        /// runs in the tracked task. Options carries the engine reader window.
        /// Success transfers raw ownership; failure leaves raw engine-owned.
        /// Adapter and its configuration must outlive this blocking call.
        pub fn serveAdapted(self: *Self, listener: anytype, adapter: anytype) !void {
            const L = ListenerType(@TypeOf(listener));
            const A = AdapterType(@TypeOf(adapter));
            const Wrapped = A.Connection(L.RawConnection);
            comptime validateListener(L);
            const engine = try engineLimits(self.limits);
            // Allocated pools and admission permits are fixed by init.
            if (engine.read_buffer_size != self.read_pool.buffer_size or
                engine.write_buffer_size != self.write_pool.buffer_size or
                engine.max_connections != self.engine_limits.max_connections)
                return error.ResourceLimitsChanged;
            self.engine_limits = engine;
            if (comptime !Connection(Wrapped).supports_timeouts) {
                if (engine.handshake_timeout != .none) return error.HandshakeTimeoutUnsupported;
            }
            if (comptime @hasDecl(Proto, "validateTransport"))
                try Proto.validateTransport(Connection(Wrapped).timeout_capabilities, self.limits);
            if (comptime @hasDecl(A, "validateTransport"))
                try adapter.validateTransport(Connection(L.RawConnection).timeout_capabilities, engine);

            var accept_task = try zio.spawn(AcceptLoop(L, A).run, .{ self, listener, adapter });

            // Woken by shutdown() or by the accept loop exiting on its own.
            self.stop_event.wait() catch {};
            self.shutting_down.store(true, .release);
            accept_task.cancel();
            _ = accept_task.join();
            listener.close();

            self.drain();
            if (self.accept_error) |err| return err;
        }

        /// Requests graceful shutdown: stop accepting, signal
        /// shutting_down so connections exit at request boundaries. serve()
        /// then waits up to limits.drain_timeout before hard-canceling.
        /// Idempotent; callable from any task or thread.
        pub fn shutdown(self: *Self) void {
            if (self.shutdown_requested.swap(true, .acq_rel)) return;
            self.shutting_down.store(true, .release);
            self.stop_event.set();
        }

        fn AcceptLoop(comptime L: type, comptime A: type) type {
            return struct {
                fn run(server: *Self, listener: *L, adapter: *A) void {
                    defer server.stop_event.set();

                    while (true) {
                        server.metrics.beginWait();
                        var wait_clock = zio.Stopwatch.start();
                        server.conn_sem.wait() catch {
                            server.metrics.endWait(wait_clock.read().toNanoseconds());
                            return;
                        };
                        server.metrics.endWait(wait_clock.read().toNanoseconds());

                        const raw = listener.accept() catch |err| {
                            server.conn_sem.post();
                            // Widen to `anyerror`: a listener's accept error set
                            // may not name the fd-exhaustion errors, but we still
                            // want to match them when it does (e.g. TcpListener).
                            switch (@as(anyerror, err)) {
                                error.Canceled => return,
                                // FD exhaustion is transient: a burst of slow
                                // handlers can momentarily pin every fd. Back
                                // off and keep serving rather than tearing down
                                // the listener — which would also drop every
                                // healthy in-flight connection.
                                error.ProcessFdQuotaExceeded,
                                error.SystemFdQuotaExceeded,
                                => {
                                    server.metrics.increment("accept_fd_backoffs");
                                    log.warn("accept: fd limit reached ({t}); backing off {d}ms", .{ err, accept_fd_backoff_ms });
                                    zio.sleep(zio.Duration.fromMilliseconds(accept_fd_backoff_ms)) catch return;
                                    continue;
                                },
                                // Anything else is a fatal listener failure;
                                // surface it from serve() after the drain.
                                else => {
                                    server.metrics.increment("accept_failures");
                                    server.accept_error = err;
                                    return;
                                },
                            }
                        };

                        server.metrics.start();
                        const deadline = server.engine_limits.handshake_timeout.toDeadline();
                        server.group.spawn(ConnTask(L.RawConnection, A).run, .{ server, raw, deadline, adapter }) catch |err| {
                            raw.close();
                            server.metrics.finish(false);
                            server.conn_sem.post();
                            if (err != error.Canceled) {
                                server.metrics.increment("task_spawn_failures");
                                server.accept_error = err;
                            }
                            return;
                        };
                    }
                }
            };
        }

        fn drain(self: *Self) void {
            var done: zio.ResetEvent = .init;
            var handle = zio.spawn(drainWaiter, .{ &self.group, &done }) catch {
                self.metrics.increment("forced_drains");
                self.group.cancel();
                return;
            };
            defer _ = handle.join();

            done.timedWait(self.engine_limits.drain_timeout) catch |err| {
                self.metrics.increment("forced_drains");
                if (err == error.Timeout) self.metrics.increment("drain_timeouts");
                // Drain timeout: hard-cancel stragglers. In-progress writes
                // are shield-protected inside zio where applicable.
                self.group.cancel();
            };
        }

        fn drainWaiter(group: *zio.Group, done: *zio.ResetEvent) void {
            group.wait() catch {};
            done.set();
        }

        fn ConnTask(comptime Raw: type, comptime A: type) type {
            const Wrapped = A.Connection(Raw);
            const Conn = Connection(Wrapped);
            const ConnChain = chain_mod.chain(Conn, middlewares);

            const ProtoTerminal = struct {
                server: *Self,
                pub fn call(self: @This(), conn: *Conn) anyerror!void {
                    try conn.checkEstablishmentDeadline();
                    if (comptime @hasDecl(Proto, "handshake")) try Proto.handshake(conn, self.server.app);
                    try conn.finishEstablishment();
                    if (comptime @hasDecl(Proto, "serveWithConfig"))
                        return Proto.serveWithConfig(conn, self.server.app, self.server.limits);
                    return Proto.serve(conn, self.server.app);
                }
            };

            return struct {
                fn prepareTransport(adapter: *A, raw: Raw, gpa: std.mem.Allocator, deadline: zio.Timeout, read_buffer_size: usize) !Wrapped {
                    try zio.checkCancel();
                    if (deadline == .deadline and
                        zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline.deadline.toNanoseconds())
                        return error.HandshakeTimeout;
                    if (comptime @hasDecl(A, "wrapWithOptions")) {
                        return adapter.wrapWithOptions(raw, .{ .allocator = gpa, .deadline = deadline, .read_buffer_size = read_buffer_size });
                    } else {
                        return adapter.wrap(raw, gpa, deadline);
                    }
                }

                fn run(server: *Self, accepted: Raw, deadline: zio.Timeout, adapter: *A) void {
                    defer server.conn_sem.post();
                    var detached = false;
                    defer server.metrics.finish(detached);
                    const raw = prepareTransport(adapter, accepted, server.gpa, deadline, server.engine_limits.read_buffer_size) catch |err| {
                        server.metrics.taskError("transport_failures", err);
                        accepted.shutdown();
                        accepted.close();
                        if (err != error.Canceled and err != error.ReadFailed and err != error.WriteFailed and err != error.HandshakeTimeout)
                            log.warn("transport setup error: {t}", .{err});
                        return;
                    };

                    const read_buffer = if (Conn.uses_pooled_buffers) server.read_pool.rent() catch {
                        server.metrics.increment("buffer_failures");
                        raw.close();
                        return;
                    } else &.{};
                    defer if (Conn.uses_pooled_buffers) server.read_pool.give(read_buffer);
                    const write_buffer = if (Conn.uses_pooled_buffers) server.write_pool.rent() catch {
                        server.metrics.increment("buffer_failures");
                        raw.close();
                        return;
                    } else &.{};
                    defer if (Conn.uses_pooled_buffers) server.write_pool.give(write_buffer);

                    var conn = Conn.init(
                        raw,
                        read_buffer,
                        write_buffer,
                        &server.engine_limits,
                        &server.shutting_down,
                        server.gpa,
                    );
                    // Only successful checked detachment transfers raw close;
                    // pooled buffers and permit remain task-owned.
                    defer {
                        detached = conn.raw_detached;
                        if (!detached) {
                            raw.shutdown();
                            raw.close();
                        }
                    }
                    conn.beginEstablishment(deadline) catch |err| {
                        server.metrics.taskError("connection_errors", err);
                        return;
                    };

                    ConnChain.run(&conn, ProtoTerminal{ .server = server }) catch |err| {
                        server.metrics.taskError("connection_errors", err);
                        switch (err) {
                            // Routine connection terminations (cancel during
                            // drain, peer reset/timeout) are not server faults.
                            error.Canceled, error.ReadFailed, error.WriteFailed, error.EndOfStream, error.BodyTooSlow, error.HandshakeTimeout => {},
                            else => log.warn("connection handler error: {t} (peer: {f})", .{ err, conn.remoteInfo() }),
                        }
                        return;
                    };
                    if (comptime @hasDecl(A, "finish")) {
                        // Successful chain completion is the only normal-end
                        // signal. A detached transport may already be freed.
                        if (!conn.raw_detached) {
                            // zio cancellation errors are consumable: a handler
                            // may catch one and still return success during drain.
                            if (server.group.isCanceled()) {
                                server.metrics.taskError("connection_errors", error.Canceled);
                                return;
                            }
                            zio.checkCancel() catch |err| {
                                server.metrics.taskError("connection_errors", err);
                                return;
                            };
                            adapter.finish(raw) catch |err| {
                                server.metrics.taskError("connection_errors", err);
                                if (err != error.Canceled and err != error.ReadFailed and err != error.WriteFailed and err != error.Timeout)
                                    log.warn("transport finish error: {t} (peer: {f})", .{ err, conn.remoteInfo() });
                            };
                        }
                    }
                }
            };
        }
    };
}

fn ListenerType(comptime P: type) type {
    const info = @typeInfo(P);
    if (info != .pointer or info.pointer.size != .one) {
        @compileError("talon.StreamServer.serve: listener must be a pointer to a listener, got " ++
            @typeName(P));
    }
    return info.pointer.child;
}

fn AdapterType(comptime P: type) type {
    const info = @typeInfo(P);
    if (info != .pointer or info.pointer.size != .one)
        @compileError("talon.StreamServer.serveAdapted: adapter must be a pointer to one adapter");
    const A = info.pointer.child;
    if (!@hasDecl(A, "Connection"))
        @compileError("talon.StreamServer.serveAdapted: adapter is missing 'Connection'");
    if (!@hasDecl(A, "wrapWithOptions") and !@hasDecl(A, "wrap"))
        @compileError("talon.StreamServer.serveAdapted: adapter needs 'wrapWithOptions' or 'wrap'");
    return A;
}

fn validateListener(comptime L: type) void {
    inline for (.{ "accept", "close" }) |decl| {
        if (!@hasDecl(L, decl)) {
            @compileError("talon.StreamServer.serve: listener type '" ++ @typeName(L) ++
                "' is missing '" ++ decl ++ "' (listener contract)");
        }
    }
    if (!@hasDecl(L, "RawConnection")) {
        @compileError("talon.StreamServer.serve: listener type '" ++ @typeName(L) ++
            "' must declare its raw connection type as 'pub const RawConnection'");
    }
}

//! StreamServer: protocol-agnostic stream engine (design doc §5).
//!
//! `Proto` is a comptime contract: given a ready connection, it decides how to
//! speak. Implement one `Proto.serve` and inherit the engine's foundation —
//! listener abstraction, connection limits, graceful shutdown, and (from M1)
//! connection middleware, buffer pool, and the framing toolbox.
//!
//!   Proto.serve(conn: *Connection(Raw), app: *App) anyerror!void
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
pub const Connection = connection.Connection;
const BufferPool = buffer_pool.BufferPool;

const log = std.log.scoped(.talon);

// §5.4 size classes (M1: one pool per purpose; M3 generalizes).
const write_buffer_size = 4 * 1024;

/// StreamServer without connection middleware.
pub fn StreamServer(comptime Proto: type, comptime App: type) type {
    return StreamServerWith(Proto, App, .{});
}

/// StreamServer with a comptime connection middleware chain (§5.3). Each
/// middleware is `fn (conn: *Connection, next: anytype) !void` (or a struct
/// type with `run`); it may inspect/wrap the connection, publish remote
/// identity, or reject by returning without calling next.
pub fn StreamServerWith(comptime Proto: type, comptime App: type, comptime middlewares: anytype) type {
    comptime {
        if (!@hasDecl(Proto, "serve")) {
            @compileError("talon.StreamServer: Proto type '" ++ @typeName(Proto) ++
                "' must declare 'pub fn serve(conn: anytype, app: *App) !void'");
        }
    }

    return struct {
        gpa: std.mem.Allocator,
        app: *App,
        limits: Limits,
        group: zio.Group = .init,
        conn_sem: zio.Semaphore,
        shutting_down: std.atomic.Value(bool) = .init(false),
        shutdown_requested: std.atomic.Value(bool) = .init(false),
        stop_event: zio.ResetEvent = .init,
        accept_error: ?anyerror = null,
        /// Read buffers double as the head accumulation window, so they are
        /// sized to limits.max_header_size (16K default — §5.4 read class).
        read_pool: BufferPool,
        write_pool: BufferPool,

        const Self = @This();

        pub const Options = struct {
            limits: Limits = .{},
        };

        pub fn init(gpa: std.mem.Allocator, app: *App, options: Options) !Self {
            var read_pool = try BufferPool.init(gpa, .{
                .buffer_size = @max(options.limits.max_header_size, 1024),
            });
            errdefer read_pool.deinit();
            const write_pool = try BufferPool.init(gpa, .{
                .buffer_size = write_buffer_size,
            });
            return .{
                .gpa = gpa,
                .app = app,
                .limits = options.limits,
                .conn_sem = .{ .permits = options.limits.max_connections },
                .read_pool = read_pool,
                .write_pool = write_pool,
            };
        }

        /// Call after serve() has returned. In Debug builds this also
        /// reports any buffer rented but never returned (§5.4).
        pub fn deinit(self: *Self) void {
            self.read_pool.deinit();
            self.write_pool.deinit();
        }

        /// Runs the server. Blocks the calling coroutine until shutdown() is
        /// requested (or the listener fails), then drains in-flight
        /// connections per §5.8 and returns. Closes the listener.
        ///
        /// `listener` is a pointer to any type satisfying the listener
        /// contract (§5.2): accept() / close() / RawConnection.
        ///
        /// The accept loop runs as an internal task so that shutdown can
        /// interrupt a pending accept via zio task cancellation — closing the
        /// listener fd does NOT wake a parked accept on all platforms
        /// (observed on macOS/kqueue).
        pub fn serve(self: *Self, listener: anytype) !void {
            const L = ListenerType(@TypeOf(listener));
            comptime validateListener(L);

            var accept_task = try zio.spawn(AcceptLoop(L).run, .{ self, listener });

            // Woken by shutdown() or by the accept loop exiting on its own.
            self.stop_event.wait() catch {};
            self.shutting_down.store(true, .release);
            accept_task.cancel();
            _ = accept_task.join();
            listener.close();

            self.drain();
            if (self.accept_error) |err| return err;
        }

        /// Requests graceful shutdown (§5.8): stop accepting, signal
        /// shutting_down so connections exit at request boundaries. serve()
        /// then waits up to limits.drain_timeout before hard-canceling.
        /// Idempotent; callable from any task or thread.
        pub fn shutdown(self: *Self) void {
            if (self.shutdown_requested.swap(true, .acq_rel)) return;
            self.shutting_down.store(true, .release);
            self.stop_event.set();
        }

        fn AcceptLoop(comptime L: type) type {
            return struct {
                fn run(server: *Self, listener: *L) void {
                    defer server.stop_event.set();

                    while (true) {
                        server.conn_sem.wait() catch return;

                        const raw = listener.accept() catch |err| {
                            server.conn_sem.post();
                            switch (err) {
                                error.Canceled => {},
                                // Anything else is a fatal listener failure;
                                // surface it from serve() after the drain.
                                else => server.accept_error = err,
                            }
                            return;
                        };

                        server.group.spawn(ConnTask(L.RawConnection).run, .{ server, raw }) catch |err| {
                            raw.close();
                            server.conn_sem.post();
                            if (err != error.Canceled) server.accept_error = err;
                            return;
                        };
                    }
                }
            };
        }

        fn drain(self: *Self) void {
            var done: zio.ResetEvent = .init;
            var handle = zio.spawn(drainWaiter, .{ &self.group, &done }) catch {
                self.group.cancel();
                return;
            };
            defer _ = handle.join();

            done.timedWait(self.limits.drain_timeout) catch {
                // Drain timeout: hard-cancel stragglers. In-progress writes
                // are shield-protected inside zio where applicable.
                self.group.cancel();
            };
        }

        fn drainWaiter(group: *zio.Group, done: *zio.ResetEvent) void {
            group.wait() catch {};
            done.set();
        }

        fn ConnTask(comptime Raw: type) type {
            const Conn = Connection(Raw);
            const ConnChain = chain_mod.chain(Conn, middlewares);

            const ProtoTerminal = struct {
                app: *App,
                pub fn call(self: @This(), conn: *Conn) anyerror!void {
                    return Proto.serve(conn, self.app);
                }
            };

            return struct {
                fn run(server: *Self, raw: Raw) void {
                    defer server.conn_sem.post();

                    const read_buffer = server.read_pool.rent() catch {
                        raw.close();
                        return;
                    };
                    defer server.read_pool.give(read_buffer);
                    const write_buffer = server.write_pool.rent() catch {
                        raw.close();
                        return;
                    };
                    defer server.write_pool.give(write_buffer);

                    var arena = std.heap.ArenaAllocator.init(server.gpa);
                    defer arena.deinit();

                    var conn = Conn.init(
                        raw,
                        read_buffer,
                        write_buffer,
                        &server.limits,
                        &server.shutting_down,
                        &arena,
                    );
                    // Hijacked connections become the hijacker's to close;
                    // note the pooled buffers still return above, so a
                    // hijacker must re-buffer (M0/M1 hijack contract).
                    defer if (!conn.hijacked) {
                        raw.shutdown();
                        raw.close();
                    };

                    ConnChain.run(&conn, ProtoTerminal{ .app = server.app }) catch |err| switch (err) {
                        // Routine connection terminations (cancel during
                        // drain, peer reset/timeout) are not server faults.
                        error.Canceled, error.ReadFailed, error.WriteFailed, error.EndOfStream => {},
                        else => log.warn("connection handler error: {t} (peer: {f})", .{ err, raw.remoteInfo() }),
                    };
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

fn validateListener(comptime L: type) void {
    inline for (.{ "accept", "close" }) |decl| {
        if (!@hasDecl(L, decl)) {
            @compileError("talon.StreamServer.serve: listener type '" ++ @typeName(L) ++
                "' is missing '" ++ decl ++ "' (listener contract, design doc §5.2)");
        }
    }
    if (!@hasDecl(L, "RawConnection")) {
        @compileError("talon.StreamServer.serve: listener type '" ++ @typeName(L) ++
            "' must declare its raw connection type as 'pub const RawConnection'");
    }
}

//! Protocol-facing connection wrapper (design doc §5.1).
//!
//! `Connection(Raw)` is what a `Proto.serve` implementation receives: buffered
//! `std.Io.Reader`/`std.Io.Writer` access, shutdown signal, deadline control,
//! and the hijack primitive. The concrete type is synthesized at comptime from
//! the listener's raw connection type — static dispatch, no vtables.
//!
//! NOTE: a Connection must not be moved after init (the Io interfaces resolve
//! their parent via @fieldParentPtr). Construct it in place in the connection
//! coroutine and pass pointers.

const std = @import("std");
const zio = @import("zio");
const Limits = @import("limits.zig").Limits;
const RemoteInfo = @import("listener.zig").RemoteInfo;

/// Tick for the interruptible idle wait (see Connection.waitReadable).
const idle_poll_tick: zio.Duration = .fromSeconds(1);

fn budgetExpired(stopwatch: *zio.Stopwatch, budget: zio.Timeout) bool {
    return switch (budget) {
        .none => false,
        .duration => |d| stopwatch.read().toNanoseconds() >= d.toNanoseconds(),
        .deadline => |ts| zio.Timestamp.now(.monotonic).toNanoseconds() >= ts.toNanoseconds(),
    };
}

pub fn Connection(comptime Raw: type) type {
    return struct {
        raw: Raw,
        reader_state: Raw.Reader,
        writer_state: Raw.Writer,
        limits: *const Limits,
        shutting_down: *const std.atomic.Value(bool),
        /// Per-connection arena for request-scoped allocations; protocols
        /// reset it between requests (`reset(.retain_capacity)`) so the
        /// steady-state hot path is malloc-free (§5.4).
        arena: *std.heap.ArenaAllocator,
        /// Middleware-provided remote identity (e.g. PROXY protocol);
        /// overrides the transport's own when set.
        remote_override: ?RemoteInfo = null,
        hijacked: bool = false,

        const Self = @This();

        pub fn init(
            raw: Raw,
            read_buffer: []u8,
            write_buffer: []u8,
            limits: *const Limits,
            shutting_down: *const std.atomic.Value(bool),
            arena: *std.heap.ArenaAllocator,
        ) Self {
            return .{
                .raw = raw,
                .reader_state = raw.reader(read_buffer),
                .writer_state = raw.writer(write_buffer),
                .limits = limits,
                .shutting_down = shutting_down,
                .arena = arena,
            };
        }

        /// Buffered reader over the connection (post-middleware once the
        /// connection middleware chain lands in M1).
        pub fn reader(self: *Self) *std.Io.Reader {
            return &self.reader_state.interface;
        }

        pub fn writer(self: *Self) *std.Io.Writer {
            return &self.writer_state.interface;
        }

        /// True once graceful shutdown started; protocols should finish the
        /// in-flight request and exit their connection loop (§5.8).
        pub fn isShuttingDown(self: *const Self) bool {
            return self.shutting_down.load(.acquire);
        }

        pub fn remoteInfo(self: *const Self) RemoteInfo {
            return self.remote_override orelse self.raw.remoteInfo();
        }

        /// For middleware (e.g. proxy_protocol) to publish the real client
        /// identity discovered inside the stream.
        pub fn setRemoteInfo(self: *Self, info: RemoteInfo) void {
            self.remote_override = info;
        }

        /// Per-read deadline (kernel-level zio Timeout). No-op on transports
        /// without timeout support (memory pipes) — M0 limitation.
        pub fn setReadTimeout(self: *Self, timeout: zio.Timeout) void {
            if (comptime std.meta.hasMethod(Raw.Reader, "setTimeout")) {
                self.reader_state.setTimeout(timeout);
            }
        }

        pub fn setWriteTimeout(self: *Self, timeout: zio.Timeout) void {
            if (comptime std.meta.hasMethod(Raw.Writer, "setTimeout")) {
                self.writer_state.setTimeout(timeout);
            }
        }

        pub const WaitReadableError = error{
            /// Graceful shutdown was requested while idle — the protocol
            /// should exit its connection loop (request-boundary exit, §5.8).
            ShuttingDown,
            /// `budget` elapsed without any data (e.g. keep-alive idle timeout).
            Timeout,
            EndOfStream,
            ReadFailed,
        };

        /// Interruptible idle wait at a request boundary: suspends until at
        /// least one byte is readable (already-buffered bytes count, so
        /// pipelined requests return immediately), shutdown is requested, or
        /// `budget` elapses.
        ///
        /// Implemented as short kernel-timeout read ticks with a shutdown
        /// check in between — the same 1s cadence as the §5.6 heartbeat,
        /// which takes over the wakeup duty in M3. Without this, idle
        /// keep-alive connections only die via the drain-timeout cancel,
        /// making shutdown take the full drain window.
        ///
        /// On exit the read timeout is reset to .none; set your own deadline
        /// before the next read. On transports without read timeouts
        /// (memory pipes) this is a plain blocking wait.
        pub fn waitReadable(self: *Self, budget: zio.Timeout) WaitReadableError!void {
            if (self.isShuttingDown()) return error.ShuttingDown;

            if (comptime !std.meta.hasMethod(Raw.Reader, "setTimeout")) {
                self.reader_state.interface.fill(1) catch |err| switch (err) {
                    error.EndOfStream => return error.EndOfStream,
                    error.ReadFailed => return error.ReadFailed,
                };
                return;
            }

            var stopwatch = zio.Stopwatch.start();
            defer self.setReadTimeout(.none);
            while (true) {
                self.setReadTimeout(.{ .duration = idle_poll_tick });
                self.reader_state.interface.fill(1) catch |err| switch (err) {
                    error.EndOfStream => return error.EndOfStream,
                    error.ReadFailed => {
                        const timed_out = if (self.reader_state.err) |e| e == error.Timeout else false;
                        if (timed_out) {
                            self.reader_state.err = null;
                            if (self.isShuttingDown()) return error.ShuttingDown;
                            if (budgetExpired(&stopwatch, budget)) return error.Timeout;
                            continue;
                        }
                        return error.ReadFailed;
                    },
                };
                return;
            }
        }

        /// Hijack primitive (§5.7): hands the raw connection to the caller and
        /// marks it so the server skips its own shutdown/close when serve
        /// returns. M0 contract: the hijacker owns close(); the Connection's
        /// reader/writer buffers stay valid only within Proto.serve's dynamic
        /// extent — re-buffer if the connection outlives it (full ownership
        /// transfer is M3 scope).
        pub fn hijack(self: *Self) Raw {
            self.hijacked = true;
            return self.raw;
        }
    };
}

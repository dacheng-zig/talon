//! Protocol-facing connection wrapper.
//!
//! `Connection(Raw)` is what a `Proto.serve` implementation receives: buffered
//! `std.Io.Reader`/`std.Io.Writer` access, shutdown signal, deadline control,
//! and checked raw detachment. The concrete type is synthesized at comptime from
//! the listener's raw or adapter output type. Transport selection is static;
//! standard Io interfaces retain their ordinary vtables.
//!
//! NOTE: a Connection must not be moved after init (the Io interfaces resolve
//! their parent via @fieldParentPtr). Construct it in place in the connection
//! coroutine and pass pointers.

const std = @import("std");
const zio = @import("zio");
const Limits = @import("limits.zig").Limits;
const RemoteInfo = @import("listener.zig").RemoteInfo;
const timeout_mod = @import("timeout.zig");

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
    const pinned = @hasDecl(Raw, "pinned_transport") and Raw.pinned_transport;
    return struct {
        raw: Raw,
        reader_state: if (pinned) void else Raw.Reader,
        writer_state: if (pinned) std.Io.Writer else Raw.Writer,
        limits: *const Limits,
        shutting_down: *const std.atomic.Value(bool),
        /// Protocol-owned allocations must end within the protocol's scope.
        gpa: std.mem.Allocator,
        /// Copied while raw is owned; remains valid after a detached pinned
        /// transport has been closed, including middleware unwinding/logging.
        transport_remote: RemoteInfo,
        /// Middleware-provided remote identity (e.g. PROXY protocol);
        /// overrides the transport's own when set.
        remote_override: ?RemoteInfo = null,
        raw_detached: bool = false,
        establishment_deadline: zio.Timeout = .none,
        establishing: bool = false,

        const Self = @This();
        pub const uses_pooled_buffers = !pinned;

        pub const timeout_capabilities: timeout_mod.Capabilities = .{
            .read = if (pinned) std.meta.hasMethod(Raw, "setReadTimeout") else std.meta.hasMethod(Raw.Reader, "setTimeout"),
            .write = if (pinned) std.meta.hasMethod(Raw, "setWriteTimeout") else std.meta.hasMethod(Raw.Writer, "setTimeout"),
        };
        pub const supports_timeouts = timeout_capabilities.read and timeout_capabilities.write;

        pub fn beginEstablishment(self: *Self, deadline: zio.Timeout) timeout_mod.Error!void {
            try timeout_capabilities.require(deadline != .none, deadline != .none);
            self.establishment_deadline = deadline.toDeadline();
            self.establishing = true;
            try self.setReadTimeout(.none);
            try self.setWriteTimeout(.none);
        }

        /// Keeps the accept-time budget for protocols with a first-message
        /// handshake, even after the connection middleware has returned.
        pub fn initialReadTimeout(self: *const Self, timeout: zio.Timeout) zio.Timeout {
            return earlierTimeout(timeout, self.establishment_deadline);
        }

        pub fn checkEstablishmentDeadline(self: *const Self) error{HandshakeTimeout}!void {
            if (self.establishment_deadline == .deadline and
                zio.Timestamp.now(.monotonic).toNanoseconds() >= self.establishment_deadline.deadline.toNanoseconds())
                return error.HandshakeTimeout;
        }

        pub fn finishEstablishment(self: *Self) (timeout_mod.Error || error{HandshakeTimeout})!void {
            try self.checkEstablishmentDeadline();
            self.establishing = false;
            try self.setReadTimeout(.none);
            try self.setWriteTimeout(.none);
        }

        pub fn init(
            raw: Raw,
            read_buffer: []u8,
            write_buffer: []u8,
            limits: *const Limits,
            shutting_down: *const std.atomic.Value(bool),
            gpa: std.mem.Allocator,
        ) Self {
            return .{
                .raw = raw,
                .reader_state = if (pinned) {} else raw.reader(read_buffer),
                .writer_state = if (pinned) .{ .vtable = &.{ .drain = pinnedDrain, .flush = pinnedFlush }, .buffer = &.{} } else raw.writer(write_buffer),
                .limits = limits,
                .shutting_down = shutting_down,
                .gpa = gpa,
                .transport_remote = raw.remoteInfo(),
            };
        }

        /// Buffered reader over the connection. Middleware and protocols share
        /// this reader; middleware may consume a preamble before calling next.
        pub fn reader(self: *Self) *std.Io.Reader {
            std.debug.assert(!self.raw_detached);
            return if (pinned) self.raw.ioReader() else &self.reader_state.interface;
        }

        pub fn writer(self: *Self) *std.Io.Writer {
            std.debug.assert(!self.raw_detached);
            return if (pinned) &self.writer_state else &self.writer_state.interface;
        }

        // Preserve the pinned transport's complete flush stack when protocols
        // call only Writer.flush; copying its Io interface would break pinning.
        fn pinnedDrain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *Self = @fieldParentPtr("writer_state", w);
            return self.raw.ioWriter().writeSplat(data, splat);
        }

        fn pinnedFlush(w: *std.Io.Writer) std.Io.Writer.Error!void {
            const self: *Self = @fieldParentPtr("writer_state", w);
            try self.raw.flush();
        }

        fn readTimedOut(self: *Self) bool {
            if (comptime pinned) return self.raw.lastReadTimedOut();
            return if (self.reader_state.err) |err| err == error.Timeout else false;
        }

        /// True once graceful shutdown started; protocols should finish the
        /// in-flight request and exit their connection loop.
        pub fn isShuttingDown(self: *const Self) bool {
            return self.shutting_down.load(.acquire);
        }

        pub fn remoteInfo(self: *const Self) RemoteInfo {
            return self.remote_override orelse self.transport_remote;
        }

        /// For middleware (e.g. proxy_protocol) to publish the real client
        /// identity discovered inside the stream.
        pub fn setRemoteInfo(self: *Self, info: RemoteInfo) void {
            self.remote_override = info;
        }

        /// Finite deadlines require transport support; .none may clear or
        /// explicitly disable an unsupported deadline.
        pub fn setReadTimeout(self: *Self, timeout: zio.Timeout) timeout_mod.Error!void {
            const effective = if (self.establishing) self.initialReadTimeout(timeout) else timeout;
            try timeout_capabilities.checkRead(effective);
            if (comptime timeout_capabilities.read) {
                if (comptime pinned) self.raw.setReadTimeout(effective) else self.reader_state.setTimeout(effective);
            }
        }

        pub fn setWriteTimeout(self: *Self, timeout: zio.Timeout) timeout_mod.Error!void {
            const effective = if (self.establishing) earlierTimeout(timeout, self.establishment_deadline) else timeout;
            try timeout_capabilities.checkWrite(effective);
            if (comptime timeout_capabilities.write) {
                if (comptime pinned) self.raw.setWriteTimeout(effective) else self.writer_state.setTimeout(effective);
            }
        }

        pub const WaitReadableError = timeout_mod.Error || error{
            /// Graceful shutdown was requested while idle — the protocol
            /// should exit its connection loop (request-boundary exit).
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
        /// check in between at a 1s cadence; no background heartbeat is used.
        /// Without this, idle keep-alive connections only die via drain-timeout cancel,
        /// making shutdown take the full drain window.
        ///
        /// On exit the read timeout is reset to .none; set your own deadline
        /// before the next read. On transports without read timeouts
        /// this is a plain blocking wait.
        pub fn waitReadable(self: *Self, budget: zio.Timeout) WaitReadableError!void {
            if (self.isShuttingDown()) return error.ShuttingDown;

            if (comptime !timeout_capabilities.read) {
                try timeout_capabilities.checkRead(budget);
                self.reader().fill(1) catch |err| switch (err) {
                    error.EndOfStream => return error.EndOfStream,
                    error.ReadFailed => return error.ReadFailed,
                };
                return;
            }

            var stopwatch = zio.Stopwatch.start();
            const deadline = budget.toDeadline();
            defer self.setReadTimeout(.none) catch unreachable;
            while (true) {
                const tick = zio.Timeout{ .duration = idle_poll_tick };
                const next_tick = tick.toDeadline();
                try self.setReadTimeout(if (deadline == .deadline and deadline.deadline.toNanoseconds() < next_tick.deadline.toNanoseconds()) deadline else next_tick);
                self.reader().fill(1) catch |err| switch (err) {
                    error.EndOfStream => return error.EndOfStream,
                    error.ReadFailed => {
                        if (self.readTimedOut()) {
                            if (comptime pinned) self.raw.clearReadError() else self.reader_state.err = null;
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

        pub const DetachError = error{
            AlreadyDetached,
            EstablishmentInProgress,
            BufferedInput,
        } || std.Io.Writer.Error;

        /// Transfers only raw's close responsibility. Refuses unread buffered
        /// input and flushes pending output before changing ownership. On any
        /// error the engine still owns raw. Use the existing reader/writer for
        /// an in-task protocol switch, which needs no detachment.
        ///
        /// After success, do not use this Connection's I/O interfaces. The
        /// caller owns raw.close() and any new buffers/deadlines; engine buffers,
        /// permit and task scope still end when Proto.serve returns. This is
        /// not an independently tracked connection/session ownership handle.
        pub fn detachRaw(self: *Self) DetachError!Raw {
            if (self.raw_detached) return error.AlreadyDetached;
            if (self.establishing) return error.EstablishmentInProgress;
            if (self.reader().bufferedLen() != 0) return error.BufferedInput;
            try self.writer().flush();
            self.raw_detached = true;
            return self.raw;
        }

        /// Deprecated name; migrate to `try conn.detachRaw()`. Now applies
        /// the same checks, rather than silently losing buffered state.
        pub fn hijack(self: *Self) DetachError!Raw {
            return self.detachRaw();
        }
    };
}

fn earlierTimeout(timeout: zio.Timeout, deadline: zio.Timeout) zio.Timeout {
    if (deadline == .none) return timeout;
    const stage = timeout.toDeadline();
    return if (stage == .none or deadline.deadline.toNanoseconds() < stage.deadline.toNanoseconds()) deadline else stage;
}

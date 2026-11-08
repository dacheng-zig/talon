//! ConnectionPool: per-origin reuse of idle outbound HTTP/1.1 connections —
//! the first performance lever of any HTTP client (amortizes the TCP, and
//! later TLS, handshake across requests). The dual of the server's
//! accept-loop: where the server pools idle *buffers*, the client pools idle
//! *connections*.
//!
//! Design (see `docs/design/http-client.md`):
//!   - Keyed by origin = scheme://host:port; each origin owns an idle free-list.
//!   - Two independent eviction knobs (the .NET lesson):
//!       * `connection_lifetime` — max age since the transport was established
//!         (the precise knob for DNS drift / backend rollout), and
//!       * `idle_timeout` — eviction of connections idle too long.
//!     Both are enforced lazily at checkout; an expired entry is closed and the
//!     search continues. `reapExpired` (driven by `Client.reapLoop`) also sweeps
//!     them proactively so idle fds are reclaimed without waiting for a checkout.
//!   - Idle free-lists are capped per-origin and globally so a burst cannot
//!     pin unbounded memory/fds.
//!   - Optional half-close detection (`validate_on_checkout`): a non-blocking
//!     liveness probe evicts a connection the peer closed while idle. Off by
//!     default — the idempotent-retry backstop already recovers from a stale
//!     connection, and the probe adds an event-loop round-trip per reuse.
//!   - Global and optional per-origin active limits share one admission queue.
//!     Pending acquisitions are bounded separately; idle caps bound retention.
//!
//! Concurrency: the same `SpinLock` as the buffer pool protects map/free-list
//! state, including allocation, freeing and container growth. Allocators used
//! under it must not suspend; critical-section cost and contention need
//! measurement. Dialing, liveness probes, connection destruction and admission
//! waits happen outside the lock. Event signaling does not suspend. Pool state
//! and configuration must not be moved/modified during concurrent operations.

const std = @import("std");
const zio = @import("zio");
const connector_mod = @import("connector.zig");
const conn_mod = @import("connection.zig");
const OriginPermit = conn_mod.OriginPermit;

pub const Origin = connector_mod.Origin;
pub const SpinLock = @import("../../core/buffer_pool.zig").SpinLock;

/// Longest origin key we format inline; longer origins simply go unpooled
/// (dialed fresh each time) rather than erroring — hostnames are ≤253 bytes.
const max_origin_key = 512;

pub const Config = struct {
    /// Idle connections retained per origin.
    max_idle_per_origin: usize = 8,
    /// Idle connections retained across all origins.
    max_idle_total: usize = 256,
    /// Decoder objects and windows across active and idle connections. null
    /// disables; zero rejects transparent decoding. Fixed at pool init.
    max_decoder_bytes: ?usize = 64 * 1024 * 1024,
    /// Sub-budget retained by idle connections. Excess decoder storage is
    /// freed on checkin without discarding a healthy transport. Fixed at init.
    max_idle_decoder_bytes: usize = 1024 * 1024,
    /// Response-head arena capacity retained per idle connection. null keeps
    /// high-water capacity; zero frees it all. Applied at checkin, after the
    /// response's borrowed slices have expired. Not an active allocation cap.
    max_retained_response_arena: ?usize = 4 * 1024,
    /// Max age of a connection since it was established; null = unlimited.
    connection_lifetime: ?zio.Duration = .fromMinutes(5),
    /// Max time a connection may sit idle before eviction; null = unlimited.
    idle_timeout: ?zio.Duration = .fromSeconds(90),
    /// Probe an idle connection's liveness before reusing it (non-blocking),
    /// evicting one the peer closed while idle. Off by default: it adds an
    /// event-loop round-trip per reuse, and the idempotent-retry backstop
    /// already recovers from a stale connection. Turn on for peers that
    /// aggressively close idle connections, to avoid the failed first attempt.
    validate_on_checkout: bool = false,
    /// Active acquisitions across all origins, including dial, Response body,
    /// SSE and WebSocket ownership. null disables; zero is invalid.
    max_active: ?usize = 256,
    /// Tasks queued for admission across all origins. 0 rejects immediately;
    /// null disables the queue bound. Does not bound application task creation.
    max_waiters: ?usize = 256,
    /// Active acquisitions per origin. null disables; zero is invalid.
    /// Idle connections remain bounded separately.
    max_per_origin: ?usize = null,
    /// One admission budget, capped by the request's absolute total deadline.
    /// Expiry returns PoolWaitTimeout; a full queue returns PoolQueueFull.
    pool_wait: zio.Timeout = .fromSeconds(10),
};

/// Observability snapshot (also drives reuse assertions in tests).
pub const Stats = struct {
    /// Connections dialed (pool misses + fresh).
    created: u64 = 0,
    /// Checkouts served from the idle free-list.
    reused: u64 = 0,
    /// Idle connections closed by a lifetime/idle eviction at checkout.
    evicted: u64 = 0,
    active: usize = 0,
    waiting: usize = 0,
    /// Reserved decoder allocations, including allocation in progress.
    decoder_bytes: usize = 0,
    idle_decoder_bytes: usize = 0,
    /// Usable response arena capacity of stored idle connections; excludes
    /// allocator/node metadata and active Response arenas.
    idle_response_arena_bytes: usize = 0,
};

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

fn admissionDeadline(wait: zio.Timeout, total: zio.Timeout) zio.Timeout {
    const a = wait.toDeadline();
    const b = total.toDeadline();
    if (a == .none) return b;
    if (b == .none) return a;
    return if (a.deadline.toNanoseconds() < b.deadline.toNanoseconds()) a else b;
}

fn deadlineExpired(deadline: zio.Timeout) bool {
    return deadline == .deadline and nowNs() >= deadline.deadline.toNanoseconds();
}

/// Pure eviction predicate (deterministic; unit-tested with explicit clocks):
/// an entry is expired when it exceeds either knob. Saturating subtraction
/// guards against a non-monotonic clock surprise.
fn entryExpired(config: Config, created_ns: u64, idle_ns: u64, now_ns: u64) bool {
    if (config.connection_lifetime) |life| {
        if (now_ns -| created_ns >= life.toNanoseconds()) return true;
    }
    if (config.idle_timeout) |idle| {
        if (now_ns -| idle_ns >= idle.toNanoseconds()) return true;
    }
    return false;
}

pub fn Pool(comptime Connector: type) type {
    const Raw = Connector.RawConnection;
    const Conn = conn_mod.ClientConnection(Raw);

    return struct {
        gpa: std.mem.Allocator,
        connector: Connector,
        config: Config,
        decoder_budget: conn_mod.DecoderBudget,
        mutex: SpinLock = .{},
        buckets: std.StringHashMap(Bucket),
        /// Referenced origin state, lazily created when max_per_origin is set.
        origins: std.StringHashMap(*OriginPermit),
        active: usize = 0,
        waiting: usize = 0,
        wait_head: ?*Waiter = null,
        wait_tail: ?*Waiter = null,
        idle_total: usize = 0,
        created: std.atomic.Value(u64) = .init(0),
        reused: std.atomic.Value(u64) = .init(0),
        evicted: std.atomic.Value(u64) = .init(0),

        const Self = @This();
        /// The concrete connection type this pool hands out.
        pub const Connection = Conn;
        /// A checked-out connection plus whether it came from the idle pool
        /// (`reused`) or was freshly dialed. The client uses `reused` to decide
        /// retry eligibility: a reused connection the peer closed while idle is
        /// an expected, safely retryable failure; a fresh dial failing is not.
        pub const Checkout = struct { conn: *Conn, reused: bool };

        const Entry = struct { conn: *Conn, idle_ns: u64 };
        const Bucket = std.ArrayList(Entry);
        const Waiter = struct {
            prev: ?*Waiter = null,
            next: ?*Waiter = null,
            origin: ?*OriginPermit,
            ready: zio.ResetEvent = .init,
            granted: bool = false,
        };

        pub fn init(gpa: std.mem.Allocator, connector: Connector, config: Config) Self {
            return .{
                .gpa = gpa,
                .connector = connector,
                .config = config,
                .decoder_budget = .{ .max_bytes = config.max_decoder_bytes, .max_idle_bytes = config.max_idle_decoder_bytes },
                .buckets = std.StringHashMap(Bucket).init(gpa),
                .origins = std.StringHashMap(*OriginPermit).init(gpa),
            };
        }

        /// Closes idle connections and frees pool state. All checkouts,
        /// Responses, WebSockets and concurrent pool operations must have
        /// finished before deinit; they borrow the pool and its origin state.
        pub fn deinit(self: *Self) void {
            std.debug.assert(self.active == 0 and self.waiting == 0);
            var it = self.buckets.iterator();
            while (it.next()) |e| {
                for (e.value_ptr.items) |entry| entry.conn.destroy();
                e.value_ptr.deinit(self.gpa);
                self.gpa.free(e.key_ptr.*);
            }
            self.buckets.deinit();

            var sit = self.origins.iterator();
            while (sit.next()) |e| {
                self.gpa.destroy(e.value_ptr.*);
                self.gpa.free(e.key_ptr.*);
            }
            self.origins.deinit();
            const decoders = self.decoder_budget.snapshot();
            std.debug.assert(decoders.bytes == 0 and decoders.idle_bytes == 0);
        }

        pub fn stats(self: *const Self) Stats {
            // Logical constness: synchronization changes no observable state.
            const mutex: *SpinLock = @constCast(&self.mutex);
            mutex.lock();
            defer mutex.unlock();
            const decoders = @constCast(&self.decoder_budget).snapshot();
            var arena_bytes: usize = 0;
            var buckets = self.buckets.valueIterator();
            while (buckets.next()) |bucket| {
                for (bucket.items) |entry| arena_bytes += entry.conn.arena.queryCapacity();
            }
            return .{
                .created = self.created.load(.monotonic),
                .reused = self.reused.load(.monotonic),
                .evicted = self.evicted.load(.monotonic),
                .active = self.active,
                .waiting = self.waiting,
                .decoder_bytes = decoders.bytes,
                .idle_decoder_bytes = decoders.idle_bytes,
                .idle_response_arena_bytes = arena_bytes,
            };
        }

        /// Returns a connection for `origin`: a live idle one if available,
        /// otherwise a freshly dialed one (bounded by `connect_timeout`). The
        /// caller owns it until `checkin`.
        pub fn checkout(self: *Self, origin: Origin, connect_timeout: zio.Timeout) !Checkout {
            return self.checkoutWithDeadline(origin, connect_timeout, .none);
        }

        /// request_deadline is an absolute total budget; it caps admission as
        /// well as connect in Client. Direct pool users may pass .none.
        pub fn checkoutWithDeadline(self: *Self, origin: Origin, connect_timeout: zio.Timeout, request_deadline: zio.Timeout) !Checkout {
            try zio.checkCancel();
            if (self.config.max_decoder_bytes != self.decoder_budget.max_bytes or
                self.config.max_idle_decoder_bytes != self.decoder_budget.max_idle_bytes)
                return error.ResourceLimitsChanged;
            try Conn.timeout_capabilities.require(self.config.validate_on_checkout, false);
            if (self.config.max_active == 0 or self.config.max_per_origin == 0) return error.InvalidPoolLimit;
            var key_buf: [max_origin_key]u8 = undefined;
            const key: ?[]const u8 = formatKey(&key_buf, origin);
            var permit: ?*OriginPermit = null;
            if (self.config.max_per_origin != null) {
                // Unpoolable long keys still obey admission. This temporary
                // key is copied into referenced origin state, never retained.
                const owned = if (key == null) try std.fmt.allocPrint(self.gpa, "{s}://{s}:{d}", .{
                    @tagName(origin.scheme), origin.host, origin.port,
                }) else null;
                defer if (owned) |k| self.gpa.free(k);
                permit = try self.retainOrigin(key orelse owned.?);
            }
            errdefer if (permit) |state| self.releaseOrigin(state);
            const total_deadline = request_deadline.toDeadline();
            try self.acquire(permit, admissionDeadline(self.config.pool_wait, total_deadline), total_deadline);
            errdefer self.releaseSlot(permit);
            // An expired/stale idle connection can leave an empty warm bucket.
            // Redial failure must reclaim it even when permits are disabled.
            errdefer if (key) |k| self.dropEmptyBucketIfPresent(k);

            if (key) |k| {
                // Pop newest-first (LIFO keeps the warmest connection hot and
                // lets the cold tail age out for eviction). Validate outside
                // the lock; an expired entry is closed and we try the next.
                while (self.popIdle(k)) |entry| {
                    if (entryExpired(self.config, entry.conn.created_ns, entry.idle_ns, nowNs()) or
                        (self.config.validate_on_checkout and !entry.conn.isLikelyLive()))
                    {
                        _ = self.evicted.fetchAdd(1, .monotonic);
                        entry.conn.destroy();
                        continue;
                    }
                    _ = self.reused.fetchAdd(1, .monotonic);
                    entry.conn.activateDecoders();
                    entry.conn.pool_permit = permit;
                    entry.conn.pool_admitted = true;
                    return .{ .conn = entry.conn, .reused = true };
                }
            }
            const conn = try self.dial(origin, key, connect_timeout);
            // A best-effort key allocation may fail even though dialing succeeds.
            // Such a connection cannot later identify its empty warm bucket.
            if (conn.origin_key.len == 0) {
                if (key) |k| self.dropEmptyBucketIfPresent(k);
            }
            conn.pool_permit = permit;
            conn.pool_admitted = true;
            return .{ .conn = conn, .reused = false };
        }

        /// Returns a connection to the pool when `reusable`, else closes it.
        /// Never errors: a connection that cannot be pooled (caps hit, alloc
        /// failure, unpoolable origin) is simply closed.
        pub fn checkin(self: *Self, conn: *Conn, reusable: bool) void {
            // Keep the origin alive until checkin has either stored or destroyed
            // the connection. The permit owns its key independently of conn.
            const permit = conn.pool_permit;
            const admitted = conn.pool_admitted;
            conn.pool_permit = null;
            conn.pool_admitted = false;
            defer {
                if (admitted) self.releaseSlot(permit);
                if (permit) |state| self.releaseOrigin(state);
            }
            if (!reusable or conn.origin_key.len == 0) {
                self.dropEmptyBucketIfPresent(conn.origin_key);
                return conn.destroy();
            }
            // Don't pool a connection already past its lifetime — it would be
            // evicted on the very next checkout anyway.
            if (self.config.connection_lifetime) |life| {
                if (nowNs() -| conn.created_ns >= life.toNanoseconds()) {
                    self.dropEmptyBucketIfPresent(conn.origin_key);
                    return conn.destroy();
                }
            }

            conn.retainIdleDecoders();
            conn.resetResponseArena(self.config.max_retained_response_arena);
            self.mutex.lock();
            if (self.idle_total >= self.config.max_idle_total) {
                self.mutex.unlock();
                self.dropEmptyBucketIfPresent(conn.origin_key);
                return conn.destroy();
            }
            if (self.buckets.getPtr(conn.origin_key)) |bucket| {
                if (bucket.items.len >= self.config.max_idle_per_origin) {
                    self.mutex.unlock();
                    self.dropEmptyBucketIfPresent(conn.origin_key);
                    return conn.destroy();
                }
                bucket.append(self.gpa, .{ .conn = conn, .idle_ns = nowNs() }) catch {
                    self.mutex.unlock();
                    return conn.destroy();
                };
                self.idle_total += 1;
                self.mutex.unlock();
                return;
            }

            // A newly created bucket must own its key independently of every
            // connection's lifetime. Existing buckets take the fast path above:
            // duplicating and immediately freeing a key on every checkin made
            // keep-alive reuse pay an allocator round-trip per response.
            const owned_key = self.gpa.dupe(u8, conn.origin_key) catch {
                self.mutex.unlock();
                return conn.destroy();
            };
            const gop = self.buckets.getOrPut(owned_key) catch {
                self.gpa.free(owned_key);
                self.mutex.unlock();
                return conn.destroy();
            };
            std.debug.assert(!gop.found_existing);
            gop.key_ptr.* = owned_key;
            gop.value_ptr.* = .empty;
            if (gop.value_ptr.items.len >= self.config.max_idle_per_origin) {
                self.mutex.unlock();
                self.dropEmptyBucketIfPresent(conn.origin_key);
                return conn.destroy();
            }
            gop.value_ptr.append(self.gpa, .{ .conn = conn, .idle_ns = nowNs() }) catch {
                self.mutex.unlock();
                self.dropEmptyBucketIfPresent(conn.origin_key);
                return conn.destroy();
            };
            self.idle_total += 1;
            self.mutex.unlock();
        }

        /// Proactively closes idle connections past their lifetime/idle
        /// deadline, so idle fds are
        /// reclaimed without waiting for the next checkout. Returns the count
        /// reaped. Safe to call concurrently with checkout/checkin. Spawn
        /// `Client.reapLoop` into a Group to run this on a cadence.
        pub fn reapExpired(self: *Self) usize {
            const now = nowNs();
            var reaped: usize = 0;
            // Pop one expired entry at a time under the lock, then destroy it
            // outside the lock (destroy may suspend in raw.close; the critical
            // section must stay suspension-free, like the rest of the pool).
            while (self.popExpired(now)) |conn| {
                conn.destroy();
                reaped += 1;
            }
            if (reaped != 0) _ = self.evicted.fetchAdd(reaped, .monotonic);
            return reaped;
        }

        /// Destroys a snapshot of all current idle connections, including their
        /// decoder/arena/transport storage and bucket metadata. Active checkouts
        /// and admission waiters retain ownership. Concurrent checkin may cache
        /// new entries after the snapshot; this call does not chase them.
        /// May suspend in raw.close(); safe with checkout/checkin, not deinit().
        pub fn trimIdleConnections(self: *Self) usize {
            self.mutex.lock();
            var idle = self.buckets;
            self.buckets = std.StringHashMap(Bucket).init(self.gpa);
            const count = self.idle_total;
            self.idle_total = 0;
            var keys = idle.keyIterator();
            while (keys.next()) |key| self.collectOrigin(key.*);
            if (self.origins.count() == 0) self.origins.clearAndFree();
            self.mutex.unlock();

            var it = idle.iterator();
            while (it.next()) |bucket| {
                for (bucket.value_ptr.items) |entry| entry.conn.destroy();
                bucket.value_ptr.deinit(self.gpa);
                self.gpa.free(bucket.key_ptr.*);
            }
            idle.deinit();
            if (count != 0) _ = self.evicted.fetchAdd(count, .monotonic);
            return count;
        }

        // ── internals ───────────────────────────────────────────────────────

        fn popExpired(self: *Self, now_ns: u64) ?*Conn {
            self.mutex.lock();
            defer self.mutex.unlock();
            var it = self.buckets.iterator();
            while (it.next()) |e| {
                for (e.value_ptr.items, 0..) |entry, i| {
                    if (entryExpired(self.config, entry.conn.created_ns, entry.idle_ns, now_ns)) {
                        _ = e.value_ptr.swapRemove(i); // order irrelevant for idle conns
                        self.idle_total -= 1;
                        const conn = entry.conn;
                        // Reclaim the bucket's map slot + owned key once it goes
                        // empty (else the map grows without bound across origins
                        // for a long-lived client). Safe mid-iteration only
                        // because we return immediately, not resuming `it`.
                        if (e.value_ptr.items.len == 0) self.dropBucket(e.key_ptr.*);
                        return conn;
                    }
                }
            }
            return null;
        }

        /// Removes an empty bucket from the map, freeing its owned key and the
        /// list's backing storage. Caller holds the lock; `key` need not be the
        /// map's owned copy — `fetchRemove` returns it for freeing. Bounds map
        /// growth to currently-active origins rather than every origin ever seen.
        fn dropBucket(self: *Self, key: []const u8) void {
            if (self.buckets.fetchRemove(key)) |kv| {
                var bucket = kv.value;
                bucket.deinit(self.gpa);
                self.collectOrigin(kv.key);
                self.gpa.free(kv.key);
            }
        }

        fn dropEmptyBucketIfPresent(self: *Self, key: []const u8) void {
            if (key.len == 0) return;
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.buckets.getPtr(key)) |bucket| {
                if (bucket.items.len == 0) self.dropBucket(key);
            }
        }

        /// Retain before admission, so a returning holder cannot free origin
        /// state that a waiter is using. Release on every checkout/checkin path.
        fn retainOrigin(self: *Self, key: []const u8) !*OriginPermit {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.origins.get(key)) |state| {
                state.users += 1;
                return state;
            }
            const owned = try self.gpa.dupe(u8, key);
            errdefer self.gpa.free(owned);
            const state = try self.gpa.create(OriginPermit);
            errdefer self.gpa.destroy(state);
            state.* = .{ .key = owned };
            try self.origins.put(owned, state);
            return state;
        }

        fn releaseOrigin(self: *Self, state: *OriginPermit) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            std.debug.assert(state.users != 0);
            state.users -= 1;
            self.collectOrigin(state.key);
        }

        fn available(self: *Self, origin: ?*OriginPermit) bool {
            if (self.config.max_active) |max| if (self.active >= max) return false;
            if (origin) |state| if (state.active >= self.config.max_per_origin.?) return false;
            return true;
        }

        fn grant(self: *Self, origin: ?*OriginPermit) void {
            self.active += 1;
            if (origin) |state| state.active += 1;
        }

        fn removeWaiter(self: *Self, waiter: *Waiter) void {
            if (waiter.prev) |p| p.next = waiter.next else self.wait_head = waiter.next;
            if (waiter.next) |n| n.prev = waiter.prev else self.wait_tail = waiter.prev;
            self.waiting -= 1;
        }

        /// Oldest eligible first; a saturated origin does not stall other
        /// origins. Grants both reservations before waking, so no slot hoarding.
        /// Event.set does not suspend; the waiter reacquires mutex before exit.
        fn wakeWaiters(self: *Self) void {
            var next = self.wait_head;
            while (next) |waiter| {
                next = waiter.next;
                if (!self.available(waiter.origin)) continue;
                self.grant(waiter.origin);
                self.removeWaiter(waiter);
                waiter.granted = true;
                waiter.ready.set();
            }
        }

        fn acquire(self: *Self, origin: ?*OriginPermit, deadline: zio.Timeout, total_deadline: zio.Timeout) !void {
            var waiter: Waiter = .{ .origin = origin };
            self.mutex.lock();
            self.wakeWaiters();
            if (deadlineExpired(total_deadline)) {
                self.mutex.unlock();
                return error.PoolWaitTimeout;
            }
            if (self.available(origin)) {
                self.grant(origin);
                self.mutex.unlock();
                return;
            }
            if (deadlineExpired(deadline)) {
                self.mutex.unlock();
                return error.PoolWaitTimeout;
            }
            if (self.config.max_waiters) |max| {
                if (self.waiting >= max) {
                    self.mutex.unlock();
                    return error.PoolQueueFull;
                }
            }
            waiter.prev = self.wait_tail;
            if (self.wait_tail) |tail| tail.next = &waiter else self.wait_head = &waiter;
            self.wait_tail = &waiter;
            self.waiting += 1;
            self.mutex.unlock();

            const wait_error: ?anyerror = if (waitForGrant(&waiter, deadline)) |_| null else |err| err;
            self.mutex.lock();
            // Even a grant racing cancellation/expiry must be returned before
            // the stack waiter disappears. The origin reference remains live.
            if (wait_error == null and !deadlineExpired(deadline)) {
                std.debug.assert(waiter.granted);
                self.mutex.unlock();
                return;
            }
            if (waiter.granted) {
                self.revoke(origin);
            } else self.removeWaiter(&waiter);
            self.wakeWaiters();
            self.mutex.unlock();
            if (wait_error) |err| if (err == error.Canceled) return error.Canceled;
            return error.PoolWaitTimeout;
        }

        fn waitForGrant(waiter: *Waiter, deadline: zio.Timeout) !void {
            try waiter.ready.timedWait(deadline);
            // Event's already-set fast path does not check cancellation.
            try zio.checkCancel();
        }

        fn releaseSlot(self: *Self, origin: ?*OriginPermit) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.revoke(origin);
            self.wakeWaiters();
        }

        fn revoke(self: *Self, origin: ?*OriginPermit) void {
            std.debug.assert(self.active != 0);
            self.active -= 1;
            if (origin) |state| {
                std.debug.assert(state.active != 0);
                state.active -= 1;
            }
        }

        /// Under the pool lock. Idle connections retain warm origin state;
        /// removing their last bucket triggers collection as well.
        fn collectOrigin(self: *Self, key: []const u8) void {
            const state = self.origins.get(key) orelse return;
            if (state.users != 0) return;
            if (self.buckets.get(key)) |bucket| if (bucket.items.len != 0) return;
            _ = self.origins.remove(key);
            self.gpa.free(state.key);
            self.gpa.destroy(state);
        }

        fn popIdle(self: *Self, key: []const u8) ?Entry {
            self.mutex.lock();
            defer self.mutex.unlock();
            const bucket = self.buckets.getPtr(key) orelse return null;
            const entry = bucket.pop() orelse return null;
            self.idle_total -= 1;
            // Keep an empty bucket while its connection is checked out. A
            // closed-loop client commonly has one idle connection per origin;
            // removing the bucket here would force its next checkin to rebuild
            // the map entry and backing array on every request.
            return entry;
        }

        fn dial(self: *Self, origin: Origin, key: ?[]const u8, connect_timeout: zio.Timeout) !*Conn {
            const raw = try self.connector.connect(origin, connect_timeout);
            // `create` takes ownership of `raw` and closes it on every error
            // path, so no `raw.close()` here (that double-closed the fd on a
            // mid-create OOM — see ClientConnection.create).
            const conn = try Conn.create(self.gpa, raw);
            conn.decoder_budget = &self.decoder_budget;
            conn.created_ns = nowNs();
            // An owned key lets this connection re-bucket on checkin. If the
            // origin was unpoolable (too long) or the dupe fails, the key stays
            // empty and the connection is simply never returned to the pool.
            // Permit ownership is stored separately, so even an unpooled
            // connection whose key allocation fails keeps its concurrency slot.
            if (key) |k| conn.origin_key = self.gpa.dupe(u8, k) catch &.{};
            _ = self.created.fetchAdd(1, .monotonic);
            return conn;
        }
    };
}

/// "scheme://host:port" into `buf`; null if it does not fit (origin unpooled).
fn formatKey(buf: []u8, origin: Origin) ?[]const u8 {
    return std.fmt.bufPrint(buf, "{s}://{s}:{d}", .{
        @tagName(origin.scheme), origin.host, origin.port,
    }) catch null;
}

// ── Tests ──────────────────────────────────────────────────────────────────
// Pool checkout/checkin reuse is covered end-to-end against the real server in
// tests/http_client_test.zig. Here we pin the pure eviction logic and key
// formatting deterministically.

test "entryExpired: lifetime knob" {
    const cfg: Config = .{ .connection_lifetime = .fromSeconds(5), .idle_timeout = null };
    const created: u64 = 1_000;
    // 4s after creation: alive. 5s: expired (>=).
    try std.testing.expect(!entryExpired(cfg, created, created, created + 4 * std.time.ns_per_s));
    try std.testing.expect(entryExpired(cfg, created, created, created + 5 * std.time.ns_per_s));
}

test "entryExpired: idle knob is independent of lifetime" {
    const cfg: Config = .{ .connection_lifetime = .fromMinutes(10), .idle_timeout = .fromSeconds(2) };
    const created: u64 = 0;
    const idle_at: u64 = 100 * std.time.ns_per_s; // long-lived but recently idle-stamped
    try std.testing.expect(!entryExpired(cfg, created, idle_at, idle_at + 1 * std.time.ns_per_s));
    try std.testing.expect(entryExpired(cfg, created, idle_at, idle_at + 2 * std.time.ns_per_s));
}

test "entryExpired: both knobs null never expires" {
    const cfg: Config = .{ .connection_lifetime = null, .idle_timeout = null };
    try std.testing.expect(!entryExpired(cfg, 0, 0, std.math.maxInt(u64)));
}

test "formatKey: scheme/host/port and overflow" {
    var buf: [max_origin_key]u8 = undefined;
    try std.testing.expectEqualStrings("http://example.com:80", formatKey(&buf, .{
        .scheme = .http,
        .host = "example.com",
        .port = 80,
    }).?);
    var tiny: [4]u8 = undefined;
    try std.testing.expect(formatKey(&tiny, .{ .host = "example.com", .port = 80 }) == null);
}

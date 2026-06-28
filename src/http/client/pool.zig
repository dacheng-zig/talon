//! ConnectionPool: per-origin reuse of idle outbound HTTP/1.1 connections —
//! the first performance lever of any HTTP client (amortizes the TCP, and
//! later TLS, handshake across requests). The dual of the server's
//! accept-loop: where the server pools idle *buffers*, the client pools idle
//! *connections*.
//!
//! Design (mirrors `docs/talon-client-architecture.md §6`):
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
//!   - Optional in-flight `max_per_origin` backpressure via a per-origin
//!     `zio.Semaphore` (the undici unbounded-connections footgun fix); the idle
//!     caps bound retained connections independently.
//!
//! Concurrency: the free-list mutations are O(1) with no suspension points, so
//! the same `SpinLock` the buffer pool uses fits here — dialing, the liveness
//! probe, and connection destroy all happen strictly outside the lock. Stat
//! counters are atomic so observability never contends with the critical
//! section. The in-flight semaphore's wait/post likewise suspend outside the
//! lock (only its lazy creation is under it).

const std = @import("std");
const zio = @import("zio");
const connector_mod = @import("connector.zig");
const conn_mod = @import("connection.zig");

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
    /// Max concurrent in-flight requests per origin (the undici unbounded-
    /// connections footgun fix, §6). A checkout waits for a permit; a checkin
    /// releases it. null = unlimited (no backpressure). Idle connections are
    /// bounded separately by `max_idle_per_origin`.
    max_per_origin: ?usize = null,
    /// How long a checkout waits for a per-origin permit before giving up with
    /// `error.PoolWaitTimeout`. Only used when `max_per_origin` is set.
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
};

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
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
        mutex: SpinLock = .{},
        buckets: std.StringHashMap(Bucket),
        /// Per-origin in-flight permits (lazily created when max_per_origin is
        /// set). Heap-allocated so the Semaphore address stays stable.
        sems: std.StringHashMap(*zio.Semaphore),
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

        pub fn init(gpa: std.mem.Allocator, connector: Connector, config: Config) Self {
            return .{
                .gpa = gpa,
                .connector = connector,
                .config = config,
                .buckets = std.StringHashMap(Bucket).init(gpa),
                .sems = std.StringHashMap(*zio.Semaphore).init(gpa),
            };
        }

        /// Closes every idle connection and frees all pool memory. In-flight
        /// connections (checked out, not yet returned) are owned by their
        /// Response and are not touched here.
        pub fn deinit(self: *Self) void {
            var it = self.buckets.iterator();
            while (it.next()) |e| {
                for (e.value_ptr.items) |entry| entry.conn.destroy();
                e.value_ptr.deinit(self.gpa);
                self.gpa.free(e.key_ptr.*);
            }
            self.buckets.deinit();

            var sit = self.sems.iterator();
            while (sit.next()) |e| {
                self.gpa.destroy(e.value_ptr.*);
                self.gpa.free(e.key_ptr.*);
            }
            self.sems.deinit();
        }

        pub fn stats(self: *const Self) Stats {
            return .{
                .created = self.created.load(.monotonic),
                .reused = self.reused.load(.monotonic),
                .evicted = self.evicted.load(.monotonic),
            };
        }

        /// Returns a connection for `origin`: a live idle one if available,
        /// otherwise a freshly dialed one (bounded by `connect_timeout`). The
        /// caller owns it until `checkin`.
        pub fn checkout(self: *Self, origin: Origin, connect_timeout: zio.Timeout) !Checkout {
            var key_buf: [max_origin_key]u8 = undefined;
            const key: ?[]const u8 = formatKey(&key_buf, origin);

            // In-flight backpressure: acquire a per-origin permit before
            // reusing/dialing. Held until the matching checkin releases it.
            if (self.config.max_per_origin) |max| {
                if (key) |k| {
                    const sem = try self.originSem(k, max);
                    sem.timedWait(self.config.pool_wait) catch return error.PoolWaitTimeout;
                }
            }
            // On any failure to produce a connection, release the permit we just
            // took (a returned connection carries it to its checkin instead).
            errdefer if (self.config.max_per_origin != null) {
                if (key) |k| if (self.semFor(k)) |s| s.post();
            };

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
                    return .{ .conn = entry.conn, .reused = true };
                }
            }
            return .{ .conn = try self.dial(origin, key, connect_timeout), .reused = false };
        }

        /// Returns a connection to the pool when `reusable`, else closes it.
        /// Never errors: a connection that cannot be pooled (caps hit, alloc
        /// failure, unpoolable origin) is simply closed.
        pub fn checkin(self: *Self, conn: *Conn, reusable: bool) void {
            // Release the in-flight permit this connection's checkout held. Done
            // first and unconditionally so it is never skipped by an early
            // return below (a leaked permit would wedge the origin).
            if (self.config.max_per_origin != null and conn.origin_key.len != 0) {
                if (self.semFor(conn.origin_key)) |s| s.post();
            }
            if (!reusable or conn.origin_key.len == 0) return conn.destroy();
            // Don't pool a connection already past its lifetime — it would be
            // evicted on the very next checkout anyway.
            if (self.config.connection_lifetime) |life| {
                if (nowNs() -| conn.created_ns >= life.toNanoseconds()) return conn.destroy();
            }

            self.mutex.lock();
            if (self.idle_total >= self.config.max_idle_total) {
                self.mutex.unlock();
                return conn.destroy();
            }
            // The map must own its key independently of any connection's
            // lifetime (a pooled conn may be destroyed while siblings keep the
            // bucket alive). Own the key BEFORE getOrPut: a getOrPut that
            // inserts a slot then fails its follow-up dupe cannot truly roll
            // back — `remove` of an undefined-keyed slot mismatches and leaves a
            // poisoned entry that corrupts deinit. dupe → getOrPut → free the
            // surplus copy on a hit.
            const owned_key = self.gpa.dupe(u8, conn.origin_key) catch {
                self.mutex.unlock();
                return conn.destroy();
            };
            const gop = self.buckets.getOrPut(owned_key) catch {
                self.gpa.free(owned_key);
                self.mutex.unlock();
                return conn.destroy();
            };
            if (gop.found_existing) {
                self.gpa.free(owned_key);
            } else {
                gop.key_ptr.* = owned_key;
                gop.value_ptr.* = .empty;
            }
            if (gop.value_ptr.items.len >= self.config.max_idle_per_origin) {
                self.mutex.unlock();
                return conn.destroy();
            }
            gop.value_ptr.append(self.gpa, .{ .conn = conn, .idle_ns = nowNs() }) catch {
                self.mutex.unlock();
                return conn.destroy();
            };
            self.idle_total += 1;
            self.mutex.unlock();
        }

        /// Proactively closes idle connections past their lifetime/idle
        /// deadline (the dual of the server's heartbeat sweep), so idle fds are
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
                self.gpa.free(kv.key);
            }
        }

        /// Gets or lazily creates the per-origin in-flight semaphore (heap, so
        /// its address is stable across map growth). Allocation happens under
        /// the lock — no suspension point, consistent with the rest of the pool.
        fn originSem(self: *Self, key: []const u8, max: usize) !*zio.Semaphore {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.sems.get(key)) |s| return s;
            // Build the owned key + semaphore before touching the map, so a slot
            // is never left holding an undefined key on OOM (same rollback
            // hazard as checkin). getOrPut here is guaranteed !found_existing
            // (we hold the lock and `get` just missed); if it OOMs, the errdefers
            // free the unused key/sem and no poisoned slot remains.
            const owned = try self.gpa.dupe(u8, key);
            errdefer self.gpa.free(owned);
            const sem = try self.gpa.create(zio.Semaphore);
            errdefer self.gpa.destroy(sem);
            sem.* = .{ .permits = max };
            const gop = try self.sems.getOrPut(owned);
            gop.key_ptr.* = owned;
            gop.value_ptr.* = sem;
            return sem;
        }

        fn semFor(self: *Self, key: []const u8) ?*zio.Semaphore {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.sems.get(key);
        }

        fn popIdle(self: *Self, key: []const u8) ?Entry {
            self.mutex.lock();
            defer self.mutex.unlock();
            const bucket = self.buckets.getPtr(key) orelse return null;
            const entry = bucket.pop() orelse return null;
            self.idle_total -= 1;
            // Drop the bucket once empty so the map tracks active origins, not
            // every origin ever dialed (unbounded growth for a long-lived
            // client). `bucket` is invalidated by the removal; we use neither
            // after.
            if (bucket.items.len == 0) self.dropBucket(key);
            return entry;
        }

        fn dial(self: *Self, origin: Origin, key: ?[]const u8, connect_timeout: zio.Timeout) !*Conn {
            const raw = try self.connector.connect(origin, connect_timeout);
            // `create` takes ownership of `raw` and closes it on every error
            // path, so no `raw.close()` here (that double-closed the fd on a
            // mid-create OOM — see ClientConnection.create).
            const conn = try Conn.create(self.gpa, raw);
            conn.created_ns = nowNs();
            // An owned key lets this connection re-bucket on checkin. If the
            // origin was unpoolable (too long) or the dupe fails, the key stays
            // empty and the connection is simply never returned to the pool.
            if (key) |k| conn.origin_key = self.gpa.dupe(u8, k) catch blk: {
                // Recording the key failed (OOM): this connection can never be
                // pooled. But checkout already took an in-flight permit for this
                // origin, and checkin releases by `origin_key` — which stays
                // empty here, so the release would be skipped and the permit
                // leaked, permanently wedging the origin. Release it now, while
                // `k` is still in hand (the only place that key is available
                // without a stored copy). errdefer in checkout does not fire
                // because dial returns this connection successfully.
                if (self.config.max_per_origin != null) {
                    if (self.semFor(k)) |s| s.post();
                }
                break :blk &.{};
            };
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

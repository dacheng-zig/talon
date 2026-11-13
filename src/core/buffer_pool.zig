//! Buffer pool.
//!
//! One size class per pool instance (the server runs one pool per
//! purpose: independently configured read and write capacities). A future 4K/16K/64K
//! three-tier free-list extends this — the API is already per-size so that
//! lands without churn.
//!
//! Debug borrow tracking (Netty ResourceLeakDetector lesson, lightweight):
//! in Debug builds each rented buffer records its borrow site via
//! @returnAddress; deinit() reports buffers never given back. GPA's leak
//! check only covers malloc'd memory — "rented but never returned" is its
//! blind spot. Release builds compile all of it away.

const std = @import("std");
const builtin = @import("builtin");

const track_borrows = builtin.mode == .debug;

const log = std.log.scoped(.talon);

/// Runtime-agnostic lock shared with the client connection pool. Protected
/// operations include allocation, freeing and container growth; they are not
/// uniformly O(1). Allocators used under this lock must not suspend, and
/// multi-executor contention needs measurement.
pub const SpinLock = struct {
    state: std.atomic.Mutex = .unlocked,

    pub fn lock(self: *SpinLock) void {
        while (!self.state.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn unlock(self: *SpinLock) void {
        self.state.unlock();
    }
};

pub const BufferPool = struct {
    /// Concurrent use requires a thread-safe, non-suspending allocator. Payload
    /// calls run outside mutex; metadata growth still runs under it. Allocator
    /// callbacks must not reenter this pool. Join all calls before deinit.
    gpa: std.mem.Allocator,
    buffer_size: usize,
    /// Idle buffers kept for reuse; excess frees back to gpa.
    max_idle: usize,
    mutex: SpinLock = .{},
    free: std.ArrayList([]u8),
    borrows: if (track_borrows) std.AutoHashMap(usize, usize) else void,

    pub const Options = struct {
        buffer_size: usize,
        max_idle: usize = 256,
    };

    pub fn init(gpa: std.mem.Allocator, options: Options) !BufferPool {
        return .{
            .gpa = gpa,
            .buffer_size = options.buffer_size,
            .max_idle = options.max_idle,
            .free = try std.ArrayList([]u8).initCapacity(gpa, @min(options.max_idle, 64)),
            .borrows = if (track_borrows) std.AutoHashMap(usize, usize).init(gpa) else {},
        };
    }

    /// Frees idle buffers. In Debug builds, reports rented-but-never-returned
    /// buffers with their borrow addresses (resolve with `atos`/`addr2line`).
    pub fn deinit(self: *BufferPool) void {
        if (track_borrows) {
            var it = self.borrows.iterator();
            while (it.next()) |entry| {
                log.err("buffer pool leak: buffer 0x{x} rented at 0x{x} was never returned", .{
                    entry.key_ptr.*, entry.value_ptr.*,
                });
            }
            std.debug.assert(self.borrows.count() == 0);
            self.borrows.deinit();
        }
        for (self.free.items) |buf| self.gpa.free(buf);
        self.free.deinit(self.gpa);
    }

    pub fn rent(self: *BufferPool) ![]u8 {
        const return_addr = @returnAddress();
        self.mutex.lock();
        const cached = self.free.pop();
        self.mutex.unlock();
        const buf = cached orelse try self.gpa.alloc(u8, self.buffer_size);
        errdefer self.gpa.free(buf); // debug tracking allocation may fail
        if (track_borrows) {
            self.mutex.lock();
            defer self.mutex.unlock();
            try self.borrows.put(@intFromPtr(buf.ptr), return_addr);
        }
        return buf;
    }

    /// Releases a snapshot of currently idle buffers and its list storage.
    /// Borrowed buffers remain valid; concurrent give() may create a new cache.
    /// Returns payload bytes freed, excluding metadata and allocator overhead.
    pub fn trimIdle(self: *BufferPool) usize {
        self.mutex.lock();
        var idle = self.free;
        self.free = .empty;
        var tracking: if (track_borrows) ?std.AutoHashMap(usize, usize) else void = if (track_borrows) null else {};
        if (track_borrows and self.borrows.count() == 0) {
            tracking = self.borrows;
            self.borrows = std.AutoHashMap(usize, usize).init(self.gpa);
        }
        self.mutex.unlock();

        if (track_borrows) {
            if (tracking) |*map| map.deinit();
        }
        var bytes: usize = 0;
        for (idle.items) |buf| {
            bytes +|= buf.len;
            self.gpa.free(buf);
        }
        idle.deinit(self.gpa);
        return bytes;
    }

    pub fn give(self: *BufferPool, buf: []u8) void {
        std.debug.assert(buf.len == self.buffer_size);
        self.mutex.lock();
        if (track_borrows) {
            const removed = self.borrows.remove(@intFromPtr(buf.ptr));
            std.debug.assert(removed); // returning a buffer this pool never rented
        }
        if (self.free.items.len >= self.max_idle) {
            self.mutex.unlock();
            self.gpa.free(buf);
            return;
        }
        self.free.append(self.gpa, buf) catch {
            self.mutex.unlock();
            self.gpa.free(buf);
            return;
        };
        self.mutex.unlock();
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

test "BufferPool: trimming idle storage restores shared budget without invalidating borrowed buffers" {
    const Budget = @import("allocation_budget.zig");
    var budget = Budget.init(std.testing.allocator, 4096);
    defer budget.deinit();
    const a = budget.allocator();
    {
        var pool = try BufferPool.init(a, .{ .buffer_size = 512, .max_idle = 2 });
        defer pool.deinit();
        const live = try pool.rent();
        @memset(live, 'x');
        const idle = try pool.rent();
        pool.give(idle);
        const fill = try a.alloc(u8, 4096 - budget.stats().reserved_bytes);
        defer a.free(fill);
        try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 512));
        try std.testing.expectEqual(@as(usize, 512), pool.trimIdle());
        try std.testing.expectEqualStrings(&@as([512]u8, @splat('x')), live);
        const recovered = try a.alloc(u8, 512);
        a.free(recovered);
        pool.give(live);
        try std.testing.expectEqual(@as(usize, 512), pool.trimIdle());
        try std.testing.expectEqual(@as(usize, 0), pool.trimIdle());
        // New rents still work; trimming doesn't disable future retention.
        const next = try pool.rent();
        pool.give(next);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
}

test "BufferPool: rent/give reuses buffers" {
    var pool = try BufferPool.init(std.testing.allocator, .{ .buffer_size = 64 });
    defer pool.deinit();

    const a = try pool.rent();
    try std.testing.expectEqual(64, a.len);
    pool.give(a);

    const b = try pool.rent();
    try std.testing.expectEqual(a.ptr, b.ptr); // reused, not reallocated
    pool.give(b);
}

test "BufferPool: max_idle caps retained buffers" {
    var pool = try BufferPool.init(std.testing.allocator, .{ .buffer_size = 16, .max_idle = 1 });
    defer pool.deinit();

    const a = try pool.rent();
    const b = try pool.rent();
    pool.give(a);
    pool.give(b); // over max_idle: freed, not retained
    try std.testing.expectEqual(1, pool.free.items.len);
}

test "BufferPool: debug borrow tracking flags unreturned buffers" {
    if (!track_borrows) return error.SkipZigTest;

    var pool = try BufferPool.init(std.testing.allocator, .{ .buffer_size = 16 });
    const a = try pool.rent();
    try std.testing.expectEqual(1, pool.borrows.count());
    pool.give(a);
    try std.testing.expectEqual(0, pool.borrows.count());
    pool.deinit();
}

test "BufferPool: allocation failure during borrow tracking does not leak" {
    const F = struct {
        fn run(gpa: std.mem.Allocator) !void {
            var pool = try BufferPool.init(gpa, .{ .buffer_size = 64 });
            defer pool.deinit();
            const buf = try pool.rent();
            pool.give(buf);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.run, .{});
}

// Pause a selected payload or detached metadata call. The parent probes the
// lock first, so the old lock scope fails without deadlock.
const PausedPayloadAllocator = struct {
    const payload_size = 4093;
    const Operation = enum { alloc, free, metadata_free };
    operation: Operation,
    armed: bool = false,
    entered: std.atomic.Value(bool) = .init(false),
    released: std.atomic.Value(bool) = .init(false),

    fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn pause(self: *@This(), operation: Operation, len: usize) void {
        if (!self.armed) return;
        const selected = if (self.operation == .metadata_free)
            operation == .free and len != payload_size
        else
            self.operation == operation and len == payload_size;
        if (!selected) return;
        self.entered.store(true, .release);
        while (!self.released.load(.acquire)) std.atomic.spinLoopHint();
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.pause(.alloc, len);
        return std.testing.allocator.rawAlloc(len, alignment, ret);
    }

    fn resize(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) bool {
        return std.testing.allocator.rawResize(memory, alignment, len, ret);
    }

    fn remap(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) ?[*]u8 {
        return std.testing.allocator.rawRemap(memory, alignment, len, ret);
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.pause(.free, memory.len);
        std.testing.allocator.rawFree(memory, alignment, ret);
    }

    fn check(operation: Operation) !void {
        var child: @This() = .{ .operation = operation };
        var pool = try BufferPool.init(child.allocator(), .{ .buffer_size = payload_size, .max_idle = 1 });
        defer pool.deinit();
        const held = try pool.rent();
        const excess = if (operation == .free) pool.rent() catch |err| {
            pool.give(held);
            return err;
        } else null;
        if (operation != .alloc) pool.give(held);
        child.armed = true;
        const Worker = struct {
            fn run(p: *BufferPool, buf: ?[]u8, failed: *bool, entered: *std.atomic.Value(bool), op: Operation) void {
                if (op == .metadata_free) {
                    if (p.trimIdle() != payload_size) failed.* = true;
                    return;
                }
                const value = buf orelse (p.rent() catch {
                    failed.* = true;
                    entered.store(true, .release);
                    return;
                });
                p.give(value);
            }
        };
        var failed = false;
        const thread = std.Thread.spawn(.{}, Worker.run, .{ &pool, excess, &failed, &child.entered, operation }) catch |err| {
            child.released.store(true, .release);
            if (operation != .metadata_free) pool.give(excess orelse held);
            return err;
        };
        var joined = false;
        // Release and join even if a later cached operation fails.
        defer {
            child.released.store(true, .release);
            if (!joined) thread.join();
        }
        while (!child.entered.load(.acquire)) std.Thread.yield() catch {};
        const unlocked = pool.mutex.state.tryLock();
        if (unlocked) pool.mutex.unlock();
        if (operation == .alloc and unlocked) pool.give(held);
        if (unlocked) {
            const cached = try pool.rent();
            pool.give(cached);
        }
        child.released.store(true, .release);
        // On the baseline, the held buffer still needs returning after the
        // blocked allocator is released; do not try it while the lock is held.
        if (operation == .alloc and !unlocked) pool.give(held);
        thread.join();
        joined = true;
        try std.testing.expect(unlocked);
        try std.testing.expect(!failed);
    }
};

test "BufferPool: paused payload allocation permits cached rent and give" {
    try PausedPayloadAllocator.check(.alloc);
}

test "BufferPool: paused excess payload free permits cached rent and give" {
    try PausedPayloadAllocator.check(.free);
}

test "BufferPool: paused trimmed metadata free permits new rent and give" {
    try PausedPayloadAllocator.check(.metadata_free);
}

test "BufferPool: failed cache growth and cached borrow registration release ownership" {
    var child = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var pool = try BufferPool.init(child.allocator(), .{ .buffer_size = 64, .max_idle = 1 });
    var alive = true;
    defer if (alive) pool.deinit();
    const first = try pool.rent();
    pool.give(first);
    if (track_borrows) {
        // Empty tracking storage can be lost independently of an idle payload.
        // Force registration of a popped payload to fail, not its allocation.
        pool.borrows.clearAndFree();
        child.fail_index = child.alloc_index;
        try std.testing.expectError(error.OutOfMemory, pool.rent());
        try std.testing.expectEqual(@as(usize, 0), pool.free.items.len);
        try std.testing.expectEqual(@as(usize, 0), pool.borrows.count());
        child.fail_index = std.math.maxInt(usize);
    }
    const live = try pool.rent();
    try std.testing.expectEqual(@as(usize, 0), pool.trimIdle());
    child.fail_index = child.alloc_index;
    pool.give(live); // Rebuilding the detached idle list fails; give still owns cleanup.
    try std.testing.expectEqual(@as(usize, 0), pool.free.items.len);
    if (track_borrows) try std.testing.expectEqual(@as(usize, 0), pool.borrows.count());
    child.fail_index = std.math.maxInt(usize);
    const later = try pool.rent();
    pool.give(later);
    pool.deinit();
    alive = false;
    try std.testing.expectEqual(child.allocated_bytes, child.freed_bytes);
}

test "BufferPool: zero idle retention immediately returns the payload budget" {
    const Budget = @import("allocation_budget.zig");
    var budget = Budget.init(std.testing.allocator, 4096);
    defer budget.deinit();
    var pool = try BufferPool.init(budget.allocator(), .{ .buffer_size = 512, .max_idle = 0 });
    defer pool.deinit();
    const buf = try pool.rent();
    pool.give(buf);
    try std.testing.expectEqual(@as(usize, 0), pool.free.items.len);
    try std.testing.expectEqual(@as(usize, 0), pool.trimIdle());
    try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
}

test "BufferPool: concurrent rent give and trim preserve live data and return budget" {
    const Budget = @import("allocation_budget.zig");
    var budget = Budget.init(std.testing.allocator, 64 * 1024);
    defer budget.deinit();
    var pool = try BufferPool.init(budget.allocator(), .{ .buffer_size = 512, .max_idle = 2 });
    defer pool.deinit();
    const Worker = struct {
        fn run(p: *BufferPool, id: u8, failures: *std.atomic.Value(usize)) void {
            for (0..2000) |iteration| {
                const buf = p.rent() catch {
                    _ = failures.fetchAdd(1, .monotonic);
                    continue;
                };
                @memset(buf, id);
                if (iteration % 7 == 0) _ = p.trimIdle();
                for (buf) |byte| {
                    if (byte != id) {
                        _ = failures.fetchAdd(1, .monotonic);
                        break;
                    }
                }
                p.give(buf);
            }
        }
    };
    var failures: std.atomic.Value(usize) = .init(0);
    var threads: [4]std.Thread = undefined;
    var spawned: usize = 0;
    defer for (threads[0..spawned]) |thread| thread.join();
    for (&threads, 0..) |*thread, id| {
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{ &pool, @as(u8, @intCast(id)), &failures });
        spawned += 1;
    }
    for (threads) |thread| thread.join();
    spawned = 0;
    try std.testing.expectEqual(@as(usize, 0), failures.load(.acquire));
    try std.testing.expect(pool.free.items.len <= pool.max_idle);
    _ = pool.trimIdle();
    try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
}

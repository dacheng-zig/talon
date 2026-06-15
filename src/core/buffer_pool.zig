//! Buffer pool (design doc §5.4).
//!
//! M1: one size class per pool instance (the server runs one pool per
//! purpose: read 16K, write 4K). M3 extends to the 4K/16K/64K three-tier
//! free-list — the API is already per-size so that lands without churn.
//!
//! Debug borrow tracking (Netty ResourceLeakDetector lesson, lightweight):
//! in Debug builds each rented buffer records its borrow site via
//! @returnAddress; deinit() reports buffers never given back. GPA's leak
//! check only covers malloc'd memory — "rented but never returned" is its
//! blind spot. Release builds compile all of it away.

const std = @import("std");
const builtin = @import("builtin");

const track_borrows = builtin.mode == .Debug;

const log = std.log.scoped(.talon);

/// The critical sections are O(1) with no suspension points, so a spin lock
/// is cheaper than a coroutine mutex and keeps the pool runtime-agnostic.
const SpinLock = struct {
    state: std.atomic.Mutex = .unlocked,

    fn lock(self: *SpinLock) void {
        while (!self.state.tryLock()) std.atomic.spinLoopHint();
    }

    fn unlock(self: *SpinLock) void {
        self.state.unlock();
    }
};

pub const BufferPool = struct {
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
        defer self.mutex.unlock();
        const buf = self.free.pop() orelse try self.gpa.alloc(u8, self.buffer_size);
        if (track_borrows) {
            try self.borrows.put(@intFromPtr(buf.ptr), return_addr);
        }
        return buf;
    }

    pub fn give(self: *BufferPool, buf: []u8) void {
        std.debug.assert(buf.len == self.buffer_size);
        self.mutex.lock();
        defer self.mutex.unlock();
        if (track_borrows) {
            const removed = self.borrows.remove(@intFromPtr(buf.ptr));
            std.debug.assert(removed); // returning a buffer this pool never rented
        }
        if (self.free.items.len >= self.max_idle) {
            self.gpa.free(buf);
            return;
        }
        self.free.append(self.gpa, buf) catch {
            self.gpa.free(buf);
        };
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

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

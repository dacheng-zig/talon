//! Cumulative requested-byte budget for allocations sharing one allocator.
//! Includes retained pool/arena capacity and in-flight reservations, not RSS.
const std = @import("std");
const AllocationBudget = @This();

child: std.mem.Allocator,
max_bytes: usize,
reserved: std.atomic.Value(usize) = .init(0),
peak: std.atomic.Value(usize) = .init(0),
denied: std.atomic.Value(usize) = .init(0),

pub const Stats = struct {
    reserved_bytes: usize,
    peak_reserved_bytes: usize,
    denied_requests: usize,
};

/// Keep this object at a stable address until all handles and allocations end.
/// Do not copy or mutate it after creating an allocator handle. The child must
/// provide the thread safety/non-suspending behavior required by its consumers.
pub fn init(child: std.mem.Allocator, max_bytes: usize) AllocationBudget {
    return .{ .child = child, .max_bytes = max_bytes };
}

pub fn allocator(self: *AllocationBudget) std.mem.Allocator {
    return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
}

/// Independent atomic observations, not a consistent multi-field snapshot.
pub fn stats(self: *const AllocationBudget) Stats {
    return .{
        .reserved_bytes = self.reserved.load(.acquire),
        .peak_reserved_bytes = self.peak.load(.acquire),
        .denied_requests = self.denied.load(.acquire),
    };
}

/// No resources are owned here; this checks that all charges have been returned.
pub fn deinit(self: *AllocationBudget) void {
    std.debug.assert(self.reserved.load(.acquire) == 0);
    self.* = undefined;
}

fn reserve(self: *AllocationBudget, bytes: usize) bool {
    if (bytes == 0) return true;
    var used = self.reserved.load(.monotonic);
    while (true) {
        if (bytes > self.max_bytes - used) {
            var denied = self.denied.load(.monotonic);
            while (self.denied.cmpxchgWeak(denied, denied +| 1, .monotonic, .monotonic)) |actual| denied = actual;
            return false;
        }
        const next = used + bytes;
        if (self.reserved.cmpxchgWeak(used, next, .acq_rel, .monotonic)) |actual| {
            used = actual;
        } else {
            _ = self.peak.fetchMax(next, .monotonic);
            return true;
        }
    }
}

fn release(self: *AllocationBudget, bytes: usize) void {
    const old = self.reserved.fetchSub(bytes, .acq_rel);
    std.debug.assert(old >= bytes);
}

fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
    const self: *AllocationBudget = @ptrCast(@alignCast(ctx));
    if (!self.reserve(len)) return null;
    return self.child.rawAlloc(len, alignment, ret) orelse {
        self.release(len);
        return null;
    };
}

fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret: usize) bool {
    const self: *AllocationBudget = @ptrCast(@alignCast(ctx));
    const growth = new_len -| memory.len;
    if (!self.reserve(growth)) return false;
    if (!self.child.rawResize(memory, alignment, new_len, ret)) {
        self.release(growth);
        return false;
    }
    self.release(memory.len -| new_len);
    return true;
}

fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret: usize) ?[*]u8 {
    const self: *AllocationBudget = @ptrCast(@alignCast(ctx));
    const growth = new_len -| memory.len;
    if (!self.reserve(growth)) return null;
    const result = self.child.rawRemap(memory, alignment, new_len, ret) orelse {
        self.release(growth);
        return null;
    };
    self.release(memory.len -| new_len);
    return result;
}

fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
    const self: *AllocationBudget = @ptrCast(@alignCast(ctx));
    self.child.rawFree(memory, alignment, ret);
    self.release(memory.len);
}

test "AllocationBudget: zero, exact capacity, alignment and release restore capacity" {
    var budget = init(std.testing.allocator, 64);
    defer budget.deinit();
    const a = budget.allocator();
    const block = try a.alignedAlloc(u8, .fromByteUnits(64), 64);
    try std.testing.expectEqual(@as(usize, 0), @intFromPtr(block.ptr) % 64);
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 1));
    try std.testing.expectEqual(@as(usize, 64), budget.stats().reserved_bytes);
    try std.testing.expectEqual(@as(usize, 1), budget.stats().denied_requests);
    a.free(block);
    const later = try a.alloc(u8, 32);
    a.free(later);
    try std.testing.expectEqual(@as(usize, 64), budget.stats().peak_reserved_bytes);
    try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
    var zero = init(std.testing.allocator, 0);
    defer zero.deinit();
    const empty = try zero.allocator().alloc(u8, 0);
    zero.allocator().free(empty);
    try std.testing.expectError(error.OutOfMemory, zero.allocator().alloc(u8, 1));
    try std.testing.expectEqual(@as(usize, 0), zero.stats().peak_reserved_bytes);
}

test "AllocationBudget: child allocation failures release reservations without budget denial" {
    var child = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var budget = init(child.allocator(), 16);
    defer budget.deinit();
    try std.testing.expectError(error.OutOfMemory, budget.allocator().alloc(u8, 16));
    try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
    try std.testing.expectEqual(@as(usize, 16), budget.stats().peak_reserved_bytes);
    try std.testing.expectEqual(@as(usize, 0), budget.stats().denied_requests);
    child.fail_index = std.math.maxInt(usize);
    const block = try budget.allocator().alloc(u8, 16);
    budget.allocator().free(block);
}

test "AllocationBudget: resize/remap growth rejection and shrink keep exact requested-byte charges" {
    var backing: [256]u8 = undefined;
    var child = std.heap.FixedBufferAllocator.init(&backing);
    var budget = init(child.allocator(), 64);
    defer budget.deinit();
    const a = budget.allocator();
    var block = try a.alloc(u8, 16);
    @memset(block, 'x');
    try std.testing.expect(a.resize(block, 32));
    block = block.ptr[0..32];
    try std.testing.expectEqual(@as(usize, 32), budget.stats().reserved_bytes);
    try std.testing.expect(!a.resize(block, 65));
    try std.testing.expectEqual(null, a.remap(block, 65));
    try std.testing.expectEqual(@as(usize, 32), budget.stats().reserved_bytes);
    block = a.remap(block, 64).?;
    try std.testing.expectEqualStrings(&@as([16]u8, @splat('x')), block[0..16]);
    try std.testing.expectEqual(@as(usize, 64), budget.stats().reserved_bytes);
    try std.testing.expect(a.resize(block, 8));
    block = block.ptr[0..8];
    block = a.remap(block, 4).?;
    try std.testing.expectEqual(@as(usize, 4), budget.stats().reserved_bytes);
    a.free(block);
    try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
    try std.testing.expectEqual(@as(usize, 2), budget.stats().denied_requests);
}

test "AllocationBudget: child resize/remap failures roll back only growth reservations" {
    var child = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    var budget = init(child.allocator(), 64);
    defer budget.deinit();
    const a = budget.allocator();
    const block = try a.alloc(u8, 16);
    defer a.free(block);
    try std.testing.expect(!a.resize(block, 32));
    try std.testing.expectEqual(null, a.remap(block, 32));
    try std.testing.expect(!a.resize(block, 8));
    try std.testing.expectEqual(null, a.remap(block, 8));
    try std.testing.expectEqual(@as(usize, 16), budget.stats().reserved_bytes);
    try std.testing.expectEqual(@as(usize, 0), budget.stats().denied_requests);
}

test "AllocationBudget: realloc fallback must cover old and new allocations until free" {
    var child = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    var budget = init(child.allocator(), 64);
    defer budget.deinit();
    const a = budget.allocator();
    var block = try a.alloc(u8, 40);
    @memset(block, 'x');
    try std.testing.expectError(error.OutOfMemory, a.realloc(block, 48));
    try std.testing.expectEqual(@as(usize, 40), budget.stats().reserved_bytes);
    try std.testing.expectEqualStrings(&@as([40]u8, @splat('x')), block);
    block = try a.realloc(block, 24);
    try std.testing.expectEqual(@as(usize, 24), budget.stats().reserved_bytes);
    try std.testing.expectEqual(@as(usize, 64), budget.stats().peak_reserved_bytes);
    try std.testing.expectEqualStrings(&@as([24]u8, @splat('x')), block);
    a.free(block);
}

test "AllocationBudget: reservation arithmetic and denial counters do not overflow" {
    var budget = init(std.testing.allocator, std.math.maxInt(usize));
    defer budget.deinit();
    try std.testing.expect(budget.reserve(std.math.maxInt(usize)));
    budget.denied.store(std.math.maxInt(usize), .monotonic);
    try std.testing.expect(!budget.reserve(1));
    try std.testing.expectEqual(std.math.maxInt(usize), budget.stats().reserved_bytes);
    try std.testing.expectEqual(std.math.maxInt(usize), budget.stats().denied_requests);
    budget.release(std.math.maxInt(usize));
}

test "AllocationBudget: concurrent alloc/free contention cannot exceed the shared cap" {
    var budget = init(std.testing.allocator, 64);
    defer budget.deinit();
    const F = struct {
        fn run(b: *AllocationBudget, failures: *std.atomic.Value(usize)) void {
            const a = b.allocator();
            for (0..2000) |_| {
                const block = a.alloc(u8, 32) catch continue;
                if (b.stats().reserved_bytes > 64) _ = failures.fetchAdd(1, .monotonic);
                @memset(block, 'x');
                a.free(block);
            }
        }
    };
    var failures: std.atomic.Value(usize) = .init(0);
    var threads: [4]std.Thread = undefined;
    var spawned: usize = 0;
    defer for (threads[0..spawned]) |thread| thread.join();
    for (&threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, F.run, .{ &budget, &failures });
        spawned += 1;
    }
    // Join before observing final accounting; the deferred join only protects
    // partially-created thread sets if spawn fails.
    for (threads) |thread| thread.join();
    spawned = 0;
    try std.testing.expectEqual(@as(usize, 0), failures.load(.acquire));
    try std.testing.expect(budget.stats().peak_reserved_bytes <= 64);
    try std.testing.expectEqual(@as(usize, 0), budget.stats().reserved_bytes);
}

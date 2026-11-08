//! Bounded fixed-timeline arrivals. HTTP validation and pool policy stay with
//! the caller; this module owns generator capacity, timing and outcome counts.
const std = @import("std");
const zio = @import("zio");

pub const Options = struct {
    rate: usize,
    limit: usize,
};

const Outcome = enum { pending, success, queue_full, pool_timeout, generator_dropped, failed };
const Record = struct {
    due_ns: u64 = 0,
    dispatch_lag_ns: u64 = 0,
    start_lag_ns: u64 = 0,
    service_ns: u64 = 0,
    scheduled_ns: u64 = 0,
    outcome: Outcome = .pending,
    failure: ?anyerror = null,
};

const Latency = enum {
    dispatch_lag,
    task_start_lag,
    success_scheduled,
    success_service,
    rejected_scheduled,

    fn sample(self: Latency, record: Record) ?u64 {
        return switch (self) {
            .dispatch_lag => record.dispatch_lag_ns,
            .task_start_lag => if (record.outcome != .generator_dropped) record.start_lag_ns else null,
            .success_scheduled => if (record.outcome == .success) record.scheduled_ns else null,
            .success_service => if (record.outcome == .success) record.service_ns else null,
            .rejected_scheduled => if (record.outcome == .queue_full or record.outcome == .pool_timeout) record.scheduled_ns else null,
        };
    }
};

pub const Summary = struct {
    offered: usize = 0,
    success: usize = 0,
    queue_full: usize = 0,
    pool_timeout: usize = 0,
    generator_dropped: usize = 0,
    failed: usize = 0,
    first_error: ?anyerror = null,

    fn add(self: *Summary, record: Record) !void {
        if (record.outcome == .pending) return error.IncompleteArrival;
        self.offered += 1;
        switch (record.outcome) {
            .pending => unreachable,
            .success => self.success += 1,
            .queue_full => self.queue_full += 1,
            .pool_timeout => self.pool_timeout += 1,
            .generator_dropped => self.generator_dropped += 1,
            .failed => {
                self.failed += 1;
                if (self.first_error == null) self.first_error = record.failure;
            },
        }
    }

    pub fn failure(self: Summary, allow_overload: bool) ?anyerror {
        if (self.failed != 0) return error.UnexpectedRequestFailure;
        if (!allow_overload and self.success != self.offered) return error.OverloadObserved;
        return null;
    }
};

const Slots = struct {
    current: std.atomic.Value(usize) = .init(0),
    peak: std.atomic.Value(usize) = .init(0),

    fn acquire(self: *Slots, limit: usize) bool {
        var current = self.current.load(.acquire);
        while (current < limit) {
            if (self.current.cmpxchgWeak(current, current + 1, .acq_rel, .acquire)) |changed| {
                current = changed;
            } else {
                _ = self.peak.fetchMax(current + 1, .monotonic);
                return true;
            }
        }
        return false;
    }

    fn release(self: *Slots) void {
        const old = self.current.fetchSub(1, .acq_rel);
        std.debug.assert(old != 0);
    }
};

// Compute from the index, never by repeatedly adding a truncated interval.
pub fn offsetNs(index: usize, rate: usize) u64 {
    std.debug.assert(rate != 0);
    return @intCast(@as(u128, index) * std.time.ns_per_s / rate);
}

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

fn waitUntil(deadline: u64) !void {
    while (true) {
        try zio.checkCancel();
        const now = nowNs();
        if (now >= deadline) return;
        try zio.sleep(.fromNanoseconds(deadline - now));
    }
}

fn outcome(err: anyerror) Outcome {
    return switch (err) {
        error.PoolQueueFull => .queue_full,
        error.PoolWaitTimeout => .pool_timeout,
        else => .failed,
    };
}

fn logLatency(name: []const u8, samples: []u64) void {
    std.mem.sort(u64, samples, {}, std.sort.asc(u64));
    std.log.info("latency={s} samples={d} p50_ns={d} p95_ns={d} p99_ns={d} max_ns={d}", .{
        name, samples.len, rank(samples, 50), rank(samples, 95), rank(samples, 99), rank(samples, 100),
    });
}

fn rank(sorted: []const u64, percent: usize) u64 {
    if (sorted.len == 0) return 0;
    return sorted[(sorted.len * percent + 99) / 100 - 1];
}

// No new allocations or hot-path counters: reuse latency scratch after join.
fn errorCodes(records: []const Record, scratch: []u64) ![]u64 {
    if (scratch.len < records.len) return error.InsufficientScratch;
    var count: usize = 0;
    for (records) |record| {
        if (record.failure) |err| {
            if (record.outcome != outcome(err)) return error.InvalidArrivalRecord;
            scratch[count] = @intFromError(err);
            count += 1;
        } else switch (record.outcome) {
            .success, .generator_dropped => {},
            else => return error.InvalidArrivalRecord,
        }
    }
    const codes = scratch[0..count];
    std.mem.sort(u64, codes, {}, std.sort.asc(u64));
    return codes;
}

fn logErrors(records: []const Record, scratch: []u64) !void {
    const codes = try errorCodes(records, scratch);
    var distinct: usize = 0;
    for (codes, 0..) |code, i| if (i == 0 or code != codes[i - 1]) {
        distinct += 1;
    };
    std.log.info("arrival_errors_version=1 samples={d} distinct={d}", .{ codes.len, distinct });
    var i: usize = 0;
    while (i < codes.len) {
        const err: anyerror = @errorFromInt(@as(u16, @intCast(codes[i])));
        var end = i + 1;
        while (end < codes.len and codes[end] == codes[i]) : (end += 1) {}
        std.log.info("arrival_error={s} outcome={s} count={d}", .{ @errorName(err), @tagName(outcome(err)), end - i });
        i = end;
    }
}

// Caller supplies bounded scratch and a synchronous, cancelable request.
// Each task owns one record; only the dispatcher changes the offered set.
pub fn measure(gpa: std.mem.Allocator, comptime func: anytype, args: anytype, options: Options, scratch: []u64) !Summary {
    std.debug.assert(scratch.len != 0 and options.rate != 0 and options.limit != 0);
    const records = try gpa.alloc(Record, scratch.len);
    defer gpa.free(records);
    @memset(records, .{});
    const Args = @TypeOf(args);
    const Worker = struct {
        fn attempt(request_args: Args, record: *Record, slots: *Slots) void {
            defer slots.release();
            const start = nowNs();
            record.start_lag_ns = start - record.due_ns;
            @call(.auto, func, request_args) catch |err| {
                record.failure = err;
                record.outcome = outcome(err);
                const finish = nowNs();
                record.service_ns = finish - start;
                record.scheduled_ns = finish - record.due_ns;
                return;
            };
            const finish = nowNs();
            record.service_ns = finish - start;
            record.scheduled_ns = finish - record.due_ns;
            record.outcome = .success;
        }
    };

    var slots: Slots = .{};
    var workers: zio.Group = .init;
    defer workers.cancel();
    const start = nowNs();
    const planned_span = offsetNs(records.len, options.rate);
    const end = try std.math.add(u64, start, planned_span);
    for (records, 0..) |*record, i| {
        record.due_ns = start + offsetNs(i, options.rate);
        try waitUntil(record.due_ns);
        record.dispatch_lag_ns = nowNs() - record.due_ns;
        if (!slots.acquire(options.limit)) {
            record.outcome = .generator_dropped;
            continue;
        }
        workers.spawn(Worker.attempt, .{ args, record, &slots }) catch |err| {
            slots.release();
            return err;
        };
    }
    try waitUntil(end);
    const offer_elapsed = nowNs() - start;
    try workers.wait();
    const elapsed = nowNs() - start;
    if (workers.hasFailed() or workers.isCanceled() or slots.current.load(.acquire) != 0) return error.UnreleasedGenerator;

    var summary: Summary = .{};
    for (records) |record| try summary.add(record);
    std.debug.assert(summary.offered == summary.success + summary.queue_full + summary.pool_timeout + summary.generator_dropped + summary.failed);
    const seconds = @as(f64, @floatFromInt(elapsed)) / std.time.ns_per_s;
    std.log.info("offered={d} success={d} pool_queue_full={d} pool_wait_timeout={d} generator_dropped={d} failed={d} first_error={s} generator_peak_in_flight={d} generator_in_flight=0", .{
        summary.offered, summary.success, summary.queue_full, summary.pool_timeout, summary.generator_dropped, summary.failed, if (summary.first_error) |err| @errorName(err) else "none", slots.peak.load(.acquire),
    });
    std.log.info("planned_span_ns={d} offer_elapsed_ns={d} drain_elapsed_ns={d} completion_elapsed_ns={d} success_per_second_including_drain={d:.2}", .{
        planned_span, offer_elapsed, elapsed - offer_elapsed, elapsed, @as(f64, @floatFromInt(summary.success)) / seconds,
    });
    try logErrors(records, scratch);
    for (std.enums.values(Latency)) |metric| {
        var count: usize = 0;
        for (records) |record| {
            scratch[count] = metric.sample(record) orelse continue;
            count += 1;
        }
        logLatency(@tagName(metric), scratch[0..count]);
    }
    return summary;
}

test "arrival: index-based timeline preserves fractions and does not drift" {
    try std.testing.expectEqual(@as(u64, 0), offsetNs(0, 3));
    try std.testing.expectEqual(@as(u64, 333_333_333), offsetNs(1, 3));
    try std.testing.expectEqual(@as(u64, 1_000_000_000), offsetNs(3, 3));
    try std.testing.expectEqual(@as(u64, 60_000_000_000), offsetNs(180, 3));
}

test "arrival: error distribution preserves every actual error and excludes non-errors" {
    const records = [_]Record{
        .{ .outcome = .failed, .failure = error.AddressUnavailable },
        .{ .outcome = .success },
        .{ .outcome = .failed, .failure = error.UnexpectedStatus },
        .{ .outcome = .queue_full, .failure = error.PoolQueueFull },
        .{ .outcome = .generator_dropped },
        .{ .outcome = .failed, .failure = error.AddressUnavailable },
        .{ .outcome = .pool_timeout, .failure = error.PoolWaitTimeout },
    };
    var scratch: [records.len]u64 = undefined;
    const codes = try errorCodes(&records, &scratch);
    try std.testing.expectEqual(@as(usize, 5), codes.len);
    var expected = [_]u64{ @intFromError(error.AddressUnavailable), @intFromError(error.AddressUnavailable), @intFromError(error.UnexpectedStatus), @intFromError(error.PoolQueueFull), @intFromError(error.PoolWaitTimeout) };
    std.mem.sort(u64, &expected, {}, std.sort.asc(u64));
    try std.testing.expectEqualSlices(u64, &expected, codes);
    var address_count: usize = 0;
    for (codes) |code| if (code == @intFromError(error.AddressUnavailable)) {
        address_count += 1;
    };
    try std.testing.expectEqual(@as(usize, 2), address_count);
    try std.testing.expectEqual(@as(usize, 0), (try errorCodes(&.{}, &.{})).len);
    try std.testing.expectError(error.InsufficientScratch, errorCodes(&records, scratch[0..1]));
    for ([_]Record{
        .{ .outcome = .success, .failure = error.OutOfMemory },
        .{ .outcome = .failed },
        .{ .outcome = .queue_full, .failure = error.AddressUnavailable },
        .{ .outcome = .pending },
    }) |invalid| try std.testing.expectError(error.InvalidArrivalRecord, errorCodes(&.{invalid}, scratch[0..1]));
}

test "arrival: saturated slots reject without acquiring and are reusable" {
    var slots: Slots = .{};
    try std.testing.expect(slots.acquire(2));
    try std.testing.expect(slots.acquire(2));
    try std.testing.expect(!slots.acquire(2));
    try std.testing.expectEqual(@as(usize, 2), slots.current.load(.acquire));
    slots.release();
    try std.testing.expect(slots.acquire(2));
    slots.release();
    slots.release();
    try std.testing.expectEqual(@as(usize, 0), slots.current.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), slots.peak.load(.acquire));
}

test "arrival: overload permission cannot mask unexpected errors or incomplete arrivals" {
    var summary: Summary = .{};
    try summary.add(.{ .outcome = .success });
    try summary.add(.{ .outcome = outcome(error.PoolQueueFull) });
    try summary.add(.{ .outcome = outcome(error.PoolWaitTimeout) });
    try summary.add(.{ .outcome = .generator_dropped });
    try std.testing.expectEqual(@as(usize, 4), summary.offered);
    try std.testing.expectEqual(error.OverloadObserved, summary.failure(false).?);
    try std.testing.expect(summary.failure(true) == null);
    try summary.add(.{ .outcome = outcome(error.OutOfMemory), .failure = error.OutOfMemory });
    try std.testing.expectEqual(error.UnexpectedRequestFailure, summary.failure(true).?);
    try std.testing.expectEqual(error.OutOfMemory, summary.first_error.?);
    try std.testing.expectEqual(.failed, outcome(error.Canceled));
    try std.testing.expectEqual(.failed, outcome(error.UnexpectedStatus));
    try std.testing.expectError(error.IncompleteArrival, summary.add(.{}));
    try std.testing.expectEqual(@as(u64, 0), rank(&.{}, 99));
    try std.testing.expectEqual(@as(u64, 7), rank(&.{7}, 99));
}

test "arrival: real tasks preserve rejection counts and empty success samples on two executors" {
    const F = struct {
        fn reject() !void {
            return error.PoolQueueFull;
        }
        fn invalidResponse() !void {
            return error.UnexpectedStatus;
        }
        fn run() !void {
            var samples: [4]u64 = undefined;
            const rejected = try measure(std.testing.allocator, reject, .{}, .{ .rate = 1000, .limit = 4 }, &samples);
            try std.testing.expectEqual(@as(usize, 4), rejected.offered);
            try std.testing.expectEqual(@as(usize, 4), rejected.queue_full);
            try std.testing.expectEqual(@as(usize, 0), rejected.success);
            try std.testing.expect(rejected.failure(true) == null);
            const invalid = try measure(std.testing.allocator, invalidResponse, .{}, .{ .rate = 1000, .limit = 1 }, samples[0..1]);
            try std.testing.expectEqual(@as(usize, 1), invalid.failed);
            try std.testing.expectEqual(error.UnexpectedStatus, invalid.first_error.?);
            try std.testing.expectEqual(error.UnexpectedRequestFailure, invalid.failure(true).?);
        }
    };
    for ([_]u8{ 1, 2 }) |executors| {
        const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
        defer rt.deinit();
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.run, .{});
        try group.wait();
        try std.testing.expect(!group.hasFailed() and !group.isCanceled());
    }
}

test "arrival: canceling dispatcher joins in-flight requests before freeing records" {
    const F = struct {
        const State = struct {
            entered: zio.ResetEvent = .init,
            active: std.atomic.Value(usize) = .init(0),
        };
        fn request(state: *State) !void {
            _ = state.active.fetchAdd(1, .acq_rel);
            defer _ = state.active.fetchSub(1, .acq_rel);
            state.entered.set();
            try zio.sleep(.fromSeconds(60));
        }
        fn dispatch(state: *State) !void {
            var samples: [33]u64 = undefined;
            _ = try measure(std.testing.allocator, request, .{state}, .{ .rate = 1000, .limit = 2 }, &samples);
        }
        fn run() !void {
            var state: State = .{};
            var task = try zio.spawn(dispatch, .{&state});
            defer task.cancel();
            try state.entered.wait();
            task.cancel();
            try std.testing.expectError(error.Canceled, task.join());
            try std.testing.expectEqual(@as(usize, 0), state.active.load(.acquire));
        }
    };
    for ([_]u8{ 1, 2 }) |executors| {
        const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(executors) });
        defer rt.deinit();
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.run, .{});
        try group.wait();
        try std.testing.expect(!group.hasFailed() and !group.isCanceled());
    }
}

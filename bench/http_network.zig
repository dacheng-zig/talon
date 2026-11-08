//! TCP/TLS workload with closed-loop, arrival-rate and idle/recovery phases.
//! See docs/benchmarks/README.md.
const std = @import("std");
const builtin = @import("builtin");
const zio = @import("zio");
const talon = @import("talon");
const http = talon.http.client;
const arrival = @import("arrival.zig");

const Config = struct {
    url: []const u8,
    requests: usize = 10_000,
    concurrency: usize = 16,
    executors: u8 = 1,
    warmup: usize = 32,
    body_bytes: usize = 17,
    mode: enum { keepalive, cold } = .keepalive,
    trust: enum { system, @"self-signed" } = .system,
    budget_bytes: usize = 64 * 1024 * 1024,
    idle_ms: usize = 0,
    idle_action: enum { retain, trim } = .retain,
    rate: usize = 0,
    pool_active: ?usize = null,
    pool_waiters: ?usize = null,
    pool_wait_ms: ?usize = null,
    allow_overload: enum { no, yes } = .no,

    fn activeLimit(self: Config) usize {
        return self.pool_active orelse self.concurrency;
    }

    fn clientOptions(self: Config, comptime Client: type) Client.Options {
        return .{
            .pool = .{
                .max_active = self.activeLimit(),
                .max_waiters = self.pool_waiters orelse self.concurrency,
                .pool_wait = if (self.pool_wait_ms) |ms| .fromMilliseconds(@intCast(ms)) else .fromSeconds(10),
                .max_per_origin = self.activeLimit(),
                .max_idle_per_origin = self.concurrency,
                .max_idle_total = self.concurrency,
            },
            .retry = .{ .max_retries = 0 },
            .redirect = .{ .max = 0 },
            .decompress = false,
        };
    }

    fn parse(args: []const []const u8) !Config {
        if (args.len == 0) return error.MissingUrl;
        var c: Config = .{ .url = args[0] };
        if (!std.mem.startsWith(u8, c.url, "http://") and !std.mem.startsWith(u8, c.url, "https://")) return error.InvalidUrl;
        var i: usize = 1;
        while (i < args.len) : (i += 2) {
            if (i + 1 == args.len) return error.MissingOptionValue;
            const key = args[i];
            const value = args[i + 1];
            if (std.mem.eql(u8, key, "--mode")) {
                c.mode = std.meta.stringToEnum(@FieldType(Config, "mode"), value) orelse return error.InvalidMode;
            } else if (std.mem.eql(u8, key, "--trust")) {
                c.trust = std.meta.stringToEnum(@FieldType(Config, "trust"), value) orelse return error.InvalidTrust;
            } else if (std.mem.eql(u8, key, "--idle-action")) {
                c.idle_action = std.meta.stringToEnum(@FieldType(Config, "idle_action"), value) orelse return error.InvalidIdleAction;
            } else if (std.mem.eql(u8, key, "--allow-overload")) {
                c.allow_overload = std.meta.stringToEnum(@FieldType(Config, "allow_overload"), value) orelse return error.InvalidOverloadOption;
            } else {
                const n = try std.fmt.parseInt(usize, value, 10);
                if (std.mem.eql(u8, key, "--requests")) {
                    c.requests = n;
                } else if (std.mem.eql(u8, key, "--concurrency")) {
                    c.concurrency = n;
                } else if (std.mem.eql(u8, key, "--executors")) {
                    c.executors = std.math.cast(u8, n) orelse return error.InvalidRange;
                } else if (std.mem.eql(u8, key, "--warmup")) {
                    c.warmup = n;
                } else if (std.mem.eql(u8, key, "--body-bytes")) {
                    c.body_bytes = n;
                } else if (std.mem.eql(u8, key, "--budget-bytes")) {
                    c.budget_bytes = n;
                } else if (std.mem.eql(u8, key, "--idle-ms")) {
                    c.idle_ms = n;
                } else if (std.mem.eql(u8, key, "--rate")) {
                    c.rate = n;
                } else if (std.mem.eql(u8, key, "--pool-active")) {
                    c.pool_active = n;
                } else if (std.mem.eql(u8, key, "--pool-waiters")) {
                    c.pool_waiters = n;
                } else if (std.mem.eql(u8, key, "--pool-wait-ms")) {
                    c.pool_wait_ms = n;
                } else return error.UnknownOption;
            }
        }
        // zio v0.18's executor mask has one bit per executor.
        if (c.requests == 0 or c.requests > 1_000_000 or
            c.concurrency == 0 or c.concurrency > 4096 or c.concurrency > c.requests or
            c.executors == 0 or c.executors > @bitSizeOf(usize) or
            c.warmup > 1_000_000 or c.body_bytes > 16 * 1024 * 1024 or c.budget_bytes == 0 or
            c.idle_ms > 60_000 or (c.idle_ms != 0 and c.mode == .cold) or
            (c.idle_ms == 0 and c.idle_action != .retain))
            return error.InvalidRange;
        if (c.activeLimit() == 0 or c.activeLimit() > 4096 or
            (c.pool_waiters orelse c.concurrency) > 4096 or (c.pool_wait_ms orelse 10_000) > 60_000 or
            c.rate > 1_000_000 or (c.rate != 0 and (c.idle_ms != 0 or arrival.offsetNs(c.requests, c.rate) > 1800 * std.time.ns_per_s)) or
            (c.rate == 0 and c.allow_overload == .yes)) return error.InvalidRange;
        return c;
    }
};

// Instrument only successful allocation/growth calls. AllocationBudget owns
// byte accounting; this layer forwards the allocator contract unchanged.
const Meter = struct {
    child: std.mem.Allocator,
    allocations: std.atomic.Value(usize) = .init(0),
    growths: std.atomic.Value(usize) = .init(0),

    fn allocator(self: *Meter) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *Meter = @ptrCast(@alignCast(ctx));
        const result = self.child.rawAlloc(len, alignment, ret) orelse return null;
        _ = self.allocations.fetchAdd(1, .monotonic);
        return result;
    }
    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) bool {
        const self: *Meter = @ptrCast(@alignCast(ctx));
        if (!self.child.rawResize(memory, alignment, len, ret)) return false;
        if (len > memory.len) _ = self.growths.fetchAdd(1, .monotonic);
        return true;
    }
    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) ?[*]u8 {
        const self: *Meter = @ptrCast(@alignCast(ctx));
        const result = self.child.rawRemap(memory, alignment, len, ret) orelse return null;
        if (len > memory.len) _ = self.growths.fetchAdd(1, .monotonic);
        return result;
    }
    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
        const self: *Meter = @ptrCast(@alignCast(ctx));
        self.child.rawFree(memory, alignment, ret);
    }
};

const Gate = struct {
    ready: std.atomic.Value(usize) = .init(0),
    all_ready: zio.ResetEvent = .init,
    start: zio.ResetEvent = .init,
};

fn request(c: anytype, config: Config) !void {
    const response = try c.requestUrl(.{ .url = config.url, .keep_alive = config.mode == .keepalive, .extra_headers = &.{.{ .name = "accept-encoding", .value = "identity" }} });
    var complete = false;
    defer {
        if (!complete) response.conn.abortResponse();
        response.deinit();
    }
    if (response.status() != 200) return error.UnexpectedStatus;
    var scratch: [4096]u8 = undefined;
    var bytes: usize = 0;
    while (true) {
        const n = try response.bodyReader().readSliceShort(&scratch);
        if (n == 0) break;
        bytes = std.math.add(usize, bytes, n) catch return error.BodyTooLarge;
        if (bytes > config.body_bytes) return error.UnexpectedBodyLength;
    }
    if (bytes != config.body_bytes) return error.UnexpectedBodyLength;
    complete = true;
}

fn Workers(comptime Client: type) type {
    return struct {
        fn warm(c: *Client, config: Config) !void {
            for (0..config.warmup) |_| try request(c, config);
        }
        fn measure(c: *Client, config: Config, samples: []u64, gate: *Gate) !void {
            if (gate.ready.fetchAdd(1, .acq_rel) + 1 == config.concurrency) gate.all_ready.set();
            try gate.start.wait();
            for (samples) |*sample| {
                var clock = zio.Stopwatch.start();
                try request(c, config);
                sample.* = clock.read().toNanoseconds();
            }
        }
    };
}

// Nearest rank, including the slowest sample at p100. Sorted, nonempty input.
fn percentile(sorted: []const u64, percent: usize) u64 {
    std.debug.assert(sorted.len != 0 and percent > 0 and percent <= 100);
    return sorted[(sorted.len * percent + 99) / 100 - 1];
}

fn measureBatch(c: anytype, config: Config, samples: []u64) !u64 {
    const W = Workers(@TypeOf(c.*));
    var gate: Gate = .{};
    var workers: zio.Group = .init;
    defer workers.cancel();
    for (0..config.concurrency) |i| {
        const begin = samples.len * i / config.concurrency;
        const end = samples.len * (i + 1) / config.concurrency;
        try workers.spawn(W.measure, .{ c, config, samples[begin..end], &gate });
    }
    try gate.all_ready.wait();
    var clock = zio.Stopwatch.start();
    gate.start.set();
    try workers.wait();
    const elapsed = clock.read().toNanoseconds();
    if (workers.hasFailed() or workers.isCanceled()) return error.MeasurementFailed;
    return elapsed;
}

fn idleSnapshot(c: anytype, budget: *talon.core.AllocationBudget, meter: *Meter, phase: []const u8, elapsed_ns: u64) !void {
    const pool = c.poolStats();
    if (pool.active != 0 or pool.waiting != 0) return error.UnreleasedAdmission;
    std.log.info("phase={s} idle_elapsed_ns={d} client_requested_bytes={d} idle_response_arena_bytes={d} decoder_bytes={d} idle_decoder_bytes={d} successful_alloc_calls_total={d} successful_growth_calls_total={d} created_total={d} reused_total={d} evicted_total={d} active={d} waiting={d}", .{
        phase, elapsed_ns, budget.stats().reserved_bytes, pool.idle_response_arena_bytes, pool.decoder_bytes, pool.idle_decoder_bytes, meter.allocations.load(.acquire), meter.growths.load(.acquire), pool.created, pool.reused, pool.evicted, pool.active, pool.waiting,
    });
}

fn idleRecovery(c: anytype, config: Config, samples: []u64, budget: *talon.core.AllocationBudget, meter: *Meter) !void {
    var clock = zio.Stopwatch.start();
    std.log.info("idle_ms={d} idle_action={s} recovery_requests={d}", .{ config.idle_ms, @tagName(config.idle_action), config.concurrency });
    try idleSnapshot(c, budget, meter, "before_idle", clock.read().toNanoseconds());
    try zio.sleep(.fromMilliseconds(@intCast(config.idle_ms)));
    try idleSnapshot(c, budget, meter, "before_action", clock.read().toNanoseconds());
    var action_clock = zio.Stopwatch.start();
    const trimmed = if (config.idle_action == .trim) c.trimIdleConnections() else 0;
    const action_ns = action_clock.read().toNanoseconds();
    std.log.info("idle_action_ns={d} idle_trimmed_connections={d}", .{ action_ns, trimmed });
    try idleSnapshot(c, budget, meter, "after_action", clock.read().toNanoseconds());
    try zio.sleep(.fromMilliseconds(@intCast(config.idle_ms)));
    try idleSnapshot(c, budget, meter, "before_recovery", clock.read().toNanoseconds());

    const before = c.poolStats();
    const alloc_before = meter.allocations.load(.acquire);
    const growth_before = meter.growths.load(.acquire);
    // Reuse the already-reported sample storage; one first request per worker.
    const recovery = samples[0..config.concurrency];
    const elapsed = try measureBatch(c, config, recovery);
    const after = c.poolStats();
    try idleSnapshot(c, budget, meter, "after_recovery", clock.read().toNanoseconds());
    std.mem.sort(u64, recovery, {}, std.sort.asc(u64));
    std.log.info("recovery_elapsed_ns={d} recovery_p50_ns={d} recovery_p95_ns={d} recovery_p99_ns={d} recovery_max_ns={d} recovery_created={d} recovery_reused={d} recovery_evicted={d} recovery_alloc_calls={d} recovery_growth_calls={d}", .{
        elapsed, percentile(recovery, 50), percentile(recovery, 95), percentile(recovery, 99), recovery[recovery.len - 1], after.created - before.created, after.reused - before.reused, after.evicted - before.evicted, meter.allocations.load(.acquire) - alloc_before, meter.growths.load(.acquire) - growth_before,
    });
}

fn run(c: anytype, config: Config, samples: []u64, budget: *talon.core.AllocationBudget, meter: *Meter, gpa: std.mem.Allocator) !void {
    const W = Workers(@TypeOf(c.*));
    var warmers: zio.Group = .init;
    defer warmers.cancel();
    for (0..@min(config.concurrency, config.activeLimit())) |_| try warmers.spawn(W.warm, .{ c, config });
    try warmers.wait();
    if (warmers.hasFailed() or warmers.isCanceled()) return error.WarmupFailed;

    const before = c.poolStats();
    const alloc_before = meter.allocations.load(.acquire);
    const growth_before = meter.growths.load(.acquire);
    std.log.info("model={s} transport={s} mode={s} trust={s} optimize={s} requests={d} concurrency={d} executors={d} warmup_per_worker={d} body_bytes={d} budget_limit={d}", .{
        if (config.rate == 0) "closed_loop" else "arrival_rate", if (std.mem.startsWith(u8, config.url, "https://")) "tls" else "tcp", @tagName(config.mode), @tagName(config.trust), @tagName(builtin.mode), config.requests, config.concurrency, config.executors, config.warmup, config.body_bytes, config.budget_bytes,
    });
    var result_error: ?anyerror = null;
    if (config.rate != 0) {
        std.log.info("rate={d} pool_active_limit={d} pool_waiter_limit={d} pool_wait_ms={d} warmup_workers={d} allow_overload={s}", .{
            config.rate, config.activeLimit(), config.pool_waiters orelse config.concurrency, config.pool_wait_ms orelse 10_000, @min(config.concurrency, config.activeLimit()), @tagName(config.allow_overload),
        });
        const summary = try arrival.measure(gpa, request, .{ c, config }, .{ .rate = config.rate, .limit = config.concurrency }, samples);
        result_error = summary.failure(config.allow_overload == .yes);
    } else {
        const elapsed = try measureBatch(c, config, samples);
        std.mem.sort(u64, samples, {}, std.sort.asc(u64));
        const seconds = @as(f64, @floatFromInt(elapsed)) / std.time.ns_per_s;
        std.log.info("elapsed_ns={d} requests_per_second={d:.2} p50_ns={d} p95_ns={d} p99_ns={d} max_ns={d}", .{ elapsed, @as(f64, @floatFromInt(config.requests)) / seconds, percentile(samples, 50), percentile(samples, 95), percentile(samples, 99), samples[samples.len - 1] });
    }
    const after = c.poolStats();
    if (after.active != 0 or after.waiting != 0) return error.UnreleasedAdmission;
    const allocations = meter.allocations.load(.acquire) - alloc_before;
    const growths = meter.growths.load(.acquire) - growth_before;
    const retained = budget.stats();
    std.log.info("created={d} reused={d} evicted={d} successful_alloc_calls={d} successful_growth_calls={d} active={d} waiting={d}", .{ after.created - before.created, after.reused - before.reused, after.evicted - before.evicted, allocations, growths, after.active, after.waiting });
    if (config.idle_ms != 0) try idleRecovery(c, config, samples, budget, meter);
    const trimmed = c.trimIdleConnections();
    const after_trim = budget.stats().reserved_bytes;
    // retained/peak describe the original measured burst; final trim follows
    // recovery when enabled and is distinct from the measured idle action.
    std.log.info("client_requested_peak_bytes_including_warmup={d} client_retained_bytes={d} trimmed_connections={d} client_bytes_after_trim={d} denied_allocator_calls={d}", .{ retained.peak_reserved_bytes, retained.reserved_bytes, trimmed, after_trim, retained.denied_requests });
    if (result_error) |err| return err;
}

fn workload(gpa: std.mem.Allocator, io: std.Io, config: Config, samples: []u64) !void {
    var budget = talon.core.AllocationBudget.init(gpa, config.budget_bytes);
    defer budget.deinit();
    var meter: Meter = .{ .child = budget.allocator() };
    const counted = meter.allocator();
    var run_error: ?anyerror = null;
    if (std.mem.startsWith(u8, config.url, "https://")) {
        var store: http.RootStore = .{};
        defer store.deinit(gpa);
        if (config.trust == .system) try store.load(gpa, io);
        var c = http.TlsClient.init(counted, .{ .gpa = counted, .io = io, .verification = if (config.trust == .system) .{ .system = &store } else .self_signed }, config.clientOptions(http.TlsClient));
        {
            defer c.deinit();
            run(&c, config, samples, &budget, &meter, gpa) catch |err| {
                run_error = err;
            };
        }
    } else {
        const Client = http.Client(http.TcpConnector);
        var c = Client.init(counted, .{}, config.clientOptions(Client));
        {
            defer c.deinit();
            run(&c, config, samples, &budget, &meter, gpa) catch |err| {
                run_error = err;
            };
        }
    }
    if (budget.stats().reserved_bytes != 0) return error.UnreleasedClientAllocation;
    std.log.info("client_bytes_after_deinit=0", .{});
    if (run_error) |err| return err;
}

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const config = Config.parse(@ptrCast(args[1..])) catch |err| {
        std.log.err("{s}; usage: http_network_bench URL [--requests N] [--concurrency N] [--executors N] [--warmup N] [--body-bytes N] [--mode keepalive|cold] [--trust system|self-signed] [--budget-bytes N] [--idle-ms N] [--idle-action retain|trim] [--rate N] [--pool-active N] [--pool-waiters N] [--pool-wait-ms N] [--allow-overload no|yes]", .{@errorName(err)});
        return err;
    };
    const rt = try zio.Runtime.init(init.gpa, .{ .executors = .exact(config.executors), .stack_pool = .{ .maximum_size = 8 * 1024 * 1024, .committed_size = 64 * 1024 } });
    defer rt.deinit();
    const samples = try init.gpa.alloc(u64, config.requests);
    defer init.gpa.free(samples);
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(workload, .{ init.gpa, rt.io(), config, samples });
    try group.wait();
    if (group.hasFailed() or group.isCanceled()) return error.BenchmarkFailed;
}

test "network benchmark: bounded configuration and explicit modes" {
    const c = try Config.parse(&.{ "https://localhost/", "--requests", "33", "--concurrency", "2", "--executors", "2", "--mode", "cold", "--trust", "self-signed" });
    try std.testing.expectEqual(@as(usize, 33), c.requests);
    try std.testing.expectEqual(.cold, c.mode);
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--requests", "0" }));
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--requests", "1000001" }));
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--executors", "0" }));
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--executors", "65" }));
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--concurrency", "4097" }));
    try std.testing.expectError(error.InvalidMode, Config.parse(&.{ "http://localhost/", "--mode", "other" }));
    try std.testing.expectError(error.MissingOptionValue, Config.parse(&.{ "http://localhost/", "--warmup" }));
}

test "network benchmark: nearest-rank percentiles include small-sample tails" {
    try std.testing.expectEqual(@as(u64, 9), percentile(&.{9}, 99));
    try std.testing.expectEqual(@as(u64, 2), percentile(&.{ 1, 2, 3, 4 }, 50));
    try std.testing.expectEqual(@as(u64, 4), percentile(&.{ 1, 2, 3, 4 }, 95));
    try std.testing.expectEqual(@as(u64, 4), percentile(&.{ 1, 2, 3, 4 }, 100));
}

test "network benchmark: idle comparisons require cached connections and bounded observation windows" {
    const c = try Config.parse(&.{ "http://localhost/", "--idle-ms", "60000", "--idle-action", "trim" });
    try std.testing.expectEqual(@as(usize, 60000), c.idle_ms);
    try std.testing.expectEqual(.trim, c.idle_action);
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--idle-ms", "60001" }));
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--idle-ms", "1", "--mode", "cold" }));
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--idle-action", "trim" }));
    try std.testing.expectError(error.InvalidIdleAction, Config.parse(&.{ "http://localhost/", "--idle-ms", "1", "--idle-action", "other" }));
}

test "network benchmark: independent arrival and pool bounds are explicit" {
    const c = try Config.parse(&.{ "http://localhost/", "--rate", "1000", "--pool-active", "2", "--pool-waiters", "0", "--pool-wait-ms", "0", "--allow-overload", "yes" });
    try std.testing.expectEqual(@as(usize, 2), c.activeLimit());
    try std.testing.expectEqual(@as(?usize, 0), c.pool_waiters);
    try std.testing.expectEqual(.yes, c.allow_overload);
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--rate", "1000001" }));
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--requests", "1801", "--rate", "1" }));
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--rate", "1000", "--idle-ms", "1" }));
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--pool-active", "0" }));
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--pool-waiters", "4097" }));
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--pool-wait-ms", "60001" }));
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--allow-overload", "yes" }));
}

test "network benchmark: long arrival windows keep independent request and time bounds" {
    const long = try Config.parse(&.{ "http://localhost/", "--requests", "900000", "--rate", "500" });
    try std.testing.expectEqual(@as(u64, 1800 * std.time.ns_per_s), arrival.offsetNs(long.requests, long.rate));
    _ = try Config.parse(&.{ "http://localhost/", "--requests", "1000000", "--rate", "2000" });
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--requests", "900001", "--rate", "500" }));
    try std.testing.expectError(error.InvalidRange, Config.parse(&.{ "http://localhost/", "--requests", "1000001", "--rate", "2000" }));
}

test {
    _ = arrival;
}

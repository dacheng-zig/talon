//! Generic middleware chain combinator.
//!
//! One mechanism serves three consumers: talon connection-level middleware,
//! talon datagram packet-level middleware, and wing request-level middleware.
//! The chain is generic over the context type and expands at comptime into
//! nested inline calls — no vtables, no indirect dispatch.
//!
//! A middleware is either:
//!   - a plain function: `fn (ctx: *Ctx, next: anytype) !void`
//!   - a struct type with `pub fn run(ctx: *Ctx, next: anytype) !void`,
//!     optionally declaring `pub const provides = .{Capability, ...}` and
//!     `pub const requires = .{Capability, ...}` (capabilities are types).
//!
//! `requires` is checked at comptime against the `provides` of all earlier
//! middlewares in the chain, turning middleware-ordering bugs into compile
//! errors.
//!
//! Inside a middleware, call `try next.call(ctx)` to continue the chain;
//! returning without calling `next` short-circuits it. Code before `next`
//! runs inbound, code after runs outbound — a single around-style
//! abstraction is naturally bidirectional (Netty inbound/outbound style).

const std = @import("std");

/// Composes `middlewares` (a tuple) into a static call chain over `*Ctx`.
/// The returned type exposes:
///   - `run(ctx, handler)` — invoke the chain; `handler` is the terminal,
///     either `fn (*Ctx) !void` or a value with `pub fn call(self, ctx) !void`
///   - `provides(F)` — comptime: whether any middleware provides capability F
pub fn chain(comptime Ctx: type, comptime middlewares: anytype) type {
    comptime validate(Ctx, middlewares);
    const count = @typeInfo(@TypeOf(middlewares)).@"struct".fields.len;

    return struct {
        /// Comptime capability query: does any middleware in this chain
        /// declare `F` in its `provides`? Used by Connection type synthesis.
        pub fn provides(comptime F: type) bool {
            comptime var i: usize = 0;
            inline while (i < count) : (i += 1) {
                if (comptime providesAt(middlewares, i, F)) return true;
            }
            return false;
        }

        pub fn run(ctx: *Ctx, handler: anytype) anyerror!void {
            const next: Next(0, @TypeOf(handler)) = .{ .handler = handler };
            return next.call(ctx);
        }

        fn Next(comptime index: usize, comptime Handler: type) type {
            return struct {
                handler: Handler,

                pub inline fn call(self: @This(), ctx: *Ctx) anyerror!void {
                    if (comptime index == count) {
                        return invokeTerminal(Ctx, self.handler, ctx);
                    } else {
                        const nxt: Next(index + 1, Handler) = .{ .handler = self.handler };
                        const mw = middlewares[index];
                        if (comptime @TypeOf(mw) == type) {
                            return mw.run(ctx, nxt);
                        } else {
                            return mw(ctx, nxt);
                        }
                    }
                }
            };
        }
    };
}

fn invokeTerminal(comptime Ctx: type, handler: anytype, ctx: *Ctx) anyerror!void {
    const H = @TypeOf(handler);
    if (comptime @typeInfo(H) == .@"fn" or
        (@typeInfo(H) == .pointer and @typeInfo(@typeInfo(H).pointer.child) == .@"fn"))
    {
        return handler(ctx);
    } else if (comptime std.meta.hasMethod(H, "call")) {
        return handler.call(ctx);
    } else {
        @compileError("talon.chain: terminal handler must be 'fn (*" ++
            @typeName(Ctx) ++ ") !void' or provide a 'call' method, got " ++ @typeName(H));
    }
}

fn validate(comptime Ctx: type, comptime middlewares: anytype) void {
    const Mws = @TypeOf(middlewares);
    const info = @typeInfo(Mws);
    if (info != .@"struct" or !info.@"struct".is_tuple) {
        @compileError("talon.chain: middlewares must be a tuple literal like .{ mw1, mw2 }, got " ++
            @typeName(Mws));
    }

    inline for (info.@"struct".fields, 0..) |_, i| {
        const mw = middlewares[i];
        const Mw = @TypeOf(mw);
        if (Mw == type) {
            validateStructMiddleware(Ctx, middlewares, mw, i);
        } else if (@typeInfo(Mw) == .@"fn") {
            validateFnSignature(Ctx, Mw, i, @typeName(Mw));
        } else {
            @compileError(std.fmt.comptimePrint(
                "talon.chain: middleware #{d} must be a function 'fn (ctx: *Ctx, next: anytype) !void' " ++
                    "or a struct type with a 'run' declaration, got {s}",
                .{ i, @typeName(Mw) },
            ));
        }
    }
}

fn validateStructMiddleware(
    comptime Ctx: type,
    comptime middlewares: anytype,
    comptime Mw: type,
    comptime index: usize,
) void {
    if (!@hasDecl(Mw, "run")) {
        @compileError(std.fmt.comptimePrint(
            "talon.chain: middleware #{d} ({s}) must declare 'pub fn run(ctx: *Ctx, next: anytype) !void'",
            .{ index, @typeName(Mw) },
        ));
    }
    validateFnSignature(Ctx, @TypeOf(Mw.run), index, @typeName(Mw));

    if (@hasDecl(Mw, "requires")) {
        inline for (Mw.requires) |Req| {
            if (!providedBefore(middlewares, index, Req)) {
                @compileError(std.fmt.comptimePrint(
                    "talon.chain: middleware #{d} ({s}) requires capability '{s}' " ++
                        "but no earlier middleware provides it; check middleware order",
                    .{ index, @typeName(Mw), @typeName(Req) },
                ));
            }
        }
    }
}

fn validateFnSignature(comptime Ctx: type, comptime F: type, comptime index: usize, comptime name: []const u8) void {
    const fn_info = @typeInfo(F).@"fn";
    if (fn_info.params.len != 2) {
        @compileError(std.fmt.comptimePrint(
            "talon.chain: middleware #{d} ({s}) must take exactly (ctx: *Ctx, next: anytype), found {d} parameter(s)",
            .{ index, name, fn_info.params.len },
        ));
    }
    // First param is either the concrete *Ctx or anytype (generic, reusable
    // across context types). anytype shows up as a null param type.
    if (fn_info.params[0].type) |P| {
        if (P != *Ctx) {
            @compileError(std.fmt.comptimePrint(
                "talon.chain: middleware #{d} ({s}) first parameter must be *{s}, got {s}",
                .{ index, name, @typeName(Ctx), @typeName(P) },
            ));
        }
    }
}

fn providedBefore(comptime middlewares: anytype, comptime index: usize, comptime F: type) bool {
    comptime var i: usize = 0;
    inline while (i < index) : (i += 1) {
        if (providesAt(middlewares, i, F)) return true;
    }
    return false;
}

fn providesAt(comptime middlewares: anytype, comptime i: usize, comptime F: type) bool {
    const mw = middlewares[i];
    if (@TypeOf(mw) != type) return false;
    if (!@hasDecl(mw, "provides")) return false;
    inline for (mw.provides) |P| {
        if (P == F) return true;
    }
    return false;
}

// ── Tests ────────────────────────────────────────────────────────────────

const TestCtx = struct {
    log: std.ArrayList(u8),
    gpa: std.mem.Allocator,

    fn mark(self: *TestCtx, c: u8) !void {
        try self.log.append(self.gpa, c);
    }

    fn init(gpa: std.mem.Allocator) !TestCtx {
        return .{ .log = try std.ArrayList(u8).initCapacity(gpa, 16), .gpa = gpa };
    }

    fn deinit(self: *TestCtx) void {
        self.log.deinit(self.gpa);
    }
};

fn mwA(ctx: *TestCtx, next: anytype) anyerror!void {
    try ctx.mark('a');
    try next.call(ctx);
    try ctx.mark('A');
}

fn mwB(ctx: *TestCtx, next: anytype) anyerror!void {
    try ctx.mark('b');
    try next.call(ctx);
    try ctx.mark('B');
}

fn terminalH(ctx: *TestCtx) anyerror!void {
    try ctx.mark('h');
}

test "chain: around-style ordering (inbound before next, outbound after)" {
    var ctx = try TestCtx.init(std.testing.allocator);
    defer ctx.deinit();

    const C = chain(TestCtx, .{ mwA, mwB });
    try C.run(&ctx, terminalH);
    try std.testing.expectEqualStrings("abhBA", ctx.log.items);
}

test "chain: empty chain invokes terminal directly" {
    var ctx = try TestCtx.init(std.testing.allocator);
    defer ctx.deinit();

    const C = chain(TestCtx, .{});
    try C.run(&ctx, terminalH);
    try std.testing.expectEqualStrings("h", ctx.log.items);
}

const StructMw = struct {
    pub fn run(ctx: *TestCtx, next: anytype) anyerror!void {
        try ctx.mark('s');
        try next.call(ctx);
        try ctx.mark('S');
    }
};

test "chain: mixed fn and struct middleware" {
    var ctx = try TestCtx.init(std.testing.allocator);
    defer ctx.deinit();

    const C = chain(TestCtx, .{ mwA, StructMw, mwB });
    try C.run(&ctx, terminalH);
    try std.testing.expectEqualStrings("asbhBSA", ctx.log.items);
}

const ShortCircuitMw = struct {
    pub fn run(ctx: *TestCtx, next: anytype) anyerror!void {
        _ = next; // reject: never continue the chain
        try ctx.mark('x');
    }
};

test "chain: middleware can short-circuit without calling next" {
    var ctx = try TestCtx.init(std.testing.allocator);
    defer ctx.deinit();

    const C = chain(TestCtx, .{ mwA, ShortCircuitMw, mwB });
    try C.run(&ctx, terminalH);
    // mwB and the terminal never run; mwA still unwinds.
    try std.testing.expectEqualStrings("axA", ctx.log.items);
}

fn failingTerminal(ctx: *TestCtx) anyerror!void {
    _ = ctx;
    return error.Boom;
}

test "chain: error from terminal propagates through the chain" {
    var ctx = try TestCtx.init(std.testing.allocator);
    defer ctx.deinit();

    const C = chain(TestCtx, .{ mwA, mwB });
    try std.testing.expectError(error.Boom, C.run(&ctx, failingTerminal));
    // Outbound marks never run past the error.
    try std.testing.expectEqualStrings("ab", ctx.log.items);
}

fn failingMw(ctx: *TestCtx, next: anytype) anyerror!void {
    _ = next;
    _ = ctx;
    return error.Rejected;
}

test "chain: error from middleware propagates" {
    var ctx = try TestCtx.init(std.testing.allocator);
    defer ctx.deinit();

    const C = chain(TestCtx, .{ mwA, failingMw, mwB });
    try std.testing.expectError(error.Rejected, C.run(&ctx, terminalH));
    try std.testing.expectEqualStrings("a", ctx.log.items);
}

const StatefulTerminal = struct {
    hits: *u32,
    pub fn call(self: @This(), ctx: *TestCtx) anyerror!void {
        _ = ctx;
        self.hits.* += 1;
    }
};

test "chain: terminal handler can be a stateful value with a call method" {
    var ctx = try TestCtx.init(std.testing.allocator);
    defer ctx.deinit();

    var hits: u32 = 0;
    const C = chain(TestCtx, .{mwA});
    try C.run(&ctx, StatefulTerminal{ .hits = &hits });
    try std.testing.expectEqual(1, hits);
}

// Capability types for provides/requires tests (capabilities are types).
const TlsInfo = struct { cipher: []const u8 };
const PeerInfo = struct { addr: u32 };

const ProvidesTls = struct {
    pub const provides = .{TlsInfo};
    pub fn run(ctx: *TestCtx, next: anytype) anyerror!void {
        try ctx.mark('t');
        try next.call(ctx);
    }
};

const NeedsTls = struct {
    pub const requires = .{TlsInfo};
    pub fn run(ctx: *TestCtx, next: anytype) anyerror!void {
        try ctx.mark('n');
        try next.call(ctx);
    }
};

test "chain: requires satisfied by earlier provides compiles and runs" {
    var ctx = try TestCtx.init(std.testing.allocator);
    defer ctx.deinit();

    const C = chain(TestCtx, .{ ProvidesTls, NeedsTls });
    try C.run(&ctx, terminalH);
    try std.testing.expectEqualStrings("tnh", ctx.log.items);
}

test "chain: provides() comptime capability query" {
    const C = chain(TestCtx, .{ ProvidesTls, NeedsTls });
    try std.testing.expect(comptime C.provides(TlsInfo));
    try std.testing.expect(comptime !C.provides(PeerInfo));
}

// Generic middleware (anytype ctx) is reusable across context types — the
// comptime version of tower's ecosystem effect.
const OtherCtx = struct {
    count: u32 = 0,
};

const GenericCounter = struct {
    pub fn run(ctx: anytype, next: anytype) anyerror!void {
        if (comptime @hasField(@TypeOf(ctx.*), "count")) ctx.count += 1;
        try next.call(ctx);
    }
};

fn otherTerminal(ctx: *OtherCtx) anyerror!void {
    ctx.count += 10;
}

test "chain: generic middleware reuses across context types" {
    var other: OtherCtx = .{};
    const C2 = chain(OtherCtx, .{GenericCounter});
    try C2.run(&other, otherTerminal);
    try std.testing.expectEqual(11, other.count);

    var ctx = try TestCtx.init(std.testing.allocator);
    defer ctx.deinit();
    const C1 = chain(TestCtx, .{GenericCounter});
    try C1.run(&ctx, terminalH);
    try std.testing.expectEqualStrings("h", ctx.log.items);
}

const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

// Notify the controller only after a real payload allocation. This makes the
// drain test cancel a partial body rather than racing with header parsing.
const FrameAllocator = struct {
    counted: std.testing.FailingAllocator,
    allocated: zio.ResetEvent = .init,

    fn allocator(self: *FrameAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = std.mem.Allocator.noResize,
            .remap = std.mem.Allocator.noRemap,
            .free = free,
        } };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *FrameAllocator = @ptrCast(@alignCast(ctx));
        const result = self.counted.allocator().rawAlloc(len, alignment, ret) orelse return null;
        self.allocated.set();
        return result;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
        const self: *FrameAllocator = @ptrCast(@alignCast(ctx));
        self.counted.allocator().rawFree(memory, alignment, ret);
    }
};

test "OwnedLengthPrefixed: engine echoes large frames through a tiny window and drain frees a partial body" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    const App = struct {
        memory: FrameAllocator = .{ .counted = .init(std.testing.allocator, .{}) },
        completed: bool = false,
        read_failed: bool = false,
    };
    const Proto = struct {
        pub fn serve(conn: anytype, app: *App) !void {
            var frames = talon.core.framing.OwnedLengthPrefixed(.{ .max_frame = 8192 }).init(conn.reader(), app.memory.allocator());
            var frame = (frames.next() catch |err| {
                app.read_failed = err == error.ReadFailed;
                try std.testing.expectError(error.FrameReaderFailed, frames.next());
                return err;
            }).?;
            defer frame.deinit();
            try conn.writer().writeInt(u32, @intCast(frame.payload.len), .big);
            try conn.writer().writeAll(frame.payload);
            try conn.writer().flush();
            app.completed = true;
        }
    };
    const Server = talon.core.StreamServer(Proto, App);
    for ([_]bool{ false, true }) |stall| {
        var app: App = .{};
        var listener = try talon.core.MemoryListener.init(std.testing.allocator, .{ .pipe_buffer_size = 31 });
        defer listener.deinit();
        var server = try Server.init(std.testing.allocator, &app, .{ .limits = .{
            .read_buffer_size = 7,
            .write_buffer_size = 11,
            .drain_timeout = .fromMilliseconds(20),
        } });
        defer server.deinit();
        var done: zio.ResetEvent = .init;
        const F = struct {
            fn serve(s: *Server, l: *talon.core.MemoryListener, finished: *zio.ResetEvent) !void {
                try s.serve(l);
                finished.set();
            }
            fn client(s: *Server, l: *talon.core.MemoryListener, a: *App, finished: *zio.ResetEvent, partial: bool) !void {
                defer s.shutdown();
                const raw = try l.connect();
                defer raw.close();
                var writer = raw.writer(&.{});
                writer.setTimeout(.fromSeconds(2));
                try writer.interface.writeInt(u32, 8192, .big);
                const fragment = [_]u8{'x'} ** 137;
                var left: usize = if (partial) 3 else 8192;
                while (left != 0) {
                    const n = @min(left, fragment.len);
                    try writer.interface.writeAll(fragment[0..n]);
                    left -= n;
                }
                if (partial) {
                    try a.memory.allocated.wait();
                    s.shutdown();
                    // Keep peer open, forcing drain cancellation rather than EOF.
                    try finished.wait();
                } else {
                    var buffer: [19]u8 = undefined;
                    var reader = raw.reader(&buffer);
                    reader.setTimeout(.fromSeconds(2));
                    try std.testing.expectEqual(@as(u32, 8192), try reader.interface.takeInt(u32, .big));
                    for (0..8192) |_| try std.testing.expectEqual(@as(u8, 'x'), try reader.interface.takeByte());
                }
            }
        };
        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(F.serve, .{ &server, &listener, &done });
        try group.spawn(F.client, .{ &server, &listener, &app, &done, stall });
        try group.wait();
        try std.testing.expect(!group.hasFailed());
        try std.testing.expectEqual(!stall, app.completed);
        try std.testing.expectEqual(stall, app.read_failed);
        try std.testing.expectEqual(@as(usize, 8192), app.memory.counted.allocated_bytes);
        try std.testing.expectEqual(app.memory.counted.allocated_bytes, app.memory.counted.freed_bytes);
    }
}

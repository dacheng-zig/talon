//! In-process byte pipe with coroutine-blocking semantics.
//!
//! Building block of the memory transport: both ends of a
//! MemoryListener connection are `std.Io.Reader`/`std.Io.Writer` backed by a
//! pair of these pipes, so the whole stack can be tested without sockets.

const std = @import("std");
const zio = @import("zio");

pub const ReadError = error{Canceled};
pub const WriteError = error{ Canceled, BrokenPipe };

/// Single-direction byte ring buffer. One writer end, one reader end; reads
/// and writes suspend the calling coroutine when the buffer is empty/full.
pub const Pipe = struct {
    mutex: zio.Mutex = .init,
    readable: zio.Condition = .init,
    writable: zio.Condition = .init,
    buffer: []u8,
    head: usize = 0,
    count: usize = 0,
    write_closed: bool = false,
    read_closed: bool = false,

    pub fn init(buffer: []u8) Pipe {
        return .{ .buffer = buffer };
    }

    /// Reads up to dest.len bytes. Suspends while the pipe is empty.
    /// Returns 0 on end-of-stream (write end closed and buffer drained,
    /// or own read end closed).
    pub fn read(self: *Pipe, dest: []u8) ReadError!usize {
        if (dest.len == 0) return 0;
        try self.mutex.lock();
        defer self.mutex.unlock();

        while (self.count == 0) {
            if (self.write_closed or self.read_closed) return 0;
            try self.readable.wait(&self.mutex);
        }

        const n = @min(dest.len, self.count);
        const first = @min(n, self.buffer.len - self.head);
        @memcpy(dest[0..first], self.buffer[self.head..][0..first]);
        @memcpy(dest[first..n], self.buffer[0 .. n - first]);
        self.head = (self.head + n) % self.buffer.len;
        self.count -= n;
        self.writable.signal();
        return n;
    }

    /// Writes up to src.len bytes, suspending while the pipe is full.
    /// Returns the number of bytes written (at least 1 on success).
    pub fn write(self: *Pipe, src: []const u8) WriteError!usize {
        if (src.len == 0) return 0;
        try self.mutex.lock();
        defer self.mutex.unlock();

        while (true) {
            if (self.read_closed or self.write_closed) return error.BrokenPipe;
            if (self.count < self.buffer.len) break;
            try self.writable.wait(&self.mutex);
        }

        const n = @min(src.len, self.buffer.len - self.count);
        const tail = (self.head + self.count) % self.buffer.len;
        const first = @min(n, self.buffer.len - tail);
        @memcpy(self.buffer[tail..][0..first], src[0..first]);
        @memcpy(self.buffer[0 .. n - first], src[first..n]);
        self.count += n;
        self.readable.signal();
        return n;
    }

    pub fn writeAll(self: *Pipe, src: []const u8) WriteError!void {
        var offset: usize = 0;
        while (offset < src.len) {
            offset += try self.write(src[offset..]);
        }
    }

    /// Closes the write end. Pending readers drain buffered bytes, then see EOF.
    pub fn closeWrite(self: *Pipe) void {
        self.mutex.lockUncancelable();
        defer self.mutex.unlock();
        self.write_closed = true;
        self.readable.broadcast();
        self.writable.broadcast();
    }

    /// Closes the read end. Subsequent writes fail with error.BrokenPipe.
    pub fn closeRead(self: *Pipe) void {
        self.mutex.lockUncancelable();
        defer self.mutex.unlock();
        self.read_closed = true;
        self.readable.broadcast();
        self.writable.broadcast();
    }
};

/// `std.Io.Reader` over a Pipe. Mirrors zio's Stream.Reader shape: the
/// concrete error lands in `err`, the interface reports error.ReadFailed.
pub const PipeReader = struct {
    pipe: *Pipe,
    interface: std.Io.Reader,
    err: ?ReadError = null,

    pub fn init(pipe: *Pipe, buffer: []u8) PipeReader {
        return .{
            .pipe = pipe,
            .interface = .{
                .vtable = &.{ .stream = streamImpl },
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    fn streamImpl(io_r: *std.Io.Reader, io_w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *PipeReader = @alignCast(@fieldParentPtr("interface", io_r));
        const dest = limit.slice(try io_w.writableSliceGreedy(1));
        const n = self.pipe.read(dest) catch |err| {
            self.err = err;
            return error.ReadFailed;
        };
        if (n == 0) return error.EndOfStream;
        io_w.advance(n);
        return n;
    }
};

/// `std.Io.Writer` over a Pipe.
pub const PipeWriter = struct {
    pipe: *Pipe,
    interface: std.Io.Writer,
    err: ?WriteError = null,

    pub fn init(pipe: *Pipe, buffer: []u8) PipeWriter {
        return .{
            .pipe = pipe,
            .interface = .{
                .vtable = &.{ .drain = drainImpl },
                .buffer = buffer,
            },
        };
    }

    fn drainImpl(io_w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *PipeWriter = @alignCast(@fieldParentPtr("interface", io_w));
        const buffered = io_w.buffered();
        var total: usize = 0;

        self.pipe.writeAll(buffered) catch |err| {
            self.err = err;
            return error.WriteFailed;
        };
        total += buffered.len;

        if (data.len > 0) {
            for (data[0 .. data.len - 1]) |slice| {
                self.pipe.writeAll(slice) catch |err| {
                    self.err = err;
                    return error.WriteFailed;
                };
                total += slice.len;
            }
            const last = data[data.len - 1];
            for (0..splat) |_| {
                self.pipe.writeAll(last) catch |err| {
                    self.err = err;
                    return error.WriteFailed;
                };
                total += last.len;
            }
        }
        return io_w.consume(total);
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

test "Pipe: write then read round-trips bytes" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var storage: [16]u8 = undefined;
    var pipe = Pipe.init(&storage);

    const Fns = struct {
        fn run(p: *Pipe) !void {
            try p.writeAll("hello");
            var dest: [16]u8 = undefined;
            const n = try p.read(&dest);
            try std.testing.expectEqualStrings("hello", dest[0..n]);
        }
    };
    var handle = try rt.spawn(Fns.run, .{&pipe});
    try handle.join();
}

test "Pipe: blocking write resumes when reader drains; EOF after closeWrite" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var storage: [4]u8 = undefined;
    var pipe = Pipe.init(&storage);

    const Fns = struct {
        fn writer(p: *Pipe) !void {
            // 8 bytes through a 4-byte ring: must suspend until drained.
            try p.writeAll("abcdefgh");
            p.closeWrite();
        }
        fn reader(p: *Pipe, out: *std.ArrayList(u8), gpa: std.mem.Allocator) !void {
            var dest: [3]u8 = undefined;
            while (true) {
                const n = try p.read(&dest);
                if (n == 0) break; // EOF
                try out.appendSlice(gpa, dest[0..n]);
            }
        }
    };

    var out = try std.ArrayList(u8).initCapacity(std.testing.allocator, 8);
    defer out.deinit(std.testing.allocator);

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.writer, .{&pipe});
    try group.spawn(Fns.reader, .{ &pipe, &out, std.testing.allocator });
    try group.wait();

    try std.testing.expectEqualStrings("abcdefgh", out.items);
}

test "Pipe: write after closeRead fails with BrokenPipe" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var storage: [4]u8 = undefined;
    var pipe = Pipe.init(&storage);

    const Fns = struct {
        fn run(p: *Pipe) !void {
            p.closeRead();
            try std.testing.expectError(error.BrokenPipe, p.write("x"));
        }
    };
    var handle = try rt.spawn(Fns.run, .{&pipe});
    try handle.join();
}

test "PipeReader/PipeWriter: std.Io interfaces round-trip with delimiter" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var storage: [64]u8 = undefined;
    var pipe = Pipe.init(&storage);

    const Fns = struct {
        fn producer(p: *Pipe) !void {
            var wbuf: [8]u8 = undefined;
            var w = PipeWriter.init(p, &wbuf);
            try w.interface.print("line-{d}\n", .{42});
            try w.interface.flush();
            p.closeWrite();
        }
        fn consumer(p: *Pipe) !void {
            var rbuf: [32]u8 = undefined;
            var r = PipeReader.init(p, &rbuf);
            const line = try r.interface.takeDelimiterInclusive('\n');
            try std.testing.expectEqualStrings("line-42\n", line);
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Fns.producer, .{&pipe});
    try group.spawn(Fns.consumer, .{&pipe});
    try group.wait();
    try std.testing.expect(!group.hasFailed());
}

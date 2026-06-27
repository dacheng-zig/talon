//! Listener abstraction.
//!
//! Comptime duck-typed contract, validated by StreamServer:
//!   RawConnection: type           — the raw connection produced by accept()
//!   accept(self) !RawConnection   — suspends until a new connection arrives
//!   close(self) void              — stops the listener
//!
//! A raw connection must provide:
//!   Reader / Writer: types with an `interface: std.Io.Reader/Writer` field
//!   reader(self, buf) Reader / writer(self, buf) Writer
//!   close(self) void, shutdown(self) void, remoteInfo(self) RemoteInfo
//!
//! MemoryListener is a first-class citizen, not test scaffolding bolted on
//! later (Kestrel TestServer lesson).

const std = @import("std");
const zio = @import("zio");
const pipe_mod = @import("pipe.zig");

pub const Pipe = pipe_mod.Pipe;
pub const PipeReader = pipe_mod.PipeReader;
pub const PipeWriter = pipe_mod.PipeWriter;

pub const RemoteInfo = union(enum) {
    net: zio.net.Address,
    memory,

    pub fn format(self: RemoteInfo, w: *std.Io.Writer) std.Io.Writer.Error!void {
        switch (self) {
            .net => |addr| try addr.format(w),
            .memory => try w.writeAll("memory"),
        }
    }
};

// ── TCP ──────────────────────────────────────────────────────────────────

pub const TcpConnection = struct {
    stream: zio.net.Stream,

    pub const Reader = zio.net.Stream.Reader;
    pub const Writer = zio.net.Stream.Writer;

    pub fn reader(self: TcpConnection, buffer: []u8) Reader {
        return self.stream.reader(buffer);
    }

    pub fn writer(self: TcpConnection, buffer: []u8) Writer {
        return self.stream.writer(buffer);
    }

    pub fn close(self: TcpConnection) void {
        self.stream.close();
    }

    pub fn shutdown(self: TcpConnection) void {
        self.stream.shutdown(.both) catch {};
    }

    pub fn remoteInfo(self: TcpConnection) RemoteInfo {
        return .{ .net = self.stream.socket.address };
    }
};

pub const TcpListener = struct {
    server: zio.net.Server,

    pub const RawConnection = TcpConnection;

    pub const Options = struct {
        kernel_backlog: u31 = 128,
        reuse_address: bool = true,
    };

    pub fn listen(addr: zio.net.IpAddress, options: Options) !TcpListener {
        const server = try addr.listen(.{
            .kernel_backlog = options.kernel_backlog,
            .reuse_address = options.reuse_address,
        });
        return .{ .server = server };
    }

    pub fn accept(self: *TcpListener) !TcpConnection {
        const stream = try self.server.accept(.{});
        // Latency over throughput by default; response writes are already
        // batched through the buffered Writer (TCP_NODELAY).
        stream.socket.setNoDelay(true) catch {};
        return .{ .stream = stream };
    }

    pub fn close(self: *TcpListener) void {
        self.server.close();
    }
};

// ── Memory ───────────────────────────────────────────────────────────────

pub const MemoryConnection = struct {
    /// Pipe this side reads from (peer writes into it).
    recv: *Pipe,
    /// Pipe this side writes to (peer reads from it).
    send: *Pipe,

    pub const Reader = PipeReader;
    pub const Writer = PipeWriter;

    pub fn reader(self: MemoryConnection, buffer: []u8) Reader {
        return PipeReader.init(self.recv, buffer);
    }

    pub fn writer(self: MemoryConnection, buffer: []u8) Writer {
        return PipeWriter.init(self.send, buffer);
    }

    pub fn close(self: MemoryConnection) void {
        self.recv.closeRead();
        self.send.closeWrite();
    }

    pub fn shutdown(self: MemoryConnection) void {
        self.send.closeWrite();
    }

    pub fn remoteInfo(_: MemoryConnection) RemoteInfo {
        return .memory;
    }
};

/// In-process listener: connect() hands the server side of a fresh pipe pair
/// to accept() through a zio.Channel. Connection memory (pipe rings) lives
/// until deinit(), so test clients may outlive individual connections.
pub const MemoryListener = struct {
    gpa: std.mem.Allocator,
    queue: zio.Channel(MemoryConnection),
    queue_storage: []MemoryConnection,
    pairs: std.ArrayList(*Pair),
    pairs_mutex: zio.Mutex = .init,
    options: Options,

    pub const RawConnection = MemoryConnection;

    pub const Options = struct {
        /// Capacity of each direction's ring buffer.
        pipe_buffer_size: usize = 16 * 1024,
        /// Max connections waiting in accept queue.
        backlog: usize = 16,
    };

    const Pair = struct {
        client_to_server: Pipe,
        server_to_client: Pipe,
    };

    pub fn init(gpa: std.mem.Allocator, options: Options) !MemoryListener {
        const queue_storage = try gpa.alloc(MemoryConnection, options.backlog);
        errdefer gpa.free(queue_storage);
        return .{
            .gpa = gpa,
            .queue = zio.Channel(MemoryConnection).init(queue_storage),
            .queue_storage = queue_storage,
            .pairs = try std.ArrayList(*Pair).initCapacity(gpa, 8),
            .options = options,
        };
    }

    pub fn deinit(self: *MemoryListener) void {
        for (self.pairs.items) |pair| {
            self.gpa.free(pair.client_to_server.buffer);
            self.gpa.free(pair.server_to_client.buffer);
            self.gpa.destroy(pair);
        }
        self.pairs.deinit(self.gpa);
        self.gpa.free(self.queue_storage);
    }

    /// Client side: creates a connection pair, queues the server side for
    /// accept(), returns the client side.
    pub fn connect(self: *MemoryListener) !MemoryConnection {
        const pair = try self.gpa.create(Pair);
        errdefer self.gpa.destroy(pair);
        const c2s_buf = try self.gpa.alloc(u8, self.options.pipe_buffer_size);
        errdefer self.gpa.free(c2s_buf);
        const s2c_buf = try self.gpa.alloc(u8, self.options.pipe_buffer_size);
        errdefer self.gpa.free(s2c_buf);
        pair.* = .{
            .client_to_server = Pipe.init(c2s_buf),
            .server_to_client = Pipe.init(s2c_buf),
        };

        {
            try self.pairs_mutex.lock();
            defer self.pairs_mutex.unlock();
            try self.pairs.append(self.gpa, pair);
        }

        try self.queue.send(.{
            .recv = &pair.client_to_server,
            .send = &pair.server_to_client,
        });
        return .{
            .recv = &pair.server_to_client,
            .send = &pair.client_to_server,
        };
    }

    pub fn accept(self: *MemoryListener) !MemoryConnection {
        return self.queue.receive();
    }

    pub fn close(self: *MemoryListener) void {
        self.queue.close(.immediate);
    }
};

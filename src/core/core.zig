//! talon-core: protocol-agnostic network service engine.
//!
//! Stream engine + shared foundation. The datagram sibling engine
//! (DatagramServer + SessionTable) is not yet implemented, demand-driven; the
//! shared foundation here is already designed for both consumers.

const std = @import("std");

pub const chain = @import("chain.zig").chain;

pub const Limits = @import("limits.zig").Limits;
pub const DataRate = @import("limits.zig").DataRate;

pub const Pipe = @import("pipe.zig").Pipe;
pub const PipeReader = @import("pipe.zig").PipeReader;
pub const PipeWriter = @import("pipe.zig").PipeWriter;

pub const RemoteInfo = @import("listener.zig").RemoteInfo;
pub const TcpListener = @import("listener.zig").TcpListener;
pub const TcpConnection = @import("listener.zig").TcpConnection;
pub const MemoryListener = @import("listener.zig").MemoryListener;
pub const MemoryConnection = @import("listener.zig").MemoryConnection;

pub const Connection = @import("connection.zig").Connection;
pub const StreamServer = @import("stream_server.zig").StreamServer;
pub const StreamServerWith = @import("stream_server.zig").StreamServerWith;

pub const BufferPool = @import("buffer_pool.zig").BufferPool;

pub const framing = @import("framing.zig");
pub const middleware = @import("middleware.zig");

test {
    std.testing.refAllDecls(@This());
    _ = @import("chain.zig");
    _ = @import("pipe.zig");
    _ = @import("listener.zig");
    _ = @import("stream_server.zig");
    _ = @import("buffer_pool.zig");
    _ = @import("framing.zig");
    _ = @import("middleware.zig");
}

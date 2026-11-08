//! talon-core: protocol-agnostic network service engine.
//!
const std = @import("std");

pub const chain = @import("chain.zig").chain;

pub const Limits = @import("limits.zig").Limits;
pub const TimeoutCapabilities = @import("timeout.zig").Capabilities;
pub const TimeoutError = @import("timeout.zig").Error;

pub const Pipe = @import("pipe.zig").Pipe;
pub const PipeReader = @import("pipe.zig").PipeReader;
pub const PipeWriter = @import("pipe.zig").PipeWriter;

pub const RemoteInfo = @import("listener.zig").RemoteInfo;
pub const TcpListener = @import("listener.zig").TcpListener;
pub const TcpConnection = @import("listener.zig").TcpConnection;
pub const MemoryListener = @import("listener.zig").MemoryListener;
pub const MemoryConnection = @import("listener.zig").MemoryConnection;
pub const TcpEndpoint = @import("dial.zig").TcpEndpoint;
pub const TcpDialer = @import("dial.zig").TcpDialer;
pub const MemoryDialer = @import("dial.zig").MemoryDialer;

pub const Connection = @import("connection.zig").Connection;
pub const StreamServer = @import("stream_server.zig").StreamServer;
pub const WrapOptions = @import("stream_server.zig").WrapOptions;
pub const StreamServerWith = @import("stream_server.zig").StreamServerWith;

pub const BufferPool = @import("buffer_pool.zig").BufferPool;
pub const AllocationBudget = @import("allocation_budget.zig");

pub const framing = @import("framing.zig");
pub const middleware = @import("middleware.zig");

test {
    std.testing.refAllDecls(@This());
    _ = @import("chain.zig");
    _ = @import("pipe.zig");
    _ = @import("listener.zig");
    _ = @import("dial.zig");
    _ = @import("stream_server.zig");
    _ = @import("buffer_pool.zig");
    _ = @import("allocation_budget.zig");
    _ = @import("framing.zig");
    _ = @import("middleware.zig");
}

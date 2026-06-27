//! talon: the single public module = protocol-agnostic engine + HTTP package.
//!
//! The top-level surface is the engine (in src/core/, surfaced here as
//! `talon.core` plus flattened aliases for convenience); the HTTP/1.1 protocol
//! layer — `Server`, `Request`/`Response`, and the request/response
//! vocabulary — lives under `talon.http`.

const std = @import("std");

/// The protocol-agnostic engine (src/core/). Custom-protocol authors build on
/// `talon.core.StreamServer` / `talon.core.framing` / … directly.
pub const core = @import("core/core.zig");

pub const Limits = core.Limits;
pub const StreamServer = core.StreamServer;
pub const StreamServerWith = core.StreamServerWith;
pub const TcpListener = core.TcpListener;
pub const MemoryListener = core.MemoryListener;
pub const chain = core.chain;
pub const middleware = core.middleware;

/// The HTTP/1.1 protocol package: `Server(App)`, `Request`/`Response`,
/// and the surrounding request/response types.
pub const http = @import("http/http.zig");

test {
    std.testing.refAllDecls(@This());
    _ = http;
}

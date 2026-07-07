//! talon.http.server: inbound HTTP/1.1 server building blocks.
//!
//! Scope: the connection-loop protocol (`Http1Protocol`) plugged into
//! talon-core's StreamServer, plus the request/response types the handler
//! contract is written against and the streaming upgrades that ride on a live
//! response — Server-Sent Events (`EventStream`) and WebSocket (`ws`). The
//! wire codec these build on is shared with the client and lives in
//! `../codec/`; this layer is the server-direction dual of `../client/`.
//!
//! `http.zig` surfaces these at the package root for ergonomics
//! (`talon.http.Request`, `talon.http.Server(App)`, …); this file is their
//! single internal entry point, the mirror of `client/client.zig`.

pub const Http1Protocol = @import("protocol.zig").Http1Protocol;
pub const Request = @import("request.zig").Request;
pub const Response = @import("response.zig").Response;

/// Server-side Server-Sent Events stream (opened via `Response.startEventStream`).
pub const EventStream = @import("event_stream.zig").EventStream;

/// Server-side WebSocket: `ws.upgrade(req, .{})` → message read/write loop.
pub const ws = @import("ws.zig");

test {
    @import("std").testing.refAllDecls(@This());
    _ = @import("protocol.zig");
    _ = @import("request.zig");
    _ = @import("response.zig");
    _ = @import("event_stream.zig");
    _ = ws;
}

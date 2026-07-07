//! talon.http: HTTP/1.1 package = direction-neutral codec + server + client.
//!
//! In-house single-pass parser, streaming BodyReader, arena per request,
//! vectored write path — std.http fully replaced. Public contract:
//! `Server(App)` with `App.handle(req: *Request, res: *Response)`.
//!
//! Layout mirrors the two directions: `server/` (inbound: Http1Protocol,
//! Request/Response, SSE, WebSocket) and `client/` (outbound), over a shared
//! `codec/`. The HTTP wire codec is carved into `talon.http.codec`
//! (request/response parse + encode + body), consumed by both directions.
//! The shared, direction-neutral
//! vocabulary (Method/Version/Header/Status) is surfaced at this package root
//! for ergonomics — `talon.http.Method` etc. — while its canonical home stays
//! in the codec (defined in `codec/codec.zig`, the contract boundary);
//! machinery types (parser, BodyReader, …) remain reached through
//! `talon.http.codec`.

const std = @import("std");

/// The protocol-agnostic engine this HTTP layer builds on. The engine's own
/// surface lives at talon top-level (talon.StreamServer, talon.TcpListener,
/// …); this package's constructors build on it directly.
const core = @import("../core/core.zig");

/// Direction-neutral HTTP/1.1 wire codec, shared by server and client.
pub const codec = @import("codec/codec.zig");

/// Outbound HTTP/1.1 client: Connector + ClientConnection + Client.
pub const client = @import("client/client.zig");

/// Inbound HTTP/1.1 server: Http1Protocol + Request/Response + SSE/WebSocket.
const server = @import("server/server.zig");

/// Server-side request/response types and protocol.
pub const Http1Protocol = server.Http1Protocol;
pub const Request = server.Request;
pub const Response = server.Response;

/// Server-side Server-Sent Events stream (opened via `Response.startEventStream`).
pub const EventStream = server.EventStream;

/// Server-side WebSocket: `ws.upgrade(req, .{})` → message read/write loop.
pub const ws = server.ws;

/// Shared HTTP vocabulary, surfaced at the package root so callers can write
/// `talon.http.Method` etc. Canonical definitions live in `codec/codec.zig`;
/// these are thin re-exports through the codec boundary.
pub const Method = codec.Method;
pub const Version = codec.Version;
pub const Header = codec.Header;
pub const Status = codec.Status;

/// The default talon entrypoint.
/// App contract: `pub fn handle(self: *App, req: *Request, res: *Response) !void`
pub fn Server(comptime App: type) type {
    return core.StreamServer(Http1Protocol(App), App);
}

/// Server with a connection middleware chain.
pub fn ServerWith(comptime App: type, comptime middlewares: anytype) type {
    return core.StreamServerWith(Http1Protocol(App), App, middlewares);
}

test {
    std.testing.refAllDecls(@This());
    _ = codec;
    _ = client;
    _ = server;
}

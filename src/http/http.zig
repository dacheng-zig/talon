//! talon.http: HTTP/1.1 protocol package = talon.core StreamServer +
//! Http1Protocol (§5.5).
//!
//! M1: in-house single-pass parser, streaming BodyReader, arena per request,
//! vectored write path — std.http fully replaced. Public contract (§2):
//! `Server(App)` with `App.handle(req: *Request, res: *Response)`.

const std = @import("std");

/// The protocol-agnostic engine this HTTP layer builds on. The engine's own
/// surface lives at talon top-level (talon.StreamServer, talon.TcpListener,
/// …); this package's constructors build on it directly.
const core = @import("../core/core.zig");

pub const parser = @import("parser.zig");
pub const Method = parser.Method;
pub const Header = parser.Header;
pub const BodyReader = @import("body.zig").BodyReader;
pub const Status = @import("encode.zig").Status;
pub const Http1Protocol = @import("protocol.zig").Http1Protocol;
pub const Request = @import("request.zig").Request;
pub const Response = @import("response.zig").Response;

/// The default talon entrypoint (§2 public contract item 1).
/// App contract: `pub fn handle(self: *App, req: *Request, res: *Response) !void`
pub fn Server(comptime App: type) type {
    return core.StreamServer(Http1Protocol(App), App);
}

/// Server with a connection middleware chain (§5.3).
pub fn ServerWith(comptime App: type, comptime middlewares: anytype) type {
    return core.StreamServerWith(Http1Protocol(App), App, middlewares);
}

test {
    std.testing.refAllDecls(@This());
    _ = @import("parser.zig");
    _ = @import("body.zig");
    _ = @import("encode.zig");
    _ = @import("protocol.zig");
}

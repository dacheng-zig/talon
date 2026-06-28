//! talon.http.client: outbound HTTP/1.1 client.
//!
//! Scope: keep-alive round-trips over a comptime-injected Connector, with a
//! per-origin connection pool that reuses idle connections (the first
//! performance lever — see `pool.zig`). On top of the pool the Client adds the
//! policy layer: automatic redirect following (cross-origin credential
//! stripping), transparent retry of idempotent requests on a reused connection
//! the peer closed, per-stage + whole-request timeouts, gzip/deflate/zstd
//! decompression, and a request-level middleware chain (bearerAuth, a minimal
//! cookie jar). The moving parts are the reused codec + the
//! Connector/ClientConnection/Pool duals of the server.
//!
//! The Client owns the pool and is designed to be long-lived and shared across
//! requests (the "correct usage = obvious usage" principle: a long-lived Client
//! reuses connections; a fresh Client per request would defeat pooling).
//!
//! Usage:
//!   var c = Client(TcpConnector).init(gpa, .{}, .{});
//!   defer c.deinit();                       // closes all idle pooled connections
//!   var resp = try c.get(.{ .host = "example.com", .port = 80 }, "/");
//!   defer resp.deinit();                    // drains body, pools or closes the conn
//!   const code = resp.status();
//!   _ = try resp.bodyReader().streamRemaining(&sink);

const std = @import("std");
const zio = @import("zio");
const codec = @import("../codec/codec.zig");
const connector_mod = @import("connector.zig");
const conn_mod = @import("connection.zig");
const pool_mod = @import("pool.zig");
const cookies_mod = @import("cookies.zig");
const tls_mod = @import("tls.zig");
const chain_mod = @import("../../core/chain.zig");

pub const CookieJar = cookies_mod.CookieJar;

pub const Origin = connector_mod.Origin;
pub const TcpConnector = connector_mod.TcpConnector;
pub const MemoryConnector = connector_mod.MemoryConnector;

/// TLS outbound transport (std `crypto.tls.Client` over zio sockets). A
/// `TlsConnector` dials `https` (and plaintext `http`) origins; `RootStore`
/// holds the system trust anchors; `Verification` selects the trust policy.
pub const TlsConnector = tls_mod.TlsConnector;
pub const RootStore = tls_mod.RootStore;
pub const Verification = tls_mod.Verification;
pub const Method = codec.Method;
pub const Header = codec.Header;
pub const PoolConfig = pool_mod.Config;
pub const PoolStats = pool_mod.Stats;

/// Request body source: `.none`, in-memory `.bytes`, or a streaming
/// `.reader`(known length)/`.chunked`(unknown length) upload. See
/// `connection.Body`.
pub const Body = conn_mod.Body;

/// Policy default request headers, emitted unless the caller overrides them via
/// `extra_headers` (case-insensitive). `Accept-Encoding` is advertised only
/// when the client can decode the result (`decompress`), so a server's
/// compressed reply is always decodable.
const default_user_agent = "talon-http-client/1.0";
const default_accept = "*/*";
const default_accept_encoding = "gzip, deflate, zstd";

/// Case-insensitive presence check over a caller header list (used to suppress
/// a policy default the caller already set).
fn headerPresent(headers: []const Header, name: []const u8) bool {
    for (headers) |h| {
        if (std.ascii.eqlIgnoreCase(h.name, name)) return true;
    }
    return false;
}

/// Per-stage deadlines, all kernel-level zio timeouts. The defenses against a
/// slow/stalled peer hanging the caller (the requests-had-no-default-timeout
/// lesson — see docs §3). `.none` disables a stage. A whole-request `total`
/// deadline (coroutine-level cancel) is a follow-up.
pub const Timeouts = struct {
    /// DNS + TCP handshake when dialing a fresh connection.
    connect: zio.Timeout = .fromSeconds(10),
    /// Per-read deadline; bounds reading the response head and, since it stays
    /// set, each subsequent streamed-body read.
    read: zio.Timeout = .fromSeconds(30),
    /// Per-write deadline; bounds sending the request head and body.
    write: zio.Timeout = .fromSeconds(30),
    /// Whole-request deadline spanning connect + write + read + streamed body
    /// (and across retries). Caps each stage to the tighter of its own timeout
    /// and the time left, so a peer dribbling bytes just under the per-read
    /// timeout still cannot drag the call out indefinitely. `.none` disables.
    /// Only enforced on transports with timeout support (no-op on memory pipes).
    total: zio.Timeout = .fromSeconds(60),
};

/// Resolves the whole-request deadline to an absolute timestamp (once, at
/// request start), or null when disabled.
fn totalDeadline(total: zio.Timeout) ?zio.Timestamp {
    return switch (total.toDeadline()) {
        .none => null,
        .deadline => |ts| ts,
        .duration => unreachable, // toDeadline never yields a duration
    };
}

/// Caps a per-stage timeout to the whole-request deadline: returns whichever of
/// the stage timeout (resolved to an absolute deadline now) and `total_deadline`
/// comes first. With no total, the stage timeout passes through unchanged.
fn cap(stage: zio.Timeout, total_deadline: ?zio.Timestamp) zio.Timeout {
    const td = total_deadline orelse return stage;
    return switch (stage.toDeadline()) {
        .none => .{ .deadline = td },
        .deadline => |sd| .{ .deadline = if (sd.toNanoseconds() < td.toNanoseconds()) sd else td },
        .duration => unreachable,
    };
}

fn deadlinePassed(total_deadline: ?zio.Timestamp) bool {
    const td = total_deadline orelse return false;
    return zio.Timestamp.now(.monotonic).toNanoseconds() >= td.toNanoseconds();
}

/// Transparent-retry policy. A pooled keep-alive connection the server closed
/// while idle is the common, expected failure (OkHttp/Go/reqwest all retry it):
/// the caller received nothing, so re-running an idempotent request on a fresh
/// connection is safe. Only *reused* connections and *idempotent* methods
/// qualify — a fresh-dial failure or a non-idempotent method is a real error.
pub const RetryConfig = struct {
    /// Max retries after a reused connection fails mid round-trip. 0 disables.
    max_retries: u8 = 1,
};

/// RFC 9110 §9.2.2 idempotent methods: re-execution has the same effect, so a
/// retry that the server may or may not have already seen is safe.
fn isIdempotent(method: Method) bool {
    return switch (method) {
        .GET, .HEAD, .PUT, .DELETE, .OPTIONS, .TRACE => true,
        .POST, .PATCH, .CONNECT, .other => false,
    };
}

const Scheme = Origin.Scheme;

// ── Built-in request middleware ─────────────────────────────────────────────

/// A request middleware that adds `Authorization: Bearer <token>` to every
/// outbound request. Use in a chain: `ClientWith(C, .{ bearerAuth("xyz") })`.
/// The token is comptime so the header value is a stable static string. On a
/// cross-origin redirect the credential is stripped by the redirect policy
/// (same as a caller-supplied Authorization) unless `unrestricted_auth`.
pub fn bearerAuth(comptime token: []const u8) type {
    return struct {
        pub fn run(ctx: anytype, next: anytype) anyerror!void {
            if (!ctx.addHeader(.{ .name = "authorization", .value = "Bearer " ++ token }))
                return error.TooManyHeaders;
            try next.call(ctx);
        }
    };
}

/// Request middleware: a minimal cookie jar. Before the call it attaches the
/// stored `Cookie` header for the request host; after it stores any
/// `Set-Cookie` from the response. No-op unless `Options.cookie_jar` is set.
/// Use in a chain: `ClientWith(C, .{ cookies })`.
///
/// The attached Cookie header rides `ctx.spec.extra_headers`, so a cross-origin
/// redirect strips it (docs §15.4) — host-keyed storage plus that stripping
/// keep cookies from leaking across origins. Set-Cookie is attributed to the
/// request's origin host (the redirect-hop attribution nuance is out of scope
/// for this minimal jar).
pub const cookies = struct {
    pub fn run(ctx: anytype, next: anytype) anyerror!void {
        const jar = ctx.client.cookie_jar orelse return next.call(ctx);
        // The header value lives in this stack frame, which stays alive across
        // next.call — the request is encoded within that dynamic extent.
        var buf: [4096]u8 = undefined;
        const is_https = ctx.spec.origin.scheme == .https;
        if (jar.cookieHeader(ctx.spec.origin.host, is_https, &buf)) |cookie_value| {
            if (!ctx.addHeader(.{ .name = "cookie", .value = cookie_value }))
                return error.TooManyHeaders;
        }
        try next.call(ctx);
        if (ctx.response) |resp| {
            // Attribute Set-Cookie to the origin that actually produced the
            // response — the final hop after any redirect, not the initial
            // request origin. Otherwise a cross-origin redirect to B would file
            // B's cookies under host A, breaking host isolation. The connection
            // is still live here (the caller has not yet deinit'd it), so its
            // origin_key is valid.
            const store_host = hostFromOriginKey(resp.conn.origin_key) orelse ctx.spec.origin.host;
            for (resp.conn.head.headers) |h| {
                if (std.ascii.eqlIgnoreCase(h.name, "set-cookie"))
                    jar.store(store_host, h.value);
            }
        }
    }
};

/// Automatic redirect-following policy (a Client built-in, OkHttp's "network"
/// layer — runs inside any future request-level middleware).
pub const RedirectConfig = struct {
    /// Max redirects followed before giving up; the redirect response is then
    /// returned as-is. 0 disables following entirely.
    max: u8 = 10,
    /// Keep `Authorization`/`Cookie` when redirected to a *different* origin.
    /// Off by default — stripping them is the safe default (libcurl lesson):
    /// credentials for host A must not leak to host B.
    unrestricted_auth: bool = false,
};

/// Redirect status codes that carry a `Location` to follow (RFC 9110 §15.4).
fn isRedirectStatus(status: u16) bool {
    return status == 301 or status == 302 or status == 303 or status == 307 or status == 308;
}

/// Whether the redirect rewrites the method to GET and drops the body: 303
/// always, and 301/302 for POST (the de-facto browser/curl behavior). 307/308
/// preserve method and body.
fn redirectRewritesToGet(status: u16, method: Method) bool {
    return status == 303 or ((status == 301 or status == 302) and method == .POST);
}

/// Credentials/identity headers stripped on a cross-origin redirect.
const sensitive_headers = [_][]const u8{ "authorization", "cookie", "proxy-authorization" };

fn isSensitiveHeader(name: []const u8) bool {
    for (sensitive_headers) |h| {
        if (std.ascii.eqlIgnoreCase(name, h)) return true;
    }
    return false;
}

/// Allocates a copy of `headers` with the sensitive ones removed (the Header
/// structs still borrow the caller's name/value slices).
fn stripSensitive(gpa: std.mem.Allocator, headers: []const Header) ![]Header {
    var keep: usize = 0;
    for (headers) |h| {
        if (!isSensitiveHeader(h.name)) keep += 1;
    }
    const out = try gpa.alloc(Header, keep);
    var i: usize = 0;
    for (headers) |h| {
        if (!isSensitiveHeader(h.name)) {
            out[i] = h;
            i += 1;
        }
    }
    return out;
}

fn startsWithIgnoreCase(s: []const u8, prefix: []const u8) bool {
    return s.len >= prefix.len and std.ascii.eqlIgnoreCase(s[0..prefix.len], prefix);
}

fn sameOrigin(a: Origin, b: Origin) bool {
    return a.scheme == b.scheme and a.port == b.port and std.ascii.eqlIgnoreCase(a.host, b.host);
}

/// Recovers the host from a pool origin key ("scheme://host:port"); null if the
/// key is empty/unpoolable. Used to attribute `Set-Cookie` to the connection's
/// actual (post-redirect) origin rather than the initial request origin.
fn hostFromOriginKey(key: []const u8) ?[]const u8 {
    const after = if (std.mem.indexOf(u8, key, "://")) |i| key[i + 3 ..] else return null;
    const colon = std.mem.lastIndexOfScalar(u8, after, ':') orelse return null;
    return if (colon == 0) null else after[0..colon];
}

const AbsoluteUrl = struct { scheme: Scheme, host: []const u8, port: u16, target: []const u8 };

/// Parses an absolute `http(s)://authority/target` URL; null if `loc` is not an
/// absolute http(s) URL (then it is treated as a relative reference).
fn parseAbsoluteUrl(loc: []const u8) ?AbsoluteUrl {
    const scheme: Scheme, const rest: []const u8 = if (startsWithIgnoreCase(loc, "https://"))
        .{ .https, loc[8..] }
    else if (startsWithIgnoreCase(loc, "http://"))
        .{ .http, loc[7..] }
    else
        return null;

    // Authority runs up to the first '/' or '?'.
    var auth_end: usize = rest.len;
    for (rest, 0..) |c, i| {
        if (c == '/' or c == '?') {
            auth_end = i;
            break;
        }
    }
    const authority = rest[0..auth_end];
    if (authority.len == 0) return null;
    const target = rest[auth_end..];

    var host: []const u8 = authority;
    var port: u16 = Origin.defaultPort(scheme);
    if (authority[0] == '[') {
        // Bracketed IPv6 literal; store the bare address (no brackets).
        const close = std.mem.indexOfScalar(u8, authority, ']') orelse return null;
        host = authority[1..close];
        const after = authority[close + 1 ..];
        if (after.len != 0) {
            if (after[0] != ':') return null;
            port = std.fmt.parseInt(u16, after[1..], 10) catch return null;
        }
    } else if (std.mem.lastIndexOfScalar(u8, authority, ':')) |colon| {
        host = authority[0..colon];
        port = std.fmt.parseInt(u16, authority[colon + 1 ..], 10) catch return null;
    }
    if (host.len == 0) return null;
    return .{ .scheme = scheme, .host = host, .port = port, .target = target };
}

/// A resolved redirect target. `origin.host` and `target` borrow `buf`, which
/// the caller owns and frees.
const ResolvedLocation = struct { origin: Origin, target: []const u8, buf: []u8 };

/// Resolves a `Location` value against the current origin+target into an owned
/// target. Supports absolute URLs, absolute paths ("/x"), and basic relative
/// references (merged against the current path's directory). Fragments are
/// dropped. Returns error.BadLocation for inputs it cannot resolve.
fn resolveLocation(
    gpa: std.mem.Allocator,
    base: Origin,
    base_target: []const u8,
    location_in: []const u8,
) !ResolvedLocation {
    const location = blk: {
        if (std.mem.indexOfScalar(u8, location_in, '#')) |h| break :blk location_in[0..h];
        break :blk location_in;
    };
    if (location.len == 0) return error.BadLocation;

    var tbuf: [2048]u8 = undefined;
    var scheme = base.scheme;
    var host = base.host;
    var port = base.port;
    var target: []const u8 = undefined;

    if (parseAbsoluteUrl(location)) |abs| {
        scheme = abs.scheme;
        host = abs.host;
        port = abs.port;
        target = if (abs.target.len == 0)
            "/"
        else if (abs.target[0] == '?') blk: { // authority then query, no path
            if (1 + abs.target.len > tbuf.len) return error.BadLocation;
            tbuf[0] = '/';
            @memcpy(tbuf[1..][0..abs.target.len], abs.target);
            break :blk tbuf[0 .. 1 + abs.target.len];
        } else abs.target;
    } else if (location[0] == '/') {
        target = location; // same origin, origin-form path
    } else {
        // Relative reference: merge against the base path's directory.
        const q = std.mem.indexOfScalar(u8, base_target, '?') orelse base_target.len;
        const slash = std.mem.lastIndexOfScalar(u8, base_target[0..q], '/') orelse
            return error.BadLocation;
        const dir = base_target[0 .. slash + 1];
        if (dir.len + location.len > tbuf.len) return error.BadLocation;
        @memcpy(tbuf[0..dir.len], dir);
        @memcpy(tbuf[dir.len..][0..location.len], location);
        target = tbuf[0 .. dir.len + location.len];
    }

    const buf = try gpa.alloc(u8, host.len + target.len);
    @memcpy(buf[0..host.len], host);
    @memcpy(buf[host.len..], target);
    return .{
        .origin = .{ .scheme = scheme, .host = buf[0..host.len], .port = port },
        .target = buf[host.len..],
        .buf = buf,
    };
}

/// Default talon HTTP client over TCP (default specialization, no middleware).
pub const TcpClient = Client(TcpConnector);

/// TLS-capable client: dials `https` over TLS and `http` plaintext through one
/// pool. Opt-in (the default `TcpClient` stays zero-overhead plaintext); build
/// one with a `TlsConnector` carrying a loaded `RootStore`:
///   var store: client.RootStore = .{};
///   try store.load(gpa, io);
///   defer store.deinit(gpa);
///   var c = client.TlsClient.init(gpa, .{ .gpa = gpa, .io = io,
///       .verification = .{ .system = &store } }, .{});
pub const TlsClient = Client(TlsConnector);

/// Client with no request-level middleware. Most callers use this; reach for
/// `ClientWith` to attach a middleware chain (the dual of `Server`/`ServerWith`).
pub fn Client(comptime Connector: type) type {
    return ClientWith(Connector, .{});
}

/// Client with a request-level middleware chain. `mws` is a tuple of
/// middlewares (the same `talon.core.chain` mechanism the server uses for
/// connection middleware), each `fn (ctx: *RequestCtx, next: anytype) !void`
/// or a struct with such a `run`. They wrap the whole logical call — redirects
/// and retries included — running once per call (OkHttp's "application"
/// interceptor layer): code before `next.call(ctx)` sees/mutates the outbound
/// request (`ctx.spec`), code after sees the final `ctx.response`. Ordering
/// constraints (`requires`/`provides`) are checked at comptime.
pub fn ClientWith(comptime Connector: type, comptime mws: anytype) type {
    comptime {
        if (!@hasDecl(Connector, "RawConnection"))
            @compileError("talon.http.client.Client: Connector '" ++ @typeName(Connector) ++
                "' must declare `pub const RawConnection`");
        if (!std.meta.hasFn(Connector, "connect"))
            @compileError("talon.http.client.Client: Connector '" ++ @typeName(Connector) ++
                "' must declare `pub fn connect(self, origin: Origin) !RawConnection`");
    }

    const Raw = Connector.RawConnection;
    const Conn = conn_mod.ClientConnection(Raw);
    const Pool = pool_mod.Pool(Connector);

    return struct {
        gpa: std.mem.Allocator,
        pool: Pool,
        /// Caps the response body a peer may stream into this client (DoS
        /// guard against a malicious/compromised server, incl. close-delimited
        /// bodies). null disables the cap. Settable after init.
        max_response_body: ?u64,
        /// Per-stage deadlines applied to every round-trip. Settable after init.
        timeouts: Timeouts,
        /// Transparent-retry policy for failed reused connections.
        retry: RetryConfig,
        /// Automatic redirect-following policy. Settable after init.
        redirect: RedirectConfig,
        /// Transparently decode `Content-Encoding: gzip`/`deflate`/`zstd`
        /// response bodies. Settable after init.
        decompress: bool,
        /// Optional caller-owned cookie jar used by the `cookies` middleware.
        /// null = no cookie handling. The Client only borrows it.
        cookie_jar: ?*CookieJar,

        const Self = @This();
        const default_max_response_body: u64 = 16 * 1024 * 1024;

        pub const Options = struct {
            /// null disables the cap.
            max_response_body: ?u64 = default_max_response_body,
            pool: PoolConfig = .{},
            timeouts: Timeouts = .{},
            retry: RetryConfig = .{},
            redirect: RedirectConfig = .{},
            /// Transparently decode gzip/deflate/zstd response bodies.
            decompress: bool = true,
            /// Caller-owned cookie jar for the `cookies` middleware (null = off).
            cookie_jar: ?*CookieJar = null,
        };

        /// A received response. Borrows a live connection until `deinit`; header
        /// and reason slices are valid until then. Body is streamed lazily.
        pub const Response = struct {
            conn: *Conn,
            pool: *Pool,

            pub fn status(self: Response) u16 {
                return self.conn.head.status;
            }

            pub fn reason(self: Response) []const u8 {
                return self.conn.head.reason;
            }

            /// Case-insensitive header lookup; slice valid until `deinit`.
            pub fn header(self: Response, name: []const u8) ?[]const u8 {
                return self.conn.head.header(name);
            }

            /// Streaming response body (`std.Io.Reader`). When the client
            /// decoded a gzip/deflate/zstd `Content-Encoding`, this is the
            /// decoded stream; otherwise the raw body. For untrusted compressed bodies,
            /// bound the read (the decoder can amplify — see `readResponse`).
            pub fn bodyReader(self: Response) *std.Io.Reader {
                return self.conn.bodyReader();
            }

            /// Reads the full (decoded) body into a caller-owned buffer, bounded
            /// by `max_decoded` bytes; returns `error.ResponseTooLarge` if the
            /// decoded body exceeds it. The bound is mandatory because the
            /// client's `max_response_body` only caps the *compressed* input —
            /// a `Content-Encoding` decoder can amplify a small body into a huge
            /// one (a decompression bomb), so the *decoded* output needs its own
            /// cap. Caller frees the returned slice.
            pub fn readAllAlloc(self: Response, gpa: std.mem.Allocator, max_decoded: usize) ![]u8 {
                // Stream the whole (decoded) body, then reject if it exceeds the
                // cap. `streamRemaining` (unlimited) is used deliberately: a
                // finite Limit must NOT be passed to the BodyReader — when its
                // budget hits 0 mid-body its `stream` returns 0 (not EndOfStream)
                // and any read-to-end loop spins forever (std `allocRemaining`
                // has the same hazard against this reader). The decoded output
                // is bounded by available memory during the read (an extreme
                // decompression bomb surfaces as `error.OutOfMemory`); the
                // `max_decoded` check then rejects anything that fit in memory
                // but is still over the policy cap. The compressed *input* is
                // already capped upstream by `max_response_body`.
                var aw: std.Io.Writer.Allocating = .init(gpa);
                errdefer aw.deinit();
                _ = self.bodyReader().streamRemaining(&aw.writer) catch |err| switch (err) {
                    error.ReadFailed => return error.ReadFailed,
                    error.WriteFailed => return error.OutOfMemory,
                };
                if (aw.written().len > max_decoded) return error.ResponseTooLarge;
                return aw.toOwnedSlice();
            }

            /// Reads and parses the (decoded) body as JSON into `T` — the
            /// comptime-typed deserialization of docs §7. Returns a
            /// `std.json.Parsed(T)`; the caller owns it (`defer parsed.deinit()`)
            /// and reads `parsed.value`.
            ///
            /// `max_decoded` bounds the decoded body (see `readAllAlloc`): the
            /// client's `max_response_body` only caps the compressed input, so
            /// this cap is what protects against a decompression bomb from an
            /// untrusted server. Returns `error.ResponseTooLarge` past the cap.
            pub fn json(
                self: Response,
                comptime T: type,
                gpa: std.mem.Allocator,
                max_decoded: usize,
            ) !std.json.Parsed(T) {
                var aw: std.Io.Writer.Allocating = .init(gpa);
                defer aw.deinit();
                _ = self.bodyReader().streamRemaining(&aw.writer) catch |err| switch (err) {
                    error.ReadFailed => return error.ReadFailed,
                    error.WriteFailed => return error.OutOfMemory,
                };
                if (aw.written().len > max_decoded) return error.ResponseTooLarge;
                return std.json.parseFromSlice(T, gpa, aw.written(), .{});
            }

            /// Drains any unread body (so the transport ends cleanly) and hands
            /// the connection back to the pool — returned for reuse if the
            /// response was keep-alive and the body framed cleanly, closed
            /// otherwise.
            pub fn deinit(self: Response) void {
                self.conn.body.discard() catch {};
                self.pool.checkin(self.conn, self.conn.reusableAfterDrain());
            }
        };

        pub const RequestSpec = struct {
            method: Method = .GET,
            origin: Origin,
            /// origin-form request target, e.g. "/path?query".
            target: []const u8 = "/",
            extra_headers: []const Header = &.{},
            /// Request body: in-memory `.bytes` or a streaming `.reader`/
            /// `.chunked` upload (large files without buffering). Default none.
            body: Body = .none,
            /// Keep the connection alive for pooling (default). Set false to
            /// send `connection: close`; that response is then never pooled.
            keep_alive: bool = true,
        };

        pub fn init(gpa: std.mem.Allocator, connector: Connector, options: Options) Self {
            return .{
                .gpa = gpa,
                .pool = Pool.init(gpa, connector, options.pool),
                .max_response_body = options.max_response_body,
                .timeouts = options.timeouts,
                .retry = options.retry,
                .redirect = options.redirect,
                .decompress = options.decompress,
                .cookie_jar = options.cookie_jar,
            };
        }

        pub fn deinit(self: *Self) void {
            self.pool.deinit();
        }

        /// Live pool counters (created / reused / evicted).
        pub fn poolStats(self: *const Self) PoolStats {
            return self.pool.stats();
        }

        /// Closes idle pooled connections past their lifetime/idle deadline now,
        /// returning the count reaped. For one-off reclamation; for continuous
        /// reclamation spawn `reapLoop` into a Group.
        pub fn reapIdle(self: *Self) usize {
            return self.pool.reapExpired();
        }

        /// Runs `reapIdle` every `interval` until canceled — the proactive idle
        /// reaper (dual of the server's heartbeat). Spawn it into the same
        /// `zio.Group` that owns the Client; canceling the group ends the loop:
        ///   try group.spawn(Client.reapLoop, .{ &client, .fromSeconds(30) });
        pub fn reapLoop(self: *Self, interval: zio.Duration) void {
            while (true) {
                zio.sleep(interval) catch return; // canceled → stop
                _ = self.pool.reapExpired();
            }
        }

        /// Mutable context threaded through the request middleware chain. A
        /// middleware mutates `spec` before `next` (outbound) and reads
        /// `response` after `next` (inbound); the terminal fills `response`.
        pub const RequestCtx = struct {
            client: *Self,
            spec: RequestSpec,
            response: ?Response = null,
            /// Headers appended by middleware via `addHeader`, merged with
            /// `spec.extra_headers` by the terminal. Cross-origin redirects
            /// strip the sensitive ones uniformly, so a `bearerAuth`-added
            /// Authorization is dropped on a foreign hop just like a caller's.
            added: [max_added_headers]Header = undefined,
            added_n: usize = 0,

            /// Appends an outbound header (name/value must outlive the call).
            /// Returns false if the per-request header budget is exhausted —
            /// callers should surface that rather than silently drop a header
            /// (dropping, say, Authorization would be a security footgun).
            pub fn addHeader(self: *RequestCtx, h: Header) bool {
                if (self.added_n >= self.added.len) return false;
                self.added[self.added_n] = h;
                self.added_n += 1;
                return true;
            }
        };

        const max_added_headers = 8;
        const Chain = chain_mod.chain(RequestCtx, mws);

        /// Performs a logical request: runs the request-level middleware chain
        /// around the mechanism (redirect following + connection retry). The
        /// returned Response borrows a live connection; the caller must
        /// `resp.deinit()` to return or close it.
        pub fn request(self: *Self, spec: RequestSpec) !Response {
            var ctx: RequestCtx = .{ .client = self, .spec = spec };
            try Chain.run(&ctx, terminal);
            // The terminal always sets response on success; a middleware that
            // short-circuits the chain must set it too (else this fires).
            return ctx.response orelse error.NoResponse;
        }

        /// Chain terminal: the actual network call (redirects + retries). Merges
        /// any middleware-added headers into the outbound spec first.
        fn terminal(ctx: *RequestCtx) anyerror!void {
            if (ctx.added_n == 0) {
                ctx.response = try ctx.client.sendFollowingRedirects(ctx.spec);
                return;
            }
            const base = ctx.spec.extra_headers;
            const merged = try ctx.client.gpa.alloc(Header, base.len + ctx.added_n);
            defer ctx.client.gpa.free(merged);
            @memcpy(merged[0..base.len], base);
            @memcpy(merged[base.len..], ctx.added[0..ctx.added_n]);
            var spec = ctx.spec;
            spec.extra_headers = merged;
            ctx.response = try ctx.client.sendFollowingRedirects(spec);
        }

        /// One round-trip plus automatic redirect following (per `redirect`)
        /// and connection retry (per `retry`).
        ///
        /// Redirects are followed by re-issuing to the `Location` target, with
        /// method rewriting (303 / POST-on-301-302 → GET) and, on a cross-origin
        /// hop, stripping of `Authorization`/`Cookie` unless `unrestricted_auth`.
        /// On reaching `redirect.max`, or a redirect with no/invalid `Location`,
        /// the redirect response itself is returned.
        fn sendFollowingRedirects(self: *Self, spec: RequestSpec) !Response {
            // One absolute deadline for the whole logical request, resolved once
            // and shared across every redirect hop and retry — so `total` bounds
            // the entire call, not each hop independently (else N redirects could
            // each consume the full budget).
            const deadline = totalDeadline(self.timeouts.total);
            var cur = spec;
            var redirects: u8 = 0;
            // Owns strings derived from each hop's Location (host+target); freed
            // when superseded by the next hop or at return.
            var loc_buf: ?[]u8 = null;
            defer if (loc_buf) |b| self.gpa.free(b);
            // Owns the cross-origin-filtered header slice, if any.
            var hdr_buf: ?[]Header = null;
            defer if (hdr_buf) |h| self.gpa.free(h);

            while (true) {
                const resp = try self.send(cur, deadline);
                const status = resp.status();
                if (redirects >= self.redirect.max or !isRedirectStatus(status)) return resp;

                const location = resp.header("location") orelse return resp;
                // A redirect that preserves method+body (307/308, or 301/302 on
                // a non-POST) would require re-sending the body. A streaming
                // upload was already consumed by the send above and cannot be
                // replayed, so hand the redirect back rather than send an empty
                // or corrupt body. POST→GET rewrites drop the body, so they are
                // always safe.
                if (!redirectRewritesToGet(status, cur.method) and !cur.body.replayable())
                    return resp;

                const resolved = resolveLocation(self.gpa, cur.origin, cur.target, location) catch
                    return resp; // unresolvable Location: hand back the redirect as-is
                resp.deinit(); // drains + pools/closes the connection; `location` now dead

                if (loc_buf) |b| self.gpa.free(b);
                loc_buf = resolved.buf;

                const cross = !sameOrigin(cur.origin, resolved.origin);
                cur.origin = resolved.origin;
                cur.target = resolved.target;
                if (redirectRewritesToGet(status, cur.method)) {
                    cur.method = .GET;
                    cur.body = .none;
                }
                if (cross and !self.redirect.unrestricted_auth) {
                    if (hdr_buf) |h| self.gpa.free(h);
                    hdr_buf = try stripSensitive(self.gpa, spec.extra_headers);
                    cur.extra_headers = hdr_buf.?;
                }
                redirects += 1;
            }
        }

        /// One logical send: a round-trip over a pooled or freshly dialed
        /// connection, with transparent retry (per `retry` policy) when a
        /// *reused* connection fails mid round-trip and the method is
        /// idempotent — that failure is almost always a server-side idle close,
        /// and no response reached the caller, so a fresh attempt is safe.
        fn send(self: *Self, spec: RequestSpec, deadline: ?zio.Timestamp) !Response {
            // Retry needs a re-sendable request: an idempotent method AND a
            // replayable body. A streaming upload is consumed on the first
            // attempt, so even an idempotent PUT with a stream body must not
            // retry (it would send an empty/short body the second time).
            const retryable = isIdempotent(spec.method) and spec.body.replayable();
            var attempt: u8 = 0;
            while (true) {
                const co = try self.pool.checkout(spec.origin, cap(self.timeouts.connect, deadline));
                // Set true by roundtrip once the first response byte arrives.
                // Past that point the server may have acted on the request, so
                // the failure must not be replayed even on an idempotent method.
                var received_response = false;
                if (self.roundtrip(co.conn, spec, deadline, &received_response)) |resp| {
                    return resp;
                } else |err| {
                    // The round-trip failed before any Response escaped, so the
                    // connection is unusable: close it rather than pool it.
                    self.pool.checkin(co.conn, false);
                    // Retry only when the request was provably not processed: no
                    // response byte received yet (idle keep-alive close, or a
                    // write that never reached the server), on a reused
                    // connection, idempotent+replayable, with budget left. A
                    // mid-response failure (received_response) is NOT retried —
                    // re-running it could double a side effect the server
                    // already applied (e.g. a DELETE).
                    if (co.reused and retryable and !received_response and
                        attempt < self.retry.max_retries and !deadlinePassed(deadline))
                    {
                        attempt += 1;
                        continue;
                    }
                    return err;
                }
            }
        }

        /// One attempt over an already-checked-out connection. On any error the
        /// connection is left for the caller to close (no checkin here).
        fn roundtrip(self: *Self, conn: *Conn, spec: RequestSpec, deadline: ?zio.Timestamp, received_response: *bool) !Response {
            // Host header: omit the port when it is the scheme default.
            var host_buf: [320]u8 = undefined;
            const host_val = if (spec.origin.port == Origin.defaultPort(spec.origin.scheme))
                spec.origin.host
            else
                try std.fmt.bufPrint(&host_buf, "{s}:{d}", .{ spec.origin.host, spec.origin.port });

            // Framing follows the body shape: known-length bodies (bytes /
            // reader) get Content-Length; an unknown-length stream gets chunked.
            const content_length: ?u64, const chunked: bool = switch (spec.body) {
                .none => .{ null, false },
                .bytes => |b| .{ b.len, false },
                .reader => |src| .{ src.len, false },
                .chunked => .{ null, true },
            };

            conn.setWriteTimeout(cap(self.timeouts.write, deadline));
            try conn.sendRequest(.{
                .method = spec.method,
                .target = spec.target,
                .host = host_val,
                .extra_headers = spec.extra_headers,
                .content_length = content_length,
                .chunked = chunked,
                .keep_alive = spec.keep_alive,
                // Policy defaults, suppressed per-header when the caller (or a
                // middleware, already merged into extra_headers) set their own.
                .user_agent = if (headerPresent(spec.extra_headers, "user-agent")) null else default_user_agent,
                .accept = if (headerPresent(spec.extra_headers, "accept")) null else default_accept,
                .accept_encoding = if (self.decompress and !headerPresent(spec.extra_headers, "accept-encoding"))
                    default_accept_encoding
                else
                    null,
            }, spec.body);

            // The read deadline stays set so the lazily streamed body inherits it
            // — and, being capped to the total deadline, the body read is bounded
            // by the whole-request budget too.
            conn.setReadTimeout(cap(self.timeouts.read, deadline));
            // Wait for the first response byte before committing to the parse.
            // A failure here (or above, in the write) means no response was
            // received, so an idempotent request was not processed and may be
            // safely retried. Once a byte arrives we flip `received_response`,
            // which blocks any retry of a mid-response failure (the server may
            // have already acted on the request).
            try conn.reader().fill(1);
            received_response.* = true;
            try conn.readResponse(self.max_response_body, self.decompress);
            return .{ .conn = conn, .pool = &self.pool };
        }

        // ── Convenience methods (origin + target) ──────────────────────────

        pub fn get(self: *Self, origin: Origin, target: []const u8) !Response {
            return self.request(.{ .method = .GET, .origin = origin, .target = target });
        }

        pub fn head(self: *Self, origin: Origin, target: []const u8) !Response {
            return self.request(.{ .method = .HEAD, .origin = origin, .target = target });
        }

        pub fn post(self: *Self, origin: Origin, target: []const u8, body: []const u8) !Response {
            return self.request(.{ .method = .POST, .origin = origin, .target = target, .body = .{ .bytes = body } });
        }

        // ── Convenience methods (full URL string) ──────────────────────────

        pub const UrlRequestSpec = struct {
            method: Method = .GET,
            /// Absolute http(s) URL, e.g. "http://host:8080/path?q=1".
            url: []const u8,
            extra_headers: []const Header = &.{},
            body: Body = .none,
            keep_alive: bool = true,
        };

        /// Like `request`, but parses an absolute URL string into origin+target.
        /// Returns `error.InvalidUrl` for a non-absolute/malformed URL.
        pub fn requestUrl(self: *Self, spec: UrlRequestSpec) !Response {
            const abs = parseAbsoluteUrl(spec.url) orelse return error.InvalidUrl;
            const origin: Origin = .{ .scheme = abs.scheme, .host = abs.host, .port = abs.port };
            const base: RequestSpec = .{
                .method = spec.method,
                .origin = origin,
                .extra_headers = spec.extra_headers,
                .body = spec.body,
                .keep_alive = spec.keep_alive,
            };
            // parseAbsoluteUrl leaves target starting with '/', '?', or empty.
            if (abs.target.len == 0) {
                return self.request(withTarget(base, "/"));
            }
            if (abs.target[0] == '/') {
                return self.request(withTarget(base, abs.target));
            }
            // "?query" with no path: origin-form needs a leading '/'. Build it
            // (lives across the whole request via defer).
            const t = try self.gpa.alloc(u8, abs.target.len + 1);
            defer self.gpa.free(t);
            t[0] = '/';
            @memcpy(t[1..], abs.target);
            return self.request(withTarget(base, t));
        }

        fn withTarget(base: RequestSpec, target: []const u8) RequestSpec {
            var s = base;
            s.target = target;
            return s;
        }

        pub fn getUrl(self: *Self, url: []const u8) !Response {
            return self.requestUrl(.{ .method = .GET, .url = url });
        }

        pub fn headUrl(self: *Self, url: []const u8) !Response {
            return self.requestUrl(.{ .method = .HEAD, .url = url });
        }

        pub fn postUrl(self: *Self, url: []const u8, body: []const u8) !Response {
            return self.requestUrl(.{ .method = .POST, .url = url, .body = .{ .bytes = body } });
        }
    };
}

test {
    std.testing.refAllDecls(@This());
    _ = connector_mod;
    _ = conn_mod;
    _ = pool_mod;
    _ = cookies_mod;
    _ = tls_mod;
}

// ── Redirect-resolution unit tests (pure; no network) ───────────────────────

test "redirect: status + method-rewrite predicates" {
    try std.testing.expect(isRedirectStatus(301) and isRedirectStatus(308));
    try std.testing.expect(!isRedirectStatus(200) and !isRedirectStatus(304));
    // 303 always → GET; 301/302 only for POST; 307/308 never.
    try std.testing.expect(redirectRewritesToGet(303, .GET));
    try std.testing.expect(redirectRewritesToGet(302, .POST));
    try std.testing.expect(!redirectRewritesToGet(302, .GET));
    try std.testing.expect(!redirectRewritesToGet(307, .POST));
}

test "parseAbsoluteUrl: scheme/host/port/target" {
    const a = parseAbsoluteUrl("http://example.com/p?q=1").?;
    try std.testing.expectEqual(Scheme.http, a.scheme);
    try std.testing.expectEqualStrings("example.com", a.host);
    try std.testing.expectEqual(@as(u16, 80), a.port);
    try std.testing.expectEqualStrings("/p?q=1", a.target);

    const b = parseAbsoluteUrl("https://h:8443/").?;
    try std.testing.expectEqual(Scheme.https, b.scheme);
    try std.testing.expectEqual(@as(u16, 8443), b.port);

    try std.testing.expect(parseAbsoluteUrl("/just/a/path") == null);
}

test "resolveLocation: absolute URL is cross-origin" {
    const base: Origin = .{ .host = "a.test", .port = 80 };
    const r = try resolveLocation(std.testing.allocator, base, "/start", "http://b.test/next");
    defer std.testing.allocator.free(r.buf);
    try std.testing.expectEqualStrings("b.test", r.origin.host);
    try std.testing.expectEqualStrings("/next", r.target);
    try std.testing.expect(!sameOrigin(base, r.origin));
}

test "resolveLocation: absolute path stays same origin" {
    const base: Origin = .{ .host = "a.test", .port = 80 };
    const r = try resolveLocation(std.testing.allocator, base, "/start?x=1", "/elsewhere?y=2");
    defer std.testing.allocator.free(r.buf);
    try std.testing.expectEqualStrings("a.test", r.origin.host);
    try std.testing.expectEqualStrings("/elsewhere?y=2", r.target);
    try std.testing.expect(sameOrigin(base, r.origin));
}

test "resolveLocation: relative reference merges against base directory" {
    const base: Origin = .{ .host = "a.test", .port = 80 };
    const r = try resolveLocation(std.testing.allocator, base, "/dir/page?q=1", "other");
    defer std.testing.allocator.free(r.buf);
    try std.testing.expectEqualStrings("/dir/other", r.target);
}

test "hostFromOriginKey: extracts host for cookie attribution" {
    try std.testing.expectEqualStrings("b.test", hostFromOriginKey("http://b.test:80").?);
    try std.testing.expectEqualStrings("api.example.com", hostFromOriginKey("https://api.example.com:443").?);
    // IPv6 literal stored bare in the key: host is everything before the port.
    try std.testing.expectEqualStrings("::1", hostFromOriginKey("http://::1:8080").?);
    // Empty / unpoolable key → null (caller falls back to the request origin).
    try std.testing.expect(hostFromOriginKey("") == null);
}

test "stripSensitive: drops auth/cookie, keeps the rest" {
    const headers = [_]Header{
        .{ .name = "Authorization", .value = "Bearer x" },
        .{ .name = "Accept", .value = "*/*" },
        .{ .name = "cookie", .value = "a=b" },
    };
    const out = try stripSensitive(std.testing.allocator, &headers);
    defer std.testing.allocator.free(out);
    try std.testing.expectEqual(@as(usize, 1), out.len);
    try std.testing.expectEqualStrings("Accept", out[0].name);
}

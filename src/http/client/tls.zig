//! Client-side TLS over the std library `std.crypto.tls.Client`, wired as a
//! `RawConnection` the `ClientConnection` consumes — the "synchronous facade
//! over coroutines" model (Go's, not Rust's sans-I/O): zio gives the TLS client
//! a real `std.Io.Reader`/`Writer` over the socket, and the handshake's
//! suspensions ride zio's coroutine just like any other socket I/O.
//!
//! Shape (why this is a *pinned* transport, unlike `TcpConnection`):
//! `std.crypto.tls.Client` is one large stateful object whose decrypted
//! `reader`/`writer` are plain `std.Io.Reader`/`Writer` fields that recover the
//! client via `@fieldParentPtr` — so the client must never move after the
//! handshake, and the two directions cannot be split into the independent
//! value-type reader/writer the plain transport contract assumes. We therefore
//! heap-pin one `State` per connection holding the socket, the encrypted-side
//! zio reader/writer (the TLS record transport), the TLS client, and all four
//! record buffers, and expose pinned `*std.Io.Reader`/`*std.Io.Writer` via the
//! `pinned_transport` marker `ClientConnection` keys on.
//!
//! A `TlsConnector` dials either scheme: `https` upgrades to TLS, `http` stays
//! plaintext (so a client built for TLS still follows an https→http redirect).
//!
//! Scope: client only. Server-side HTTPS needs a TLS *server* (absent from std)
//! and is out of scope. TLS over the in-process MemoryConnector is not provided
//! — TLS rides TCP here.

const std = @import("std");
const zio = @import("zio");
const connector_mod = @import("connector.zig");
const listener_mod = @import("../../core/listener.zig");

pub const Origin = connector_mod.Origin;

const TlsClient = std.crypto.tls.Client;
/// The std TLS client asserts its *encrypted-side* input reader has at least
/// this much buffer (one max-size ciphertext record). We size every record
/// buffer to it for uniformity and head-scan headroom.
const record_buf_len = TlsClient.min_buffer_len;

// ── Trust configuration ──────────────────────────────────────────────────────

/// System root certificate store, scanned once and shared across every TLS
/// connection a connector dials (rescanning per-connection would re-read the
/// OS trust store from disk each time). Caller-owned and long-lived: build it,
/// `load` it, hand a pointer to the `TlsConnector`, and `deinit` it after the
/// client. The std TLS verifier reads the bundle under `lock` during the
/// handshake, so the store must outlive every connection.
pub const RootStore = struct {
    bundle: std.crypto.Certificate.Bundle = .empty,
    /// Guards `bundle` for the std verifier (it takes a read lock during
    /// certificate-chain verification).
    lock: std.Io.RwLock = .init,

    /// Scans the OS trust store into `bundle`. One-time, off the hot path; the
    /// blocking file reads ride `io`.
    pub fn load(self: *RootStore, gpa: std.mem.Allocator, io: std.Io) !void {
        try self.bundle.rescan(gpa, io, std.Io.Timestamp.now(io, .real));
    }

    pub fn deinit(self: *RootStore, gpa: std.mem.Allocator) void {
        self.bundle.deinit(gpa);
    }
};

/// How the server certificate is verified. Defaults exist for testing, but the
/// production path is `.system` (full host + CA verification).
pub const Verification = union(enum) {
    /// Verify the host name (SNI + match) against the certificate AND the chain
    /// against the OS trust store. The secure default.
    system: *RootStore,
    /// Verify the host name, but accept any otherwise-valid self-signed
    /// certificate (no chain-of-trust). Test/dev only.
    self_signed,
    /// No host and no CA verification — a trusted session cannot be established.
    /// DANGER: defeats TLS authentication. Test/dev only.
    insecure_no_verification,
};

// ── Connector ────────────────────────────────────────────────────────────────

/// Connector that dials TLS for `https` origins and plaintext for `http` ones.
/// Holds its own allocator (the per-connection `State` is heap-pinned) and a
/// `std.Io` (for the handshake's entropy, wall clock, and CA bundle reads).
pub const TlsConnector = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    verification: Verification,

    pub const RawConnection = TlsTransport;

    pub fn connect(self: TlsConnector, origin: Origin, timeout: zio.Timeout) !TlsTransport {
        return switch (origin.scheme) {
            .http => TlsTransport.connectPlain(self.gpa, origin, timeout),
            .https => TlsTransport.connectTls(self.gpa, self.io, self.verification, origin, timeout),
        };
    }
};

// ── Transport ────────────────────────────────────────────────────────────────

/// A dialed connection — TLS or plaintext — presented as a pinned transport.
/// Just a handle to the heap-pinned `State`; freely movable (only `State` must
/// stay put, which it does, being heap-allocated).
pub const TlsTransport = struct {
    /// Marks this as a *pinned* transport for `ClientConnection`: it owns its
    /// own record buffers and exposes ready `*std.Io.Reader`/`*std.Io.Writer`,
    /// rather than the value-type `reader(buf)`/`writer(buf)` of plain transports.
    pub const pinned_transport = true;

    state: *State,

    /// All per-connection state, heap-pinned so the TLS client's
    /// `@fieldParentPtr`-based reader/writer and the buffer pointers stay valid.
    const State = struct {
        gpa: std.mem.Allocator,
        stream: zio.net.Stream,
        /// Encrypted-side zio reader/writer: the TLS record transport. In
        /// plaintext mode these are the HTTP reader/writer directly.
        tcp_reader: zio.net.Stream.Reader,
        tcp_writer: zio.net.Stream.Writer,
        /// The TLS session; null in plaintext mode.
        tls: ?TlsClient,
        // Record buffers (one max ciphertext record each, uniform for safety):
        //   enc_read  — encrypted bytes the TLS client pulls from the socket
        //   enc_write — encrypted bytes the TLS client pushes to the socket
        //   dec_read  — decrypted plaintext the HTTP layer reads (TLS only)
        //   clear_write — plaintext the HTTP layer writes before encryption (TLS only)
        enc_read_buf: [record_buf_len]u8 = undefined,
        enc_write_buf: [record_buf_len]u8 = undefined,
        dec_read_buf: [record_buf_len]u8 = undefined,
        clear_write_buf: [record_buf_len]u8 = undefined,
    };

    /// Short bounded read used to tell a still-open idle keep-alive socket from
    /// one the peer closed (see `isLikelyLive`). A zero-duration zio timer is
    /// not guaranteed to fire, which would hang the probe — so 1ms.
    const liveness_probe_timeout: zio.Timeout = .{ .duration = zio.Duration.fromMilliseconds(1) };

    fn allocState(gpa: std.mem.Allocator, stream: zio.net.Stream) !*State {
        const state = try gpa.create(State);
        state.* = .{
            .gpa = gpa,
            .stream = stream,
            .tcp_reader = undefined,
            .tcp_writer = undefined,
            .tls = null,
        };
        // Wire the encrypted-side reader/writer at their final pinned addresses.
        state.tcp_reader = stream.reader(&state.enc_read_buf);
        state.tcp_writer = stream.writer(&state.enc_write_buf);
        return state;
    }

    fn connectPlain(gpa: std.mem.Allocator, origin: Origin, timeout: zio.Timeout) !TlsTransport {
        const stream = try zio.net.tcpConnectToHost(origin.host, origin.port, .{ .timeout = timeout });
        stream.socket.setNoDelay(true) catch {};
        const state = allocState(gpa, stream) catch |err| {
            stream.close();
            return err;
        };
        return .{ .state = state };
    }

    fn connectTls(
        gpa: std.mem.Allocator,
        io: std.Io,
        verification: Verification,
        origin: Origin,
        timeout: zio.Timeout,
    ) !TlsTransport {
        const stream = try zio.net.tcpConnectToHost(origin.host, origin.port, .{ .timeout = timeout });
        stream.socket.setNoDelay(true) catch {};
        errdefer stream.close();

        const state = try allocState(gpa, stream);
        errdefer gpa.destroy(state);

        // The connect timeout also bounds the handshake's socket I/O.
        state.tcp_reader.setTimeout(timeout);
        state.tcp_writer.setTimeout(timeout);

        var entropy: [TlsClient.Options.entropy_len]u8 = undefined;
        try std.Io.randomSecure(io, &entropy);

        const host: @FieldType(TlsClient.Options, "host") = switch (verification) {
            .insecure_no_verification => .no_verification,
            .system, .self_signed => .{ .explicit = origin.host },
        };
        const ca: @FieldType(TlsClient.Options, "ca") = switch (verification) {
            .insecure_no_verification => .no_verification,
            .self_signed => .self_signed,
            .system => |store| .{ .bundle = .{
                .gpa = gpa,
                .io = io,
                .lock = &store.lock,
                .bundle = &store.bundle,
            } },
        };

        // errdefers above free `state` and close the socket if the handshake fails.
        state.tls = try TlsClient.init(&state.tcp_reader.interface, &state.tcp_writer.interface, .{
            .host = host,
            .ca = ca,
            .read_buffer = &state.dec_read_buf,
            .write_buffer = &state.clear_write_buf,
            .entropy = &entropy,
            .realtime_now = std.Io.Timestamp.now(io, .real),
            // Forward a bare EOF (peer closed without close_notify, which many
            // real servers do) as normal end-of-stream rather than
            // error.TlsConnectionTruncated. A Content-Length or chunked body
            // carries its own length, so the codec still detects truncation
            // there. The genuine tradeoff: a *close-delimited* body (no
            // Content-Length, not chunked) has no length signal, so under this
            // setting an active in-stream truncation is indistinguishable from a
            // clean end — the client cannot detect it. This is the deliberate,
            // industry-standard default (Go/curl/reqwest all do the same);
            // callers who must detect truncation on such bodies need an
            // application-level length/checksum.
            .allow_truncation_attacks = true,
        });
        // Clear the handshake deadline; per-stage timeouts are reapplied per
        // round-trip by the client.
        state.tcp_reader.setTimeout(.none);
        state.tcp_writer.setTimeout(.none);
        return .{ .state = state };
    }

    // ── Pinned-transport contract (consumed by ClientConnection) ──────────────

    /// The HTTP read stream: the decrypted TLS reader, or the raw socket reader
    /// in plaintext mode.
    pub fn ioReader(self: TlsTransport) *std.Io.Reader {
        if (self.state.tls) |*c| return &c.reader;
        return &self.state.tcp_reader.interface;
    }

    /// The HTTP write stream: the plaintext-side TLS writer, or the raw socket
    /// writer in plaintext mode.
    pub fn ioWriter(self: TlsTransport) *std.Io.Writer {
        if (self.state.tls) |*c| return &c.writer;
        return &self.state.tcp_writer.interface;
    }

    /// Flushes the write stack to the socket. Critically, the std TLS writer's
    /// own `flush` only *encrypts* buffered plaintext into the underlying output
    /// writer's buffer — it does not push that ciphertext onward — so a TLS
    /// flush must be followed by flushing the encrypted-side socket writer, or
    /// the request never leaves this process.
    pub fn flush(self: TlsTransport) !void {
        if (self.state.tls) |*c| try c.writer.flush();
        try self.state.tcp_writer.interface.flush();
    }

    /// Read/write deadlines always bind the *socket* I/O (the encrypted-side
    /// zio reader/writer). In TLS mode the decrypted read blocks on the
    /// underlying timed socket read, so a stalled peer is still bounded.
    pub fn setReadTimeout(self: TlsTransport, timeout: zio.Timeout) void {
        self.state.tcp_reader.setTimeout(timeout);
    }

    pub fn setWriteTimeout(self: TlsTransport, timeout: zio.Timeout) void {
        self.state.tcp_writer.setTimeout(timeout);
    }

    /// Liveness probe for an idle pooled connection: a short bounded read on the
    /// encrypted socket. A healthy idle keep-alive has no pending bytes (the
    /// probe times out → live); a peer-closed socket returns EOF at once
    /// (→ dead); any unsolicited bytes on an idle TLS connection (e.g. an alert)
    /// make it unusable (→ dead, conservatively). Must only run on a fully
    /// drained idle connection.
    pub fn isLikelyLive(self: TlsTransport) bool {
        const r = &self.state.tcp_reader;
        if (r.interface.bufferedLen() != 0) return false;
        // The decrypted side must also have no buffered plaintext.
        if (self.state.tls) |*c| if (c.reader.bufferedLen() != 0) return false;
        r.setTimeout(liveness_probe_timeout);
        defer r.setTimeout(.none);
        _ = r.interface.fill(1) catch |err| switch (err) {
            error.EndOfStream => return false,
            error.ReadFailed => {
                const timed_out = if (r.err) |e| e == error.Timeout else false;
                if (timed_out) {
                    r.err = null;
                    return true;
                }
                return false;
            },
        };
        return false; // unsolicited bytes on an idle connection → unusable
    }

    /// Sends a best-effort TLS close_notify (politeness; bounded by whatever
    /// write deadline is set), closes the socket, and frees the pinned state.
    pub fn close(self: TlsTransport) void {
        const state = self.state;
        if (state.tls) |*c| {
            c.end() catch {};
            state.tcp_writer.interface.flush() catch {};
        }
        state.stream.close();
        state.gpa.destroy(state);
    }

    /// Half-close the write side (no decrypted-side half-close needed here).
    pub fn shutdown(self: TlsTransport) void {
        self.state.stream.shutdown(.both) catch {};
    }

    pub fn remoteInfo(self: TlsTransport) listener_mod.RemoteInfo {
        return .{ .net = self.state.stream.socket.address };
    }
};

test "TlsConnector: declares the pinned-transport contract" {
    try std.testing.expect(TlsConnector.RawConnection.pinned_transport);
    // The connector is a plain value: allocator + io + verification.
    try std.testing.expect(@hasField(TlsConnector, "verification"));
}

test "record buffers are at least one max ciphertext record" {
    try std.testing.expect(record_buf_len >= std.crypto.tls.max_ciphertext_record_len);
}

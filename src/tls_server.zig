//! Optional OpenSSL server adapter. One reader, serialized TLS operations;
//! no OpenSSL operation suspends before its error code is captured.
const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");
const c = @import("openssl");

pub const Context = struct {
    handle: *c.SSL_CTX,
    alpn: ?*Alpn = null,
    identities: ?*Identities = null,

    pub const NamedIdentity = struct {
        /// Exact ASCII DNS name, matched without case. Copied at init.
        server_name: []const u8,
        certificate_chain: [:0]const u8,
        private_key: [:0]const u8,
    };

    pub const Options = struct {
        /// Server preference order; only advertise protocols the service implements.
        /// Empty disables ALPN. Identifiers are opaque bytes, copied at init.
        alpn_protocols: []const []const u8 = &.{},
        /// Unknown/absent SNI uses the default identity. No wildcard matching.
        server_identities: []const NamedIdentity = &.{},
    };

    /// Configure before serving. Context must outlive all wrapped connections.
    pub fn init(certificate_chain: [:0]const u8, private_key: [:0]const u8) !Context {
        return initWithOptions(std.heap.c_allocator, certificate_chain, private_key, .{});
    }

    /// Owns copied configuration; gpa must outlive deinit. Do not copy ownership
    /// or change policy while serving. OpenSSL internal allocations use libc.
    pub fn initWithOptions(gpa: std.mem.Allocator, certificate_chain: [:0]const u8, private_key: [:0]const u8, options: Options) !Context {
        try Identities.validate(options.server_identities);
        const alpn = if (options.alpn_protocols.len != 0) try Alpn.init(gpa, options.alpn_protocols) else null;
        errdefer if (alpn) |policy| policy.deinit();
        const ctx = try identityContext(certificate_chain, private_key, alpn);
        errdefer c.SSL_CTX_free(ctx);
        const identities = if (options.server_identities.len != 0)
            try Identities.init(gpa, ctx, options.server_identities, alpn)
        else
            null;
        return .{ .handle = ctx, .alpn = alpn, .identities = identities };
    }

    fn identityContext(certificate_chain: [:0]const u8, private_key: [:0]const u8, alpn: ?*Alpn) !*c.SSL_CTX {
        const ctx = c.SSL_CTX_new(c.TLS_server_method()) orelse return error.TlsContextFailed;
        errdefer c.SSL_CTX_free(ctx);
        if (c.SSL_CTX_ctrl(ctx, c.SSL_CTRL_SET_MIN_PROTO_VERSION, c.TLS1_2_VERSION, null) != 1 or
            c.SSL_CTX_ctrl(ctx, c.SSL_CTRL_SET_MAX_PROTO_VERSION, c.TLS1_3_VERSION, null) != 1)
            return error.TlsConfigurationFailed;
        c.talon_ssl_no_renegotiation(ctx);
        if (c.SSL_CTX_use_certificate_chain_file(ctx, certificate_chain) != 1 or
            c.SSL_CTX_use_PrivateKey_file(ctx, private_key, c.SSL_FILETYPE_PEM) != 1 or
            c.SSL_CTX_check_private_key(ctx) != 1) return error.TlsIdentityFailed;
        if (alpn) |policy| c.SSL_CTX_set_alpn_select_cb(ctx, Alpn.select, policy);
        return ctx;
    }

    pub fn deinit(self: *Context) void {
        c.SSL_CTX_free(self.handle);
        if (self.identities) |policy| policy.deinit();
        if (self.alpn) |policy| policy.deinit();
        self.* = undefined;
    }
};

/// Owns stable callback arguments and child contexts. Never owns the default.
const Identities = struct {
    gpa: std.mem.Allocator,
    default: *c.SSL_CTX,
    entries: []Entry,

    const Entry = struct { name: []u8, context: *c.SSL_CTX };

    fn validate(config: []const Context.NamedIdentity) !void {
        for (config, 0..) |identity, i| {
            const name = identity.server_name;
            if (name.len == 0 or name.len > 253) return error.InvalidServerName;
            var labels = std.mem.splitScalar(u8, name, '.');
            while (labels.next()) |label| {
                if (label.len == 0 or label.len > 63 or label[0] == '-' or label[label.len - 1] == '-')
                    return error.InvalidServerName;
                for (label) |byte| if (!std.ascii.isAlphanumeric(byte) and byte != '-') return error.InvalidServerName;
            }
            for (config[0..i]) |previous|
                if (std.ascii.eqlIgnoreCase(previous.server_name, name)) return error.DuplicateServerName;
        }
    }

    fn init(gpa: std.mem.Allocator, default: *c.SSL_CTX, config: []const Context.NamedIdentity, alpn: ?*Alpn) !*Identities {
        const self = try gpa.create(Identities);
        errdefer gpa.destroy(self);
        const entries = try gpa.alloc(Entry, config.len);
        errdefer gpa.free(entries);
        var populated: usize = 0;
        errdefer for (entries[0..populated]) |entry| {
            c.SSL_CTX_free(entry.context);
            gpa.free(entry.name);
        };
        for (config, entries, 0..) |identity, *entry, i| {
            const name = try gpa.dupe(u8, identity.server_name);
            errdefer gpa.free(name);
            const ctx = try Context.identityContext(identity.certificate_chain, identity.private_key, alpn);
            errdefer c.SSL_CTX_free(ctx);
            const id = sessionContext(i + 1);
            if (c.SSL_CTX_set_session_id_context(ctx, &id, id.len) != 1) return error.TlsConfigurationFailed;
            entry.* = .{ .name = name, .context = ctx };
            populated += 1;
        }
        const id = sessionContext(0);
        if (c.SSL_CTX_set_session_id_context(default, &id, id.len) != 1) return error.TlsConfigurationFailed;
        self.* = .{ .gpa = gpa, .default = default, .entries = entries };
        self.install(default);
        for (entries) |entry| self.install(entry.context);
        return self;
    }

    fn install(self: *Identities, ctx: *c.SSL_CTX) void {
        c.SSL_CTX_set_client_hello_cb(ctx, clientHello, self);
        c.talon_ssl_servername_callback(ctx, acknowledge, self);
    }

    fn deinit(self: *Identities) void {
        const gpa = self.gpa;
        for (self.entries) |entry| {
            c.SSL_CTX_free(entry.context);
            gpa.free(entry.name);
        }
        gpa.free(self.entries);
        gpa.destroy(self);
    }

    fn sessionContext(index: usize) [8]u8 {
        var id: [8]u8 = undefined;
        std.mem.writeInt(u64, &id, @intCast(index), .big);
        return id;
    }

    fn find(self: *const Identities, name: []const u8) ?usize {
        for (self.entries, 0..) |entry, i|
            if (std.ascii.eqlIgnoreCase(entry.name, name)) return i;
        return null;
    }

    // OpenSSL supports one host_name entry. Validate raw lengths before lookup;
    // SSL_get_servername is too late and can name the old TLS 1.2 session.
    fn parseName(extension: []const u8) ![]const u8 {
        if (extension.len < 6 or extension[2] != c.TLSEXT_NAMETYPE_host_name or
            std.mem.readInt(u16, extension[0..2], .big) != extension.len - 2 or
            std.mem.readInt(u16, extension[3..5], .big) != extension.len - 5)
            return error.InvalidServerNameExtension;
        const name = extension[5..];
        if (name.len > 255 or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidServerNameExtension;
        return name;
    }

    fn clientHello(ssl: ?*c.SSL, alert: [*c]c_int, arg: ?*anyopaque) callconv(.c) c_int {
        alert.* = c.SSL_AD_INTERNAL_ERROR;
        const session = ssl orelse return c.SSL_CLIENT_HELLO_ERROR;
        const self: *const Identities = @ptrCast(@alignCast(arg orelse return c.SSL_CLIENT_HELLO_ERROR));
        var extension: [*c]const u8 = null;
        var length: usize = 0;
        var selected: ?usize = null;
        if (c.SSL_client_hello_get0_ext(session, c.TLSEXT_TYPE_server_name, &extension, &length) == 1) {
            const name = parseName(extension[0..length]) catch {
                alert.* = c.SSL_AD_DECODE_ERROR;
                return c.SSL_CLIENT_HELLO_ERROR;
            };
            selected = self.find(name);
        }
        const ctx = if (selected) |i| self.entries[i].context else self.default;
        // A TLS 1.3 HelloRetryRequest may run this callback twice.
        if (c.SSL_get_SSL_CTX(session) != ctx and c.SSL_set_SSL_CTX(session, ctx) == null)
            return c.SSL_CLIENT_HELLO_ERROR;
        const id = sessionContext(if (selected) |i| i + 1 else 0);
        if (c.SSL_set_session_id_context(session, &id, id.len) != 1) return c.SSL_CLIENT_HELLO_ERROR;
        return c.SSL_CLIENT_HELLO_SUCCESS;
    }

    fn acknowledge(ssl: ?*c.SSL, _: [*c]c_int, arg: ?*anyopaque) callconv(.c) c_int {
        const session = ssl orelse return c.SSL_TLSEXT_ERR_ALERT_FATAL;
        const self: *const Identities = @ptrCast(@alignCast(arg orelse return c.SSL_TLSEXT_ERR_ALERT_FATAL));
        return if (c.SSL_get_SSL_CTX(session) == self.default) c.SSL_TLSEXT_ERR_NOACK else c.SSL_TLSEXT_ERR_OK;
    }
};

/// Stable callback storage, independent of the movable Context value.
const Alpn = struct {
    gpa: std.mem.Allocator,
    wire: []u8,

    fn init(gpa: std.mem.Allocator, protocols: []const []const u8) !*Alpn {
        var size: usize = 0;
        for (protocols) |protocol| {
            if (protocol.len == 0 or protocol.len > 255) return error.InvalidAlpnProtocols;
            if (size > 65535 - (1 + protocol.len)) return error.InvalidAlpnProtocols;
            size += 1 + protocol.len;
        }
        const self = try gpa.create(Alpn);
        errdefer gpa.destroy(self);
        const wire = try gpa.alloc(u8, size);
        var cursor: usize = 0;
        for (protocols) |protocol| {
            wire[cursor] = @intCast(protocol.len);
            cursor += 1;
            @memcpy(wire[cursor..][0..protocol.len], protocol);
            cursor += protocol.len;
        }
        self.* = .{ .gpa = gpa, .wire = wire };
        return self;
    }

    fn deinit(self: *Alpn) void {
        const gpa = self.gpa;
        gpa.free(self.wire);
        gpa.destroy(self);
    }

    fn select(_: ?*c.SSL, out: [*c][*c]const u8, out_len: [*c]u8, input: [*c]const u8, input_len: c_uint, arg: ?*anyopaque) callconv(.c) c_int {
        const self: *const Alpn = @ptrCast(@alignCast(arg orelse return c.SSL_TLSEXT_ERR_ALERT_FATAL));
        var selected: [*c]u8 = null;
        const result = c.SSL_select_next_proto(&selected, out_len, self.wire.ptr, @intCast(self.wire.len), input, input_len);
        // OpenSSL's NO_OVERLAP fallback belongs to NPN; ALPN must reject it.
        if (result != c.OPENSSL_NPN_NEGOTIATED) return c.SSL_TLSEXT_ERR_ALERT_FATAL;
        out.* = selected;
        return c.SSL_TLSEXT_ERR_OK;
    }
};

pub const Adapter = struct {
    context: *const Context,
    /// Opt-in local alert on successful task completion. Does not await peer.
    close_notify_timeout: ?zio.Duration = null,

    pub fn Connection(_: type) type {
        return Transport;
    }

    pub fn validateTransport(self: *Adapter, capabilities: talon.core.TimeoutCapabilities, limits: talon.core.Limits) !void {
        try capabilities.checkRead(limits.handshake_timeout);
        try capabilities.checkWrite(limits.handshake_timeout);
        if (self.close_notify_timeout) |duration|
            try capabilities.checkWrite(.{ .duration = duration });
    }

    pub fn finish(self: *Adapter, transport: Transport) !void {
        if (self.close_notify_timeout) |duration| try transport.finish(duration);
    }

    pub fn wrap(self: *Adapter, raw: talon.core.TcpConnection, gpa: std.mem.Allocator, deadline: zio.Timeout) !Transport {
        return self.wrapWithOptions(raw, .{ .allocator = gpa, .deadline = deadline, .read_buffer_size = 32 * 1024 });
    }

    pub fn wrapWithOptions(self: *Adapter, raw: talon.core.TcpConnection, options: talon.core.WrapOptions) !Transport {
        if (options.read_buffer_size == 0) return error.InvalidBufferSize;
        const gpa = options.allocator;
        const deadline = options.deadline;
        const state = try gpa.create(State);
        errdefer gpa.destroy(state);
        const clear = try gpa.alloc(u8, options.read_buffer_size);
        errdefer gpa.free(clear);
        const ssl = c.SSL_new(self.context.handle) orelse return error.TlsSessionFailed;
        errdefer c.SSL_free(ssl);
        const input = c.BIO_new(c.BIO_s_mem()) orelse return error.TlsSessionFailed;
        const output = c.BIO_new(c.BIO_s_mem()) orelse {
            _ = c.BIO_free(input);
            return error.TlsSessionFailed;
        };
        c.SSL_set_bio(ssl, input, output); // SSL owns both BIOs now.
        c.SSL_set_accept_state(ssl);
        state.* = .{ .gpa = gpa, .raw = raw, .ssl = ssl, .input = input, .output = output, .encrypted_reader = undefined, .encrypted_writer = undefined, .reader = undefined, .writer = undefined, .clear_read_buffer = clear };
        state.encrypted_reader = raw.reader(&state.encrypted_read_buffer);
        state.encrypted_writer = raw.writer(&.{});
        state.encrypted_reader.setTimeout(deadline);
        state.encrypted_writer.setTimeout(deadline);
        state.reader = .{ .vtable = &.{ .stream = State.stream }, .buffer = state.clear_read_buffer, .seek = 0, .end = 0 };
        state.writer = .{ .vtable = &.{ .drain = State.drain }, .buffer = &.{}, .end = 0 };
        state.handshake() catch |err| {
            if (err == error.Timeout) return error.HandshakeTimeout;
            return err;
        };
        // Engine reapplies the same establishment deadline before protocol start.
        return .{ .state = state };
    }
};

pub const Transport = struct {
    pub const pinned_transport = true;
    state: *State,

    /// Borrowed immutable handshake result; valid until close. Empty if absent.
    pub fn negotiatedProtocol(self: Transport) []const u8 {
        var protocol: [*c]const u8 = null;
        var length: c_uint = 0;
        c.SSL_get0_alpn_selected(self.state.ssl, &protocol, &length);
        return if (length == 0) &.{} else protocol[0..length];
    }

    pub fn ioReader(self: Transport) *std.Io.Reader {
        return &self.state.reader;
    }
    pub fn ioWriter(self: Transport) *std.Io.Writer {
        return &self.state.writer;
    }
    pub fn flush(self: Transport) std.Io.Writer.Error!void {
        self.state.mutex.lock() catch return error.WriteFailed;
        defer self.state.mutex.unlock();
        if (self.state.failed.load(.acquire)) return error.WriteFailed;
        self.state.flushOutput() catch {
            self.state.failed.store(true, .release);
            return error.WriteFailed;
        };
    }
    pub fn setReadTimeout(self: Transport, timeout: zio.Timeout) void {
        self.state.encrypted_reader.setTimeout(timeout);
    }
    pub fn setWriteTimeout(self: Transport, timeout: zio.Timeout) void {
        self.state.encrypted_writer.setTimeout(timeout);
    }
    pub fn lastReadTimedOut(self: Transport) bool {
        return !self.state.failed.load(.acquire) and self.state.readTimedOut();
    }
    pub fn clearReadError(self: Transport) void {
        if (self.lastReadTimedOut()) self.state.encrypted_reader.err = null;
    }
    pub fn remoteInfo(self: Transport) talon.core.RemoteInfo {
        return self.state.raw.remoteInfo();
    }
    pub fn shutdown(self: Transport) void {
        self.state.raw.shutdown();
    }
    /// Abortive transport release, with no I/O or implicit close_notify wait.
    /// The owner must join all users first. Use closeNotify before release if desired.
    pub fn close(self: Transport) void {
        self.state.raw.close();
        c.SSL_free(self.state.ssl);
        self.state.gpa.free(self.state.clear_read_buffer);
        self.state.gpa.destroy(self.state);
    }
    /// Sends local close_notify using the configured write deadline. Does not
    /// wait for the peer alert or prove that it received the alert.
    pub fn closeNotify(self: Transport) !void {
        try self.state.mutex.lock();
        defer self.state.mutex.unlock();
        try self.state.closeNotify();
    }

    fn finish(self: Transport, duration: zio.Duration) !void {
        try zio.checkCancel();
        // All users must already be joined. Never wait for a stray sender.
        if (!self.state.mutex.tryLock()) return error.TlsBusy;
        defer self.state.mutex.unlock();
        if (self.state.failed.load(.acquire)) return error.TlsFailed;
        errdefer self.state.failed.store(true, .release);
        const deadline = closeDeadline(duration, self.state.encrypted_writer.timeout);
        self.state.encrypted_writer.setTimeout(deadline);
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline.deadline.toNanoseconds())
            return error.Timeout;
        try self.state.closeNotify();
    }
};

const State = struct {
    gpa: std.mem.Allocator,
    raw: talon.core.TcpConnection,
    ssl: *c.SSL,
    input: *c.BIO,
    output: *c.BIO,
    mutex: zio.Mutex = .init,
    failed: std.atomic.Value(bool) = .init(false),
    encrypted_reader: talon.core.TcpConnection.Reader,
    encrypted_writer: talon.core.TcpConnection.Writer,
    reader: std.Io.Reader,
    writer: std.Io.Writer,
    encrypted_read_buffer: [18 * 1024]u8 = undefined,
    clear_read_buffer: []u8 = undefined,

    fn readTimedOut(self: *State) bool {
        return if (self.encrypted_reader.err) |err| err == error.Timeout else false;
    }

    fn flushOutput(self: *State) !void {
        // Before publication or under mutex: no SSL/BIO mutation may occur
        // while the borrowed slice crosses asynchronous raw writes.
        if (self.failed.load(.acquire)) return error.TlsFailed;
        errdefer self.failed.store(true, .release);
        if (c.BIO_method_type(self.output) != c.BIO_TYPE_MEM or
            c.BIO_test_flags(self.output, c.BIO_FLAGS_MEM_RDONLY | c.BIO_FLAGS_NONCLEAR_RST) != 0)
            return error.TlsFailed;
        var bytes: [*c]u8 = null;
        const length = c.BIO_get_mem_data(self.output, &bytes);
        if (length < 0 or (length != 0 and bytes == null)) return error.TlsFailed;
        if (length != 0) {
            try self.encrypted_writer.interface.writeAll(bytes[0..@intCast(length)]);
        }
        try self.encrypted_writer.interface.flush();
        if (length != 0 and (c.BIO_reset(self.output) != 1 or c.BIO_ctrl_pending(self.output) != 0))
            return error.TlsFailed;
    }

    // Caller holds mutex. Capture thread-local error before any coroutine I/O.
    fn closeNotify(self: *State) !void {
        if (self.failed.load(.acquire)) return error.TlsFailed;
        errdefer self.failed.store(true, .release);
        c.ERR_clear_error();
        const result = c.SSL_shutdown(self.ssl);
        const code = if (result < 0) c.SSL_get_error(self.ssl, result) else c.SSL_ERROR_NONE;
        // Fatal errors must not emit pending output or retry shutdown.
        if (code != c.SSL_ERROR_NONE and code != c.SSL_ERROR_WANT_READ) return error.TlsFailed;
        try self.flushOutput();
    }

    // Only the single reader (or handshake before publication) owns input I/O.
    // takeByte preserves partial TLS input already in BIO across idle timeouts.
    fn receive(self: *State) !void {
        const first = self.encrypted_reader.interface.takeByte() catch |err| {
            if (!self.readTimedOut()) self.failed.store(true, .release);
            if (self.encrypted_reader.err) |cause| return cause;
            return err;
        };
        errdefer self.failed.store(true, .release);
        try self.mutex.lock();
        defer self.mutex.unlock();
        if (self.failed.load(.acquire)) return error.TlsFailed;
        if (c.BIO_write(self.input, &first, 1) != 1) return error.TlsFailed;
        const bytes = self.encrypted_reader.interface.buffered();
        if (bytes.len != 0) {
            if (c.BIO_write(self.input, bytes.ptr, @intCast(bytes.len)) != bytes.len) return error.TlsFailed;
            self.encrypted_reader.interface.tossBuffered();
        }
    }

    fn handshake(self: *State) !void {
        while (true) {
            c.ERR_clear_error();
            const result = c.SSL_accept(self.ssl);
            const code = if (result == 1) c.SSL_ERROR_NONE else c.SSL_get_error(self.ssl, result);
            try self.flushOutput();
            if (code == c.SSL_ERROR_NONE) return;
            if (code != c.SSL_ERROR_WANT_READ) return error.TlsHandshakeFailed;
            try self.receive();
        }
    }

    fn readPlain(self: *State, bytes: []u8) !usize {
        while (true) {
            try self.mutex.lock();
            {
                defer self.mutex.unlock();
                if (self.failed.load(.acquire)) return error.TlsFailed;
                c.ERR_clear_error();
                var n: usize = 0;
                const result = c.SSL_read_ex(self.ssl, bytes.ptr, bytes.len, &n);
                const code = if (result == 1) c.SSL_ERROR_NONE else c.SSL_get_error(self.ssl, result);
                self.flushOutput() catch |err| {
                    self.failed.store(true, .release);
                    return err;
                };
                if (code == c.SSL_ERROR_NONE) return n;
                if (code == c.SSL_ERROR_ZERO_RETURN) return 0;
                if (code != c.SSL_ERROR_WANT_READ) {
                    self.failed.store(true, .release);
                    return error.TlsFailed;
                }
            }
            try self.receive();
        }
    }

    fn stream(reader: *std.Io.Reader, writer: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *State = @alignCast(@fieldParentPtr("reader", reader));
        const dest = limit.slice(try writer.writableSliceGreedy(1));
        const n = self.readPlain(dest) catch return error.ReadFailed;
        if (n == 0) return error.EndOfStream;
        writer.advance(n);
        return n;
    }

    fn writePlain(self: *State, bytes: []const u8) !void {
        try self.mutex.lock();
        defer self.mutex.unlock();
        if (self.failed.load(.acquire)) return error.TlsFailed;
        errdefer self.failed.store(true, .release);
        var offset: usize = 0;
        while (offset < bytes.len) {
            const chunk = bytes[offset..][0..@min(16384, bytes.len - offset)];
            c.ERR_clear_error();
            var n: usize = 0;
            const result = c.SSL_write_ex(self.ssl, chunk.ptr, chunk.len, &n);
            const code = if (result == 1) c.SSL_ERROR_NONE else c.SSL_get_error(self.ssl, result);
            try self.flushOutput();
            // No renegotiation or implicit read from a writer task. A surprising
            // WANT_READ/WANT_WRITE is terminal, never a plaintext retry.
            if (code != c.SSL_ERROR_NONE or n != chunk.len) return error.TlsFailed;
            offset += n;
        }
    }

    fn drain(writer: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *State = @alignCast(@fieldParentPtr("writer", writer));
        var total: usize = 0;
        for (data, 0..) |bytes, index| {
            const repeats = if (index + 1 == data.len) splat else 1;
            for (0..repeats) |_| {
                self.writePlain(bytes) catch return error.WriteFailed;
                total += bytes.len;
            }
        }
        return total;
    }
};

fn closeDeadline(duration: zio.Duration, write_timeout: zio.Timeout) zio.Timeout {
    const deadline = (zio.Timeout{ .duration = duration }).toDeadline();
    const write_deadline = write_timeout.toDeadline();
    return if (write_deadline == .deadline and
        write_deadline.deadline.toNanoseconds() < deadline.deadline.toNanoseconds()) write_deadline else deadline;
}

test "TLS ALPN owns opaque identifiers and selects server preference without NPN fallback" {
    var first = [_]u8{ 'a', 0, 'b' };
    const policy = try Alpn.init(std.testing.allocator, &.{ &first, "second" });
    defer policy.deinit();
    first[0] = 'z'; // Input lifetime and subsequent mutation are independent.
    var output: [*c]const u8 = null;
    var length: u8 = 0;
    const offer = "\x06second\x03a\x00b";
    try std.testing.expectEqual(@as(c_int, c.SSL_TLSEXT_ERR_OK), Alpn.select(null, &output, &length, offer.ptr, offer.len, policy));
    try std.testing.expectEqualSlices(u8, "a\x00b", output[0..length]);
    const unsupported = "\x02h2";
    try std.testing.expectEqual(@as(c_int, c.SSL_TLSEXT_ERR_ALERT_FATAL), Alpn.select(null, &output, &length, unsupported.ptr, unsupported.len, policy));
}

test "TLS ALPN rejects invalid lengths and rolls back allocation and identity failures" {
    try std.testing.expectError(error.InvalidAlpnProtocols, Alpn.init(std.testing.allocator, &.{""}));
    const too_long = @as([256]u8, @splat('x'));
    try std.testing.expectError(error.InvalidAlpnProtocols, Alpn.init(std.testing.allocator, &.{&too_long}));
    const maximum = @as([255]u8, @splat('x'));
    var protocols: [256][]const u8 = @splat(&maximum);
    try std.testing.expectError(error.InvalidAlpnProtocols, Alpn.init(std.testing.allocator, &protocols));
    protocols[255] = maximum[0..254];
    const policy = try Alpn.init(std.testing.allocator, &protocols);
    defer policy.deinit();
    try std.testing.expectEqual(@as(usize, 65535), policy.wire.len);
    const F = struct {
        fn allocate(gpa: std.mem.Allocator) !void {
            const allocated = try Alpn.init(gpa, &.{"http/1.1"});
            defer allocated.deinit();
        }
        fn invalidIdentity(gpa: std.mem.Allocator) !void {
            var ctx = Context.initWithOptions(gpa, "", "", .{ .alpn_protocols = &.{"http/1.1"} }) catch |err| {
                if (err == error.TlsIdentityFailed) return;
                return err;
            };
            ctx.deinit();
            return error.TestUnexpectedResult;
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.allocate, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.invalidIdentity, .{});
}

test "TLS named identity configuration rejects ambiguous DNS names before loading certificates" {
    const invalid = [_][]const u8{ "", ".alpha", "alpha.", "a..b", "*.alpha", "-alpha", "alpha-", "a_b", "a\x00b", "caf\xc3\xa9" };
    for (invalid) |name| {
        try std.testing.expectError(error.InvalidServerName, Context.initWithOptions(std.testing.allocator, "", "", .{
            .server_identities = &.{.{ .server_name = name, .certificate_chain = "", .private_key = "" }},
        }));
    }
    const label = @as([64]u8, @splat('a'));
    const too_long = @as([254]u8, @splat('a'));
    for ([_][]const u8{ &label, &too_long }) |name|
        try std.testing.expectError(error.InvalidServerName, Identities.validate(&.{.{ .server_name = name, .certificate_chain = "", .private_key = "" }}));
    try Identities.validate(&.{.{ .server_name = "A-1.xn--bcher-kva.example", .certificate_chain = "", .private_key = "" }});
    const longest = (@as([63]u8, @splat('a'))) ++ "." ++ (@as([63]u8, @splat('b'))) ++ "." ++ (@as([63]u8, @splat('c'))) ++ "." ++ (@as([61]u8, @splat('d')));
    try std.testing.expectEqual(@as(usize, 253), longest.len);
    try Identities.validate(&.{.{ .server_name = longest, .certificate_chain = "", .private_key = "" }});
    try std.testing.expectError(error.DuplicateServerName, Context.initWithOptions(std.testing.allocator, "", "", .{
        .server_identities = &.{
            .{ .server_name = "Alpha.localhost", .certificate_chain = "", .private_key = "" },
            .{ .server_name = "alpha.LOCALHOST", .certificate_chain = "", .private_key = "" },
        },
    }));
}

test "TLS early SNI lookup bounds raw extension parsing and rolls back failed identity load" {
    const valid = "\x00\x08\x00\x00\x05alpha";
    try std.testing.expectEqualStrings("alpha", try Identities.parseName(valid));
    for (0..valid.len) |length|
        try std.testing.expectError(error.InvalidServerNameExtension, Identities.parseName(valid[0..length]));
    for ([_][]const u8{ "\x00\x08\x01\x00\x05alpha", "\x00\x08\x00\x00\x04alpha", "\x00\x08\x00\x00\x05a\x00pha", valid ++ "\x00" }) |extension|
        try std.testing.expectError(error.InvalidServerNameExtension, Identities.parseName(extension));
    const F = struct {
        fn invalidIdentity(gpa: std.mem.Allocator) !void {
            // A bare SSL_CTX is sufficient to exercise map/name allocations
            // before the invalid first child certificate aborts construction.
            const ctx = c.SSL_CTX_new(c.TLS_server_method()) orelse return error.TlsContextFailed;
            defer c.SSL_CTX_free(ctx);
            const identities = Identities.init(gpa, ctx, &.{.{ .server_name = "alpha.localhost", .certificate_chain = "", .private_key = "" }}, null) catch |err| {
                if (err == error.TlsIdentityFailed) return;
                return err;
            };
            identities.deinit();
            return error.TestUnexpectedResult;
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.invalidIdentity, .{});
}

test "TLS finish rejects busy, fatal and expired transports before SSL or I/O" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var state: State = .{
        .gpa = std.testing.allocator,
        .raw = undefined,
        .ssl = undefined,
        .input = undefined,
        .output = undefined,
        .encrypted_reader = undefined,
        .encrypted_writer = undefined,
        .reader = undefined,
        .writer = undefined,
    };
    const transport: Transport = .{ .state = &state };
    try state.mutex.lock();
    try std.testing.expectError(error.TlsBusy, transport.finish(.fromSeconds(1)));
    state.mutex.unlock();
    state.failed.store(true, .release);
    try std.testing.expectError(error.TlsFailed, transport.finish(.fromSeconds(1)));
    state.failed.store(false, .release);
    const expired: zio.Timeout = .{ .deadline = .fromNanoseconds(0) };
    state.encrypted_writer.timeout = expired;
    try std.testing.expectError(error.Timeout, transport.finish(.fromSeconds(1)));
    try std.testing.expectEqual(expired, state.encrypted_writer.timeout);
    try std.testing.expect(state.failed.load(.acquire));
}

test "TLS ciphertext flush to a non-reading peer keeps one absolute deadline" {
    try testBorrowedOutputFailure(false);
}

test "TLS canceled borrowed output releases mutex and cannot replay its written prefix" {
    try testBorrowedOutputFailure(true);
}

fn testBorrowedOutputFailure(cancel_output: bool) !void {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var listener = try talon.TcpListener.listen(try zio.net.IpAddress.parseIp4("127.0.0.1", 0), .{});
    defer listener.close();
    const F = struct {
        fn connect(address: zio.net.IpAddress) !zio.net.Stream {
            return address.connect(.{});
        }
        fn flush(state: *State, result: *?std.Io.Writer.Error) void {
            const transport: Transport = .{ .state = state };
            transport.flush() catch |err| {
                result.* = err;
            };
        }
    };
    var task = try zio.spawn(F.connect, .{listener.server.socket.address.ip});
    defer task.cancel();
    const raw = try listener.accept();
    defer raw.close();
    const peer = try task.join();
    defer peer.close();
    try raw.stream.socket.setSendBufferSize(4096);
    try peer.socket.setReceiveBufferSize(4096);
    const output = c.BIO_new(c.BIO_s_mem()) orelse return error.TlsSessionFailed;
    defer _ = c.BIO_free(output);
    const bytes = try std.testing.allocator.alloc(u8, 4 * 1024 * 1024);
    defer std.testing.allocator.free(bytes);
    @memset(bytes, 'x');
    try std.testing.expectEqual(@as(c_int, @intCast(bytes.len)), c.BIO_write(output, bytes.ptr, @intCast(bytes.len)));
    var state: State = .{
        .gpa = std.testing.allocator,
        .raw = raw,
        .ssl = undefined,
        .input = undefined,
        .output = output,
        .encrypted_reader = undefined,
        .encrypted_writer = undefined,
        .reader = undefined,
        .writer = undefined,
    };
    state.encrypted_writer = raw.writer(&.{});
    state.writer = .{ .vtable = &.{ .drain = State.drain }, .buffer = &.{}, .end = 0 };
    const started = zio.Timestamp.now(.monotonic);
    const deadline = closeDeadline(if (cancel_output) .fromSeconds(2) else .fromMilliseconds(30), .fromSeconds(60));
    state.encrypted_writer.setTimeout(deadline);
    if (cancel_output) {
        var result: ?std.Io.Writer.Error = null;
        var sender = try zio.spawn(F.flush, .{ &state, &result });
        defer sender.cancel();
        try zio.sleep(.fromMilliseconds(30));
        sender.cancel();
        try std.testing.expectEqual(error.WriteFailed, result.?);
        try std.testing.expectEqual(error.Canceled, state.encrypted_writer.err.?);
        try std.testing.expect(state.mutex.tryLock());
        state.mutex.unlock();
    } else {
        try std.testing.expectError(error.WriteFailed, state.flushOutput());
        try std.testing.expectEqual(error.Timeout, state.encrypted_writer.err.?);
    }
    try std.testing.expectEqual(deadline, state.encrypted_writer.timeout);
    try std.testing.expect(c.BIO_ctrl_pending(output) != 0);
    try std.testing.expect(state.failed.load(.acquire));
    try std.testing.expectError(error.TlsFailed, state.flushOutput());
    try std.testing.expectEqual(bytes.len, c.BIO_ctrl_pending(output));
    const transport: Transport = .{ .state = &state };
    try std.testing.expectError(error.WriteFailed, transport.flush());
    try std.testing.expectError(error.WriteFailed, transport.ioWriter().writeAll("must not replay"));
    try std.testing.expectError(error.TlsFailed, transport.closeNotify());
    try std.testing.expectError(error.TlsFailed, transport.finish(.fromSeconds(1)));
    var scratch: [4096]u8 = undefined;
    try std.testing.expectError(error.TlsFailed, state.readPlain(&scratch));
    var received: usize = 0;
    while (true) {
        const n = peer.read(&scratch, .fromMilliseconds(20)) catch |err| {
            if (err == error.Timeout) break;
            return err;
        };
        try std.testing.expect(n != 0 and std.mem.allEqual(u8, scratch[0..n], 'x'));
        received += n;
    }
    try std.testing.expect(received > 0 and received < bytes.len);
    try std.testing.expectError(error.Timeout, peer.read(&scratch, .fromMilliseconds(10)));
    try std.testing.expect(started.durationTo(zio.Timestamp.now(.monotonic)).toMilliseconds() < 1000);
}

test "TLS borrowed output rejects non-clearing memory BIO before raw I/O" {
    const output = c.BIO_new(c.BIO_s_mem()) orelse return error.TlsSessionFailed;
    defer _ = c.BIO_free(output);
    try std.testing.expectEqual(@as(c_int, 3), c.BIO_write(output, "old", 3));
    for ([_]c_int{ c.BIO_FLAGS_MEM_RDONLY, c.BIO_FLAGS_NONCLEAR_RST }) |flag| {
        var state: State = .{
            .gpa = std.testing.allocator,
            .raw = undefined,
            .ssl = undefined,
            .input = undefined,
            .output = output,
            .encrypted_reader = undefined,
            .encrypted_writer = undefined,
            .reader = undefined,
            .writer = undefined,
        };
        c.BIO_set_flags(output, flag);
        defer c.BIO_clear_flags(output, flag);
        try std.testing.expectError(error.TlsFailed, state.flushOutput());
        try std.testing.expect(state.failed.load(.acquire));
    }
    try std.testing.expectEqual(@as(usize, 3), c.BIO_ctrl_pending(output));
}

test "TLS window and State allocation failures leave raw caller-owned" {
    var context: Context = undefined;
    var adapter: Adapter = .{ .context = &context };
    try std.testing.expectError(error.InvalidBufferSize, adapter.wrapWithOptions(undefined, .{
        .allocator = std.testing.allocator,
        .deadline = .none,
        .read_buffer_size = 0,
    }));
    for (0..2) |index| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = index });
        try std.testing.expectError(error.OutOfMemory, adapter.wrapWithOptions(undefined, .{
            .allocator = failing.allocator(),
            .deadline = .none,
            .read_buffer_size = 66560,
        }));
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        try std.testing.expectEqual(index, failing.allocations);
    }
}

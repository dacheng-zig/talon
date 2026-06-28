//! Minimal cookie jar (RFC 6265 safe subset).
//!
//! Scope (YAGNI — the design's "minimal correct subset"): host-keyed storage of
//! `name=value` pairs, honoring the two attributes that carry security/lifetime
//! meaning: `Secure` and expiry (`Max-Age`/`Expires`). `Domain`/`Path`/`SameSite`
//! are still parsed off and ignored; cookies are stored host-only and sent back
//! only to the exact host that set them. Full RFC 6265 (domain matching, path
//! scoping) remains out of scope — a complete jar belongs in an upstream package.
//!
//! Security:
//!   - Host-keying means host A's cookies are never retrieved for host B.
//!     `Set-Cookie` is filed under the origin that actually produced the
//!     response (the final hop after any redirect), so a cross-origin redirect
//!     to B files B's cookies under B, not the initial host A. Combined with the
//!     redirect policy stripping `Cookie` on a cross-origin hop (docs §15.4),
//!     credentials cannot leak across origins.
//!   - `Secure` cookies are sent only over HTTPS: `cookieHeader` takes the
//!     request scheme and skips Secure entries on a plaintext request, so an
//!     https-set credential cannot leak over a later http:// request to the same
//!     host (a real downgrade path, since this client follows https→http).
//!   - Expiry/deletion are honored: a `Max-Age`/`Expires` in the past (the
//!     standard logout idiom, e.g. `Set-Cookie: sid=; Max-Age=0`) removes the
//!     entry instead of resurrecting it as an empty value, and expired entries
//!     are never sent and are reaped lazily — so a revoked session cookie stops
//!     being replayed.
//!
//! Lifetime: the jar is owned by the caller (create it, pass `&jar` via
//! `Client.Options.cookie_jar`, `defer jar.deinit()`); the Client only borrows
//! it. Shared across request coroutines, so access is guarded by a spin lock
//! (O(1) critical sections, same discipline as the buffer/connection pools).

const std = @import("std");
const zio = @import("zio");

pub const SpinLock = @import("../../core/buffer_pool.zig").SpinLock;

/// Current wall-clock time in unix seconds. Cookie expiry is wall-clock (not
/// monotonic); same clock source as the server's Date header (response_encode).
fn nowSeconds() i64 {
    return @intCast(@divFloor(zio.Timestamp.now(.realtime).toNanoseconds(), std.time.ns_per_s));
}

/// Separates host from cookie name in the flat storage key. Neither a hostname
/// nor a cookie name may contain NUL, so it is an unambiguous delimiter.
const sep = '\x00';

/// A stored cookie: its value plus the two attributes that change whether/when
/// it may be sent. `value` and the map key are heap-owned.
const Entry = struct {
    value: []const u8,
    secure: bool,
    /// Absolute expiry, unix seconds; null = session cookie (no expiry).
    expires_at: ?i64,
};

pub const CookieJar = struct {
    gpa: std.mem.Allocator,
    mutex: SpinLock = .{},
    /// Flat map: key = "host\x00name" (owned), value = `Entry` (value owned).
    entries: std.StringHashMap(Entry),

    pub fn init(gpa: std.mem.Allocator) CookieJar {
        return .{ .gpa = gpa, .entries = std.StringHashMap(Entry).init(gpa) };
    }

    pub fn deinit(self: *CookieJar) void {
        var it = self.entries.iterator();
        while (it.next()) |e| {
            self.gpa.free(e.key_ptr.*);
            self.gpa.free(e.value_ptr.value);
        }
        self.entries.deinit();
    }

    /// Stores a `Set-Cookie` value under `host`. Best-effort: a malformed header
    /// or an allocation failure is silently dropped (a cookie that can't be
    /// stored must never crash or wedge the request). An already-expired cookie
    /// (`Max-Age<=0` or a past `Expires`) removes any existing entry for that
    /// name — the standard deletion idiom — rather than storing it.
    pub fn store(self: *CookieJar, host: []const u8, set_cookie: []const u8) void {
        // Take the cookie-pair (everything before the first attribute ';').
        const pair_end = std.mem.indexOfScalar(u8, set_cookie, ';') orelse set_cookie.len;
        const pair = set_cookie[0..pair_end];
        const eq = std.mem.indexOfScalar(u8, pair, '=') orelse return;
        const name = std.mem.trim(u8, pair[0..eq], " \t");
        const value = std.mem.trim(u8, pair[eq + 1 ..], " \t");
        if (name.len == 0) return;

        const attrs = if (pair_end < set_cookie.len) set_cookie[pair_end + 1 ..] else "";
        const parsed = parseAttrs(attrs);

        var key_buf: [512]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{s}{c}{s}", .{ host, sep, name }) catch return;

        self.mutex.lock();
        defer self.mutex.unlock();

        // Deletion: an expired cookie removes any prior entry for this name and
        // stores nothing (otherwise a logout `Set-Cookie: sid=; Max-Age=0` would
        // resurrect `sid` as an empty value that we keep replaying).
        if (parsed.expired) {
            if (self.entries.fetchRemove(key)) |kv| {
                self.gpa.free(kv.key);
                self.gpa.free(kv.value.value);
            }
            return;
        }

        const new_value = self.gpa.dupe(u8, value) catch return;
        const entry: Entry = .{ .value = new_value, .secure = parsed.secure, .expires_at = parsed.expires_at };
        // Own the key before getOrPut: a getOrPut that inserts a slot then fails
        // its follow-up dupe cannot truly roll back (`remove` of an
        // undefined-keyed slot mismatches, poisoning the map for deinit). Look
        // up first; only dupe the key on a genuine insert.
        if (self.entries.getEntry(key)) |existing| {
            self.gpa.free(existing.value_ptr.value); // replace prior value for this name
            existing.value_ptr.* = entry;
            return;
        }
        const owned_key = self.gpa.dupe(u8, key) catch {
            self.gpa.free(new_value);
            return;
        };
        self.entries.put(owned_key, entry) catch {
            self.gpa.free(owned_key);
            self.gpa.free(new_value);
            return;
        };
    }

    /// Builds the `Cookie` header value ("n1=v1; n2=v2") for `host` into `buf`.
    /// `is_https` is the request scheme: `Secure` cookies are skipped on a
    /// plaintext request so they never ride an http:// connection. Expired
    /// entries are skipped (and lazily removed). Returns the slice, or null if
    /// the host has no sendable cookies or they don't fit (in which case none
    /// are sent — never a truncated/corrupt header).
    pub fn cookieHeader(self: *CookieJar, host: []const u8, is_https: bool, buf: []u8) ?[]const u8 {
        const now = nowSeconds();

        self.mutex.lock();
        defer self.mutex.unlock();

        var w: std.Io.Writer = .fixed(buf);
        var count: usize = 0;
        var it = self.entries.iterator();
        while (it.next()) |e| {
            const key = e.key_ptr.*;
            // Match keys of the form "<host>\x00<name>".
            if (key.len <= host.len + 1) continue;
            if (key[host.len] != sep) continue;
            if (!std.mem.eql(u8, key[0..host.len], host)) continue;
            // Secure cookies never ride a plaintext request.
            if (e.value_ptr.secure and !is_https) continue;
            // Expired entries are not sent (lazy reap happens below, outside the
            // iterator to avoid invalidating it).
            if (e.value_ptr.expires_at) |exp| if (exp <= now) continue;
            const name = key[host.len + 1 ..];
            if (count != 0) w.writeAll("; ") catch return null;
            w.writeAll(name) catch return null;
            w.writeByte('=') catch return null;
            w.writeAll(e.value_ptr.value) catch return null;
            count += 1;
        }
        self.reapExpired(now);
        if (count == 0) return null;
        return w.buffered();
    }

    /// Removes entries whose expiry has passed. Caller holds the lock. Collect
    /// keys first, then remove — mutating the map mid-iteration is unsafe.
    fn reapExpired(self: *CookieJar, now: i64) void {
        var victims: [16][]const u8 = undefined;
        var n: usize = 0;
        var it = self.entries.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.expires_at) |exp| {
                if (exp <= now) {
                    if (n == victims.len) break; // reap the rest on a later call
                    victims[n] = e.key_ptr.*;
                    n += 1;
                }
            }
        }
        for (victims[0..n]) |k| {
            if (self.entries.fetchRemove(k)) |kv| {
                self.gpa.free(kv.key);
                self.gpa.free(kv.value.value);
            }
        }
    }
};

const ParsedAttrs = struct {
    secure: bool = false,
    /// Absolute expiry (unix seconds); null = session cookie.
    expires_at: ?i64 = null,
    /// Already expired at store time (Max-Age<=0 or past Expires) → delete.
    expired: bool = false,
};

/// Parses the attribute portion of a Set-Cookie (after the `name=value` pair),
/// extracting `Secure` and the effective expiry. Per RFC 6265 §5.3 `Max-Age`
/// takes precedence over `Expires`. Unknown attributes are ignored.
fn parseAttrs(attrs: []const u8) ParsedAttrs {
    const now = nowSeconds();
    var secure = false;
    var max_age: ?i64 = null;
    var expires: ?i64 = null;

    var it = std.mem.splitScalar(u8, attrs, ';');
    while (it.next()) |raw| {
        const attr = std.mem.trim(u8, raw, " \t");
        if (attr.len == 0) continue;
        const aeq = std.mem.indexOfScalar(u8, attr, '=');
        const aname = if (aeq) |i| attr[0..i] else attr;
        const aval = if (aeq) |i| std.mem.trim(u8, attr[i + 1 ..], " \t") else "";
        if (std.ascii.eqlIgnoreCase(aname, "secure")) {
            secure = true;
        } else if (std.ascii.eqlIgnoreCase(aname, "max-age")) {
            max_age = std.fmt.parseInt(i64, aval, 10) catch null;
        } else if (std.ascii.eqlIgnoreCase(aname, "expires")) {
            expires = parseHttpDate(aval);
        }
    }

    // Max-Age wins over Expires when both are present.
    if (max_age) |ma| {
        if (ma <= 0) return .{ .secure = secure, .expired = true };
        return .{ .secure = secure, .expires_at = now + ma };
    }
    if (expires) |exp| {
        if (exp <= now) return .{ .secure = secure, .expired = true };
        return .{ .secure = secure, .expires_at = exp };
    }
    return .{ .secure = secure }; // session cookie
}

fn monthFromName(s: []const u8) ?i64 {
    const names = [_][]const u8{ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
    for (names, 1..) |m, i| {
        if (std.ascii.eqlIgnoreCase(s, m)) return @intCast(i);
    }
    return null;
}

/// Days since the unix epoch for a civil (proleptic Gregorian) date. Howard
/// Hinnant's `days_from_civil`; valid for any year, exact integer arithmetic.
fn daysFromCivil(y_in: i64, m: i64, d: i64) i64 {
    const y = if (m <= 2) y_in - 1 else y_in;
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400; // [0, 399]
    const mp = @mod(m + 9, 12); // Mar=0 … Feb=11
    const doy = @divFloor(153 * mp + 2, 5) + d - 1; // [0, 365]
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy; // [0, 146096]
    return era * 146097 + doe - 719468;
}

/// Parses an IMF-fixdate ("Sun, 06 Nov 1994 08:49:37 GMT", the format modern
/// servers send) to unix seconds. Best-effort: obsolete RFC 850 / asctime forms
/// return null and the cookie is treated as a session cookie.
fn parseHttpDate(s: []const u8) ?i64 {
    var it = std.mem.tokenizeAny(u8, s, " \t");
    _ = it.next() orelse return null; // weekday ("Sun,")
    const day = std.fmt.parseInt(i64, it.next() orelse return null, 10) catch return null;
    const month = monthFromName(it.next() orelse return null) orelse return null;
    const year = std.fmt.parseInt(i64, it.next() orelse return null, 10) catch return null;
    const time_s = it.next() orelse return null;

    var tit = std.mem.splitScalar(u8, time_s, ':');
    const hh = std.fmt.parseInt(i64, tit.next() orelse return null, 10) catch return null;
    const mm = std.fmt.parseInt(i64, tit.next() orelse return null, 10) catch return null;
    const ss = std.fmt.parseInt(i64, tit.next() orelse return null, 10) catch return null;
    if (day < 1 or day > 31 or hh > 23 or mm > 59 or ss > 60) return null;

    return daysFromCivil(year, month, day) * 86400 + hh * 3600 + mm * 60 + ss;
}

// ── Tests ────────────────────────────────────────────────────────────────

test "CookieJar: store then retrieve per host" {
    var jar = CookieJar.init(std.testing.allocator);
    defer jar.deinit();

    jar.store("a.test", "sid=abc; Path=/; HttpOnly");
    jar.store("a.test", "theme=dark");
    jar.store("b.test", "other=1");

    var buf: [256]u8 = undefined;
    const a = jar.cookieHeader("a.test", false, &buf).?;
    // Both a.test cookies present (order is map-iteration-dependent).
    try std.testing.expect(std.mem.indexOf(u8, a, "sid=abc") != null);
    try std.testing.expect(std.mem.indexOf(u8, a, "theme=dark") != null);
    // b.test's cookie must not appear under a.test.
    try std.testing.expect(std.mem.indexOf(u8, a, "other") == null);
}

test "CookieJar: overwrite same name, unknown host is null" {
    var jar = CookieJar.init(std.testing.allocator);
    defer jar.deinit();

    jar.store("h", "k=v1");
    jar.store("h", "k=v2"); // replaces v1
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("k=v2", jar.cookieHeader("h", false, &buf).?);
    try std.testing.expect(jar.cookieHeader("nope", false, &buf) == null);
}

test "CookieJar: malformed Set-Cookie is ignored" {
    var jar = CookieJar.init(std.testing.allocator);
    defer jar.deinit();

    jar.store("h", "novalue"); // no '='
    jar.store("h", "=v"); // empty name
    var buf: [64]u8 = undefined;
    try std.testing.expect(jar.cookieHeader("h", false, &buf) == null);
}

test "CookieJar: Secure cookie withheld over plaintext, sent over https" {
    var jar = CookieJar.init(std.testing.allocator);
    defer jar.deinit();

    jar.store("a.test", "sid=secret; Secure; HttpOnly");
    jar.store("a.test", "plain=1");

    var buf: [128]u8 = undefined;
    // http request: Secure cookie withheld, plain one sent.
    const over_http = jar.cookieHeader("a.test", false, &buf).?;
    try std.testing.expect(std.mem.indexOf(u8, over_http, "sid=secret") == null);
    try std.testing.expect(std.mem.indexOf(u8, over_http, "plain=1") != null);
    // https request: both present.
    var buf2: [128]u8 = undefined;
    const over_https = jar.cookieHeader("a.test", true, &buf2).?;
    try std.testing.expect(std.mem.indexOf(u8, over_https, "sid=secret") != null);
}

test "CookieJar: Max-Age=0 deletes an existing cookie" {
    var jar = CookieJar.init(std.testing.allocator);
    defer jar.deinit();

    jar.store("h", "sid=live");
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("sid=live", jar.cookieHeader("h", true, &buf).?);

    // Logout: server clears the cookie.
    jar.store("h", "sid=; Max-Age=0");
    try std.testing.expect(jar.cookieHeader("h", true, &buf) == null);
}

test "CookieJar: expired Expires is not stored; future is honored" {
    var jar = CookieJar.init(std.testing.allocator);
    defer jar.deinit();

    jar.store("h", "old=1; Expires=Thu, 01 Jan 1970 00:00:00 GMT");
    jar.store("h", "new=1; Expires=Mon, 01 Jan 2300 00:00:00 GMT");
    var buf: [128]u8 = undefined;
    const hdr = jar.cookieHeader("h", true, &buf).?;
    try std.testing.expect(std.mem.indexOf(u8, hdr, "old=1") == null);
    try std.testing.expect(std.mem.indexOf(u8, hdr, "new=1") != null);
}

test "parseHttpDate: IMF-fixdate epoch and a known date" {
    try std.testing.expectEqual(@as(?i64, 0), parseHttpDate("Thu, 01 Jan 1970 00:00:00 GMT"));
    // 2026-06-11 08:30:00 UTC (matches response_encode formatHttpDate test).
    try std.testing.expectEqual(@as(?i64, 1781166600), parseHttpDate("Thu, 11 Jun 2026 08:30:00 GMT"));
    try std.testing.expect(parseHttpDate("garbage") == null);
}

//! IP reference identities and bounded iPAddress SAN verification. No DNS/IO.
const std = @import("std");
const zio = @import("zio");

pub const Ip = union(enum) {
    ipv4: [4]u8,
    ipv6: [16]u8,

    /// Classify with the same literal parser used by the TCP dialer.
    pub fn parse(host: []const u8) ?Ip {
        const address = zio.net.IpAddress.parseIp(host, 0) catch return null;
        return switch (address.getFamily()) {
            .ipv4 => .{ .ipv4 = @bitCast(address.in.addr) },
            .ipv6 => .{ .ipv6 = address.in6.addr },
        };
    }

    fn bytes(self: *const Ip) []const u8 {
        return switch (self.*) {
            .ipv4 => |*v| v,
            .ipv6 => |*v| v,
        };
    }
};

pub const VerifyError = error{ CertificateHostMismatch, CertificateFieldHasInvalidLength };

const Element = struct { tag: u8, value: []const u8, end: usize };

// std's Certificate.der.Element.parse assumes enough input bytes. SAN is
// attacker controlled, so bound every header, length and value here instead.
fn element(bytes: []const u8, offset: usize) VerifyError!Element {
    if (offset > bytes.len or bytes.len - offset < 2) return error.CertificateFieldHasInvalidLength;
    const tag = bytes[offset];
    var begin = offset + 2;
    const first = bytes[offset + 1];
    var length: usize = first;
    if (first & 0x80 != 0) {
        const size = first & 0x7f;
        if (size == 0 or size > 4 or size > bytes.len - begin) return error.CertificateFieldHasInvalidLength;
        if (bytes[begin] == 0) return error.CertificateFieldHasInvalidLength;
        length = 0;
        for (bytes[begin..][0..size]) |b| length = (length << 8) | b;
        begin += size;
        if (length < 128) return error.CertificateFieldHasInvalidLength;
    }
    if (length > bytes.len - begin) return error.CertificateFieldHasInvalidLength;
    return .{ .tag = tag, .value = bytes[begin..][0..length], .end = begin + length };
}

/// SAN must be a complete DER GeneralNames SEQUENCE. An IP match never uses
/// dNSName/CN or wildcard strings, and must not hide malformed later entries.
pub fn verifySan(ip: Ip, san: []const u8) VerifyError!void {
    if (san.len == 0) return error.CertificateHostMismatch;
    const names = try element(san, 0);
    if (names.tag != 0x30 or names.end != san.len) return error.CertificateFieldHasInvalidLength;
    var matched = false;
    var offset: usize = 0;
    while (offset < names.value.len) {
        const name = try element(names.value, offset);
        offset = name.end;
        if (name.tag & 0xc0 != 0x80 or name.tag & 0x1f > 8) return error.CertificateFieldHasInvalidLength;
        if (name.tag & 0x1f != 7) continue;
        if (name.tag != 0x87 or (name.value.len != 4 and name.value.len != 16)) return error.CertificateFieldHasInvalidLength;
        matched = matched or std.mem.eql(u8, ip.bytes(), name.value);
    }
    if (!matched) return error.CertificateHostMismatch;
}

test "TLS identity: literal classification and IPv6 text normalization" {
    try std.testing.expectEqualSlices(u8, &.{ 127, 0, 0, 1 }, &Ip.parse("127.0.0.1").?.ipv4);
    try std.testing.expectEqualDeep(Ip.parse("2001:DB8::1").?, Ip.parse("2001:db8:0:0:0:0:0:1").?);
    try std.testing.expect(Ip.parse("localhost") == null);
    try std.testing.expect(Ip.parse("[::1]") == null);
}

test "TLS identity: exact IPv4/IPv6 SANs and multiple entries" {
    const san = [_]u8{ 0x30, 0x18, 0x87, 4, 127, 0, 0, 1, 0x87, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };
    try verifySan(Ip.parse("127.0.0.1").?, &san);
    try verifySan(Ip.parse("::1").?, &san);
    try std.testing.expectError(error.CertificateHostMismatch, verifySan(Ip.parse("127.0.0.2").?, &san));
    try std.testing.expectError(error.CertificateHostMismatch, verifySan(Ip.parse("::2").?, &san));
}

test "TLS identity: DNS strings and missing SAN cannot authenticate IP" {
    const ip = Ip.parse("127.0.0.1").?;
    try std.testing.expectError(error.CertificateHostMismatch, verifySan(ip, "\x30\x0b\x82\x09127.0.0.1"));
    try std.testing.expectError(error.CertificateHostMismatch, verifySan(ip, &.{}));
    try std.testing.expectError(error.CertificateHostMismatch, verifySan(ip, &.{ 0x30, 0 }));
}

test "TLS identity: IPv4 and mapped IPv6 are different SAN byte identities" {
    const san = [_]u8{ 0x30, 6, 0x87, 4, 127, 0, 0, 1 };
    try std.testing.expectError(error.CertificateHostMismatch, verifySan(Ip.parse("::ffff:7f00:1").?, &san));
}

test "TLS identity: bounded DER rejects every truncated matching SAN prefix" {
    const san = [_]u8{ 0x30, 6, 0x87, 4, 127, 0, 0, 1 };
    const ip = Ip.parse("127.0.0.1").?;
    for (1..san.len) |length| try std.testing.expectError(error.CertificateFieldHasInvalidLength, verifySan(ip, san[0..length]));
}

test "TLS identity: malformed lengths/tags and trailing corruption fail closed" {
    const ip = Ip.parse("127.0.0.1").?;
    const cases = [_][]const u8{
        &.{ 0x30, 0x80 }, // indefinite length
        &.{ 0x30, 0x81, 0 }, // noncanonical long length
        &.{ 0x30, 0x84, 0xff, 0xff, 0xff, 0xff },
        &.{ 0x30, 0x82, 1 }, // truncated length header
        &.{ 0x31, 6, 0x87, 4, 127, 0, 0, 1 },
        &.{ 0x30, 6, 0x07, 4, 127, 0, 0, 1 },
        &.{ 0x30, 3, 0x87, 1, 127 },
        &.{ 0x30, 6, 0xa7, 4, 127, 0, 0, 1 },
        &.{ 0x30, 2, 0x89, 0 },
        &.{ 0x30, 7, 0x87, 4, 127, 0, 0, 1, 0x87 },
        &.{ 0x30, 6, 0x87, 4, 127, 0, 0, 1, 0 },
    };
    for (cases) |san| try std.testing.expectError(error.CertificateFieldHasInvalidLength, verifySan(ip, san));
}

test "TLS identity: canonical long-form GeneralNames with a later IP match" {
    var san: [140]u8 = undefined;
    san[0..5].* = .{ 0x30, 0x81, 137, 0x82, 127 };
    @memset(san[5..132], 'a');
    san[132..].* = .{ 0x87, 4, 127, 0, 0, 1, 0x82, 0 };
    try verifySan(Ip.parse("127.0.0.1").?, &san);
}

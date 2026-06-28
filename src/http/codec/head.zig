//! Shared head-end scanner. Finds the CRLFCRLF
//! terminator in the buffered window, refilling as needed. Both the server
//! request loop and the client response round-trip used a byte-identical copy
//! of this — with the subtle `search_start` rewind that must straddle refill
//! boundaries — so it lives here once (DRY).
//!
//! PRECONDITION (security-critical): the reader's buffer MUST be strictly
//! larger than `max_header_size`. Then a head exceeding the limit grows the
//! window past `max_header_size` and trips the `> max_header_size` guard
//! *before* the buffer fills. If the buffer were sized exactly to
//! `max_header_size`, a peer could fill it with non-terminator bytes; the next
//! `fillMore` on a full buffer hits std's rebase assert (panic in safe builds)
//! or spins returning 0 (ReleaseFast). Call sites (stream_server read pool,
//! client connection) size their buffers with slack to honor this.

const std = @import("std");

pub const HeadEndError = error{
    HeadersTooLarge,
    /// Peer closed before sending anything. Server: a clean keep-alive end.
    /// Client: the server closed before responding.
    CleanClose,
    /// Peer closed mid-head.
    TruncatedHead,
    ReadFailed,
};

/// Returns the head length INCLUDING the final "\r\n\r\n".
///
/// `pending` (nullable) is flushed before any blocking refill: the peer may be
/// waiting on queued output before it sends the rest of the head, so never
/// park on read with unflushed bytes (server write path; client passes null).
pub fn findHeadEnd(
    r: *std.Io.Reader,
    max_header_size: usize,
    pending: ?*std.Io.Writer,
) HeadEndError!usize {
    var search_start: usize = 0;
    while (true) {
        const window = r.buffered();
        if (std.mem.indexOfPos(u8, window, search_start, "\r\n\r\n")) |idx| {
            const head_len = idx + 4;
            if (head_len > max_header_size) return error.HeadersTooLarge;
            return head_len;
        }
        if (window.len > max_header_size) return error.HeadersTooLarge;
        search_start = window.len -| 3;
        if (pending) |w| w.flush() catch return error.ReadFailed;
        r.fillMore() catch |err| switch (err) {
            error.EndOfStream => return if (r.bufferedLen() == 0)
                error.CleanClose
            else
                error.TruncatedHead,
            error.ReadFailed => return error.ReadFailed,
        };
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

test "findHeadEnd: locates terminator across refills" {
    var r: std.Io.Reader = .fixed("GET / HTTP/1.1\r\nHost: h\r\n\r\nBODY");
    const len = try findHeadEnd(&r, 1024, null);
    try std.testing.expectEqual(27, len);
    try std.testing.expectEqualStrings("GET / HTTP/1.1\r\nHost: h\r\n\r\n", r.buffered()[0..len]);
}

test "findHeadEnd: oversized head rejected, clean close detected" {
    const big = [_]u8{'a'} ** 128;
    var r: std.Io.Reader = .fixed(&big);
    try std.testing.expectError(error.HeadersTooLarge, findHeadEnd(&r, 64, null));

    var r2: std.Io.Reader = .fixed("");
    try std.testing.expectError(error.CleanClose, findHeadEnd(&r2, 64, null));

    var r3: std.Io.Reader = .fixed("GET / HT");
    try std.testing.expectError(error.TruncatedHead, findHeadEnd(&r3, 64, null));
}

test "findHeadEnd: head larger than max (buffer has slack) is HeadersTooLarge" {
    // Mirrors the call-site invariant: buffer strictly larger than the limit,
    // so an oversized head trips `> max_header_size` before the buffer fills.
    var backing: [128]u8 = @splat('a'); // 128-byte buffer (slack over max=64)
    var r: std.Io.Reader = .fixed(&backing);
    try std.testing.expectError(error.HeadersTooLarge, findHeadEnd(&r, 64, null));
}

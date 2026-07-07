//! Server-side SSE response helper: a thin layer over the chunked writer that
//! serializes events and flushes each one to the socket immediately.
//!
//! The default headers close the two production traps in one move: nginx
//! response buffering (`x-accel-buffering: no`) and cache misclassification
//! (`cache-control: no-cache`). Heartbeats are cooperative — call `comment`
//! from the produce loop; there is no background timer contending for the
//! connection writer.

const std = @import("std");
const encode = @import("../codec/response_encode.zig");
const sse = @import("../codec/sse.zig");

pub const Event = sse.Event;

/// Headers every event stream carries. Content-Type marks the stream, no-cache
/// keeps intermediaries from storing it, x-accel-buffering tells nginx to relay
/// bytes instead of buffering the (never-ending) response.
pub const default_headers = [_]@import("../codec/codec.zig").Header{
    .{ .name = "content-type", .value = "text/event-stream" },
    .{ .name = "cache-control", .value = "no-cache" },
    .{ .name = "x-accel-buffering", .value = "no" },
};

pub const SendError = sse.SerializeError;

pub const EventStream = struct {
    chunked: encode.ChunkedBodyWriter,

    /// Serializes one event and pushes it to the client. Returns a write error
    /// (surfaced from the flush) when the peer has gone away — the produce loop
    /// should treat that as disconnect and stop.
    pub fn send(self: *EventStream, event: Event) SendError!void {
        try sse.serialize(&self.chunked.interface, event);
        try self.flush();
    }

    /// Writes a comment line (`: text`) and flushes — the standard heartbeat.
    /// It reaches the peer (so a dead connection surfaces as a write error) but
    /// dispatches no client-side event.
    pub fn comment(self: *EventStream, text: []const u8) SendError!void {
        try sse.serialize(&self.chunked.interface, .{ .comment = text });
        try self.flush();
    }

    /// Ends the stream with the chunked terminator. Optional: dropping the
    /// connection is also a valid end, and the client will reconnect.
    pub fn finish(self: *EventStream) !void {
        try self.chunked.finish();
        try self.chunked.out.flush();
    }

    fn flush(self: *EventStream) !void {
        try self.chunked.interface.flush(); // emit the chunk into the conn buffer
        try self.chunked.out.flush(); // push the conn buffer to the socket
    }
};

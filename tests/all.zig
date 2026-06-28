//! Integration / non-unit test aggregator. These tests live outside src/ so
//! the library source stays focused on business logic; they drive talon only
//! through its public module surface. Run via `zig build test`.

test {
    _ = @import("http_server_test.zig");
    _ = @import("http_client_test.zig");
    _ = @import("http_client_tls_test.zig");
    _ = @import("stream_server_test.zig");
    _ = @import("parser_fuzz_test.zig");
    _ = @import("response_parser_fuzz_test.zig");
    _ = @import("body_test.zig");
    _ = @import("body_fuzz_test.zig");
    _ = @import("listener_test.zig");
}

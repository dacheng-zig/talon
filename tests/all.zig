//! Integration / non-unit test aggregator. These tests live outside src/ so
//! the library source stays focused on business logic; they drive talon only
//! through its public module surface. Run via `zig build test`.

test {
    _ = @import("http_server_test.zig");
    _ = @import("response_test.zig");
    _ = @import("limits_test.zig");
    _ = @import("timeout_capabilities_test.zig");
    _ = @import("http_client_test.zig");
    _ = @import("pool_budget_test.zig");
    _ = @import("http_client_tls_test.zig");
    _ = @import("stream_server_test.zig");
    _ = @import("server_stats_test.zig");
    _ = @import("transport_adapter_test.zig");
    _ = @import("owned_framing_test.zig");
    _ = @import("dial_test.zig");
    _ = @import("tcp_user_timeout_test.zig");
    _ = @import("allocation_budget_test.zig");
    _ = @import("budget_pressure_test.zig");
    _ = @import("http_arena_budget_test.zig");
    _ = @import("head_error_test.zig");
    _ = @import("idle_reclaim_test.zig");
    _ = @import("parser_fuzz_test.zig");
    _ = @import("response_parser_fuzz_test.zig");
    _ = @import("body_test.zig");
    _ = @import("body_fuzz_test.zig");
    _ = @import("sse_fuzz_test.zig");
    _ = @import("sse_test.zig");
    _ = @import("upgrade_test.zig");
    _ = @import("ws_test.zig");
    _ = @import("ws_concurrency_test.zig");
    _ = @import("ws_send_budget_test.zig");
    _ = @import("ws_fragment_send_test.zig");
    _ = @import("ws_shutdown_test.zig");
    _ = @import("ws_client_test.zig");
    _ = @import("ws_fuzz_test.zig");
    _ = @import("listener_test.zig");
}

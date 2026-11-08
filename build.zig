const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zio_dep = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });
    const zio_mod = zio_dep.module("zio");

    // talon: the single exported module. The protocol-agnostic engine
    // (src/core/, surfaced as `talon.core` + flattened top-level aliases) and
    // the HTTP package (`talon.http`) are pulled in by relative import, not a
    // separately published module.
    const talon_mod = b.addModule("talon", .{
        .root_source_file = b.path("src/talon.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "zio", .module = zio_mod },
        },
    });

    if (b.option(bool, "tls-server", "Enable optional OpenSSL server module and probe") orelse false) {
        const include_dir = std.mem.trim(u8, b.run(&.{ "pkg-config", "--variable=includedir", "openssl" }), " \r\n");
        const translated = b.addTranslateC(.{ .root_source_file = b.path("src/tls_openssl.h"), .target = target, .optimize = optimize, .link_libc = true });
        translated.addSystemIncludePath(.{ .cwd_relative = include_dir });
        const openssl_mod = translated.createModule();
        const tls_mod = b.addModule("talon_tls", .{
            .root_source_file = b.path("src/tls_server.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{ .{ .name = "zio", .module = zio_mod }, .{ .name = "talon", .module = talon_mod }, .{ .name = "openssl", .module = openssl_mod } },
            .link_libc = true,
        });
        tls_mod.linkSystemLibrary("ssl", .{ .use_pkg_config = .force });
        tls_mod.linkSystemLibrary("crypto", .{ .use_pkg_config = .force });
        tls_mod.addCSourceFile(.{ .file = b.path("src/tls_openssl.c"), .flags = &.{} });
        const probe = b.addExecutable(.{
            .name = "tls_server_probe",
            .root_module = b.createModule(.{ .root_source_file = b.path("tests/tls_server_probe.zig"), .target = target, .optimize = optimize, .imports = &.{ .{ .name = "zio", .module = zio_mod }, .{ .name = "talon", .module = talon_mod }, .{ .name = "talon_tls", .module = tls_mod } } }),
        });
        b.step("tls-server-probe", "Build the optional real TLS server probe").dependOn(&b.addInstallArtifact(probe, .{}).step);
        b.step("tls-server-test", "Test optional TLS lifecycle guards").dependOn(&b.addRunArtifact(b.addTest(.{ .root_module = tls_mod })).step);
    }

    const talon_tests = b.addTest(.{ .root_module = talon_mod });
    const run_talon_tests = b.addRunArtifact(talon_tests);

    // Integration / non-unit tests live in tests/, outside the library source,
    // and drive talon only through its public module surface.
    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/all.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zio", .module = zio_mod },
                .{ .name = "talon", .module = talon_mod },
            },
        }),
    });
    const run_integration_tests = b.addRunArtifact(integration_tests);

    const test_step = b.step("test", "Run unit and integration tests");
    const check_step = b.step("check", "Compile the default test suites without running them");
    check_step.dependOn(&talon_tests.step);
    check_step.dependOn(&integration_tests.step);
    test_step.dependOn(&run_talon_tests.step);
    test_step.dependOn(&run_integration_tests.step);

    // Independent network workload; deliberately not a default installed demo.
    const bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/http_network.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "zio", .module = zio_mod },
            .{ .name = "talon", .module = talon_mod },
        },
    });
    const bench = b.addExecutable(.{ .name = "http_network_bench", .root_module = bench_mod });
    const install_bench = b.addInstallArtifact(bench, .{});
    const bench_step = b.step("bench", "Build the TCP/TLS network benchmark");
    bench_step.dependOn(&install_bench.step);
    const bench_tests = b.addTest(.{ .root_module = bench_mod });
    check_step.dependOn(&bench_tests.step);
    const run_bench_tests = b.addRunArtifact(bench_tests);
    test_step.dependOn(&run_bench_tests.step);

    const tls_probe = b.addExecutable(.{
        .name = "tls_probe",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/tls_probe.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zio", .module = zio_mod },
                .{ .name = "talon", .module = talon_mod },
            },
        }),
    });
    const install_tls_probe = b.addInstallArtifact(tls_probe, .{});
    b.step("tls-probe", "Build the local TLS certificate acceptance probe").dependOn(&install_tls_probe.step);

    // Examples both link the single talon module; resp speaks a non-HTTP
    // protocol using only talon.core.
    const examples = [_]struct { name: []const u8, src: []const u8 }{
        .{ .name = "http", .src = "examples/http.zig" },
        .{ .name = "http_client", .src = "examples/http_client.zig" },
        .{ .name = "http_get", .src = "examples/http_get.zig" },
        .{ .name = "https_get", .src = "examples/https_get.zig" },
        .{ .name = "http_client_bench", .src = "examples/http_client_bench.zig" },
        .{ .name = "resp", .src = "examples/resp.zig" },
        .{ .name = "sse", .src = "examples/sse.zig" },
        .{ .name = "ws", .src = "examples/ws.zig" },
        .{ .name = "ws_client", .src = "examples/ws_client.zig" },
    };
    for (examples) |ex| {
        const exe = b.addExecutable(.{
            .name = ex.name,
            .root_module = b.createModule(.{
                .root_source_file = b.path(ex.src),
                .target = target,
                .optimize = optimize,
                .imports = &.{
                    .{ .name = "zio", .module = zio_mod },
                    .{ .name = "talon", .module = talon_mod },
                },
            }),
        });
        b.installArtifact(exe);
        const run_cmd = b.addRunArtifact(exe);
        run_cmd.step.dependOn(b.getInstallStep());
        // Forward args after `--`, e.g. `zig build run-http_get -- http://host/`.
        if (b.args) |args| run_cmd.addArgs(args);
        const run_step = b.step(b.fmt("run-{s}", .{ex.name}), b.fmt("Run the {s} example", .{ex.name}));
        run_step.dependOn(&run_cmd.step);
    }
}

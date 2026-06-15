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

    // Core/http boundary guard (design doc §10): compile the engine rooted at
    // src/core/ with only zio in the graph. Because Zig forbids importing files
    // outside a module's root dir, a stray core→http dependency (src/http/ is
    // outside src/core/) fails this compile. Compile-only — the engine's tests
    // run once as part of talon_tests below.
    const core_boundary = b.addObject(.{
        .name = "core-boundary",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/core/core.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zio", .module = zio_mod },
            },
        }),
    });

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
    test_step.dependOn(&core_boundary.step);
    test_step.dependOn(&run_talon_tests.step);
    test_step.dependOn(&run_integration_tests.step);

    // Examples both link the single talon module; resp speaks a non-HTTP
    // protocol using only talon.core.
    const examples = [_]struct { name: []const u8, src: []const u8 }{
        .{ .name = "http", .src = "examples/http.zig" },
        .{ .name = "resp", .src = "examples/resp.zig" },
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
        const run_step = b.step(b.fmt("run-{s}", .{ex.name}), b.fmt("Run the {s} example", .{ex.name}));
        run_step.dependOn(&run_cmd.step);
    }
}

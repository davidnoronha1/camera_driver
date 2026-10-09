const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── library module ───────────────────────────────────────────────────
    const mod = b.addModule("camera_driver", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // The repo's own example configs double as test fixtures, embedded from
    // their single source of truth rather than copied.
    const fixtures = [_][2][]const u8{
        .{ "cfg_example", "../config/example_pipeline.yaml" },
        .{ "cfg_orbbec_lucid", "../config/orbbec_lucid_pipeline.yaml" },
        .{ "cfg_debug_display", "../config/debug_display.yaml" },
    };
    for (fixtures) |f| {
        mod.addAnonymousImport(f[0], .{ .root_source_file = b.path(f[1]) });
    }

    // ── native CLI ───────────────────────────────────────────────────────
    const exe = b.addExecutable(.{
        .name = "camera-driver-plan",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "camera_driver", .module = mod }},
        }),
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.addPassthruArgs();
    b.step("run", "Run camera-driver-plan (pass args after --)").dependOn(&run_cmd.step);

    // ── wasm module (no emscripten, no WASI) ─────────────────────────────
    const wasm_target = b.resolveTargetQuery(.{ .cpu_arch = .wasm32, .os_tag = .freestanding });
    const wasm_lib = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSmall,
    });
    const wasm = b.addExecutable(.{
        .name = "camera_driver",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/wasm.zig"),
            .target = wasm_target,
            .optimize = .ReleaseSmall,
            .imports = &.{.{ .name = "camera_driver", .module = wasm_lib }},
        }),
    });
    wasm.entry = .disabled;
    wasm.rdynamic = true;
    const install_wasm = b.addInstallFileWithDir(wasm.getEmittedBin(), .{ .custom = "../web" }, "camera_driver.wasm");
    b.step("wasm", "Build web/camera_driver.wasm").dependOn(&install_wasm.step);

    // ── tests ────────────────────────────────────────────────────────────
    const tests = b.addTest(.{ .root_module = mod });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);
}

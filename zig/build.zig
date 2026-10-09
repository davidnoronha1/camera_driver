const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── library module ───────────────────────────────────────────────────
    const mod = b.addModule("camera_driver", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true, // dlopen for native plugins and the GStreamer runner
    });

    // The C plugin header, translated, so a test can check the hand-written
    // Zig mirrors in plugin_abi.zig against the real thing.
    const header = b.addTranslateC(.{
        .root_source_file = b.path("include/camera_driver_plugin.h"),
        .target = target,
        .optimize = optimize,
    });
    mod.addImport("plugin_header", header.createModule());

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
            .link_libc = true,
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

    // ── test fixtures: a toy native plugin and a stub GStreamer ──────────
    const toy = addCLib(b, "cd_toy_plugin", "test/toy_plugin.c", target, optimize);
    const stub = addCLib(b, "cd_stub_gst", "test/stub_gst.c", target, optimize);
    const cpp = b.createModule(.{ .target = target, .optimize = optimize, .link_libc = true, .link_libcpp = true });
    cpp.addIncludePath(b.path("include"));
    cpp.addCSourceFile(.{ .file = b.path("examples/cpp_plugin.cpp"), .flags = &.{ "-std=c++17", "-fvisibility=hidden" } });
    const cpp_plugin = b.addLibrary(.{ .name = "cd_example_cpp", .linkage = .dynamic, .root_module = cpp });
    const install_cpp = b.addInstallArtifact(cpp_plugin, .{});
    b.step("example-plugin", "Build the C++ example plugin (zig-out/lib/libcd_example_cpp.so)").dependOn(&install_cpp.step);
    const install_toy = b.addInstallArtifact(toy, .{});
    const install_stub = b.addInstallArtifact(stub, .{});

    // ── tests ────────────────────────────────────────────────────────────
    const tests = b.addTest(.{ .root_module = mod });
    const run_tests = b.addRunArtifact(tests);
    run_tests.step.dependOn(&install_toy.step);
    run_tests.step.dependOn(&install_cpp.step);
    run_tests.step.dependOn(&install_stub.step);
    // Tests load the fixtures installed under zig-out/lib, relative to the package.
    run_tests.setCwd(b.path("."));
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);
}

fn addCLib(
    b: *std.Build,
    name: []const u8,
    source: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Step.Compile {
    const m = b.createModule(.{ .target = target, .optimize = optimize, .link_libc = true });
    m.addIncludePath(b.path("include"));
    m.addCSourceFile(.{ .file = b.path(source), .flags = &.{ "-std=c11", "-fvisibility=hidden" } });
    return b.addLibrary(.{ .name = name, .linkage = .dynamic, .root_module = m });
}

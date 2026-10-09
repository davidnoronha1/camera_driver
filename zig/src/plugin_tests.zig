//! Integration tests for the plugin system, run against real shared libraries
//! built from test/*.c (a toy plugin and a stub GStreamer). Native only.

const std = @import("std");
const cd_exec = @import("exec.zig");
const plugin_desktop = @import("plugin_desktop.zig");
const registry_mod = @import("registry.zig");
const runner_gst = @import("runner_gst.zig");
const runner_mod = @import("runner.zig");
const session = @import("session.zig");
const manifest = @import("manifest.zig");

const c = struct {
    extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;
    extern "c" fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
    extern "c" fn unsetenv(name: [*:0]const u8) c_int;
};

fn libPath(a: std.mem.Allocator, file: []const u8) ![]const u8 {
    const dir = if (c.getenv("CD_TEST_LIB_DIR")) |d| std.mem.span(d) else "zig-out/lib";
    return std.fs.path.join(a, &.{ dir, file });
}

const toy_config =
    \\elements:
    \\  - type: CustomSrcElement
    \\    format: RGB
    \\    width: 320
    \\    height: 240
    \\  - type: ToyTag
    \\    tag: hello
    \\    gain: 3
    \\    labels: [alpha, beta]
    \\  - type: DisplayPublisher
;

test "native plugin: loads, registers, and lowers for gst" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var reg = try registry_mod.Registry.init(std.testing.allocator);
    defer reg.deinit();

    var d: plugin_desktop.Diagnostic = .{};
    try plugin_desktop.load(&reg, try libPath(a, "libcd_toy_plugin.so"), &d);

    const e = reg.find("ToyTag").?;
    try std.testing.expectEqualStrings("libcd_toy_plugin.so", e.origin);
    try std.testing.expectEqualStrings("tag", e.required[0]);
    try std.testing.expect(reg.findRunner("toy-runner") != null);

    const r = session.run(a, .{ .registry = &reg, .source = toy_config, .backend = .gst });
    try std.testing.expect(r == .gst);
    const launch = r.gst.out.launch;
    try std.testing.expect(std.mem.indexOf(u8, launch, "identity name=toy_tag_0_tag_hello_g3_l2_alpha") != null);
    // Hardware knowledge reaches the plugin.
    try std.testing.expect(std.mem.indexOf(u8, launch, "silent=true") == null);

    var found = false;
    for (r.gst.out.plan.handles) |h| {
        if (std.mem.eql(u8, h.name, "toy_handle")) found = true;
    }
    try std.testing.expect(found);

    const nv = session.run(a, .{ .registry = &reg, .source = toy_config, .hw = .{ .has_nvh264enc = true } });
    try std.testing.expect(std.mem.indexOf(u8, nv.gst.out.launch, "silent=true") != null);
}

test "native plugin: required options are enforced and warnings surface" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var reg = try registry_mod.Registry.init(std.testing.allocator);
    defer reg.deinit();
    try plugin_desktop.load(&reg, try libPath(a, "libcd_toy_plugin.so"), null);

    const missing = session.run(a, .{ .registry = &reg, .source = "elements:\n  - type: ToyTag\n" });
    try std.testing.expect(missing == .failure);
    try std.testing.expectEqualStrings("ToyTag: 'tag' is required", missing.failure.message);

    const warned = session.run(a, .{ .registry = &reg, .source = "elements:\n  - type: ToyTag\n    tag: warn\n" });
    try std.testing.expect(warned == .gst);
    try std.testing.expect(std.mem.indexOf(u8, warned.gst.out.plan.warnings[0], "tag looks suspicious") != null);
}

test "native plugin: web lowering uses the declared impl or explains the gap" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var reg = try registry_mod.Registry.init(std.testing.allocator);
    defer reg.deinit();
    try plugin_desktop.load(&reg, try libPath(a, "libcd_toy_plugin.so"), null);

    const ok = session.run(a, .{ .registry = &reg, .source = toy_config, .backend = .web });
    try std.testing.expect(ok == .web);
    try std.testing.expect(ok.web.out.ok);
    try std.testing.expect(std.mem.indexOf(u8, ok.web.out.json, "\"impl\":\"toy-tag\"") != null);

    const nw = session.run(a, .{
        .registry = &reg,
        .backend = .web,
        .source = "elements:\n  - type: CustomSrcElement\n  - type: ToyNoWeb\n",
    });
    try std.testing.expect(!nw.web.out.ok);
    try std.testing.expectEqualStrings("toy has no web version", nw.web.out.unsupported[0].reason);
}

test "native plugin: bad libraries fail with a message" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var reg = try registry_mod.Registry.init(std.testing.allocator);
    defer reg.deinit();
    var d: plugin_desktop.Diagnostic = .{};
    try std.testing.expectError(error.OpenFailed, plugin_desktop.load(&reg, "/nonexistent/libnope.so", &d));
    try std.testing.expect(std.mem.indexOf(u8, d.message, "/nonexistent/libnope.so") != null);
    // A real library with no cd_plugin_init.
    try std.testing.expectError(error.MissingInit, plugin_desktop.load(&reg, try libPath(a, "libcd_stub_gst.so"), &d));
    try std.testing.expect(std.mem.indexOf(u8, d.message, "cd_plugin_init") != null);
}

test "swappable runner: plugin runner executes the plan and element hooks run around it" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var reg = try registry_mod.Registry.init(std.testing.allocator);
    defer reg.deinit();
    const toy_path = try libPath(a, "libcd_toy_plugin.so");
    try plugin_desktop.load(&reg, toy_path, null);

    const r = session.run(a, .{ .registry = &reg, .source = toy_config });
    const g = r.gst;
    const view: runner_mod.PlanView = .{ .backend = .gst, .text = g.out.launch, .handles = g.out.plan.handles, .side_files = g.out.plan.side_files };

    const toy_runner = reg.findRunner("toy-runner").?;
    const outcome = try cd_exec.execute(a, &reg, toy_runner, view, g.out.plan.chain, .{ .write_side_files = false });
    try std.testing.expect(outcome == .finished);

    // The runner saw the same launch string the planner produced...
    var lib = try std.DynLib.open(toy_path);
    defer lib.close();
    const last_launch = lib.lookup(*const fn () callconv(.c) [*:0]const u8, "toy_last_launch").?;
    try std.testing.expectEqualStrings(g.out.launch, std.mem.span(last_launch()));
    // ...and the element lifecycle wrapped play/wait in order. The runner
    // (not the host) decides when the native pipeline exists.
    const events = lib.lookup(*const fn () callconv(.c) [*:0]const u8, "toy_events").?;
    try std.testing.expectEqualStrings(
        "create;setup;play;wait;bringdown;destroy;runner-destroy;",
        std.mem.span(events()),
    );

    // Selecting a different runner is just a different name.
    const dry = reg.findRunner("dry-run").?;
    const o2 = try cd_exec.execute(a, &reg, dry, view, g.out.plan.chain, .{ .write_side_files = false });
    try std.testing.expect(o2 == .finished);

    // A runner that cannot build reports why.
    const failing = session.run(a, .{ .registry = &reg, .source = "elements:\n  - type: GstElement\n    element: FAIL\n" });
    const fview: runner_mod.PlanView = .{ .backend = .gst, .text = failing.gst.out.launch };
    const o3 = try cd_exec.execute(a, &reg, toy_runner, fview, failing.gst.out.plan.chain, .{ .write_side_files = false });
    try std.testing.expectEqualStrings("toy runner refuses this pipeline", o3.failed);
}

test "gst runner: drives libgstreamer through dlopen (stub)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var reg = try registry_mod.Registry.init(std.testing.allocator);
    defer reg.deinit();

    const stub = try libPath(a, "libcd_stub_gst.so");
    _ = c.setenv("CD_GSTREAMER_LIB", (try a.dupeSentinel(u8, stub, 0)).ptr, 1);
    _ = c.setenv("CD_GLIB_LIB", (try a.dupeSentinel(u8, stub, 0)).ptr, 1);
    runner_mod.interrupt_requested.store(false, .release);

    var lib = try std.DynLib.open(stub);
    defer lib.close();
    const calls = lib.lookup(*const fn () callconv(.c) [*:0]const u8, "stub_calls").?;
    const last = lib.lookup(*const fn () callconv(.c) [*:0]const u8, "stub_last_launch").?;
    const reset = lib.lookup(*const fn () callconv(.c) void, "stub_reset").?;

    const run = runner_gst.runner();
    const chain: []const @import("plan.zig").Segment = &.{};

    // Healthy pipeline: parsed, played, ends on EOS, torn down.
    reset();
    var good: runner_mod.PlanView = .{ .backend = .gst, .text = "videotestsrc ! fakesink" };
    var out = try cd_exec.execute(a, &reg, run, good, chain, .{ .write_side_files = false });
    try std.testing.expect(out == .finished);
    try std.testing.expectEqualStrings("videotestsrc ! fakesink", std.mem.span(last()));
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(calls()), "parse;PLAYING;") != null);
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(calls()), "NULL;") != null);

    // Parse error is surfaced with GStreamer's message.
    reset();
    good.text = "nonsense ! fakesink";
    out = try cd_exec.execute(a, &reg, run, good, chain, .{ .write_side_files = false });
    try std.testing.expect(std.mem.indexOf(u8, out.failed, "no element \"nonsense\"") != null);

    // A bus error ends the run as a failure.
    reset();
    good.text = "videotestsrc ! ERRORPIPE";
    out = try cd_exec.execute(a, &reg, run, good, chain, .{ .write_side_files = false });
    try std.testing.expect(std.mem.indexOf(u8, out.failed, "GStreamer error: boom") != null);

    // Playing can be refused.
    reset();
    good.text = "videotestsrc ! NOPLAY";
    out = try cd_exec.execute(a, &reg, run, good, chain, .{ .write_side_files = false });
    try std.testing.expectEqualStrings("runner could not start the pipeline", out.failed);

    // A signal-style interrupt ends a pipeline that would otherwise run on.
    reset();
    runner_mod.interrupt_requested.store(true, .release);
    good.text = "videotestsrc ! fakesink";
    out = try cd_exec.execute(a, &reg, run, good, chain, .{ .write_side_files = false });
    try std.testing.expect(out == .finished);
    runner_mod.interrupt_requested.store(false, .release);
}

test "gst runner: missing GStreamer is a runtime error, not a crash" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var reg = try registry_mod.Registry.init(std.testing.allocator);
    defer reg.deinit();
    _ = c.setenv("CD_GSTREAMER_LIB", "/nonexistent/libgstreamer.so", 1);
    const out = try cd_exec.execute(a, &reg, runner_gst.runner(), .{ .backend = .gst, .text = "fakesink" }, &.{}, .{ .write_side_files = false });
    try std.testing.expect(std.mem.indexOf(u8, out.failed, "is GStreamer installed") != null);
}

test "manifest plugin lowers declaratively on gst and web" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var reg = try registry_mod.Registry.init(std.testing.allocator);
    defer reg.deinit();
    _ = try manifest.register(&reg,
        \\{"plugin":"demo","elements":[
        \\ {"type":"TimeOverlay","prefix":"time_overlay","role":"transform","required":["font"],
        \\  "preferred_inputs":["I420"],
        \\  "gst":{"template":"timeoverlay name={name} font-desc=\"{font}\" valignment={valign|top}","out":"same",
        \\         "handles":[{"name":"{name}","kind":"element"}]},
        \\  "web":{"impl":"overlay-time"}},
        \\ {"type":"GstOnlyThing","prefix":"gst_only","role":"sink",
        \\  "gst":{"template":"fakesink name={name}"},
        \\  "web":{"unsupported":"nothing to render to"}}
        \\]}
    , null);

    const src =
        \\elements:
        \\  - type: CustomSrcElement
        \\    format: I420
        \\  - type: TimeOverlay
        \\    font: Sans 20
        \\  - type: GstOnlyThing
    ;
    const g = session.run(a, .{ .registry = &reg, .source = src });
    try std.testing.expectEqualStrings(
        "appsrc name=custom_src_0 caps=video/x-raw,format=I420,width=640,height=480,framerate=30/1 format=time is-live=true block=true do-timestamp=true" ++
            " ! timeoverlay name=time_overlay_0 font-desc=\"Sans 20\" valignment=top ! fakesink name=gst_only_0",
        g.gst.out.launch,
    );

    const w = session.run(a, .{ .registry = &reg, .source = src, .backend = .web });
    try std.testing.expect(!w.web.out.ok);
    try std.testing.expectEqualStrings("nothing to render to", w.web.out.unsupported[0].reason);
    try std.testing.expect(std.mem.indexOf(u8, w.web.out.json, "\"impl\":\"overlay-time\"") != null);

    // `required` is enforced before the template is ever rendered.
    const bad = session.run(a, .{ .registry = &reg, .source = "elements:\n  - type: TimeOverlay\n  - type: DisplayPublisher\n" });
    try std.testing.expect(bad == .failure);
    try std.testing.expectEqualStrings("TimeOverlay: 'font' is required", bad.failure.message);
}

test "gst runner: runs a real pipeline on the system GStreamer, plugin hooks included (skipped if absent)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    _ = c.unsetenv("CD_GSTREAMER_LIB");
    _ = c.unsetenv("CD_GLIB_LIB");
    runner_mod.interrupt_requested.store(false, .release);

    var probe = std.DynLib.open("libgstreamer-1.0.so.0") catch return error.SkipZigTest;
    probe.close();

    var reg = try registry_mod.Registry.init(std.testing.allocator);
    defer reg.deinit();
    const toy_path = try libPath(a, "libcd_toy_plugin.so");
    try plugin_desktop.load(&reg, toy_path, null);

    // Only gst "coreelements" are assumed to exist.
    const src =
        \\elements:
        \\  - type: GstElement
        \\    element: fakesrc
        \\    properties: num-buffers=20 sizetype=fixed sizemax=64
        \\  - type: ToyTag
        \\    tag: real
        \\  - type: MuxElement
        \\    branches:
        \\      - - type: GstElement
        \\          element: queue
        \\        - type: GstElement
        \\          element: fakesink
        \\      - - type: GstElement
        \\          element: fakesink
    ;
    const r = session.run(a, .{ .registry = &reg, .source = src });
    try std.testing.expect(r == .gst);
    const view: runner_mod.PlanView = .{ .backend = .gst, .text = r.gst.out.launch };
    const out = try cd_exec.execute(a, &reg, runner_gst.runner(), view, r.gst.out.plan.chain, .{ .write_side_files = false });
    switch (out) {
        .finished => {},
        .failed => |m| {
            std.debug.print("real gst run failed: {s}\n", .{m});
            return error.TestUnexpectedResult;
        },
    }

    // The plugin's setup hook ran against the real GstPipeline*.
    var lib = try std.DynLib.open(toy_path);
    defer lib.close();
    const events = lib.lookup(*const fn () callconv(.c) [*:0]const u8, "toy_events").?;
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(events()), "create;setup;") != null);
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(events()), "setup-bad") == null);

    // A real error comes back with GStreamer's own explanation.
    const bad = session.run(a, .{ .registry = &reg, .source = "elements:\n  - type: GstElement\n    element: filesrc\n    properties: location=/no/such/file\n  - type: GstElement\n    element: fakesink\n" });
    const bview: runner_mod.PlanView = .{ .backend = .gst, .text = bad.gst.out.launch };
    const o2 = try cd_exec.execute(a, &reg, runner_gst.runner(), bview, bad.gst.out.plan.chain, .{ .write_side_files = false });
    try std.testing.expect(o2 == .failed);
    try std.testing.expect(std.mem.indexOf(u8, o2.failed, "/no/such/file") != null);
}

test "a C++ plugin (std::string, classes, exceptions caught) works through the C ABI" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var reg = try registry_mod.Registry.init(std.testing.allocator);
    defer reg.deinit();
    try plugin_desktop.load(&reg, try libPath(a, "libcd_example_cpp.so"), null);

    const r = session.run(a, .{
        .registry = &reg,
        .source = "elements:\n  - type: GstElement\n    element: audiotestsrc\n  - type: CppGain\n    gain: 2\n  - type: GstElement\n    element: fakesink\n",
    });
    try std.testing.expectEqualStrings(
        "audiotestsrc ! audioamplify name=cpp_gain_0 amplification=2.000 ! fakesink",
        r.gst.out.launch,
    );

    // gain given as a float exercises the other accessor; missing gain is the required-option error.
    const f = session.run(a, .{ .registry = &reg, .source = "elements:\n  - type: CppGain\n    gain: 0.5\n" });
    try std.testing.expect(std.mem.indexOf(u8, f.gst.out.launch, "amplification=0.500") != null);
    const m = session.run(a, .{ .registry = &reg, .source = "elements:\n  - type: CppGain\n" });
    try std.testing.expectEqualStrings("CppGain: 'gain' is required", m.failure.message);

    // Web: no impl, and it says why.
    const w = session.run(a, .{ .registry = &reg, .backend = .web, .source = "elements:\n  - type: CppGain\n    gain: 1\n" });
    try std.testing.expectEqualStrings("audio-only example element", w.web.out.unsupported[0].reason);
}

pub const props = @import("props.zig");
pub const caps = @import("caps.zig");
pub const yaml = @import("yaml.zig");
pub const calib = @import("calib.zig");
pub const plugin_abi = @import("plugin_abi.zig");
pub const registry = @import("registry.zig");
pub const runner = @import("runner.zig");
pub const native = @import("native.zig");
pub const manifest = @import("manifest.zig");

const is_wasm = @import("builtin").cpu.arch.isWasm();
// Native-only: dlopen-based plugin loading, the GStreamer runner, execution.
pub const plugin_desktop = if (is_wasm) struct {} else @import("plugin_desktop.zig");
pub const runner_gst = if (is_wasm) struct {} else @import("runner_gst.zig");
pub const exec = if (is_wasm) struct {} else @import("exec.zig");
pub const hw = @import("hw.zig");
pub const elements = @import("elements.zig");
pub const graph = @import("graph.zig");
pub const plan = @import("plan.zig");
pub const json = @import("json.zig");
pub const negotiate = @import("negotiate.zig");
pub const gst_factory = @import("gst_factory.zig");
pub const undistort = @import("undistort.zig");
pub const web_factory = @import("web_factory.zig");
pub const session = @import("session.zig");
pub const tests = if (@import("builtin").is_test) @import("tests.zig") else struct {};

test {
    @import("std").testing.refAllDecls(@This());
    if (!is_wasm) _ = @import("plugin_tests.zig");
}

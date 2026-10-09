//! The desktop plugin backend: loads native shared libraries that speak the
//! C ABI in include/camera_driver_plugin.h. Native only (needs libc/dlopen).

const std = @import("std");
const builtin = @import("builtin");
const abi = @import("plugin_abi.zig");
const native = @import("native.zig");
const registry_mod = @import("registry.zig");

pub const LoadError = error{
    OpenFailed,
    MissingInit,
    AbiMismatch,
    InitFailed,
    OutOfMemory,
};

pub const Diagnostic = struct { message: []const u8 = "" };

fn closeLib(handle: *anyopaque) void {
    const dl: *std.DynLib = @ptrCast(@alignCast(handle));
    dl.close();
}

/// Load `path`, call its `cd_plugin_init`, and keep the library open until
/// the registry is deinitialised (its function pointers live in the registry).
pub fn load(reg: *registry_mod.Registry, path: []const u8, diag: ?*Diagnostic) LoadError!void {
    const a = reg.alloc();
    const set = struct {
        fn msg(al: std.mem.Allocator, d: ?*Diagnostic, comptime fmt: []const u8, args: anytype) void {
            if (d) |dd| dd.message = std.fmt.allocPrint(al, fmt, args) catch "plugin load failed";
        }
    }.msg;

    const dl = try a.create(std.DynLib);
    dl.* = std.DynLib.open(path) catch {
        set(a, diag, "cannot open plugin '{s}'", .{path});
        return error.OpenFailed;
    };
    errdefer dl.close();

    const init = dl.lookup(abi.InitFn, "cd_plugin_init") orelse {
        set(a, diag, "'{s}' does not export cd_plugin_init", .{path});
        return error.MissingInit;
    };

    native.init_registry = reg;
    native.init_origin = try a.dupe(u8, std.fs.path.basename(path));
    defer native.init_registry = null;

    const rc = init(&native.host_api);
    if (rc != 0) {
        set(a, diag, "'{s}': cd_plugin_init returned {d} (ABI host={d})", .{ path, rc, abi.abi_version });
        return if (rc == -100) error.AbiMismatch else error.InitFailed;
    }

    try reg.libs.append(a, .{ .path = try a.dupe(u8, path), .close = closeLib, .handle = dl });
}

test "abi struct layout matches the C header" {
    const c = @import("plugin_header");
    try std.testing.expectEqual(@sizeOf(c.cd_caps), @sizeOf(abi.Caps));
    try std.testing.expectEqual(@sizeOf(c.cd_str), @sizeOf(abi.Str));
    try std.testing.expectEqual(@sizeOf(c.cd_node), @sizeOf(abi.Node));
    try std.testing.expectEqual(@sizeOf(c.cd_lowered), @sizeOf(abi.Lowered));
    try std.testing.expectEqual(@sizeOf(c.cd_element_def), @sizeOf(abi.ElementDef));
    try std.testing.expectEqual(@sizeOf(c.cd_runner_def), @sizeOf(abi.RunnerDef));
    try std.testing.expectEqual(@sizeOf(c.cd_plan_view), @sizeOf(abi.PlanView));
    try std.testing.expectEqual(@sizeOf(c.cd_host_api), @sizeOf(abi.HostApi));
    try std.testing.expectEqual(@offsetOf(c.cd_element_def, "lower"), @offsetOf(abi.ElementDef, "lower"));
    try std.testing.expectEqual(@offsetOf(c.cd_element_def, "destroy"), @offsetOf(abi.ElementDef, "destroy"));
    try std.testing.expectEqual(@offsetOf(c.cd_runner_def, "destroy"), @offsetOf(abi.RunnerDef, "destroy"));
    try std.testing.expectEqual(@as(c_uint, abi.abi_version), c.CD_ABI_VERSION);
}

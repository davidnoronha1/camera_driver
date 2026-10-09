//! Host side of the native plugin ABI: the `cd_host_api` function table that
//! plugins call, and the glue that lets the gst factory lower a plugin
//! element. Contains no dlopen (see plugin_desktop.zig) so it also compiles
//! for wasm, where it is simply never used.

const std = @import("std");
const Allocator = std.mem.Allocator;
const abi = @import("plugin_abi.zig");
const caps_mod = @import("caps.zig");
const graph_mod = @import("graph.zig");
const hw_mod = @import("hw.zig");
const plan_mod = @import("plan.zig");
const props = @import("props.zig");
const reg = @import("registry.zig");
const runner_mod = @import("runner.zig");

const Caps = caps_mod.Caps;
const PixelFormat = caps_mod.PixelFormat;

/// Concrete type behind the opaque `cd_lower_ctx*`.
pub const LowerState = struct {
    ctx: *plan_mod.LowerCtx,
    hw: hw_mod.HwCaps,
    node_type: []const u8,
};

/// Registry being populated while `cd_plugin_init` runs. Plugin init is not
/// reentrant and not thread-safe, which is the only time this is set.
pub var init_registry: ?*reg.Registry = null;
pub var init_origin: []const u8 = "";

fn toCaps(c: Caps) abi.Caps {
    return .{
        .format = @backingInt(c.format),
        .width = c.width,
        .height = c.height,
        .fps_num = c.fps_num,
        .fps_den = c.fps_den,
        .is_nvmm = @intFromBool(c.is_nvmm),
        .is_any = @intFromBool(c.is_any),
    };
}

fn fromCaps(c: abi.Caps) Caps {
    const count = @backingInt(PixelFormat.bayer_gbrg) + 1; // last variant; header enum matches
    const fmt_idx: usize = @intCast(@max(c.format, 0));
    return .{
        .format = if (fmt_idx < count) @fromBackingInt(@intCast(fmt_idx)) else .unknown,
        .width = c.width,
        .height = c.height,
        .fps_num = c.fps_num,
        .fps_den = c.fps_den,
        .is_nvmm = c.is_nvmm != 0,
        .is_any = c.is_any != 0,
    };
}

fn dupeZ(gpa: Allocator, s: []const u8) Allocator.Error![:0]u8 {
    return gpa.dupeSentinel(u8, s, 0);
}

fn propsOf(p: *const abi.Props) *const props.Props {
    return @ptrCast(@alignCast(p));
}

fn lookup(p: *const abi.Props, key: [*:0]const u8) ?props.Value {
    return propsOf(p).get(std.mem.span(key));
}

// ── host API callbacks ───────────────────────────────────────────────────

fn registerElement(def: *const abi.ElementDef) callconv(.c) c_int {
    const r = init_registry orelse return -1;
    const a = r.alloc();

    const copy = a.create(abi.ElementDef) catch return -1;
    copy.* = def.*;

    var required: std.ArrayList([]const u8) = .empty;
    if (def.required) |list| {
        var i: usize = 0;
        while (list[i]) |name| : (i += 1) {
            required.append(a, a.dupe(u8, std.mem.span(name)) catch return -1) catch return -1;
        }
    }
    r.addElement(.{
        .type_name = a.dupe(u8, std.mem.span(def.type_name)) catch return -1,
        .prefix = a.dupe(u8, std.mem.span(def.name_prefix)) catch return -1,
        .role = switch (def.role) {
            0 => .source,
            1 => .transform,
            2 => .mux,
            else => .sink,
        },
        .required = required.items,
        .backends = def.backends,
        .kind = .{ .native = copy },
        .origin = init_origin,
    }) catch return -2;
    return 0;
}

fn registerRunner(def: *const abi.RunnerDef) callconv(.c) c_int {
    const r = init_registry orelse return -1;
    const a = r.alloc();
    const copy = a.create(abi.RunnerDef) catch return -1;
    copy.* = def.*;
    const backend: runner_mod.Backend = if (std.mem.eql(u8, std.mem.span(def.backend), "web")) .web else .gst;
    r.addRunner(.{
        .name = a.dupe(u8, std.mem.span(def.name)) catch return -1,
        .backend = backend,
        .ptr = copy,
        .vtable = &native_runner_vtable,
    }) catch return -1;
    return 0;
}

fn propsGetString(p: *const abi.Props, key: [*:0]const u8, out: *abi.Str) callconv(.c) c_int {
    const v = lookup(p, key) orelse return 0;
    const s = v.asString() orelse return 0;
    out.* = .{ .ptr = s.ptr, .len = s.len };
    return 1;
}
fn propsGetInt(p: *const abi.Props, key: [*:0]const u8, out: *i64) callconv(.c) c_int {
    const v = lookup(p, key) orelse return 0;
    out.* = v.asInt() orelse return 0;
    return 1;
}
fn propsGetFloat(p: *const abi.Props, key: [*:0]const u8, out: *f64) callconv(.c) c_int {
    const v = lookup(p, key) orelse return 0;
    out.* = v.asFloat() orelse return 0;
    return 1;
}
fn propsGetBool(p: *const abi.Props, key: [*:0]const u8, out: *c_int) callconv(.c) c_int {
    const v = lookup(p, key) orelse return 0;
    out.* = @intFromBool(v.asBool() orelse return 0);
    return 1;
}
fn propsListLen(p: *const abi.Props, key: [*:0]const u8) callconv(.c) usize {
    const v = lookup(p, key) orelse return 0;
    return (v.asList() orelse return 0).len;
}
fn propsListString(p: *const abi.Props, key: [*:0]const u8, i: usize, out: *abi.Str) callconv(.c) c_int {
    const v = lookup(p, key) orelse return 0;
    const l = v.asList() orelse return 0;
    if (i >= l.len) return 0;
    const s = l[i].asString() orelse return 0;
    out.* = .{ .ptr = s.ptr, .len = s.len };
    return 1;
}

fn stateOf(c: *abi.LowerCtx) *LowerState {
    return @ptrCast(@alignCast(c));
}

fn addHandle(c: *abi.LowerCtx, name: [*:0]const u8, kind: i32, type_name: [*:0]const u8) callconv(.c) void {
    const st = stateOf(c);
    const k: plan_mod.HandleKind = switch (kind) {
        0 => .frame_source,
        1 => .frame_sink,
        2 => .topic_writer,
        3 => .topic_reader,
        else => .element,
    };
    const gpa = st.ctx.gpa;
    st.ctx.out.handle(
        gpa.dupe(u8, std.mem.span(name)) catch return,
        k,
        gpa.dupe(u8, std.mem.span(type_name)) catch return,
    ) catch {};
}

fn warn(c: *abi.LowerCtx, message: [*:0]const u8) callconv(.c) void {
    const st = stateOf(c);
    st.ctx.out.warn("{s}: {s}", .{ st.node_type, std.mem.span(message) }) catch {};
}

fn hasElement(c: *const abi.LowerCtx, gst_element: [*:0]const u8) callconv(.c) c_int {
    const st: *const LowerState = @ptrCast(@alignCast(c));
    return @intFromBool(st.hw.hasElement(std.mem.span(gst_element)));
}

pub const host_api: abi.HostApi = .{
    .abi_version = abi.abi_version,
    .register_element = registerElement,
    .register_runner = registerRunner,
    .props_get_string = propsGetString,
    .props_get_int = propsGetInt,
    .props_get_float = propsGetFloat,
    .props_get_bool = propsGetBool,
    .props_list_len = propsListLen,
    .props_list_string = propsListString,
    .add_handle = addHandle,
    .warn = warn,
    .has_element = hasElement,
};

// ── element ops (used by the gst factory) ────────────────────────────────

/// A `graph.Node` rendered as the C view. Strings are NUL-terminated copies.
const CNode = struct {
    node: abi.Node,
};

fn cNode(gpa: Allocator, n: *const graph_mod.Node) !CNode {
    return .{ .node = .{
        .type_name = (try dupeZ(gpa, n.type_name)).ptr,
        .name = (try dupeZ(gpa, n.name)).ptr,
        .props = @ptrCast(&n.props),
    } };
}

pub fn preferredInputs(gpa: Allocator, def: *const abi.ElementDef, n: *const graph_mod.Node) ![]const PixelFormat {
    const f = def.preferred_inputs orelse return &.{};
    var cn = try cNode(gpa, n);
    var buf: [32]i32 = undefined;
    const count = @min(f(&cn.node, &buf, buf.len), buf.len);
    const out = try gpa.alloc(PixelFormat, count);
    for (buf[0..count], 0..) |v, i| out[i] = fromCaps(.{ .format = v }).format;
    return out;
}

pub const LowerError = error{ PluginFailed, NoGstLowering, OutOfMemory };

pub fn lower(
    def: *const abi.ElementDef,
    ctx: *plan_mod.LowerCtx,
    hw: hw_mod.HwCaps,
    n: *const graph_mod.Node,
) LowerError!plan_mod.Lowered {
    const f = def.lower orelse return error.NoGstLowering;
    var state: LowerState = .{ .ctx = ctx, .hw = hw, .node_type = n.type_name };
    var cn = try cNode(ctx.gpa, n);
    const upstream = toCaps(ctx.upstream);
    var out: abi.Lowered = .{};
    const rc = f(&host_api, @ptrCast(&state), &cn.node, &upstream, &out);
    if (rc != 0) {
        ctx.out.warn("{s}: plugin lower() failed with code {d}", .{ n.type_name, rc }) catch {};
        return error.PluginFailed;
    }
    return .{ .text = try ctx.gpa.dupe(u8, out.text.slice()), .out = fromCaps(out.out) };
}

// ── native runner adapter ────────────────────────────────────────────────

const NativeSession = struct {
    def: *const abi.RunnerDef,
    session: ?*anyopaque,
};

fn nativeDef(ptr: ?*anyopaque) *const abi.RunnerDef {
    return @ptrCast(@alignCast(ptr.?));
}

fn nativeBuild(ptr: ?*anyopaque, gpa: Allocator, plan: runner_mod.PlanView, err: *[]const u8) ?*anyopaque {
    const def = nativeDef(ptr);

    const handles = gpa.alloc(abi.Handle, plan.handles.len) catch return null;
    for (plan.handles, 0..) |h, i| handles[i] = .{
        .name = (dupeZ(gpa, h.name) catch return null).ptr,
        .kind = @backingInt(h.kind),
        .type_name = (dupeZ(gpa, h.type_name) catch return null).ptr,
    };
    const files = gpa.alloc(abi.SideFile, plan.side_files.len) catch return null;
    for (plan.side_files, 0..) |f, i| files[i] = .{
        .path = (dupeZ(gpa, f.path) catch return null).ptr,
        .contents = .{ .ptr = f.contents.ptr, .len = f.contents.len },
    };
    const view: abi.PlanView = .{
        .backend = if (plan.backend == .web) "web" else "gst",
        .launch = .{ .ptr = plan.text.ptr, .len = plan.text.len },
        .handles = handles.ptr,
        .n_handles = handles.len,
        .side_files = files.ptr,
        .n_side_files = files.len,
    };

    var errbuf: [512]u8 = undefined;
    errbuf[0] = 0;
    const session = def.build(def.user, &view, &errbuf, errbuf.len);
    if (session == null) {
        err.* = gpa.dupe(u8, std.mem.sliceTo(&errbuf, 0)) catch "build failed";
        return null;
    }
    const out = gpa.create(NativeSession) catch return null;
    out.* = .{ .def = def, .session = session };
    return out;
}

fn ns(p: *anyopaque) *NativeSession {
    return @ptrCast(@alignCast(p));
}
fn nativeHandle(s: *anyopaque) ?*anyopaque {
    return ns(s).def.native_handle(ns(s).session);
}
fn nativeFind(s: *anyopaque, name: [:0]const u8) ?*anyopaque {
    return ns(s).def.find_element(ns(s).session, name.ptr);
}
fn nativePlay(s: *anyopaque) bool {
    return ns(s).def.play(ns(s).session) == 0;
}
fn nativeWait(s: *anyopaque) bool {
    return ns(s).def.wait(ns(s).session) == 0;
}
fn nativeStop(s: *anyopaque) void {
    ns(s).def.stop(ns(s).session);
}
fn nativeDestroy(s: *anyopaque) void {
    ns(s).def.destroy(ns(s).session);
}

fn nativeErrorText(_: *anyopaque) ?[]const u8 {
    return null;
}

pub const native_runner_vtable: runner_mod.Runner.VTable = .{
    .errorText = nativeErrorText,
    .build = nativeBuild,
    .nativeHandle = nativeHandle,
    .findElement = nativeFind,
    .play = nativePlay,
    .wait = nativeWait,
    .stop = nativeStop,
    .destroy = nativeDestroy,
};

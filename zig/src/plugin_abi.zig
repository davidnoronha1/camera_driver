//! Zig mirrors of `include/camera_driver_plugin.h`. Kept hand-written so the
//! module also compiles for wasm (no libc, no cImport); a test in
//! plugin_desktop.zig compares sizes/offsets against the real header.

pub const abi_version: u32 = 1;

pub const Role = enum(i32) { source = 0, transform = 1, mux = 2, sink = 3 };

pub const backend_gst: u32 = 1 << 0;
pub const backend_web: u32 = 1 << 1;

pub const Props = opaque {};
pub const LowerCtx = opaque {};

pub const Caps = extern struct {
    format: i32 = 0,
    width: i32 = 0,
    height: i32 = 0,
    fps_num: i32 = 30,
    fps_den: i32 = 1,
    is_nvmm: u8 = 0,
    is_any: u8 = 0,
};

pub const Str = extern struct {
    ptr: ?[*]const u8 = null,
    len: usize = 0,

    pub fn slice(self: Str) []const u8 {
        return if (self.ptr) |p| p[0..self.len] else "";
    }
};

pub const Node = extern struct {
    type_name: [*:0]const u8,
    name: [*:0]const u8,
    props: *const Props,
};

pub const Lowered = extern struct {
    text: Str = .{},
    out: Caps = .{},
};

pub const Handle = extern struct {
    name: [*:0]const u8,
    kind: i32,
    type_name: [*:0]const u8,
};

pub const SideFile = extern struct {
    path: [*:0]const u8,
    contents: Str,
};

pub const HostApi = extern struct {
    abi_version: u32,
    register_element: *const fn (def: *const ElementDef) callconv(.c) c_int,
    register_runner: *const fn (def: *const RunnerDef) callconv(.c) c_int,
    props_get_string: *const fn (p: *const Props, key: [*:0]const u8, out: *Str) callconv(.c) c_int,
    props_get_int: *const fn (p: *const Props, key: [*:0]const u8, out: *i64) callconv(.c) c_int,
    props_get_float: *const fn (p: *const Props, key: [*:0]const u8, out: *f64) callconv(.c) c_int,
    props_get_bool: *const fn (p: *const Props, key: [*:0]const u8, out: *c_int) callconv(.c) c_int,
    props_list_len: *const fn (p: *const Props, key: [*:0]const u8) callconv(.c) usize,
    props_list_string: *const fn (p: *const Props, key: [*:0]const u8, i: usize, out: *Str) callconv(.c) c_int,
    add_handle: *const fn (ctx: *LowerCtx, name: [*:0]const u8, kind: i32, type_name: [*:0]const u8) callconv(.c) void,
    warn: *const fn (ctx: *LowerCtx, message: [*:0]const u8) callconv(.c) void,
    has_element: *const fn (ctx: *const LowerCtx, gst_element: [*:0]const u8) callconv(.c) c_int,
};

pub const ElementDef = extern struct {
    type_name: [*:0]const u8,
    name_prefix: [*:0]const u8,
    role: i32,
    backends: u32,
    required: ?[*:null]const ?[*:0]const u8 = null,
    preferred_inputs: ?*const fn (node: *const Node, out: [*]i32, cap: usize) callconv(.c) usize = null,
    lower: ?*const fn (host: *const HostApi, ctx: *LowerCtx, node: *const Node, upstream: *const Caps, out: *Lowered) callconv(.c) c_int = null,
    web_impl: ?[*:0]const u8 = null,
    web_reason: ?[*:0]const u8 = null,
    create: ?*const fn (node: *const Node) callconv(.c) ?*anyopaque = null,
    setup: ?*const fn (self: ?*anyopaque, native_pipeline: ?*anyopaque) callconv(.c) c_int = null,
    bringdown: ?*const fn (self: ?*anyopaque, native_pipeline: ?*anyopaque) callconv(.c) void = null,
    destroy: ?*const fn (self: ?*anyopaque) callconv(.c) void = null,
};

pub const PlanView = extern struct {
    backend: [*:0]const u8,
    launch: Str,
    handles: ?[*]const Handle,
    n_handles: usize,
    side_files: ?[*]const SideFile,
    n_side_files: usize,
};

pub const RunnerDef = extern struct {
    name: [*:0]const u8,
    backend: [*:0]const u8,
    user: ?*anyopaque = null,
    build: *const fn (user: ?*anyopaque, plan: *const PlanView, err: [*]u8, err_cap: usize) callconv(.c) ?*anyopaque,
    native_handle: *const fn (session: ?*anyopaque) callconv(.c) ?*anyopaque,
    find_element: *const fn (session: ?*anyopaque, name: [*:0]const u8) callconv(.c) ?*anyopaque,
    play: *const fn (session: ?*anyopaque) callconv(.c) c_int,
    wait: *const fn (session: ?*anyopaque) callconv(.c) c_int,
    stop: *const fn (session: ?*anyopaque) callconv(.c) void,
    destroy: *const fn (session: ?*anyopaque) callconv(.c) void,
};

pub const InitFn = *const fn (host: *const HostApi) callconv(.c) c_int;

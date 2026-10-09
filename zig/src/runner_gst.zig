//! Default desktop runner: builds the plan's launch string with GStreamer and
//! plays it. GStreamer is *not* a build-time dependency: libgstreamer/libglib
//! are dlopen()ed on first use, and the ~12 functions needed are resolved by
//! name. That keeps `zig build` free of GStreamer headers and gives a clear
//! runtime error (not a link error) on a machine without GStreamer.
//!
//! The event loop is a plain bus poll rather than a GLib main loop, so there
//! is no GLib context to manage and `stop()` is just an atomic flag.
//! `CD_GSTREAMER_LIB` / `CD_GLIB_LIB` override the library paths (used by the
//! tests to load a stub).

const std = @import("std");
const Allocator = std.mem.Allocator;
const runner_mod = @import("runner.zig");

const c = struct {
    extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;
};

// GstState / GstStateChangeReturn / GstMessageType values (stable GStreamer ABI).
const GST_STATE_NULL = 1;
const GST_STATE_PLAYING = 4;
const GST_STATE_CHANGE_FAILURE = 0;
const GST_MESSAGE_EOS: c_uint = 1 << 0;
const GST_MESSAGE_ERROR: c_uint = 1 << 1;
const GST_CLOCK_TIME_MS: u64 = 1_000_000;

const GError = extern struct { domain: u32, code: c_int, message: ?[*:0]const u8 };

// Public GstMessage header (gst/gstmessage.h): GST_MESSAGE_TYPE(msg) reads
// `type` right after the embedded GstMiniObject. Needed because
// gst_bus_timed_pop_filtered() *discards* messages outside the mask, so
// ERROR and EOS must be popped together and told apart afterwards.
const MiniObject = extern struct {
    type: usize,
    refcount: c_int,
    lockstate: c_int,
    flags: c_uint,
    copy: ?*anyopaque,
    dispose: ?*anyopaque,
    free: ?*anyopaque,
    priv_uint: c_uint,
    priv_pointer: ?*anyopaque,
};
const MessageHeader = extern struct { mini_object: MiniObject, type: c_uint };

fn messageType(msg: *anyopaque) c_uint {
    const h: *const MessageHeader = @ptrCast(@alignCast(msg));
    return h.type;
}

const Api = struct {
    gst: std.DynLib,
    glib: std.DynLib,

    gst_init: *const fn (argc: ?*c_int, argv: ?*anyopaque) callconv(.c) void,
    gst_parse_launch: *const fn (desc: [*:0]const u8, err: *?*GError) callconv(.c) ?*anyopaque,
    gst_element_set_state: *const fn (el: *anyopaque, state: c_int) callconv(.c) c_int,
    gst_element_get_bus: *const fn (el: *anyopaque) callconv(.c) ?*anyopaque,
    gst_bus_timed_pop_filtered: *const fn (bus: *anyopaque, timeout_ns: u64, types: c_uint) callconv(.c) ?*anyopaque,
    gst_message_parse_error: *const fn (msg: *anyopaque, err: *?*GError, dbg: *?[*:0]u8) callconv(.c) void,
    gst_message_unref: *const fn (msg: *anyopaque) callconv(.c) void,
    gst_object_unref: *const fn (obj: *anyopaque) callconv(.c) void,
    gst_bin_get_by_name: *const fn (bin: *anyopaque, name: [*:0]const u8) callconv(.c) ?*anyopaque,
    g_error_free: *const fn (err: *GError) callconv(.c) void,
    g_free: *const fn (p: ?*anyopaque) callconv(.c) void,
};

fn libPath(env: [*:0]const u8, default: []const u8) []const u8 {
    if (c.getenv(env)) |p| return std.mem.span(p);
    return default;
}

fn loadApi(gpa: Allocator, err: *[]const u8) ?*Api {
    const gst_path = libPath("CD_GSTREAMER_LIB", "libgstreamer-1.0.so.0");
    const glib_path = libPath("CD_GLIB_LIB", "libglib-2.0.so.0");

    const api = gpa.create(Api) catch return null;
    api.gst = std.DynLib.open(gst_path) catch {
        err.* = std.fmt.allocPrint(gpa, "cannot load {s} (is GStreamer installed?)", .{gst_path}) catch "cannot load libgstreamer";
        return null;
    };
    api.glib = std.DynLib.open(glib_path) catch {
        err.* = std.fmt.allocPrint(gpa, "cannot load {s}", .{glib_path}) catch "cannot load libglib";
        return null;
    };

    inline for (.{
        .{ "gst_init", "gst" },
        .{ "gst_parse_launch", "gst" },
        .{ "gst_element_set_state", "gst" },
        .{ "gst_element_get_bus", "gst" },
        .{ "gst_bus_timed_pop_filtered", "gst" },
        .{ "gst_message_parse_error", "gst" },
        .{ "gst_message_unref", "gst" },
        .{ "gst_object_unref", "gst" },
        .{ "gst_bin_get_by_name", "gst" },
        .{ "g_error_free", "glib" },
        .{ "g_free", "glib" },
    }) |entry| {
        const name = entry[0];
        const lib = if (comptime std.mem.eql(u8, entry[1], "gst")) &api.gst else &api.glib;
        const F = @TypeOf(@field(api, name));
        @field(api, name) = lib.lookup(F, name) orelse {
            err.* = std.fmt.allocPrint(gpa, "GStreamer library is missing symbol {s}", .{name}) catch "missing symbol";
            return null;
        };
    }
    return api;
}

pub const Session = struct {
    api: *Api,
    pipeline: *anyopaque,
    bus: *anyopaque,
    stop_requested: std.atomic.Value(bool) = .init(false),
    error_text: ?[]const u8 = null,
    gpa: Allocator,
};

fn build(_: ?*anyopaque, gpa: Allocator, plan: runner_mod.PlanView, err: *[]const u8) ?*anyopaque {
    const api = loadApi(gpa, err) orelse return null;
    api.gst_init(null, null); // idempotent in GStreamer

    const desc = gpa.dupeSentinel(u8, plan.text, 0) catch return null;
    var gerr: ?*GError = null;
    const pipeline = api.gst_parse_launch(desc.ptr, &gerr);
    if (pipeline == null or gerr != null) {
        const msg: []const u8 = if (gerr) |e| (if (e.message) |m| std.mem.span(m) else "unknown error") else "unknown error";
        err.* = std.fmt.allocPrint(gpa, "failed to parse GStreamer pipeline: {s}", .{msg}) catch "failed to parse pipeline";
        if (gerr) |e| api.g_error_free(e);
        if (pipeline) |p| api.gst_object_unref(p);
        return null;
    }
    const bus = api.gst_element_get_bus(pipeline.?) orelse {
        err.* = "pipeline has no bus";
        return null;
    };
    const s = gpa.create(Session) catch return null;
    s.* = .{ .api = api, .pipeline = pipeline.?, .bus = bus, .gpa = gpa };
    return s;
}

fn sess(p: *anyopaque) *Session {
    return @ptrCast(@alignCast(p));
}

fn nativeHandle(p: *anyopaque) ?*anyopaque {
    return sess(p).pipeline;
}

fn findElement(p: *anyopaque, name: [:0]const u8) ?*anyopaque {
    const s = sess(p);
    return s.api.gst_bin_get_by_name(s.pipeline, name.ptr);
}

fn play(p: *anyopaque) bool {
    const s = sess(p);
    if (s.api.gst_element_set_state(s.pipeline, GST_STATE_PLAYING) != GST_STATE_CHANGE_FAILURE) return true;
    // The reason is on the bus (e.g. "Resource not found"); fetch it for the caller.
    if (s.api.gst_bus_timed_pop_filtered(s.bus, 0, GST_MESSAGE_ERROR)) |msg| {
        defer s.api.gst_message_unref(msg);
        s.error_text = takeError(s, msg);
    }
    return false;
}

fn takeError(s: *Session, msg: *anyopaque) ?[]const u8 {
    var gerr: ?*GError = null;
    var dbg: ?[*:0]u8 = null;
    s.api.gst_message_parse_error(msg, &gerr, &dbg);
    defer if (dbg) |d| s.api.g_free(d);
    const e = gerr orelse return null;
    defer s.api.g_error_free(e);
    return std.fmt.allocPrint(s.gpa, "GStreamer error: {s}{s}{s}{s}", .{
        if (e.message) |m| std.mem.span(m) else "?",
        if (dbg != null) " (" else "",
        if (dbg) |d| std.mem.span(d) else "",
        if (dbg != null) ")" else "",
    }) catch null;
}

/// Poll the bus until EOS, an error, `stop()`, or SIGINT (runner.interrupt_requested).
fn wait(p: *anyopaque) bool {
    const s = sess(p);
    while (!s.stop_requested.load(.acquire) and !runner_mod.interrupt_requested.load(.acquire)) {
        const msg = s.api.gst_bus_timed_pop_filtered(s.bus, 50 * GST_CLOCK_TIME_MS, GST_MESSAGE_ERROR | GST_MESSAGE_EOS) orelse continue;
        defer s.api.gst_message_unref(msg);

        if (messageType(msg) == GST_MESSAGE_EOS) return true;

        s.error_text = takeError(s, msg);
        return false;
    }
    return true; // stopped on request
}

fn stop(p: *anyopaque) void {
    sess(p).stop_requested.store(true, .release);
}

fn destroy(p: *anyopaque) void {
    const s = sess(p);
    _ = s.api.gst_element_set_state(s.pipeline, GST_STATE_NULL);
    s.api.gst_object_unref(s.bus);
    s.api.gst_object_unref(s.pipeline);
}

const vtable: runner_mod.Runner.VTable = .{
    .build = build,
    .nativeHandle = nativeHandle,
    .findElement = findElement,
    .play = play,
    .wait = wait,
    .stop = stop,
    .destroy = destroy,
    .errorText = errorText,
};

fn errorText(p: *anyopaque) ?[]const u8 {
    return sess(p).error_text;
}

pub fn runner() runner_mod.Runner {
    return .{ .name = "gst", .backend = .gst, .ptr = null, .vtable = &vtable };
}

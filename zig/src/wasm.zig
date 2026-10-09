//! wasm32-freestanding entry points. The module is a pure planner/compute
//! kernel: no WASI, no imports required. A host passes text in through
//! `cd_alloc`'d buffers and reads a JSON result back; see web/host.js.
//!
//!   cd_plan(...)            config text -> JSON plan (gst or web backend)
//!   cd_undistort_*          CPU lens undistortion fallback for RGBA frames

const std = @import("std");
const cd = @import("camera_driver");

const gpa = std.heap.wasm_allocator;

var result_arena: ?std.heap.ArenaAllocator = null;
var result_json: []const u8 = &.{};

export fn cd_alloc(len: usize) ?[*]u8 {
    const buf = gpa.alloc(u8, len) catch return null;
    return buf.ptr;
}

export fn cd_free(ptr: [*]u8, len: usize) void {
    gpa.free(ptr[0..len]);
}

export fn cd_result_ptr() [*]const u8 {
    return result_json.ptr;
}

export fn cd_result_len() usize {
    return result_json.len;
}

fn slice(ptr: ?[*]const u8, len: usize) []const u8 {
    return if (ptr) |p| p[0..len] else &.{};
}

/// Plan a config. Returns the JSON length (read it via cd_result_ptr/len);
/// the buffer stays valid until the next cd_plan call.
///
///   backend: 0 = gst, 1 = web
///   caps:    comma-separated GStreamer element names (gst) or WebCaps names (web)
///   metadata: optional camera_info / metadata YAML text
export fn cd_plan(
    src_ptr: [*]const u8,
    src_len: usize,
    backend: u32,
    doc_index: u32,
    caps_ptr: ?[*]const u8,
    caps_len: usize,
    meta_ptr: ?[*]const u8,
    meta_len: usize,
) usize {
    if (result_arena) |*a| a.deinit();
    result_arena = std.heap.ArenaAllocator.init(gpa);
    const arena = result_arena.?.allocator();

    var req: cd.session.Request = .{
        .source = src_ptr[0..src_len],
        .backend = if (backend == 1) .web else .gst,
        .doc_index = doc_index,
    };
    const caps = slice(caps_ptr, caps_len);
    if (caps.len > 0) {
        if (req.backend == .web) {
            req.web_caps = cd.web_factory.WebCaps.fromCsv(caps);
        } else {
            req.hw = cd.hw.HwCaps.fromCsv(arena, caps) catch cd.hw.HwCaps.software;
        }
    }
    const meta = slice(meta_ptr, meta_len);
    if (meta.len > 0) req.metadata = meta;

    const result = cd.session.run(arena, req);
    result_json = cd.session.toJson(arena, result) catch "{\"ok\":false,\"error\":\"out of memory\"}";
    return result_json.len;
}

// ── undistort ────────────────────────────────────────────────────────────

var undistort_map: ?cd.undistort.Map = null;

/// (Re)build the remap table. D follows the ROS order [k1,k2,p1,p2,k3].
/// Returns 1 on success.
export fn cd_undistort_init(
    width: u32,
    height: u32,
    fx: f64,
    fy: f64,
    cx: f64,
    cy: f64,
    k1: f64,
    k2: f64,
    p1: f64,
    p2: f64,
    k3: f64,
) u32 {
    if (undistort_map) |*m| m.deinit(gpa);
    undistort_map = null;
    var c: cd.calib.Calibration = .{ .width = @intCast(width), .height = @intCast(height) };
    c.K = .{ fx, 0, cx, 0, fy, cy, 0, 0, 1 };
    const d = [_]f64{ k1, k2, p1, p2, k3 };
    c.D = &d;
    undistort_map = cd.undistort.buildMap(gpa, c, width, height) catch return 0;
    return 1;
}

/// Remap one RGBA8 frame of the size given to cd_undistort_init.
export fn cd_undistort_frame(src: [*]const u8, dst: [*]u8) u32 {
    const m = undistort_map orelse return 0;
    const n = @as(usize, m.width) * m.height * 4;
    cd.undistort.remapRgba(m, src[0..n], dst[0..n]);
    return 1;
}

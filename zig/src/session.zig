//! One-call entry point shared by the CLI and the wasm module: config text in,
//! plan out. Everything above this file is a library; this file decides how a
//! request (text + backend + capabilities) turns into a `Result`, and how a
//! `Result` is rendered as JSON for hosts that can't call Zig directly.

const std = @import("std");
const Allocator = std.mem.Allocator;
const calib = @import("calib.zig");
const gst = @import("gst_factory.zig");
const graph_mod = @import("graph.zig");
const hw_mod = @import("hw.zig");
const json = @import("json.zig");
const plan_mod = @import("plan.zig");
const props = @import("props.zig");
const web = @import("web_factory.zig");
const yaml = @import("yaml.zig");

pub const Backend = enum { gst, web };

/// Optional file access for `pipeline.metadata_file` / `calibration_file`.
/// Hosts without a filesystem (the browser) leave this null and pass
/// `Request.metadata` instead.
pub const Files = struct {
    ctx: ?*anyopaque = null,
    read: *const fn (ctx: ?*anyopaque, gpa: Allocator, path: []const u8) ?[]const u8,
};

pub const Request = struct {
    source: []const u8,
    backend: Backend = .gst,
    /// Which `---` document to plan.
    doc_index: usize = 0,
    hw: hw_mod.HwCaps = hw_mod.HwCaps.software,
    web_caps: web.WebCaps = web.WebCaps.all,
    /// Device to assume for V4L2SrcElement (gst only).
    device: ?[]const u8 = null,
    /// Seed metadata: a full metadata doc or a bare ROS camera_info doc.
    metadata: ?[]const u8 = null,
    files: ?Files = null,
};

pub const Failure = struct {
    stage: enum { yaml, graph, lower, request },
    message: []const u8,
    line: usize = 0,
};

pub const Result = union(enum) {
    gst: struct { out: gst.Output, pipeline: props.Props, doc_count: usize },
    web: struct { out: web.Output, doc_count: usize },
    failure: Failure,
};

fn failure(stage: @FieldType(Failure, "stage"), message: []const u8, line: usize) Result {
    return .{ .failure = .{ .stage = stage, .message = message, .line = line } };
}

/// A bare camera_info doc becomes `{calibration: doc}`; a full metadata doc
/// (has `calibration:`/`pose:`) is used as is.
fn normalizeMetadata(gpa: Allocator, v: props.Value) !props.Value {
    const m = v.asMap() orelse return v;
    if (m.contains("calibration") or m.contains("pose")) return v;
    var out: props.Props = .empty;
    try out.put(gpa, "calibration", v);
    return .{ .map = out };
}

fn seedScratchpad(gpa: Allocator, req: Request, g: graph_mod.Graph, sp: *plan_mod.Scratchpad) !?Failure {
    var text: ?[]const u8 = req.metadata;
    if (text == null) {
        const path = props.getString(g.pipeline, "metadata_file") orelse props.getString(g.pipeline, "calibration_file");
        if (path) |p| {
            const files = req.files orelse return .{
                .stage = .request,
                .message = "pipeline.metadata_file/calibration_file needs file access; pass the contents as 'metadata' instead",
            };
            text = files.read(files.ctx, gpa, p) orelse return .{
                .stage = .request,
                .message = try std.fmt.allocPrint(gpa, "cannot read '{s}'", .{p}),
            };
        }
    }
    if (text) |t| {
        var d: yaml.Diagnostic = .{};
        const v = yaml.parseFirst(gpa, t, &d) catch
            return .{ .stage = .yaml, .message = try std.fmt.allocPrint(gpa, "metadata: {s}", .{d.message}), .line = d.line };
        try sp.put(gpa, calib.metadata_key, try normalizeMetadata(gpa, v));
    }
    return null;
}

pub fn run(gpa: Allocator, req: Request) Result {
    return runInner(gpa, req) catch |e| failure(.lower, @errorName(e), 0);
}

fn runInner(gpa: Allocator, req: Request) !Result {
    var ydiag: yaml.Diagnostic = .{};
    const docs = yaml.parse(gpa, req.source, &ydiag) catch |e| switch (e) {
        error.YamlSyntax => return failure(.yaml, ydiag.message, ydiag.line),
        else => return e,
    };
    if (docs.len == 0) return failure(.request, "the config is empty", 0);
    if (req.doc_index >= docs.len)
        return failure(.request, try std.fmt.allocPrint(gpa, "document {d} requested but the file has {d}", .{ req.doc_index, docs.len }), 0);

    var gdiag: graph_mod.Diagnostic = .{};
    const g = graph_mod.fromValue(gpa, docs[req.doc_index], &gdiag) catch |e| switch (e) {
        error.OutOfMemory => return e,
        else => return failure(.graph, gdiag.message, 0),
    };

    var sp: plan_mod.Scratchpad = .empty;
    if (try seedScratchpad(gpa, req, g, &sp)) |f| return .{ .failure = f };

    switch (req.backend) {
        .gst => {
            var opts: gst.Options = .{ .hw = req.hw };
            if (req.device) |d| opts.default_device = .{ .path = d };
            var f = gst.GstPipelineFactory.init(gpa, opts);
            const out = f.build(g, &sp) catch |e| return failure(.lower, lowerMessage(e), 0);
            return .{ .gst = .{ .out = out, .pipeline = g.pipeline, .doc_count = docs.len } };
        },
        .web => {
            var f = web.WebPipelineFactory.init(gpa, .{ .caps = req.web_caps });
            const out = f.build(g, &sp) catch |e| return failure(.lower, lowerMessage(e), 0);
            return .{ .web = .{ .out = out, .doc_count = docs.len } };
        },
    }
}

fn lowerMessage(e: anyerror) []const u8 {
    return switch (e) {
        error.NoH264Encoder => "no H264 encoder available (install gstreamer1.0-plugins-ugly for x264enc, or enable one with --hw)",
        error.NoJpegEncoder => "no JPEG encoder available (install gstreamer1.0-plugins-good for jpegenc, or enable one with --hw)",
        error.NoH264Decoder => "no H264 decoder available (install gstreamer1.0-libav for avdec_h264, or enable one with --hw)",
        error.EmptyOutputs => "OptimizedConverter: 'outputs' is empty",
        error.UnsupportedTarget => "OptimizedConverter: unsupported target format",
        error.UnsupportedOnGst => "element type has no GStreamer implementation",
        error.OutOfMemory => "out of memory",
        else => @errorName(e),
    };
}

// ── JSON rendering ───────────────────────────────────────────────────────

fn writeStrings(w: *json.Writer, key: []const u8, items: []const []const u8) !void {
    try w.key(key);
    try w.arrayValue();
    for (items) |i| try w.string(i);
    try w.endArray();
}

pub fn toJson(gpa: Allocator, result: Result) ![]const u8 {
    switch (result) {
        .web => |r| return r.out.json,
        .failure => |f| {
            var w = json.Writer.init(gpa);
            try w.beginObject();
            try w.key("ok");
            try w.boolean(false);
            try w.key("stage");
            try w.string(@tagName(f.stage));
            try w.key("error");
            try w.string(f.message);
            try w.key("line");
            try w.int(@intCast(f.line));
            try w.endObject();
            return w.bytes();
        },
        .gst => |r| {
            var w = json.Writer.init(gpa);
            try w.beginObject();
            try w.key("backend");
            try w.string("gst");
            try w.key("ok");
            try w.boolean(true);
            try w.key("launch");
            try w.string(r.out.launch);
            try w.key("pipeline");
            try w.map(r.pipeline);
            try w.key("handles");
            try w.arrayValue();
            for (r.out.plan.handles) |h| {
                try w.beginObject();
                try w.key("name");
                try w.string(h.name);
                try w.key("kind");
                try w.string(@tagName(h.kind));
                try w.key("type");
                try w.string(h.type_name);
                try w.endObject();
            }
            try w.endArray();
            try w.key("side_files");
            try w.arrayValue();
            for (r.out.plan.side_files) |s| {
                try w.beginObject();
                try w.key("path");
                try w.string(s.path);
                try w.key("contents");
                try w.string(s.contents);
                try w.endObject();
            }
            try w.endArray();
            try writeStrings(&w, "warnings", r.out.plan.warnings);
            try writeStrings(&w, "notes", r.out.plan.notes);
            try w.endObject();
            return w.bytes();
        },
    }
}

test "session: gst + web from the same text, json is parseable" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const src =
        \\elements:
        \\  - type: CustomSrcElement
        \\    format: RGB
        \\    width: 320
        \\    height: 240
        \\  - type: DisplayPublisher
    ;
    const g = run(a, .{ .source = src, .backend = .gst });
    const gj = try toJson(a, g);
    const gp = try std.json.parseFromSlice(std.json.Value, a, gj, .{});
    try std.testing.expect(gp.value.object.get("ok").?.bool);
    try std.testing.expect(std.mem.indexOf(u8, gp.value.object.get("launch").?.string, "appsrc name=custom_src_0") != null);

    const w = run(a, .{ .source = src, .backend = .web });
    const wp = try std.json.parseFromSlice(std.json.Value, a, try toJson(a, w), .{});
    try std.testing.expect(wp.value.object.get("ok").?.bool);
}

test "session: failures are structured" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const bad_yaml = run(a, .{ .source = "elements: [1,\n" });
    try std.testing.expectEqual(@as(usize, 1), bad_yaml.failure.line);
    try std.testing.expect(bad_yaml.failure.stage == .yaml);

    const unknown = run(a, .{ .source = "elements:\n  - type: Bogus\n" });
    try std.testing.expect(unknown.failure.stage == .graph);

    const noenc = run(a, .{
        .source = "elements:\n  - type: CustomSrcElement\n  - type: OptimizedConverter\n    outputs: [H264]\n",
        .hw = .{},
    });
    try std.testing.expect(noenc.failure.stage == .lower);
    try std.testing.expect(std.mem.indexOf(u8, noenc.failure.message, "H264 encoder") != null);

    const j = try toJson(a, noenc);
    try std.testing.expect(std.mem.indexOf(u8, j, "\"ok\":false") != null);
}

test "session: metadata seeds undistort" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const r = run(a, .{
        .source = "elements:\n  - type: CustomSrcElement\n    format: RGB\n  - type: UndistortElement\n",
        .backend = .web,
        .metadata =
        \\image_width: 640
        \\image_height: 480
        \\camera_matrix: {rows: 3, cols: 3, data: [500.0, 0.0, 320.0, 0.0, 500.0, 240.0, 0.0, 0.0, 1.0]}
        \\distortion_coefficients: {rows: 1, cols: 5, data: [-0.1, 0.0, 0.0, 0.0, 0.0]}
        ,
    });
    try std.testing.expect(std.mem.indexOf(u8, r.web.out.json, "undistort-webgpu") != null);
}

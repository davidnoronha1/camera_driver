//! Regression tests: the strings below are what the C++ elements emit for the
//! same configs (derived from src/elements/*.cpp and src/pipeline/pipeline.cpp),
//! so a failure here means the Zig port drifted from the C++ behaviour.

const std = @import("std");
const yaml = @import("yaml.zig");
const graph_mod = @import("graph.zig");
const plan_mod = @import("plan.zig");
const gst = @import("gst_factory.zig");
const hw_mod = @import("hw.zig");
const calib = @import("calib.zig");
const props = @import("props.zig");

const cfg_example = @embedFile("cfg_example");
const cfg_orbbec_lucid = @embedFile("cfg_orbbec_lucid");
const cfg_debug_display = @embedFile("cfg_debug_display");

const queue = "queue leaky=downstream max-size-buffers=2";
const sink_opts = "max-buffers=2 drop=true sync=false emit-signals=false";

fn launchFor(arena: std.mem.Allocator, doc: props.Value, opts: gst.Options) !gst.Output {
    const g = try graph_mod.fromValue(arena, doc, null);
    var f = gst.GstPipelineFactory.init(arena, opts);
    var sp: plan_mod.Scratchpad = .empty;
    return f.build(g, &sp);
}

test "example_pipeline.yaml: V4L2 negotiates MJPEG for an x264 converter" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const out = try launchFor(a, try yaml.parseFirst(a, cfg_example, null), .{});
    try std.testing.expectEqualStrings(
        "v4l2src device=/dev/video0 do-timestamp=true ! image/jpeg,width=1920,height=1080,framerate=30/1 ! jpegparse" ++
            " ! jpegdec ! videoconvert ! video/x-raw,format=I420 ! x264enc tune=zerolatency speed-preset=ultrafast bitrate=4000",
        out.launch,
    );
}

test "orbbec_lucid_pipeline.yaml: Orbbec doc (nested tee, software only)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const docs = try yaml.parse(a, cfg_orbbec_lucid, null);
    try std.testing.expectEqual(@as(usize, 3), docs.len);

    const out = try launchFor(a, docs[0], .{});
    const tee1 = "tee name=tee_1  tee_1. ! " ++ queue ++ " ! appsink name=iceoryx_pub_0 " ++ sink_opts ++
        "  tee_1. ! " ++ queue ++ " ! appsink name=mjpeg_pub_0 " ++ sink_opts;
    const expected = "v4l2src device=/dev/video0 do-timestamp=true ! video/x-raw,format=YUY2,width=1280,height=720,framerate=30/1" ++
        " ! tee name=tee_0  tee_0. ! " ++ queue ++
        " ! videoconvert ! video/x-raw,format=I420 ! jpegenc quality=85 ! jpegparse ! " ++ tee1 ++
        "  tee_0. ! " ++ queue ++ " ! fakesink sync=false";
    try std.testing.expectEqualStrings(expected, out.launch);

    // serial selection can't be resolved by a planner; it must say so.
    try std.testing.expect(out.plan.warnings.len >= 1);
}

test "orbbec_lucid_pipeline.yaml: Lucid doc (appsrc + bayer2rgb)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const docs = try yaml.parse(a, cfg_orbbec_lucid, null);
    const out = try launchFor(a, docs[1], .{});

    try std.testing.expect(std.mem.startsWith(u8, out.launch, "appsrc name=custom_src_0 caps=video/x-bayer,format=rggb,width=1920,height=1200,framerate=30/1" ++
        " format=time is-live=true block=true do-timestamp=true ! bayer2rgb ! videoconvert ! video/x-raw,format=RGB ! tee name=tee_0"));
    try std.testing.expect(std.mem.indexOf(u8, out.launch, "jpegenc quality=85 ! jpegparse") != null);

    // The appsrc is exposed as a runtime handle for application code.
    try std.testing.expectEqual(@as(usize, 1), blk: {
        var n: usize = 0;
        for (out.plan.handles) |h| {
            if (h.kind == .frame_source) n += 1;
        }
        break :blk n;
    });
    try std.testing.expectEqualStrings("custom_src_0", out.plan.handles[0].name);
}

test "orbbec_lucid_pipeline.yaml: inference doc, with and without nvunixfdsrc" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const docs = try yaml.parse(a, cfg_orbbec_lucid, null);

    const with = try launchFor(a, docs[2], .{ .hw = .{ .has_nvunixfdsrc = true } });
    try std.testing.expect(std.mem.startsWith(u8, with.launch, "nvunixfdsrc socket-path=/tmp/camera_nv.sock connection-attempts=-1 do-timestamp=true ! tee name=tee_0  tee_0. ! " ++ queue ++
        " ! nvinferserver config-file-path=configs/inferserver_1.txt"));
    try std.testing.expect(std.mem.indexOf(u8, with.launch, "config-file-path=configs/inferserver_3.txt") != null);

    const without = try launchFor(a, docs[2], .{});
    try std.testing.expect(std.mem.startsWith(u8, without.launch, "videotestsrc is-live=true pattern=smpte ! video/x-raw,format=NV12,width=1280,height=720,framerate=30/1 ! tee"));
}

test "hardware flags steer encoder choice" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const doc = try yaml.parseFirst(a,
        \\elements:
        \\  - type: CustomSrcElement
        \\    format: NV12
        \\    width: 1280
        \\    height: 720
        \\  - type: OptimizedConverter
        \\    outputs: [H264]
        \\    bitrate_kbps: 2000
        \\    resize:
        \\      width: 640
        \\      height: 360
        \\  - type: RTSPSourceElement
        \\    url: rtsp://x
    , null);
    // The third element is a source after a converter, which is odd but legal for lowering.
    const nvh = try launchFor(a, doc, .{ .hw = .{ .has_nvh264enc = true, .has_x264enc = true } });
    try std.testing.expect(std.mem.indexOf(u8, nvh.launch, "videoscale ! video/x-raw,format=NV12,width=640,height=360 ! nvh264enc bitrate=2000") != null);

    const jet = try launchFor(a, doc, .{ .hw = .{
        .has_nvvidconv = true,
        .nvvidconv_name = "nvvidconv",
        .has_nvv4l2h264enc = true,
        .has_x264enc = true,
    } });
    try std.testing.expect(std.mem.indexOf(u8, jet.launch, "nvvidconv ! video/x-raw(memory:NVMM),format=NV12,width=640,height=360 ! nvv4l2h264enc bitrate=2000") != null);

    const none = launchFor(a, doc, .{ .hw = .{} });
    try std.testing.expectError(error.NoH264Encoder, none);
}

test "undistort: nvdewarper path writes a side file, cpu path exposes a handle" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const doc = try yaml.parseFirst(a,
        \\pipeline:
        \\  type: Pipeline
        \\elements:
        \\  - type: CustomSrcElement
        \\    format: RGB
        \\    width: 640
        \\    height: 480
        \\  - type: UndistortElement
        \\  - type: DisplayPublisher
    , null);
    const cal_doc = try yaml.parseFirst(a,
        \\calibration:
        \\  image_width: 640
        \\  image_height: 480
        \\  distortion_model: plumb_bob
        \\  camera_matrix:
        \\    rows: 3
        \\    cols: 3
        \\    data: [500.0, 0.0, 320.0, 0.0, 500.0, 240.0, 0.0, 0.0, 1.0]
        \\  distortion_coefficients:
        \\    rows: 1
        \\    cols: 5
        \\    data: [-0.1, 0.01, 0.001, 0.002, 0.0]
    , null);

    const g = try graph_mod.fromValue(a, doc, null);

    // GPU path
    {
        var f = gst.GstPipelineFactory.init(a, .{ .hw = .{ .has_nvdewarper = true, .has_nvvidconv = true, .nvvidconv_name = "nvvidconv" } });
        var sp: plan_mod.Scratchpad = .empty;
        try sp.put(a, calib.metadata_key, cal_doc);
        const out = try f.build(g, &sp);
        try std.testing.expect(std.mem.indexOf(u8, out.launch, "nvvidconv ! video/x-raw(memory:NVMM),format=RGBA ! nvdewarper config-file=/tmp/camera_driver_undistort_undistort_0.cfg") != null);
        try std.testing.expectEqual(@as(usize, 1), out.plan.side_files.len);
        try std.testing.expectEqualStrings(
            "[property]\noutput-width=640\noutput-height=480\nnum-batch-buffers=1\n[surface0]\nprojection-type=3\nsurface-index=0\n" ++
                "width=640\nheight=480\nfocal-length=500;500\nsrc-x0=320\nsrc-y0=240\ndistortion=-0.1;0.01;0;0.001;0.002\n",
            out.plan.side_files[0].contents,
        );
        // Downstream sees rectified metadata.
        const after = try calib.metadataFromValue(a, sp.get(calib.metadata_key).?);
        try std.testing.expectEqual(@as(usize, 0), after.calibration.?.D.len);
        try std.testing.expectEqual(@as(f64, 500), after.calibration.?.K[0]);
    }
    // CPU path
    {
        var f = gst.GstPipelineFactory.init(a, .{});
        var sp: plan_mod.Scratchpad = .empty;
        try sp.put(a, calib.metadata_key, cal_doc);
        const out = try f.build(g, &sp);
        try std.testing.expect(std.mem.indexOf(u8, out.launch, "videoconvert ! video/x-raw,format=RGB ! camera_driver_undistort name=undistort_0_filter") != null);
        var found = false;
        for (out.plan.handles) |h| {
            if (h.kind == .element and std.mem.eql(u8, h.name, "undistort_0_filter")) found = true;
        }
        try std.testing.expect(found);
    }
    // No calibration: passthrough
    {
        var f = gst.GstPipelineFactory.init(a, .{});
        var sp: plan_mod.Scratchpad = .empty;
        const out = try f.build(g, &sp);
        try std.testing.expect(std.mem.indexOf(u8, out.launch, "undistort") == null);
    }
}

test "recorders prepend h264parse for encoded input and videoconvert for raw" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const doc = try yaml.parseFirst(a,
        \\elements:
        \\  - type: RTSPSourceElement
        \\    url: rtsp://cam/stream
        \\    latency_ms: 50
        \\  - type: MkvRecorderElement
        \\    location: /tmp/out.mkv
    , null);
    const out = try launchFor(a, doc, .{});
    try std.testing.expectEqualStrings(
        "rtspsrc location=rtsp://cam/stream latency=50 ! rtph264depay ! h264parse ! " ++
            "h264parse ! matroskamux name=mkv_recorder_0_mux ! filesink name=mkv_recorder_0_sink location=/tmp/out.mkv",
        out.launch,
    );
}

test "debug_display.yaml lowers without error" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const out = try launchFor(a, try yaml.parseFirst(a, cfg_debug_display, null), .{});
    try std.testing.expect(out.launch.len > 0);
}

// ─── web backend ─────────────────────────────────────────────────────────────

const web = @import("web_factory.zig");

fn webFor(arena: std.mem.Allocator, doc: props.Value, caps: web.WebCaps, sp: *plan_mod.Scratchpad) !web.Output {
    const g = try graph_mod.fromValue(arena, doc, null);
    var f = web.WebPipelineFactory.init(arena, .{ .caps = caps });
    return f.build(g, sp);
}

fn expectValidJson(a: std.mem.Allocator, text: []const u8) !std.json.Value {
    const parsed = std.json.parseFromSlice(std.json.Value, a, text, .{}) catch |e| {
        std.debug.print("invalid json: {s}\n{s}\n", .{ @errorName(e), text });
        return e;
    };
    return parsed.value;
}

test "web: a browser-friendly pipeline plans cleanly" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const doc = try yaml.parseFirst(a,
        \\elements:
        \\  - type: V4L2SrcElement
        \\    width: 1280
        \\    height: 720
        \\  - type: UndistortElement
        \\  - type: MuxElement
        \\    branches:
        \\      - - type: DisplayPublisher
        \\      - - type: OptimizedConverter
        \\          outputs: [MJPEG]
        \\          quality: 70
        \\        - type: CustomPublisher
    , null);
    const cal = try yaml.parseFirst(a,
        \\image_width: 1280
        \\image_height: 720
        \\camera_matrix: {rows: 3, cols: 3, data: [900.0, 0.0, 640.0, 0.0, 900.0, 360.0, 0.0, 0.0, 1.0]}
        \\distortion_coefficients: {rows: 1, cols: 5, data: [-0.2, 0.05, 0.0, 0.0, 0.0]}
    , null);
    var sp: plan_mod.Scratchpad = .empty;
    try sp.put(a, calib.metadata_key, .{ .map = blk: {
        var m: props.Props = .empty;
        try m.put(a, "calibration", cal);
        break :blk m;
    } });

    const out = try webFor(a, doc, web.WebCaps.all, &sp);
    try std.testing.expect(out.ok);
    const root = try expectValidJson(a, out.json);
    const chain = root.object.get("chain").?.array.items;
    try std.testing.expectEqualStrings("camera", chain[0].object.get("impl").?.string);
    try std.testing.expectEqualStrings("undistort-webgpu", chain[1].object.get("impl").?.string);
    try std.testing.expectEqualStrings("tee", chain[2].object.get("impl").?.string);

    const branches = chain[2].object.get("branches").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), branches.len);
    try std.testing.expectEqualStrings("canvas-display", branches[0].array.items[0].object.get("impl").?.string);
    const enc = branches[1].array.items[0];
    try std.testing.expectEqualStrings("encode-jpeg", enc.object.get("impl").?.string);
    try std.testing.expectEqual(@as(i64, 70), enc.object.get("config").?.object.get("quality").?.integer);
    const cfg_cal = chain[1].object.get("config").?.object;
    try std.testing.expectEqual(@as(usize, 9), cfg_cal.get("K").?.array.items.len);

    // Without WebGPU the same element falls back to wasm.
    var sp2: plan_mod.Scratchpad = .empty;
    try sp2.put(a, calib.metadata_key, sp.get(calib.metadata_key).?);
    const cpu = try webFor(a, doc, .{ .camera = true, .canvas = true, .jpeg_encode = true }, &sp2);
    const root2 = try expectValidJson(a, cpu.json);
    try std.testing.expectEqualStrings("undistort-cpu", root2.object.get("chain").?.array.items[1].object.get("impl").?.string);
}

test "web: native-only elements are reported, not dropped" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const docs = try yaml.parse(a, cfg_orbbec_lucid, null);
    var sp: plan_mod.Scratchpad = .empty;
    const out = try webFor(a, docs[0], web.WebCaps.all, &sp);
    try std.testing.expect(!out.ok);

    var saw_iox = false;
    var saw_mjpeg = false;
    var saw_fd = false;
    for (out.unsupported) |u| {
        if (std.mem.eql(u8, u.type_name, "IceOryxPublisher")) saw_iox = true;
        if (std.mem.eql(u8, u.type_name, "MJPEGPublisher")) saw_mjpeg = true;
        if (std.mem.eql(u8, u.type_name, "NVUnixFDPublisher")) saw_fd = true;
    }
    try std.testing.expect(saw_iox and saw_mjpeg and saw_fd);
    _ = try expectValidJson(a, out.json);
}

test "web: missing host capabilities are explained" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const doc = try yaml.parseFirst(a,
        \\elements:
        \\  - type: V4L2SrcElement
        \\  - type: OptimizedConverter
        \\    outputs: [H264]
        \\  - type: DisplayPublisher
    , null);
    var sp: plan_mod.Scratchpad = .empty;
    const out = try webFor(a, doc, .{ .camera = true, .canvas = true }, &sp);
    try std.testing.expect(!out.ok);
    try std.testing.expectEqual(@as(usize, 1), out.unsupported.len);
    try std.testing.expect(std.mem.indexOf(u8, out.unsupported[0].reason, "WebCodecs") != null);
}

test "web: caps csv" {
    const c = web.WebCaps.fromCsv("camera, webgpu,canvas");
    try std.testing.expect(c.camera and c.webgpu and c.canvas and !c.mcap);
}

test "http mjpeg source: the same element lowers on both backends" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const doc = try yaml.parseFirst(a,
        \\elements:
        \\  - type: HttpMjpegSourceElement
        \\    url: http://robot.local:8080/
        \\  - type: DisplayPublisher
    , null);
    const g = try launchFor(a, doc, .{});
    try std.testing.expectEqualStrings(
        "souphttpsrc location=http://robot.local:8080/ is-live=true do-timestamp=true ! multipartdemux ! jpegparse ! jpegdec ! autovideoconvert ! fpsdisplaysink text-overlay=false sync=false",
        g.launch,
    );
    var sp: plan_mod.Scratchpad = .empty;
    const w = try webFor(a, doc, web.WebCaps.all, &sp);
    try std.testing.expect(w.ok);
}
// end

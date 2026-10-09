//! WebPipelineFactory: lowers a `Graph` to a JSON *plan* that a browser host
//! (web/host.js) executes with web APIs. The factory never touches the DOM;
//! like the gst factory it is a pure function of (graph, host capabilities,
//! scratchpad), which is what lets the same Zig core run in wasm.
//!
//! The stage vocabulary (`impl` ids) is the contract with the host:
//!
//!   sources    camera, push-source, http-mjpeg-source, mkv-playback, mcap-source
//!   transforms encode-jpeg, encode-h264, convert, undistort-webgpu, undistort-cpu
//!   fan-out    tee
//!   sinks      canvas-display, callback-sink, record-matroska, record-mcap
//!
//! Elements with no sensible browser equivalent (V4L2 pipes, nvunixfd, iceoryx,
//! RTSP, DeepStream) are reported in `unsupported` with a hint instead of
//! being silently dropped; `ok` is false when any are present.

const std = @import("std");
const Allocator = std.mem.Allocator;
const caps_mod = @import("caps.zig");
const calib = @import("calib.zig");
const elements = @import("elements.zig");
const graph_mod = @import("graph.zig");
const json = @import("json.zig");
const plan_mod = @import("plan.zig");
const registry_mod = @import("registry.zig");
const props = @import("props.zig");

const Caps = caps_mod.Caps;
const PixelFormat = caps_mod.PixelFormat;
const Node = graph_mod.Node;
const Graph = graph_mod.Graph;
const LowerCtx = plan_mod.LowerCtx;
const Lowered = plan_mod.Lowered;
const Segment = plan_mod.Segment;
const Scratchpad = plan_mod.Scratchpad;

/// What the host browser can do; the web counterpart of `HwCaps`.
pub const WebCaps = struct {
    camera: bool = false, // navigator.mediaDevices.getUserMedia
    canvas: bool = false, // 2D canvas / OffscreenCanvas display
    jpeg_encode: bool = false, // OffscreenCanvas.convertToBlob('image/jpeg')
    webcodecs_h264_encode: bool = false,
    webcodecs_h264_decode: bool = false,
    webgpu: bool = false,
    fetch_stream: bool = false, // streaming fetch() for multipart MJPEG
    matroska: bool = false, // a JS Matroska muxer is bundled
    mcap: bool = false, // @mcap/core is bundled

    pub const all: WebCaps = .{
        .camera = true,
        .canvas = true,
        .jpeg_encode = true,
        .webcodecs_h264_encode = true,
        .webcodecs_h264_decode = true,
        .webgpu = true,
        .fetch_stream = true,
        .matroska = true,
        .mcap = true,
    };

    pub fn fromCsv(csv: []const u8) WebCaps {
        var c: WebCaps = .{};
        var it = std.mem.tokenizeAny(u8, csv, ", ");
        while (it.next()) |n| {
            inline for (comptime std.meta.fieldNames(WebCaps)) |fname| {
                if (std.mem.eql(u8, n, fname)) @field(c, fname) = true;
            }
        }
        return c;
    }
};

pub const Options = struct {
    registry: *const registry_mod.Registry,
    caps: WebCaps = WebCaps.all,
};

pub const Unsupported = struct {
    name: []const u8,
    type_name: []const u8,
    reason: []const u8,
};

pub const Output = struct {
    ok: bool,
    /// The JSON plan (see `toJson`).
    json: []const u8,
    plan: plan_mod.Plan,
    unsupported: []const Unsupported,
};

pub const WebPipelineFactory = struct {
    gpa: Allocator,
    opts: Options,
    unsupported: std.ArrayList(Unsupported) = .empty,

    pub fn init(gpa: Allocator, opts: Options) WebPipelineFactory {
        return .{ .gpa = gpa, .opts = opts };
    }

    pub fn build(self: *WebPipelineFactory, graph: Graph, scratchpad: *Scratchpad) !Output {
        const plan = try plan_mod.resolve(WebPipelineFactory, self, self.gpa, graph, scratchpad);
        const ok = self.unsupported.items.len == 0;
        return .{
            .ok = ok,
            .json = try toJson(self.gpa, graph, plan, self.unsupported.items, ok),
            .plan = plan,
            .unsupported = self.unsupported.items,
        };
    }

    // ── factory interface ────────────────────────────────────────────────

    pub fn preferredInputs(_: *WebPipelineFactory, _: *const Node) []const PixelFormat {
        // Browser frames are format-agnostic VideoFrame/ImageBitmap objects.
        return &.{};
    }

    pub fn lower(self: *WebPipelineFactory, ctx: *LowerCtx, node: *const Node, _: []const []const Segment) anyerror!Lowered {
        const t = node.type_name;
        const c = self.opts.caps;

        if (eq(t, "V4L2SrcElement")) {
            if (!c.camera) return self.no(ctx, node, "getUserMedia is not available in this host");
            return stage(ctx, "camera", null, .{
                .format = .rgba,
                .width = @intCast(props.getInt(node.props, "width", 0)),
                .height = @intCast(props.getInt(node.props, "height", 0)),
                .fps_num = @intCast(props.getInt(node.props, "fps", 30)),
            });
        }
        if (eq(t, "CustomSrcElement")) {
            try ctx.out.handle(node.name, .frame_source, t);
            return stage(ctx, "push-source", null, .{
                .format = PixelFormat.parse(props.getStringOr(node.props, "format", "RGB")),
                .width = @intCast(props.getInt(node.props, "width", 640)),
                .height = @intCast(props.getInt(node.props, "height", 480)),
                .fps_num = @intCast(props.getInt(node.props, "fps", 30)),
            });
        }
        if (eq(t, "HttpMjpegSourceElement")) {
            if (!c.fetch_stream) return self.no(ctx, node, "streaming fetch() is not available in this host");
            return stage(ctx, "http-mjpeg-source", null, Caps.ofFormat(.mjpeg));
        }
        if (eq(t, "MkvPlaybackElement")) {
            if (!c.webcodecs_h264_decode or !c.matroska)
                return self.no(ctx, node, "needs WebCodecs H.264 decoding and a Matroska demuxer");
            return stage(ctx, "mkv-playback", null, Caps.ofFormat(.rgba));
        }
        if (eq(t, "McapSourceElement")) {
            if (!c.mcap) return self.no(ctx, node, "MCAP support is not bundled with this host");
            try ctx.out.handle(node.name, .topic_reader, t);
            return stage(ctx, "mcap-source", null, Caps.ofFormat(.rgba));
        }

        if (eq(t, "OptimizedConverter")) return self.encoder(ctx, node);
        if (eq(t, "AutoVideoConverterElement")) {
            const target = PixelFormat.parse(props.getStringOr(node.props, "target", ""));
            const out_fmt: PixelFormat = if (target != .unknown) target else if (ctx.upstream.format != .unknown) ctx.upstream.format else .rgba;
            return stage(ctx, "convert", null, Caps.ofFormat(out_fmt));
        }
        if (eq(t, "UndistortElement")) return self.undistort(ctx, node);

        if (eq(t, "MuxElement")) return stage(ctx, "tee", null, Caps.any);

        if (eq(t, "DisplayPublisher")) {
            if (!c.canvas) return self.no(ctx, node, "no canvas available in this host");
            return stage(ctx, "canvas-display", null, ctx.upstream);
        }
        if (eq(t, "CustomPublisher")) {
            try ctx.out.handle(node.name, .frame_sink, t);
            return stage(ctx, "callback-sink", null, Caps.any);
        }
        if (eq(t, "MkvRecorderElement")) {
            if (!c.matroska) return self.no(ctx, node, "no Matroska muxer bundled with this host");
            return stage(ctx, "record-matroska", null, ctx.upstream);
        }
        if (eq(t, "McapSinkElement")) {
            if (!c.mcap) return self.no(ctx, node, "MCAP support is not bundled with this host");
            try ctx.out.handle(node.name, .topic_writer, t);
            return stage(ctx, "record-mcap", null, ctx.upstream);
        }

        // Things a page cannot do. The hint says what to use instead.
        if (eq(t, "RTSPSourceElement")) return self.no(ctx, node, "browsers cannot open RTSP; republish through a WebRTC/WHEP gateway or MJPEG and use HttpMjpegSourceElement");
        if (eq(t, "NvUnixFdSrcElement") or eq(t, "NVUnixFDPublisher") or eq(t, "NvCUDAPublisher") or eq(t, "IceOryxPublisher"))
            return self.no(ctx, node, "needs host IPC/GPU memory sharing (Unix sockets, NVMM, shared memory)");
        if (eq(t, "MJPEGPublisher")) return self.no(ctx, node, "a page cannot listen on a port; publish from the native pipeline and consume it with HttpMjpegSourceElement");
        if (eq(t, "ROS2Publisher")) return self.no(ctx, node, "no ROS2 in the browser; bridge via rosbridge/foxglove websocket");
        if (eq(t, "InferServerElement")) return self.no(ctx, node, "DeepStream inference is native-only; run it in the native pipeline");
        if (eq(t, "GstElement")) return self.no(ctx, node, "raw GStreamer elements have no browser equivalent");
        return self.lowerPlugin(ctx, node);
    }

    /// Elements added at runtime: both manifests and native plugins name the
    /// browser `impl` that realises them (or say why there is none).
    fn lowerPlugin(self: *WebPipelineFactory, ctx: *LowerCtx, node: *const Node) !Lowered {
        const entry = self.opts.registry.find(node.type_name) orelse
            return self.no(ctx, node, "no web implementation for this element type");

        const Info = struct { impl: ?[]const u8, reason: ?[]const u8, out_format: PixelFormat };
        const info: Info = switch (entry.kind) {
            .builtin => .{ .impl = null, .reason = null, .out_format = .unknown },
            .manifest => |m| .{ .impl = m.web_impl, .reason = m.web_reason, .out_format = m.out_format },
            .native => |def| .{
                .impl = if (def.web_impl) |p| std.mem.span(p) else null,
                .reason = if (def.web_reason) |p| std.mem.span(p) else null,
                .out_format = .unknown,
            },
        };
        if (entry.backends & @import("plugin_abi.zig").backend_web == 0 or info.impl == null)
            return self.no(ctx, node, info.reason orelse "no web implementation for this element type");

        switch (entry.kind) {
            .manifest => |m| for (m.handles) |h| {
                var missing: []const u8 = "";
                const hn = registry_mod.renderTemplate(ctx.gpa, h.name, node.name, node.props, &missing) catch continue;
                try ctx.out.handle(hn, h.kind, node.type_name);
            },
            else => {},
        }
        var out = ctx.upstream;
        if (info.out_format != .unknown) out = Caps.ofFormat(info.out_format);
        return .{ .text = try implText(ctx, info.impl.?, null), .out = out };
    }

    fn no(self: *WebPipelineFactory, ctx: *LowerCtx, node: *const Node, reason: []const u8) !Lowered {
        try self.unsupported.append(self.gpa, .{ .name = node.name, .type_name = node.type_name, .reason = reason });
        try ctx.out.warn("{s} ({s}): {s}", .{ node.name, node.type_name, reason });
        return .{ .text = try implText(ctx, "unsupported", null), .out = Caps.any };
    }

    fn encoder(self: *WebPipelineFactory, ctx: *LowerCtx, node: *const Node) !Lowered {
        const outputs = try props.getStringList(ctx.gpa, node.props, "outputs");
        if (outputs.len == 0) return self.no(ctx, node, "OptimizedConverter: outputs is empty");
        const target = PixelFormat.parse(outputs[0]);
        const has_resize = node.props.contains("resize");
        const c = self.opts.caps;

        if (ctx.upstream.format == target and !has_resize) return .{ .text = "", .out = ctx.upstream };

        var cfg: json.Writer = json.Writer.init(ctx.gpa);
        try cfg.beginObject();
        switch (target) {
            .mjpeg => {
                if (!c.jpeg_encode) return self.no(ctx, node, "JPEG encoding (OffscreenCanvas.convertToBlob) is not available");
                try cfg.key("quality");
                try cfg.int(props.getInt(node.props, "quality", 85));
            },
            .h264, .h264_nvmm => {
                if (!c.webcodecs_h264_encode) return self.no(ctx, node, "WebCodecs H.264 encoding is not available");
                try cfg.key("bitrate_kbps");
                try cfg.int(props.getInt(node.props, "bitrate_kbps", 0));
            },
            else => return self.no(ctx, node, "unsupported target format"),
        }
        if (props.getMap(node.props, "resize")) |r| {
            try cfg.key("resize");
            try cfg.map(r);
        }
        try cfg.endObject();

        const out_fmt: PixelFormat = if (target == .mjpeg) .mjpeg else .h264;
        return .{ .text = try implText(ctx, if (target == .mjpeg) "encode-jpeg" else "encode-h264", cfg.bytes()), .out = Caps.ofFormat(out_fmt) };
    }

    fn undistort(self: *WebPipelineFactory, ctx: *LowerCtx, node: *const Node) !Lowered {
        const md_value = ctx.scratchpad.get(calib.metadata_key) orelse {
            try ctx.out.note("{s}: passthrough (no camera-metadata in scratchpad)", .{node.name});
            return .{ .text = "", .out = ctx.upstream };
        };
        const md = try calib.metadataFromValue(ctx.gpa, md_value);
        const cal = md.calibration orelse {
            try ctx.out.note("{s}: passthrough (scratchpad metadata has no calibration)", .{node.name});
            return .{ .text = "", .out = ctx.upstream };
        };
        try ctx.scratchpad.put(ctx.gpa, calib.metadata_key, try calib.rectifiedValue(ctx.gpa, md_value));

        var cfg = json.Writer.init(ctx.gpa);
        try cfg.beginObject();
        try cfg.key("width");
        try cfg.int(cal.width);
        try cfg.key("height");
        try cfg.int(cal.height);
        try cfg.key("model");
        try cfg.string(cal.distortion_model);
        try cfg.key("K");
        try cfg.arrayValue();
        for (cal.K) |k| try cfg.float(k);
        try cfg.endArray();
        try cfg.key("D");
        try cfg.arrayValue();
        for (cal.D) |d| try cfg.float(d);
        try cfg.endArray();
        try cfg.endObject();

        const impl: []const u8 = if (self.opts.caps.webgpu) "undistort-webgpu" else "undistort-cpu";
        if (!self.opts.caps.webgpu) try ctx.out.warn("{s}: WebGPU unavailable; undistorting on the CPU (wasm)", .{node.name});
        var out = ctx.upstream;
        out.format = .rgba;
        return .{ .text = try implText(ctx, impl, cfg.bytes()), .out = out };
    }
};

fn stage(ctx: *LowerCtx, impl: []const u8, config: ?[]const u8, out: Caps) !Lowered {
    return .{ .text = try implText(ctx, impl, config), .out = out };
}

/// The stage-specific JSON members: `"impl":"..."` and optional `"config":{...}`.
fn implText(ctx: *LowerCtx, impl: []const u8, config: ?[]const u8) ![]const u8 {
    var w = json.Writer.init(ctx.gpa);
    try w.beginObject();
    try w.key("impl");
    try w.string(impl);
    try w.endObject();
    var body = w.bytes()[1 .. w.bytes().len - 1]; // strip braces
    if (config) |cfg| body = try std.fmt.allocPrint(ctx.gpa, "{s},\"config\":{s}", .{ body, cfg });
    return body;
}

fn eq(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

// ── JSON plan ────────────────────────────────────────────────────────────

fn writeCaps(w: *json.Writer, c: Caps) !void {
    try w.objectValue();
    if (c.is_any) {
        try w.key("any");
        try w.boolean(true);
    } else {
        try w.key("format");
        try w.string(c.format.name());
        try w.key("width");
        try w.int(c.width);
        try w.key("height");
        try w.int(c.height);
        try w.key("fps");
        try w.float(if (c.fps_den > 0) @as(f64, @floatFromInt(c.fps_num)) / @as(f64, @floatFromInt(c.fps_den)) else 0);
    }
    try w.endObject();
}

fn writeStages(w: *json.Writer, segs: []const Segment) anyerror!void {
    for (segs) |s| {
        if (s.text.len == 0) continue; // passthrough
        try w.beginObject();
        try w.key("name");
        try w.string(s.node.name);
        try w.key("type");
        try w.string(s.node.type_name);
        if (elements.find(s.node.type_name)) |spec| {
            try w.key("role");
            try w.string(@tagName(spec.role));
        }
        // Splice the factory-provided members (`"impl":...[,"config":...]`).
        // They are already valid JSON members, so write them raw.
        try w.buf.append(w.gpa, ',');
        try w.buf.appendSlice(w.gpa, s.text);
        try w.key("props");
        if (s.branches.len > 0) {
            // The branches are emitted resolved below; don't repeat the raw config.
            var trimmed: props.Props = .empty;
            var it = s.node.props.iterator();
            while (it.next()) |e| {
                if (!std.mem.eql(u8, e.key_ptr.*, "branches")) try trimmed.put(w.gpa, e.key_ptr.*, e.value_ptr.*);
            }
            try w.map(trimmed);
        } else {
            try w.map(s.node.props);
        }
        try w.key("in");
        try writeCaps(w, s.in);
        try w.key("out");
        try writeCaps(w, s.out);
        if (s.branches.len > 0) {
            try w.key("branches");
            try w.arrayValue();
            for (s.branches) |b| {
                try w.arrayValue();
                try writeStages(w, b);
                try w.endArray();
            }
            try w.endArray();
        }
        try w.endObject();
    }
}

fn writeStringArray(w: *json.Writer, key: []const u8, items: []const []const u8) !void {
    try w.key(key);
    try w.arrayValue();
    for (items) |i| try w.string(i);
    try w.endArray();
}

pub fn toJson(gpa: Allocator, graph: Graph, plan: plan_mod.Plan, unsupported: []const Unsupported, ok: bool) ![]const u8 {
    var w = json.Writer.init(gpa);
    try w.beginObject();
    try w.key("backend");
    try w.string("web");
    try w.key("ok");
    try w.boolean(ok);
    try w.key("pipeline");
    try w.map(graph.pipeline);

    try w.key("chain");
    try w.arrayValue();
    try writeStages(&w, plan.chain);
    try w.endArray();

    try w.key("handles");
    try w.arrayValue();
    for (plan.handles) |h| {
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

    try w.key("unsupported");
    try w.arrayValue();
    for (unsupported) |u| {
        try w.beginObject();
        try w.key("name");
        try w.string(u.name);
        try w.key("type");
        try w.string(u.type_name);
        try w.key("reason");
        try w.string(u.reason);
        try w.endObject();
    }
    try w.endArray();

    try writeStringArray(&w, "warnings", plan.warnings);
    try writeStringArray(&w, "notes", plan.notes);
    try w.endObject();
    return w.bytes();
}

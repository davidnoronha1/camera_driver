//! GstPipelineFactory: lowers a resolved `Graph` to a `gst_parse_launch`
//! string (plus handles for appsrc/appsink bindings and side files).
//!
//! This is a *pure function* of (graph, hardware flags, device info,
//! scratchpad): no GStreamer link, no filesystem. The emitted strings are
//! meant to be byte-identical to what the C++ elements produce, so a native
//! runtime can feed them to `gst_parse_launch` unchanged.

const std = @import("std");
const Allocator = std.mem.Allocator;
const caps_mod = @import("caps.zig");
const calib = @import("calib.zig");
const graph_mod = @import("graph.zig");
const hw_mod = @import("hw.zig");
const negotiate = @import("negotiate.zig");
const native = @import("native.zig");
const plan_mod = @import("plan.zig");
const registry_mod = @import("registry.zig");
const props = @import("props.zig");

const Caps = caps_mod.Caps;
const PixelFormat = caps_mod.PixelFormat;
const Node = graph_mod.Node;
const Graph = graph_mod.Graph;
const HwCaps = hw_mod.HwCaps;
const LowerCtx = plan_mod.LowerCtx;
const Lowered = plan_mod.Lowered;
const Segment = plan_mod.Segment;
const Scratchpad = plan_mod.Scratchpad;
const Value = props.Value;

pub const Error = error{
    NoH264Encoder,
    NoJpegEncoder,
    NoH264Decoder,
    EmptyOutputs,
    UnsupportedTarget,
    InvalidOption,
    UnsupportedOnGst,
    MissingOption,
    BadTemplate,
    PluginFailed,
    NoGstLowering,
    OutOfMemory,
};

pub const Options = struct {
    registry: *const registry_mod.Registry,
    hw: HwCaps = HwCaps.software,
    /// Per-node device info, keyed by node name (`v4l2_src_0`). A node's own
    /// `device:` option overrides the path.
    v4l2_devices: std.StringHashMapUnmanaged(negotiate.DeviceInfo) = .empty,
    default_device: negotiate.DeviceInfo = .{},
    /// Metadata embedded in recordings, keyed by file location. A playback
    /// source publishes it into the scratchpad (host reads the container).
    recording_metadata: std.StringHashMapUnmanaged(Value) = .empty,
};

pub const Output = struct {
    /// The `gst_parse_launch` description.
    launch: []const u8,
    plan: plan_mod.Plan,
};

pub const GstPipelineFactory = struct {
    gpa: Allocator,
    opts: Options,

    pub fn init(gpa: Allocator, opts: Options) GstPipelineFactory {
        return .{ .gpa = gpa, .opts = opts };
    }

    pub fn build(self: *GstPipelineFactory, graph: Graph, scratchpad: *Scratchpad) !Output {
        const plan = try plan_mod.resolve(GstPipelineFactory, self, self.gpa, graph, scratchpad);
        return .{ .launch = try assembleChain(self.gpa, plan.chain), .plan = plan };
    }

    // ── factory interface ────────────────────────────────────────────────

    pub fn preferredInputs(self: *GstPipelineFactory, node: *const Node) []const PixelFormat {
        const t = node.type_name;
        const hw = self.opts.hw;
        if (eq(t, "OptimizedConverter")) return optimizedConverterPrefs(self, node);
        if (eq(t, "MkvRecorderElement") or eq(t, "McapSinkElement"))
            return &.{ .h264, .i420, .nv12, .rgb, .bgr, .yuyv };
        if (eq(t, "DisplayPublisher")) return &.{ .bgr, .rgb, .yuyv, .mjpeg, .nv12, .i420, .rgba };
        if (eq(t, "MJPEGPublisher")) return &.{.mjpeg};
        if (eq(t, "NVUnixFDPublisher")) return if (hw.has_nvunixfdsink) &.{ .nv12_nvmm, .nv12 } else &.{};
        if (eq(t, "NvCUDAPublisher")) return &.{ .nv12_nvmm, .nv12 };
        if (eq(t, "IceOryxPublisher")) return &.{ .rgb, .bgr, .yuyv, .nv12 };
        if (eq(t, "ROS2Publisher")) return &.{.rgb};
        if (eq(t, "CustomPublisher")) return &.{ .rgb, .bgr, .yuyv };
        if (self.opts.registry.find(t)) |entry| switch (entry.kind) {
            .builtin => {},
            .manifest => |m| return m.preferred_inputs,
            .native => |def| return native.preferredInputs(self.gpa, def, node) catch &.{},
        };
        return &.{};
    }

    pub fn lower(self: *GstPipelineFactory, ctx: *LowerCtx, node: *const Node, branches: []const []const Segment) anyerror!Lowered {
        const t = node.type_name;
        if (eq(t, "V4L2SrcElement")) return self.v4l2Src(ctx, node);
        if (eq(t, "RTSPSourceElement")) return rtspSrc(ctx, node);
        if (eq(t, "CustomSrcElement")) return customSrc(ctx, node);
        if (eq(t, "HttpMjpegSourceElement")) return httpMjpegSrc(ctx, node);
        if (eq(t, "NvUnixFdSrcElement")) return self.nvUnixFdSrc(ctx, node);
        if (eq(t, "MkvPlaybackElement")) return self.mkvPlayback(ctx, node);
        if (eq(t, "McapSourceElement")) return self.mcapSource(ctx, node);
        if (eq(t, "OptimizedConverter")) return self.optimizedConverter(ctx, node);
        if (eq(t, "AutoVideoConverterElement")) return autoConverter(ctx, node);
        if (eq(t, "UndistortElement")) return self.undistort(ctx, node);
        if (eq(t, "InferServerElement")) return inferServer(ctx, node);
        if (eq(t, "GstElement")) return gstElement(ctx, node);
        if (eq(t, "MuxElement")) return mux(ctx, node, branches);
        if (eq(t, "MkvRecorderElement")) return mkvRecorder(ctx, node);
        if (eq(t, "McapSinkElement")) return mcapSink(ctx, node);
        if (eq(t, "MJPEGPublisher") or eq(t, "IceOryxPublisher") or eq(t, "ROS2Publisher") or eq(t, "NvCUDAPublisher"))
            return appSinkPublisher(ctx, node, "max-buffers=2 drop=true sync=false emit-signals=false", .frame_sink);
        if (eq(t, "CustomPublisher")) return customPublisher(ctx, node);
        if (eq(t, "DisplayPublisher")) return self.display(ctx, node);
        if (eq(t, "NVUnixFDPublisher")) return self.nvUnixFdPublisher(ctx, node);
        return self.lowerPlugin(ctx, node);
    }

    /// Elements added at runtime: declarative manifests or native plugins.
    fn lowerPlugin(self: *GstPipelineFactory, ctx: *LowerCtx, node: *const Node) !Lowered {
        const entry = self.opts.registry.find(node.type_name) orelse return error.UnsupportedOnGst;
        switch (entry.kind) {
            .builtin => return error.UnsupportedOnGst,
            .native => |def| return native.lower(def, ctx, self.opts.hw, node) catch |e| switch (e) {
                error.PluginFailed => return error.PluginFailed,
                error.NoGstLowering => return error.NoGstLowering,
                error.OutOfMemory => return error.OutOfMemory,
            },
            .manifest => |m| {
                const template = m.gst_template orelse return error.NoGstLowering;
                var missing: []const u8 = "";
                const text = registry_mod.renderTemplate(ctx.gpa, template, node.name, node.props, &missing) catch |e| switch (e) {
                    error.MissingOption => {
                        try ctx.out.warn("{s}: template for {s} needs option '{s}'", .{ node.name, node.type_name, missing });
                        return error.MissingOption;
                    },
                    error.BadTemplate => return error.BadTemplate,
                    error.OutOfMemory => return error.OutOfMemory,
                };
                for (m.handles) |h| {
                    const hn = registry_mod.renderTemplate(ctx.gpa, h.name, node.name, node.props, &missing) catch return error.BadTemplate;
                    try ctx.out.handle(hn, h.kind, node.type_name);
                }
                var out = ctx.upstream;
                if (m.out_format != .unknown) out = Caps.ofFormat(m.out_format);
                return .{ .text = text, .out = out };
            },
        }
    }

    // ── sources ──────────────────────────────────────────────────────────

    fn v4l2Src(self: *GstPipelineFactory, ctx: *LowerCtx, node: *const Node) !Lowered {
        const width: i32 = @intCast(props.getInt(node.props, "width", 0));
        const height: i32 = @intCast(props.getInt(node.props, "height", 0));
        const fps_in: i32 = @intCast(props.getInt(node.props, "fps", 30));

        var dev = self.opts.v4l2_devices.get(node.name) orelse self.opts.default_device;
        if (props.getString(node.props, "device")) |p| dev.path = p;
        if (self.opts.v4l2_devices.get(node.name) == null and props.getString(node.props, "device") == null and
            hasDeviceCriteria(node))
        {
            try ctx.out.warn(
                "{s}: serial/port/format selection needs device enumeration, which the planner cannot do; " ++
                    "assuming {s}. Pass the resolved device via Options.v4l2_devices or set 'device:'.",
                .{ node.name, dev.path },
            );
        }

        const chosen = negotiate.pickOutputFormat(ctx.downstream_prefs, dev.formats, width, height, fps_in);
        const res = negotiate.resolveResolution(dev.formats, chosen, width, height);
        const fps: i32 = if (fps_in > 0) fps_in else 30;

        var caps_s: []const u8 = undefined;
        var post: []const u8 = "";
        switch (chosen) {
            .mjpeg => {
                caps_s = try fmt(ctx, "image/jpeg,width={d},height={d},framerate={d}/1", .{ res[0], res[1], fps });
                post = " ! jpegparse";
            },
            .h264 => {
                caps_s = try fmt(ctx, "video/x-h264,width={d},height={d},framerate={d}/1", .{ res[0], res[1], fps });
                post = " ! h264parse";
            },
            .nv12 => caps_s = try fmt(ctx, "video/x-raw,format=NV12,width={d},height={d},framerate={d}/1", .{ res[0], res[1], fps }),
            else => caps_s = try fmt(ctx, "video/x-raw,format=YUY2,width={d},height={d},framerate={d}/1", .{ res[0], res[1], fps }),
        }

        return .{
            .text = try fmt(ctx, "v4l2src device={s} do-timestamp=true ! {s}{s}", .{ dev.path, caps_s, post }),
            .in = Caps.any,
            .out = .{ .format = chosen, .width = res[0], .height = res[1], .fps_num = fps, .fps_den = 1 },
        };
    }

    fn rtspSrc(ctx: *LowerCtx, node: *const Node) !Lowered {
        const url = props.getString(node.props, "url").?;
        const latency = props.getInt(node.props, "latency_ms", 100);
        const mjpeg = std.mem.eql(u8, props.getStringOr(node.props, "codec", "h264"), "mjpeg");
        const text = if (mjpeg)
            try fmt(ctx, "rtspsrc location={s} latency={d} ! rtpjpegdepay ! jpegparse", .{ url, latency })
        else
            try fmt(ctx, "rtspsrc location={s} latency={d} ! rtph264depay ! h264parse", .{ url, latency });
        return .{ .text = text, .out = Caps.ofFormat(if (mjpeg) .mjpeg else .h264) };
    }

    fn httpMjpegSrc(ctx: *LowerCtx, node: *const Node) !Lowered {
        return .{
            .text = try fmt(ctx, "souphttpsrc location={s} is-live=true do-timestamp=true ! multipartdemux ! jpegparse", .{props.getString(node.props, "url").?}),
            .out = Caps.ofFormat(.mjpeg),
        };
    }

    fn customSrc(ctx: *LowerCtx, node: *const Node) !Lowered {
        const out: Caps = .{
            .format = PixelFormat.parse(props.getStringOr(node.props, "format", "RGB")),
            .width = @intCast(props.getInt(node.props, "width", 640)),
            .height = @intCast(props.getInt(node.props, "height", 480)),
            .fps_num = @intCast(props.getInt(node.props, "fps", 30)),
            .fps_den = 1,
        };
        try ctx.out.handle(node.name, .frame_source, node.type_name);
        return .{
            .text = try fmt(ctx, "appsrc name={s} caps={s} format=time is-live=true block=true do-timestamp=true", .{
                node.name, try out.toGstCapsString(ctx.gpa),
            }),
            .out = out,
        };
    }

    fn nvUnixFdSrc(self: *GstPipelineFactory, ctx: *LowerCtx, node: *const Node) !Lowered {
        const socket = props.getString(node.props, "socket").?;
        const format = PixelFormat.parse(props.getStringOr(node.props, "format", "nv12_nvmm"));
        const width: i32 = @intCast(props.getInt(node.props, "width", 1280));
        const height: i32 = @intCast(props.getInt(node.props, "height", 720));
        const attempts = props.getInt(node.props, "connection_attempts", -1);

        if (self.opts.hw.has_nvunixfdsrc) {
            return .{
                .text = try fmt(ctx, "nvunixfdsrc socket-path={s} connection-attempts={d} do-timestamp=true", .{ socket, attempts }),
                .out = .{ .format = format, .width = width, .height = height, .is_nvmm = format == .nv12_nvmm or format == .h264_nvmm },
            };
        }
        try ctx.out.warn("{s}: nvunixfdsrc not available; falling back to a videotestsrc stand-in", .{node.name});
        return .{
            .text = try fmt(ctx, "videotestsrc is-live=true pattern=smpte ! video/x-raw,format=NV12,width={d},height={d},framerate=30/1", .{ width, height }),
            .out = .{ .format = .nv12, .width = width, .height = height },
        };
    }

    const Decoder = struct { name: []const u8, out: Caps };

    fn pickH264Decoder(self: *GstPipelineFactory) !Decoder {
        const hw = self.opts.hw;
        if (hw.has_nvv4l2h264dec) return .{ .name = "nvv4l2decoder", .out = .{ .format = .nv12_nvmm, .is_nvmm = true } };
        if (hw.has_nvh264dec) return .{ .name = "nvh264dec", .out = Caps.ofFormat(.nv12) };
        if (hw.has_avdec_h264) return .{ .name = "avdec_h264", .out = Caps.ofFormat(.i420) };
        return error.NoH264Decoder;
    }

    fn publishRecordingMetadata(self: *GstPipelineFactory, ctx: *LowerCtx, location: []const u8) !void {
        if (self.opts.recording_metadata.get(location)) |md| {
            try ctx.scratchpad.put(ctx.gpa, calib.metadata_key, md);
            try ctx.out.note("published metadata embedded in {s} to the scratchpad", .{location});
        } else {
            try ctx.out.note(
                "{s}: embedded camera metadata (if any) is read at runtime; UndistortElement downstream is a passthrough at plan time unless it is provided via Options.recording_metadata or pipeline.metadata_file",
                .{location},
            );
        }
    }

    fn mkvPlayback(self: *GstPipelineFactory, ctx: *LowerCtx, node: *const Node) !Lowered {
        const location = props.getString(node.props, "location").?;
        try self.publishRecordingMetadata(ctx, location);
        const dec = try self.pickH264Decoder();
        return .{
            .text = try fmt(ctx, "filesrc location={s} ! matroskademux name={s}_demux ! h264parse ! {s}", .{ location, node.name, dec.name }),
            .out = dec.out,
        };
    }

    /// The C++ element reads the video channel's format from the .mcap file.
    /// A planner cannot open the file, so the channel description is an
    /// option here: `codec: h264` (default) or `codec: raw` with
    /// `format`/`width`/`height`/`fps`.
    fn mcapSource(self: *GstPipelineFactory, ctx: *LowerCtx, node: *const Node) !Lowered {
        const location = props.getString(node.props, "location").?;
        try self.publishRecordingMetadata(ctx, location);
        try ctx.out.handle(try fmt(ctx, "{s}_src", .{node.name}), .frame_source, node.type_name);

        if (std.mem.eql(u8, props.getStringOr(node.props, "codec", "h264"), "raw")) {
            const out: Caps = .{
                .format = PixelFormat.parse(props.getStringOr(node.props, "format", "RGB")),
                .width = @intCast(props.getInt(node.props, "width", 0)),
                .height = @intCast(props.getInt(node.props, "height", 0)),
                .fps_num = @intCast(props.getInt(node.props, "fps", 30)),
                .fps_den = 1,
            };
            return .{
                .text = try fmt(ctx, "appsrc name={s}_src format=time is-live=true block=true caps={s}", .{ node.name, try out.toGstCapsString(ctx.gpa) }),
                .out = out,
            };
        }
        const dec = try self.pickH264Decoder();
        return .{
            .text = try fmt(ctx, "appsrc name={s}_src format=time is-live=true block=true caps=video/x-h264,stream-format=byte-stream,alignment=au ! h264parse ! {s}", .{ node.name, dec.name }),
            .out = dec.out,
        };
    }

    // ── transforms ───────────────────────────────────────────────────────

    const Resize = struct { width: i64, height: i64 };

    fn resizeOf(node: *const Node) ?Resize {
        const m = props.getMap(node.props, "resize") orelse return null;
        return .{ .width = props.getInt(m, "width", 0), .height = props.getInt(m, "height", 0) };
    }

    fn resizeStr(ctx: *LowerCtx, r: ?Resize) ![]const u8 {
        const rs = r orelse return "";
        if (rs.width <= 0 or rs.height <= 0) return "";
        return fmt(ctx, ",width={d},height={d}", .{ rs.width, rs.height });
    }

    const default_encoders = [_][]const u8{ "nvv4l2h264enc", "nvh264enc", "qsvh264enc", "v4l2h264enc", "x264enc" };

    fn chooseEncoder(self: *GstPipelineFactory, node: *const Node) !?[]const u8 {
        const hw = self.opts.hw;
        const prefs = try props.getStringList(self.gpa, node.props, "encoders");
        const list: []const []const u8 = if (prefs.len == 0) &default_encoders else prefs;
        for (list) |enc| {
            if (eq(enc, "nvv4l2h264enc") and hw.has_nvvidconv and hw.has_nvv4l2h264enc) return enc;
            if (eq(enc, "nvh264enc") and hw.has_nvh264enc) return enc;
            if (eq(enc, "qsvh264enc") and hw.has_qsvh264enc) return enc;
            if (eq(enc, "v4l2h264enc") and hw.has_v4l2h264enc) return enc;
            if (eq(enc, "x264enc") and hw.has_x264enc) return enc;
        }
        return null;
    }

    fn firstOutput(self: *GstPipelineFactory, node: *const Node) !PixelFormat {
        const list = props.getStringList(self.gpa, node.props, "outputs") catch return error.OutOfMemory;
        if (list.len == 0) return error.EmptyOutputs;
        return PixelFormat.parse(list[0]);
    }

    fn optimizedConverterPrefs(self: *GstPipelineFactory, node: *const Node) []const PixelFormat {
        const target = self.firstOutput(node) catch return &.{};
        const hw = self.opts.hw;
        if (target == .h264 or target == .h264_nvmm) {
            const enc = (self.chooseEncoder(node) catch null) orelse "";
            if (eq(enc, "nvv4l2h264enc")) return &.{ .nv12_nvmm, .nv12, .yuyv, .mjpeg };
            if (eq(enc, "nvh264enc")) return &.{ .nv12, .yuyv, .mjpeg };
            return &.{ .yuyv, .i420, .mjpeg };
        }
        if (target == .mjpeg) {
            if (hw.has_nvjpegenc) return &.{ .mjpeg, .nv12_nvmm, .nv12, .yuyv };
            return &.{ .mjpeg, .yuyv, .nv12 };
        }
        return &.{ .yuyv, .nv12, .mjpeg };
    }

    fn optimizedConverter(self: *GstPipelineFactory, ctx: *LowerCtx, node: *const Node) !Lowered {
        const target = try self.firstOutput(node);
        const input = ctx.upstream.format;
        const resize = resizeOf(node);

        if (input == target and resize == null) return .{ .text = "", .out = ctx.upstream };

        return switch (target) {
            .h264, .h264_nvmm => self.h264Path(ctx, node, input, resize),
            .mjpeg => self.mjpegPath(ctx, node, input, resize),
            else => error.UnsupportedTarget,
        };
    }

    fn h264Path(self: *GstPipelineFactory, ctx: *LowerCtx, node: *const Node, input: PixelFormat, resize: ?Resize) !Lowered {
        const hw = self.opts.hw;
        const bitrate = props.getInt(node.props, "bitrate_kbps", 0);
        const resize_s = try resizeStr(ctx, resize);
        const enc = (try self.chooseEncoder(node)) orelse return error.NoH264Encoder;
        const bitrate_s = if (bitrate > 0) try fmt(ctx, " bitrate={d}", .{bitrate}) else "";

        var text: []const u8 = undefined;
        if (eq(enc, "nvv4l2h264enc")) {
            const nvc = hw.nvvidconv_name;
            const conv: []const u8 = switch (input) {
                .nv12_nvmm => if (resize_s.len == 0) "" else try fmt(ctx, "{s} ! video/x-raw(memory:NVMM),format=NV12{s} ! ", .{ nvc, resize_s }),
                .nv12 => try fmt(ctx, "{s} ! video/x-raw(memory:NVMM),format=NV12{s} ! ", .{ nvc, resize_s }),
                .mjpeg => try fmt(ctx, "jpegdec ! {s} ! video/x-raw(memory:NVMM),format=NV12{s} ! ", .{ nvc, resize_s }),
                else => try fmt(ctx, "videoconvert ! video/x-raw,format=NV12 ! {s} ! video/x-raw(memory:NVMM),format=NV12{s} ! ", .{ nvc, resize_s }),
            };
            text = try fmt(ctx, "{s}nvv4l2h264enc{s}", .{ conv, bitrate_s });
        } else if (eq(enc, "nvh264enc")) {
            const conv: []const u8 = switch (input) {
                .nv12 => if (resize_s.len > 0) try fmt(ctx, "videoscale ! video/x-raw,format=NV12{s} ! ", .{resize_s}) else "",
                .mjpeg => try fmt(ctx, "jpegdec ! videoconvert ! video/x-raw,format=NV12{s} ! ", .{resize_s}),
                else => try fmt(ctx, "videoconvert ! video/x-raw,format=NV12{s} ! ", .{resize_s}),
            };
            text = try fmt(ctx, "{s}nvh264enc{s}", .{ conv, bitrate_s });
        } else {
            const jpeg = if (input == .mjpeg) "jpegdec ! " else "";
            const conv = try fmt(ctx, "{s}videoconvert ! video/x-raw,format=I420{s} ! ", .{ jpeg, resize_s });
            if (eq(enc, "qsvh264enc")) {
                text = try fmt(ctx, "{s}qsvh264enc{s}", .{ conv, bitrate_s });
            } else if (eq(enc, "v4l2h264enc")) {
                text = try fmt(ctx, "{s}v4l2h264enc ! video/x-h264,level=(string)4", .{conv});
            } else {
                text = try fmt(ctx, "{s}x264enc tune=zerolatency speed-preset=ultrafast{s}", .{ conv, bitrate_s });
            }
        }
        return .{ .text = text, .out = Caps.ofFormat(.h264) };
    }

    fn mjpegPath(self: *GstPipelineFactory, ctx: *LowerCtx, node: *const Node, input: PixelFormat, resize: ?Resize) !Lowered {
        const hw = self.opts.hw;
        const quality = props.getInt(node.props, "quality", 85);
        const resize_s = try resizeStr(ctx, resize);

        if (input == .mjpeg) return .{ .text = "", .out = Caps.ofFormat(.mjpeg) };

        var pre: []const u8 = undefined;
        if (input == .nv12_nvmm and hw.has_nvvidconv) {
            const nvc = hw.nvvidconv_name;
            if (hw.has_nvjpegenc) {
                pre = if (resize_s.len > 0) try fmt(ctx, "{s} ! video/x-raw(memory:NVMM),format=I420{s} ! ", .{ nvc, resize_s }) else "";
            } else {
                // The C++ original omits the trailing " ! " here, which glues the
                // caps filter onto the encoder name. Fixed in this port.
                pre = try fmt(ctx, "{s} ! video/x-raw,format=I420 ! ", .{nvc});
            }
        } else {
            pre = try fmt(ctx, "videoconvert ! video/x-raw,format=I420{s} ! ", .{resize_s});
        }

        if (hw.has_nvjpegenc) return .{ .text = try fmt(ctx, "{s}nvjpegenc quality={d} ! jpegparse", .{ pre, quality }), .out = Caps.ofFormat(.mjpeg) };
        if (hw.has_jpegenc) return .{ .text = try fmt(ctx, "{s}jpegenc quality={d} ! jpegparse", .{ pre, quality }), .out = Caps.ofFormat(.mjpeg) };
        return error.NoJpegEncoder;
    }

    fn autoConverter(ctx: *LowerCtx, node: *const Node) !Lowered {
        const target = PixelFormat.parse(props.getStringOr(node.props, "target", ""));
        const prefix = if (ctx.upstream.format.isBayer()) "bayer2rgb ! " else "";
        const suffix: []const u8 = switch (target) {
            .i420 => " ! video/x-raw,format=I420",
            .nv12 => " ! video/x-raw,format=NV12",
            .rgb => " ! video/x-raw,format=RGB",
            .bgr => " ! video/x-raw,format=BGR",
            .yuyv => " ! video/x-raw,format=YUY2",
            else => "",
        };
        const out_fmt: PixelFormat = if (target != .unknown) target else if (ctx.upstream.format != .unknown) ctx.upstream.format else .yuyv;
        try ctx.out.note("{s}: explicit conversion adds latency; prefer OptimizedConverter where it can do the job", .{node.name});
        return .{ .text = try fmt(ctx, "{s}videoconvert{s}", .{ prefix, suffix }), .out = Caps.ofFormat(out_fmt) };
    }

    fn undistort(self: *GstPipelineFactory, ctx: *LowerCtx, node: *const Node) !Lowered {
        const hw = self.opts.hw;
        const md_value = ctx.scratchpad.get(calib.metadata_key) orelse {
            try ctx.out.note("{s}: passthrough (no camera-metadata in scratchpad)", .{node.name});
            return .{ .text = "", .out = ctx.upstream };
        };
        const md = try calib.metadataFromValue(ctx.gpa, md_value);
        const c = md.calibration orelse {
            try ctx.out.note("{s}: passthrough (scratchpad metadata has no calibration)", .{node.name});
            return .{ .text = "", .out = ctx.upstream };
        };

        // Both undistort paths remap in place, so downstream sinks must see
        // identity distortion; write the rectified metadata back.
        try ctx.scratchpad.put(ctx.gpa, calib.metadata_key, try calib.rectifiedValue(ctx.gpa, md_value));

        if (hw.has_nvdewarper and hw.has_nvvidconv) {
            const path = try fmt(ctx, "/tmp/camera_driver_undistort_{s}.cfg", .{node.name});
            try ctx.out.sideFile(path, try dewarperConfig(ctx, c));
            var out = ctx.upstream;
            out.format = .rgba;
            out.is_nvmm = true;
            return .{
                .text = try fmt(ctx, "{s} ! video/x-raw(memory:NVMM),format=RGBA ! nvdewarper config-file={s}", .{ hw.nvvidconv_name, path }),
                .out = out,
            };
        }

        try ctx.out.warn("{s}: nvdewarper unavailable; undistorting on the CPU (expect an fps cost at high resolutions)", .{node.name});
        try ctx.out.handle(try fmt(ctx, "{s}_filter", .{node.name}), .element, node.type_name);
        var out = ctx.upstream;
        out.format = .rgb;
        out.is_nvmm = false;
        return .{
            .text = try fmt(ctx, "videoconvert ! video/x-raw,format=RGB ! camera_driver_undistort name={s}_filter", .{node.name}),
            .out = out,
        };
    }

    /// nvdewarper projection-type=3 config. Coefficient order is
    /// (k1,k2,k3 radial; p1,p2 tangential) = OpenCV/ROS [k1,k2,p1,p2,k3] reordered.
    fn dewarperConfig(ctx: *LowerCtx, c: calib.Calibration) ![]const u8 {
        return fmt(ctx,
            \\[property]
            \\output-width={d}
            \\output-height={d}
            \\num-batch-buffers=1
            \\[surface0]
            \\projection-type=3
            \\surface-index=0
            \\width={d}
            \\height={d}
            \\focal-length={d};{d}
            \\src-x0={d}
            \\src-y0={d}
            \\distortion={d};{d};{d};{d};{d}
            \\
        , .{
            c.width, c.height, c.width, c.height,
            c.K[0],  c.K[4],   c.K[2],  c.K[5],
            c.d(0),  c.d(1),   c.d(4),  c.d(2),
            c.d(3),
        });
    }

    fn inferServer(ctx: *LowerCtx, node: *const Node) !Lowered {
        return .{
            .text = try fmt(ctx, "nvinferserver config-file-path={s}", .{props.getString(node.props, "config_file").?}),
            .out = ctx.upstream,
        };
    }

    fn gstElement(ctx: *LowerCtx, node: *const Node) !Lowered {
        const element = props.getString(node.props, "element").?;
        const properties = props.getStringOr(node.props, "properties", "");
        return .{
            .text = if (properties.len == 0) element else try fmt(ctx, "{s} {s}", .{ element, properties }),
            .out = Caps.any,
        };
    }

    // ── fan-out ──────────────────────────────────────────────────────────

    fn mux(ctx: *LowerCtx, node: *const Node, branches: []const []const Segment) !Lowered {
        var result: std.ArrayList(u8) = .empty;
        try result.print(ctx.gpa, "tee name={s}", .{node.name});
        for (branches) |branch| {
            const s = try assembleChain(ctx.gpa, branch);
            if (s.len == 0) continue;
            try result.print(ctx.gpa, "  {s}. ! queue leaky=downstream max-size-buffers=2 ! {s}", .{ node.name, s });
        }
        return .{ .text = result.items, .out = Caps.any };
    }

    // ── sinks ────────────────────────────────────────────────────────────

    fn isH264(f: PixelFormat) bool {
        return f == .h264 or f == .h264_nvmm;
    }

    fn mkvRecorder(ctx: *LowerCtx, node: *const Node) !Lowered {
        const prefix = if (isH264(ctx.upstream.format)) "h264parse ! " else "videoconvert ! ";
        try ctx.out.handle(try fmt(ctx, "{s}_mux", .{node.name}), .element, node.type_name);
        return .{
            .text = try fmt(ctx, "{s}matroskamux name={s}_mux ! filesink name={s}_sink location={s}", .{
                prefix, node.name, node.name, props.getString(node.props, "location").?,
            }),
            .out = ctx.upstream,
        };
    }

    fn mcapSink(ctx: *LowerCtx, node: *const Node) !Lowered {
        const prefix = if (isH264(ctx.upstream.format)) "h264parse ! " else "videoconvert ! ";
        try ctx.out.handle(try fmt(ctx, "{s}_sink", .{node.name}), .topic_writer, node.type_name);
        return .{
            .text = try fmt(ctx, "{s}appsink name={s}_sink emit-signals=false sync=false drop=false", .{ prefix, node.name }),
            .out = ctx.upstream,
        };
    }

    fn appSinkPublisher(ctx: *LowerCtx, node: *const Node, tail: []const u8, kind: plan_mod.HandleKind) !Lowered {
        try ctx.out.handle(node.name, kind, node.type_name);
        return .{ .text = try fmt(ctx, "appsink name={s} {s}", .{ node.name, tail }), .out = Caps.any };
    }

    fn customPublisher(ctx: *LowerCtx, node: *const Node) !Lowered {
        const q = props.getInt(node.props, "max_queue", 4);
        return appSinkPublisher(ctx, node, try fmt(ctx, "max-buffers={d} drop=false sync=false emit-signals=false", .{q}), .frame_sink);
    }

    fn display(self: *GstPipelineFactory, ctx: *LowerCtx, _: *const Node) !Lowered {
        const decode = if (ctx.upstream.format == .mjpeg) "jpegdec ! " else "";
        const sink = if (self.opts.hw.has_nveglglessink) " video-sink=nveglglessink" else "";
        return .{
            .text = try fmt(ctx, "{s}autovideoconvert ! fpsdisplaysink{s} text-overlay=false sync=false", .{ decode, sink }),
            .out = ctx.upstream,
        };
    }

    fn nvUnixFdPublisher(self: *GstPipelineFactory, ctx: *LowerCtx, node: *const Node) !Lowered {
        const socket = props.getStringOr(node.props, "socket", "/tmp/camera_nv.sock");
        if (self.opts.hw.has_nvunixfdsink)
            return .{ .text = try fmt(ctx, "nvunixfdsink socket-path={s} sync=false", .{socket}), .out = Caps.any };
        try ctx.out.warn("{s}: nvunixfdsink not available; this branch is a no-op (fakesink)", .{node.name});
        return .{ .text = "fakesink sync=false", .out = Caps.any };
    }
};

// ── helpers ──────────────────────────────────────────────────────────────

fn eq(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

fn fmt(ctx: *LowerCtx, comptime f: []const u8, args: anytype) ![]const u8 {
    return std.fmt.allocPrint(ctx.gpa, f, args);
}

fn hasDeviceCriteria(node: *const Node) bool {
    const sel = props.getList(node.props, "selection") orelse return false;
    for (sel) |s| {
        const m = s.asMap() orelse continue;
        const mode = props.getStringOr(m, "mode", "interactive");
        if (!eq(mode, "interactive")) return true;
    }
    return false;
}

/// Join the non-empty segments with " ! ", like `Pipeline::assembleChain`.
pub fn assembleChain(gpa: Allocator, segs: []const Segment) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    for (segs) |s| {
        if (s.text.len == 0) continue;
        if (out.items.len > 0) try out.appendSlice(gpa, " ! ");
        try out.appendSlice(gpa, s.text);
    }
    return out.items;
}

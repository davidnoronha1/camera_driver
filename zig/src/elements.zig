//! The element catalogue: which `type:` names exist, how their unique
//! instance names are generated, and which options are mandatory. This is
//! the backend-neutral half of what `REGISTER_PIPELINE_ELEMENT` used to do;
//! how a type is *realised* lives in each factory (see gst_factory.zig and
//! web_factory.zig). The table is comptime, so there is no static-init
//! registration or whole-archive linking.

const std = @import("std");

pub const Role = enum { source, transform, mux, sink };

pub const Spec = struct {
    type_name: []const u8,
    /// Instance names are `<prefix>_<n>` with a per-prefix counter, matching
    /// the C++ `g_*_id` atomics (so generated gst element names line up).
    prefix: []const u8,
    role: Role,
    required: []const []const u8 = &.{},
};

pub const all = [_]Spec{
    // sources
    .{ .type_name = "V4L2SrcElement", .prefix = "v4l2_src", .role = .source },
    .{ .type_name = "RTSPSourceElement", .prefix = "rtsp_src", .role = .source, .required = &.{"url"} },
    .{ .type_name = "NvUnixFdSrcElement", .prefix = "nv_unixfd_src", .role = .source, .required = &.{"socket"} },
    .{ .type_name = "CustomSrcElement", .prefix = "custom_src", .role = .source },
    // Pulls a multipart MJPEG stream: the natural bridge between a native
    // pipeline's MJPEGPublisher and a browser (or another native pipeline).
    .{ .type_name = "HttpMjpegSourceElement", .prefix = "http_mjpeg_src", .role = .source, .required = &.{"url"} },
    .{ .type_name = "MkvPlaybackElement", .prefix = "mkv_playback", .role = .source, .required = &.{"location"} },
    .{ .type_name = "McapSourceElement", .prefix = "mcap_source", .role = .source, .required = &.{"location"} },
    // transforms
    .{ .type_name = "OptimizedConverter", .prefix = "opt_conv", .role = .transform },
    .{ .type_name = "AutoVideoConverterElement", .prefix = "auto_conv", .role = .transform },
    .{ .type_name = "UndistortElement", .prefix = "undistort", .role = .transform },
    .{ .type_name = "InferServerElement", .prefix = "infer_server", .role = .transform, .required = &.{"config_file"} },
    .{ .type_name = "GstElement", .prefix = "", .role = .transform, .required = &.{"element"} },
    // fan-out
    .{ .type_name = "MuxElement", .prefix = "tee", .role = .mux },
    // sinks
    .{ .type_name = "MkvRecorderElement", .prefix = "mkv_recorder", .role = .sink, .required = &.{"location"} },
    .{ .type_name = "McapSinkElement", .prefix = "mcap_sink", .role = .sink, .required = &.{"location"} },
    .{ .type_name = "MJPEGPublisher", .prefix = "mjpeg_pub", .role = .sink },
    .{ .type_name = "DisplayPublisher", .prefix = "display_pub", .role = .sink },
    .{ .type_name = "NVUnixFDPublisher", .prefix = "nv_unixfd", .role = .sink },
    .{ .type_name = "NvCUDAPublisher", .prefix = "nv_cuda_pub", .role = .sink },
    .{ .type_name = "IceOryxPublisher", .prefix = "iceoryx_pub", .role = .sink, .required = &.{"topic"} },
    .{ .type_name = "ROS2Publisher", .prefix = "ros2_pub", .role = .sink },
    .{ .type_name = "CustomPublisher", .prefix = "custom_pub", .role = .sink },
};

pub fn find(type_name: []const u8) ?Spec {
    inline for (all) |s| {
        if (std.mem.eql(u8, s.type_name, type_name)) return s;
    }
    return null;
}

test "every type name is unique" {
    inline for (all, 0..) |a, i| {
        inline for (all[i + 1 ..]) |b| {
            try std.testing.expect(!std.mem.eql(u8, a.type_name, b.type_name));
        }
    }
}

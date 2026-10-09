//! Camera format negotiation, shared by every backend that can enumerate a
//! capture device (port of the helpers in v4l2_src_element.cpp). The device
//! enumeration itself (ioctls, `getUserMedia().getCapabilities()`) is the
//! host's job; it hands the result in as `FormatInfo`s.

const std = @import("std");
const PixelFormat = @import("caps.zig").PixelFormat;

pub const Size = struct {
    width: u32,
    height: u32,
    /// Empty means "any frame rate".
    fps: []const u32 = &.{},
};

pub const FormatInfo = struct {
    /// Only camera-native formats matter here: yuyv, nv12, mjpeg, h264.
    format: PixelFormat,
    sizes: []const Size = &.{},
};

pub const DeviceInfo = struct {
    path: []const u8 = "/dev/video0",
    /// null = unknown; every format is assumed available at the requested size.
    formats: ?[]const FormatInfo = null,
};

fn supports(formats: ?[]const FormatInfo, fmt: PixelFormat, width: i32, height: i32, fps: i32) bool {
    const list = formats orelse return true;
    for (list) |fi| {
        if (fi.format != fmt) continue;
        for (fi.sizes) |sz| {
            if ((width == 0 or sz.width == @as(u32, @intCast(width))) and
                (height == 0 or sz.height == @as(u32, @intCast(height))))
            {
                if (sz.fps.len == 0) return true;
                for (sz.fps) |f| {
                    if (fps == 0 or f == @as(u32, @intCast(fps))) return true;
                }
            }
        }
    }
    return false;
}

/// Pick the best output format for the camera given what the next element
/// prefers and what the device offers.
pub fn pickOutputFormat(
    downstream_prefs: []const PixelFormat,
    formats: ?[]const FormatInfo,
    width: i32,
    height: i32,
    fps: i32,
) PixelFormat {
    for (downstream_prefs) |pref| switch (pref) {
        .mjpeg => if (supports(formats, .mjpeg, width, height, fps)) return .mjpeg,
        .h264, .h264_nvmm => if (supports(formats, .h264, width, height, fps)) return .h264,
        .nv12, .nv12_nvmm => if (supports(formats, .nv12, width, height, fps)) return .nv12,
        else => {},
    };

    // YUYV is the most universally supported raw format.
    if (supports(formats, .yuyv, width, height, fps)) return .yuyv;
    if (supports(formats, .mjpeg, width, height, fps)) return .mjpeg;

    if (formats) |list| {
        if (list.len > 0) switch (list[0].format) {
            .mjpeg, .nv12, .h264 => |f| return f,
            else => {},
        };
    }
    return .yuyv;
}

/// Fill in 0 width/height from what the camera actually offers.
pub fn resolveResolution(
    formats: ?[]const FormatInfo,
    fmt: PixelFormat,
    want_w: i32,
    want_h: i32,
) [2]i32 {
    if (formats) |list| for (list) |fi| {
        if (fi.format != fmt) continue;
        for (fi.sizes) |sz| {
            if ((want_w == 0 or sz.width == @as(u32, @intCast(want_w))) and
                (want_h == 0 or sz.height == @as(u32, @intCast(want_h))))
                return .{ @intCast(sz.width), @intCast(sz.height) };
        }
        if (fi.sizes.len > 0) return .{ @intCast(fi.sizes[0].width), @intCast(fi.sizes[0].height) };
    };
    return .{ if (want_w != 0) want_w else 640, if (want_h != 0) want_h else 480 };
}

test "prefers what downstream wants, falls back to YUYV" {
    const formats = [_]FormatInfo{
        .{ .format = .yuyv, .sizes = &.{.{ .width = 640, .height = 480 }} },
        .{ .format = .mjpeg, .sizes = &.{.{ .width = 1280, .height = 720 }} },
    };
    try std.testing.expectEqual(PixelFormat.mjpeg, pickOutputFormat(&.{.mjpeg}, &formats, 1280, 720, 30));
    // MJPEG only offered at 1280x720, so 640x480 falls back to YUYV.
    try std.testing.expectEqual(PixelFormat.yuyv, pickOutputFormat(&.{.mjpeg}, &formats, 640, 480, 30));
    try std.testing.expectEqual(PixelFormat.mjpeg, pickOutputFormat(&.{.mjpeg}, null, 0, 0, 30));
    const r = resolveResolution(&formats, .mjpeg, 0, 0);
    try std.testing.expectEqual(@as(i32, 1280), r[0]);
}

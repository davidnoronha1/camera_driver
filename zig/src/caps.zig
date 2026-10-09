//! Backend-neutral stream description: pixel format + geometry. Ported from
//! include/camera_driver/pipeline/caps.hpp. The GStreamer caps string is only
//! one rendering of this (see `toGstCapsString`); the web backend uses the
//! same struct to talk about WebCodecs / canvas formats.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const PixelFormat = enum {
    unknown,
    yuyv,
    nv12,
    nv12_nvmm, // NV12 in NVIDIA shared GPU memory
    i420,
    rgb,
    bgr,
    rgba,
    mjpeg,
    h264,
    h264_nvmm, // H.264 bitstream in NVIDIA memory
    bayer_rggb,
    bayer_bggr,
    bayer_grbg,
    bayer_gbrg,

    pub fn name(self: PixelFormat) []const u8 {
        return switch (self) {
            .yuyv => "YUYV",
            .nv12 => "NV12",
            .nv12_nvmm => "NV12(NVMM)",
            .i420 => "I420",
            .rgb => "RGB",
            .bgr => "BGR",
            .rgba => "RGBA",
            .mjpeg => "MJPEG",
            .h264 => "H264",
            .h264_nvmm => "H264(NVMM)",
            .bayer_rggb => "BayerRGGB",
            .bayer_bggr => "BayerBGGR",
            .bayer_grbg => "BayerGRBG",
            .bayer_gbrg => "BayerGBRG",
            .unknown => "Unknown",
        };
    }

    /// Accepts the same spellings as the C++ `pixelFormatFromString`, plus the
    /// lower-case snake names used by NvUnixFdSrcElement's `format:` option.
    pub fn parse(s: []const u8) PixelFormat {
        const table = [_]struct { []const u8, PixelFormat }{
            .{ "YUYV", .yuyv },            .{ "NV12", .nv12 },
            .{ "I420", .i420 },            .{ "RGB", .rgb },
            .{ "BGR", .bgr },              .{ "RGBA", .rgba },
            .{ "MJPEG", .mjpeg },          .{ "H264", .h264 },
            .{ "BayerRGGB", .bayer_rggb }, .{ "BayerBGGR", .bayer_bggr },
            .{ "BayerGRBG", .bayer_grbg }, .{ "BayerGBRG", .bayer_gbrg },
            .{ "nv12_nvmm", .nv12_nvmm },  .{ "NV12_NVMM", .nv12_nvmm },
        };
        for (table) |e| if (std.mem.eql(u8, e[0], s)) return e[1];
        return .unknown;
    }

    pub fn isBayer(self: PixelFormat) bool {
        return switch (self) {
            .bayer_rggb, .bayer_bggr, .bayer_grbg, .bayer_gbrg => true,
            else => false,
        };
    }

    pub fn isCompressed(self: PixelFormat) bool {
        return switch (self) {
            .mjpeg, .h264, .h264_nvmm => true,
            else => false,
        };
    }
};

pub const Caps = struct {
    format: PixelFormat = .unknown,
    width: i32 = 0,
    height: i32 = 0,
    fps_num: i32 = 30,
    fps_den: i32 = 1,
    is_nvmm: bool = false, // buffer is in GPU memory
    is_any: bool = false, // wildcard: accepts/produces any format

    pub const any: Caps = .{ .is_any = true };

    pub fn ofFormat(f: PixelFormat) Caps {
        return .{ .format = f };
    }

    pub fn compatibleWith(self: Caps, downstream: Caps) bool {
        if (self.is_any or downstream.is_any) return true;
        if (self.format == .unknown or downstream.format == .unknown) return true;
        return self.format == downstream.format;
    }

    /// Same rendering as the C++ `Caps::toGstCapsString`.
    pub fn toGstCapsString(self: Caps, gpa: Allocator) ![]const u8 {
        if (self.is_any) return "ANY";
        const base: []const u8 = switch (self.format) {
            .mjpeg => "image/jpeg",
            .h264 => "video/x-h264",
            .h264_nvmm => "video/x-h264(memory:NVMM)",
            .nv12_nvmm => "video/x-raw(memory:NVMM),format=NV12",
            .nv12 => "video/x-raw,format=NV12",
            .i420 => "video/x-raw,format=I420",
            .yuyv => "video/x-raw,format=YUY2",
            .rgb => "video/x-raw,format=RGB",
            .bgr => "video/x-raw,format=BGR",
            .rgba => "video/x-raw,format=RGBA",
            .bayer_rggb => "video/x-bayer,format=rggb",
            .bayer_bggr => "video/x-bayer,format=bggr",
            .bayer_grbg => "video/x-bayer,format=grbg",
            .bayer_gbrg => "video/x-bayer,format=gbrg",
            .unknown => return "ANY",
        };
        var out: std.ArrayList(u8) = .empty;
        try out.appendSlice(gpa, base);
        if (self.width > 0 and self.height > 0)
            try out.print(gpa, ",width={d},height={d}", .{ self.width, self.height });
        if (self.fps_num > 0 and self.fps_den > 0)
            try out.print(gpa, ",framerate={d}/{d}", .{ self.fps_num, self.fps_den });
        return out.items;
    }
};

test "gst caps string matches the C++ rendering" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const c: Caps = .{ .format = .bayer_rggb, .width = 1920, .height = 1200, .fps_num = 30 };
    try std.testing.expectEqualStrings(
        "video/x-bayer,format=rggb,width=1920,height=1200,framerate=30/1",
        try c.toGstCapsString(arena.allocator()),
    );
    try std.testing.expectEqualStrings("ANY", try Caps.any.toGstCapsString(arena.allocator()));
}

test "format parsing" {
    try std.testing.expectEqual(PixelFormat.nv12_nvmm, PixelFormat.parse("nv12_nvmm"));
    try std.testing.expectEqual(PixelFormat.unknown, PixelFormat.parse("nope"));
    try std.testing.expect(PixelFormat.bayer_gbrg.isBayer());
}

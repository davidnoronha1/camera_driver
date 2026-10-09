//! Backend hardware/capability flags (port of hw_detect.hpp's `HWCaps`).
//!
//! The *probing* is deliberately not here: the C++ version asks the live
//! GStreamer registry, a Zig/wasm core cannot. A host (CLI flag, a one-shot
//! `gst-inspect-1.0` pass, a browser feature check) tells the core which
//! elements exist and `fromElementNames` turns that into flags.

const std = @import("std");

pub const HwCaps = struct {
    has_nvjpegenc: bool = false,
    has_nvh264enc: bool = false,
    has_nvv4l2h264enc: bool = false,
    has_nvvidconv: bool = false,
    has_nvunixfdsink: bool = false,
    has_nvunixfdsrc: bool = false,
    has_qsvh264enc: bool = false,
    has_v4l2h264enc: bool = false,
    has_jpegenc: bool = false,
    has_x264enc: bool = false,

    has_nvv4l2h264dec: bool = false,
    has_nvh264dec: bool = false,
    has_avdec_h264: bool = false,
    has_nvdewarper: bool = false,
    has_nveglglessink: bool = false,

    /// "nvvidconv" or "nvvideoconvert", whichever is installed.
    nvvidconv_name: []const u8 = "",

    /// What a plain desktop with gstreamer-plugins-{good,ugly,libav} offers.
    pub const software: HwCaps = .{
        .has_jpegenc = true,
        .has_x264enc = true,
        .has_avdec_h264 = true,
    };

    pub fn fromElementNames(names: []const []const u8) HwCaps {
        var c: HwCaps = .{};
        for (names) |n| {
            if (eql(n, "nvjpegenc")) c.has_nvjpegenc = true;
            if (eql(n, "nvh264enc")) c.has_nvh264enc = true;
            if (eql(n, "nvv4l2h264enc")) c.has_nvv4l2h264enc = true;
            if (eql(n, "nvvidconv")) {
                c.has_nvvidconv = true;
                c.nvvidconv_name = "nvvidconv";
            }
            if (eql(n, "nvvideoconvert") and c.nvvidconv_name.len == 0) {
                c.has_nvvidconv = true;
                c.nvvidconv_name = "nvvideoconvert";
            }
            if (eql(n, "nvunixfdsink")) c.has_nvunixfdsink = true;
            if (eql(n, "nvunixfdsrc")) c.has_nvunixfdsrc = true;
            if (eql(n, "qsvh264enc")) c.has_qsvh264enc = true;
            if (eql(n, "v4l2h264enc")) c.has_v4l2h264enc = true;
            if (eql(n, "jpegenc")) c.has_jpegenc = true;
            if (eql(n, "x264enc")) c.has_x264enc = true;
            if (eql(n, "nvv4l2decoder")) c.has_nvv4l2h264dec = true;
            if (eql(n, "nvh264dec")) c.has_nvh264dec = true;
            if (eql(n, "avdec_h264")) c.has_avdec_h264 = true;
            if (eql(n, "nvdewarper")) c.has_nvdewarper = true;
            if (eql(n, "nveglglessink")) c.has_nveglglessink = true;
        }
        return c;
    }

    /// Is the named GStreamer element known to exist? (Used by plugins.)
    pub fn hasElement(self: HwCaps, name: []const u8) bool {
        if (eql(name, "nvjpegenc")) return self.has_nvjpegenc;
        if (eql(name, "nvh264enc")) return self.has_nvh264enc;
        if (eql(name, "nvv4l2h264enc")) return self.has_nvv4l2h264enc;
        if (eql(name, "nvvidconv")) return self.has_nvvidconv and eql(self.nvvidconv_name, "nvvidconv");
        if (eql(name, "nvvideoconvert")) return self.has_nvvidconv and eql(self.nvvidconv_name, "nvvideoconvert");
        if (eql(name, "nvunixfdsink")) return self.has_nvunixfdsink;
        if (eql(name, "nvunixfdsrc")) return self.has_nvunixfdsrc;
        if (eql(name, "qsvh264enc")) return self.has_qsvh264enc;
        if (eql(name, "v4l2h264enc")) return self.has_v4l2h264enc;
        if (eql(name, "jpegenc")) return self.has_jpegenc;
        if (eql(name, "x264enc")) return self.has_x264enc;
        if (eql(name, "nvv4l2decoder")) return self.has_nvv4l2h264dec;
        if (eql(name, "nvh264dec")) return self.has_nvh264dec;
        if (eql(name, "avdec_h264")) return self.has_avdec_h264;
        if (eql(name, "nvdewarper")) return self.has_nvdewarper;
        if (eql(name, "nveglglessink")) return self.has_nveglglessink;
        return false;
    }

    /// Parse a comma-separated element list, e.g. `"x264enc,jpegenc,nvh264enc"`.
    pub fn fromCsv(gpa: std.mem.Allocator, csv: []const u8) !HwCaps {
        var names: std.ArrayList([]const u8) = .empty;
        defer names.deinit(gpa);
        var it = std.mem.tokenizeAny(u8, csv, ", ");
        while (it.next()) |n| try names.append(gpa, n);
        return fromElementNames(names.items);
    }
};

fn eql(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

test "element names -> flags" {
    const c = try HwCaps.fromCsv(std.testing.allocator, "nvh264enc, x264enc,nvvideoconvert");
    try std.testing.expect(c.has_nvh264enc and c.has_x264enc and c.has_nvvidconv);
    try std.testing.expectEqualStrings("nvvideoconvert", c.nvvidconv_name);
    try std.testing.expect(!c.has_jpegenc);
}

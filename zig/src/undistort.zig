//! CPU lens undistortion (plumb-bob / radial-tangential), written in plain
//! Zig so it runs natively and inside the wasm module. It is the portable
//! fallback behind UndistortElement; on the web a WebGPU shader is faster.
//!
//! Like both C++ undistort paths, the output keeps the same intrinsics K and
//! image size; only the distortion D is removed. For each output pixel we
//! compute where it lands in the distorted source image and sample there
//! (bilinear), i.e. a standard inverse mapping.

const std = @import("std");
const Allocator = std.mem.Allocator;
const calib = @import("calib.zig");

pub const Map = struct {
    width: u32,
    height: u32,
    /// Two f32 per output pixel: source x, source y. NaN marks "outside".
    xy: []f32,

    pub fn deinit(self: *Map, gpa: Allocator) void {
        gpa.free(self.xy);
        self.* = undefined;
    }
};

/// OpenCV/ROS plumb_bob order: D = [k1, k2, p1, p2, k3].
pub fn buildMap(gpa: Allocator, c: calib.Calibration, width: u32, height: u32) !Map {
    const fx = c.K[0];
    const fy = c.K[4];
    const cx = c.K[2];
    const cy = c.K[5];
    const k1 = c.d(0);
    const k2 = c.d(1);
    const p1 = c.d(2);
    const p2 = c.d(3);
    const k3 = c.d(4);

    const xy = try gpa.alloc(f32, @as(usize, width) * height * 2);
    var i: usize = 0;
    var v: u32 = 0;
    while (v < height) : (v += 1) {
        var u: u32 = 0;
        while (u < width) : (u += 1) {
            const x = (@as(f64, @floatFromInt(u)) - cx) / fx;
            const y = (@as(f64, @floatFromInt(v)) - cy) / fy;
            const r2 = x * x + y * y;
            const radial = 1 + k1 * r2 + k2 * r2 * r2 + k3 * r2 * r2 * r2;
            const xd = x * radial + 2 * p1 * x * y + p2 * (r2 + 2 * x * x);
            const yd = y * radial + p1 * (r2 + 2 * y * y) + 2 * p2 * x * y;
            xy[i] = @floatCast(fx * xd + cx);
            xy[i + 1] = @floatCast(fy * yd + cy);
            i += 2;
        }
    }
    return .{ .width = width, .height = height, .xy = xy };
}

/// Remap an RGBA8 frame (`width*height*4` bytes each). Out-of-frame samples
/// become transparent black.
pub fn remapRgba(map: Map, src: []const u8, dst: []u8) void {
    const w = map.width;
    const h = map.height;
    std.debug.assert(src.len >= @as(usize, w) * h * 4 and dst.len >= @as(usize, w) * h * 4);

    var o: usize = 0;
    var i: usize = 0;
    while (o < @as(usize, w) * h) : (o += 1) {
        const sx = map.xy[i];
        const sy = map.xy[i + 1];
        i += 2;

        const out = dst[o * 4 ..][0..4];
        if (!(sx >= 0 and sy >= 0 and sx <= @as(f32, @floatFromInt(w - 1)) and sy <= @as(f32, @floatFromInt(h - 1)))) {
            out.* = .{ 0, 0, 0, 0 };
            continue;
        }
        const x0: u32 = @intFromFloat(@floor(sx));
        const y0: u32 = @intFromFloat(@floor(sy));
        const x1 = @min(x0 + 1, w - 1);
        const y1 = @min(y0 + 1, h - 1);
        const fx = sx - @as(f32, @floatFromInt(x0));
        const fy = sy - @as(f32, @floatFromInt(y0));

        inline for (0..4) |ch| {
            const p00: f32 = @floatFromInt(src[(@as(usize, y0) * w + x0) * 4 + ch]);
            const p10: f32 = @floatFromInt(src[(@as(usize, y0) * w + x1) * 4 + ch]);
            const p01: f32 = @floatFromInt(src[(@as(usize, y1) * w + x0) * 4 + ch]);
            const p11: f32 = @floatFromInt(src[(@as(usize, y1) * w + x1) * 4 + ch]);
            const top = p00 + (p10 - p00) * fx;
            const bot = p01 + (p11 - p01) * fx;
            out[ch] = @intFromFloat(@round(top + (bot - top) * fy));
        }
    }
}

test "zero distortion is the identity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var c: calib.Calibration = .{ .width = 8, .height = 6 };
    c.K = .{ 10, 0, 4, 0, 10, 3, 0, 0, 1 };
    const m = try buildMap(a, c, 8, 6);

    var src: [8 * 6 * 4]u8 = undefined;
    for (&src, 0..) |*b, n| b.* = @intCast(n % 251);
    var dst: [8 * 6 * 4]u8 = undefined;
    remapRgba(m, &src, &dst);
    try std.testing.expectEqualSlices(u8, &src, &dst);
}

test "barrel distortion pulls corner samples toward the centre" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var c: calib.Calibration = .{ .width = 64, .height = 48 };
    c.K = .{ 50, 0, 32, 0, 50, 24, 0, 0, 1 };
    const d = [_]f64{ -0.2, 0, 0, 0, 0 };
    c.D = &d;
    const m = try buildMap(a, c, 64, 48);

    // Centre pixel maps to itself.
    const centre = (24 * 64 + 32) * 2;
    try std.testing.expectApproxEqAbs(@as(f32, 32), m.xy[centre], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 24), m.xy[centre + 1], 1e-4);
    // A corner pixel samples from closer to the centre (k1 < 0 shrinks radius).
    try std.testing.expect(m.xy[0] > 0 and m.xy[1] > 0);
}

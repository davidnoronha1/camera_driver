//! Camera calibration / pose metadata (port of metadata/camera_metadata.*).
//! Reads the ROS `camera_info_manager` YAML schema directly from a parsed
//! `props.Value` tree; there is no separate serialisation step because the
//! scratchpad stores the `Value` itself.

const std = @import("std");
const Allocator = std.mem.Allocator;
const props = @import("props.zig");
const Value = props.Value;
const Props = props.Props;

pub const Calibration = struct {
    width: i32 = 0,
    height: i32 = 0,
    distortion_model: []const u8 = "",
    D: []const f64 = &.{},
    K: [9]f64 = @splat(0),
    R: [9]f64 = @splat(0),
    P: [12]f64 = @splat(0),

    /// Plumb-bob style coefficient accessor; missing entries are 0.
    pub fn d(self: Calibration, i: usize) f64 {
        return if (i < self.D.len) self.D[i] else 0;
    }
};

pub const Pose = struct {
    frame_id: []const u8 = "",
    px: f64 = 0,
    py: f64 = 0,
    pz: f64 = 0,
    qx: f64 = 0,
    qy: f64 = 0,
    qz: f64 = 0,
    qw: f64 = 1,
};

pub const Metadata = struct {
    calibration: ?Calibration = null,
    pose: ?Pose = null,
};

/// Key under which camera metadata lives in the scratchpad.
pub const metadata_key = "camera-metadata";

fn loadFixed(comptime N: usize, out: *[N]f64, matrix: ?Props) void {
    const m = matrix orelse return;
    const data = props.getList(m, "data") orelse return;
    for (data, 0..) |v, i| {
        if (i >= N) break;
        out[i] = v.asFloat() orelse 0;
    }
}

pub fn calibrationFromProps(gpa: Allocator, n: Props) !Calibration {
    var c: Calibration = .{};
    c.width = @intCast(props.getInt(n, "image_width", 0));
    c.height = @intCast(props.getInt(n, "image_height", 0));
    c.distortion_model = props.getStringOr(n, "distortion_model", "");
    loadFixed(9, &c.K, props.getMap(n, "camera_matrix"));
    loadFixed(9, &c.R, props.getMap(n, "rectification_matrix"));
    loadFixed(12, &c.P, props.getMap(n, "projection_matrix"));
    if (props.getMap(n, "distortion_coefficients")) |dc| {
        if (props.getList(dc, "data")) |data| {
            const out = try gpa.alloc(f64, data.len);
            for (data, 0..) |v, i| out[i] = v.asFloat() orelse 0;
            c.D = out;
        }
    }
    return c;
}

fn poseFromProps(n: Props) Pose {
    var p: Pose = .{};
    p.frame_id = props.getStringOr(n, "frame_id", "");
    if (props.getMap(n, "position")) |pos| {
        p.px = props.getFloat(pos, "x", 0);
        p.py = props.getFloat(pos, "y", 0);
        p.pz = props.getFloat(pos, "z", 0);
    }
    if (props.getMap(n, "orientation")) |o| {
        p.qx = props.getFloat(o, "x", 0);
        p.qy = props.getFloat(o, "y", 0);
        p.qz = props.getFloat(o, "z", 0);
        p.qw = props.getFloat(o, "w", 1);
    }
    return p;
}

/// Accepts a full metadata document (`calibration:` / `pose:` keys, what
/// `CameraMetadata::toYaml` writes).
pub fn metadataFromValue(gpa: Allocator, v: Value) !Metadata {
    var m: Metadata = .{};
    const root = v.asMap() orelse return m;
    if (props.getMap(root, "calibration")) |c| m.calibration = try calibrationFromProps(gpa, c);
    if (props.getMap(root, "pose")) |p| m.pose = poseFromProps(p);
    return m;
}

/// Accepts either a bare camera_info document or one nested under
/// `calibration:` (mirrors `calibrationFromYamlFile`).
pub fn calibrationFromValue(gpa: Allocator, v: Value) !Calibration {
    const root = v.asMap() orelse return .{};
    const node = props.getMap(root, "calibration") orelse root;
    return calibrationFromProps(gpa, node);
}

/// Build the `Value` form of metadata with the calibration's distortion
/// cleared — what UndistortElement writes back after rectifying.
pub fn rectifiedValue(gpa: Allocator, original: Value) !Value {
    const root = original.asMap() orelse return original;
    var out: Props = .empty;
    var it = root.iterator();
    while (it.next()) |e| try out.put(gpa, e.key_ptr.*, e.value_ptr.*);

    if (props.getMap(root, "calibration")) |cal| {
        var c2: Props = .empty;
        var cit = cal.iterator();
        while (cit.next()) |e| try c2.put(gpa, e.key_ptr.*, e.value_ptr.*);
        try c2.put(gpa, "distortion_model", .{ .string = "" });
        var dc: Props = .empty;
        try dc.put(gpa, "rows", .{ .int = 1 });
        try dc.put(gpa, "cols", .{ .int = 0 });
        try dc.put(gpa, "data", .{ .list = &.{} });
        try c2.put(gpa, "distortion_coefficients", .{ .map = dc });
        try out.put(gpa, "calibration", .{ .map = c2 });
    }
    return .{ .map = out };
}

test "ROS camera_info calibration loads" {
    const yaml = @import("yaml.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const doc = try yaml.parseFirst(a,
        \\image_width: 640
        \\image_height: 480
        \\distortion_model: plumb_bob
        \\camera_matrix:
        \\  rows: 3
        \\  cols: 3
        \\  data: [500.0, 0.0, 320.0, 0.0, 500.0, 240.0, 0.0, 0.0, 1.0]
        \\distortion_coefficients:
        \\  rows: 1
        \\  cols: 5
        \\  data: [-0.1, 0.01, 0.0, 0.0, 0.0]
    , null);
    const c = try calibrationFromValue(a, doc);
    try std.testing.expectEqual(@as(i32, 640), c.width);
    try std.testing.expectEqual(@as(f64, 500.0), c.K[0]);
    try std.testing.expectEqual(@as(f64, -0.1), c.d(0));
    try std.testing.expectEqual(@as(f64, 0), c.d(9));
}

//! Generic property values. Element options are plain string-keyed hash maps
//! of `Value`, not a YAML node, so the core has no dependency on any config
//! format: YAML is just one way of producing a `Props`.
//!
//! Everything here is allocated from a caller-supplied allocator that is
//! expected to be an arena; nothing is freed individually.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Props = std.StringHashMapUnmanaged(Value);

pub const Value = union(enum) {
    null,
    bool: bool,
    int: i64,
    float: f64,
    string: []const u8,
    list: []const Value,
    map: Props,

    pub fn asString(self: Value) ?[]const u8 {
        return switch (self) {
            .string => |s| s,
            else => null,
        };
    }

    pub fn asInt(self: Value) ?i64 {
        return switch (self) {
            .int => |i| i,
            .float => |f| @intFromFloat(f),
            else => null,
        };
    }

    pub fn asFloat(self: Value) ?f64 {
        return switch (self) {
            .float => |f| f,
            .int => |i| @floatFromInt(i),
            else => null,
        };
    }

    pub fn asBool(self: Value) ?bool {
        return switch (self) {
            .bool => |b| b,
            else => null,
        };
    }

    pub fn asList(self: Value) ?[]const Value {
        return switch (self) {
            .list => |l| l,
            else => null,
        };
    }

    pub fn asMap(self: Value) ?Props {
        return switch (self) {
            .map => |m| m,
            else => null,
        };
    }
};

pub fn put(props: *Props, gpa: Allocator, key: []const u8, value: Value) !void {
    try props.put(gpa, key, value);
}

pub fn has(props: Props, key: []const u8) bool {
    return props.contains(key);
}

pub fn getString(props: Props, key: []const u8) ?[]const u8 {
    return if (props.get(key)) |v| v.asString() else null;
}

pub fn getStringOr(props: Props, key: []const u8, default: []const u8) []const u8 {
    return getString(props, key) orelse default;
}

pub fn getInt(props: Props, key: []const u8, default: i64) i64 {
    return if (props.get(key)) |v| (v.asInt() orelse default) else default;
}

pub fn getFloat(props: Props, key: []const u8, default: f64) f64 {
    return if (props.get(key)) |v| (v.asFloat() orelse default) else default;
}

pub fn getBool(props: Props, key: []const u8, default: bool) bool {
    return if (props.get(key)) |v| (v.asBool() orelse default) else default;
}

pub fn getList(props: Props, key: []const u8) ?[]const Value {
    return if (props.get(key)) |v| v.asList() else null;
}

pub fn getMap(props: Props, key: []const u8) ?Props {
    return if (props.get(key)) |v| v.asMap() else null;
}

/// Collect a list-of-strings property. Missing key -> empty slice.
pub fn getStringList(gpa: Allocator, props: Props, key: []const u8) ![]const []const u8 {
    const list = getList(props, key) orelse return &.{};
    var out = try gpa.alloc([]const u8, list.len);
    var n: usize = 0;
    for (list) |v| {
        if (v.asString()) |s| {
            out[n] = s;
            n += 1;
        }
    }
    return out[0..n];
}

test "typed getters with defaults" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var p: Props = .empty;
    try put(&p, a, "port", .{ .int = 8080 });
    try put(&p, a, "name", .{ .string = "cam" });
    try put(&p, a, "on", .{ .bool = true });

    try std.testing.expectEqual(@as(i64, 8080), getInt(p, "port", 1));
    try std.testing.expectEqual(@as(i64, 7), getInt(p, "missing", 7));
    try std.testing.expectEqualStrings("cam", getString(p, "name").?);
    try std.testing.expect(getString(p, "port") == null);
    try std.testing.expect(getBool(p, "on", false));
}

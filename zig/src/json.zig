//! Minimal JSON writer: enough to emit web plans and diagnostics without
//! pulling in std.json's allocating serializer. Output is compact.

const std = @import("std");
const Allocator = std.mem.Allocator;
const props = @import("props.zig");

pub const Writer = struct {
    gpa: Allocator,
    buf: std.ArrayList(u8) = .empty,
    /// One bit per open container: has it received its first element yet?
    need_comma: [64]bool = @splat(false),
    depth: usize = 0,
    pending_value: bool = false,

    pub fn init(gpa: Allocator) Writer {
        return .{ .gpa = gpa };
    }

    pub fn bytes(self: *Writer) []const u8 {
        return self.buf.items;
    }

    fn sep(self: *Writer) !void {
        if (self.depth == 0) return;
        if (self.need_comma[self.depth - 1]) try self.buf.append(self.gpa, ',');
        self.need_comma[self.depth - 1] = true;
    }

    pub fn beginObject(self: *Writer) !void {
        try self.sep();
        try self.buf.append(self.gpa, '{');
        self.depth += 1;
        self.need_comma[self.depth - 1] = false;
    }
    pub fn endObject(self: *Writer) !void {
        self.depth -= 1;
        try self.buf.append(self.gpa, '}');
    }
    pub fn beginArray(self: *Writer) !void {
        try self.sep();
        try self.buf.append(self.gpa, '[');
        self.depth += 1;
        self.need_comma[self.depth - 1] = false;
    }
    pub fn endArray(self: *Writer) !void {
        self.depth -= 1;
        try self.buf.append(self.gpa, ']');
    }

    /// Object key; the next value call supplies its value.
    pub fn key(self: *Writer, k: []const u8) !void {
        try self.sep();
        try self.writeString(k);
        try self.buf.append(self.gpa, ':');
        // The value that follows must not emit another comma.
        self.need_comma[self.depth - 1] = false;
        self.pending_value = true;
    }
    fn valueSep(self: *Writer) !void {
        if (self.pending_value) {
            self.pending_value = false;
            // restore "has an element" for the enclosing object
            self.need_comma[self.depth - 1] = true;
            return;
        }
        try self.sep();
    }

    pub fn string(self: *Writer, s: []const u8) !void {
        try self.valueSep();
        try self.writeString(s);
    }
    pub fn int(self: *Writer, v: i64) !void {
        try self.valueSep();
        try self.buf.print(self.gpa, "{d}", .{v});
    }
    pub fn float(self: *Writer, v: f64) !void {
        try self.valueSep();
        if (std.math.isNan(v) or std.math.isInf(v)) {
            try self.buf.appendSlice(self.gpa, "null");
        } else {
            try self.buf.print(self.gpa, "{d}", .{v});
        }
    }
    pub fn boolean(self: *Writer, v: bool) !void {
        try self.valueSep();
        try self.buf.appendSlice(self.gpa, if (v) "true" else "false");
    }
    pub fn nullValue(self: *Writer) !void {
        try self.valueSep();
        try self.buf.appendSlice(self.gpa, "null");
    }

    /// Open a container as the value of the key just written.
    pub fn objectValue(self: *Writer) !void {
        try self.valueSep();
        try self.buf.append(self.gpa, '{');
        self.depth += 1;
        self.need_comma[self.depth - 1] = false;
    }
    pub fn arrayValue(self: *Writer) !void {
        try self.valueSep();
        try self.buf.append(self.gpa, '[');
        self.depth += 1;
        self.need_comma[self.depth - 1] = false;
    }

    pub fn value(self: *Writer, v: props.Value) anyerror!void {
        switch (v) {
            .null => try self.nullValue(),
            .bool => |b| try self.boolean(b),
            .int => |i| try self.int(i),
            .float => |f| try self.float(f),
            .string => |s| try self.string(s),
            .list => |l| {
                try self.arrayValue();
                for (l) |item| try self.value(item);
                try self.endArray();
            },
            .map => |m| try self.map(m),
        }
    }

    /// Properties, keys sorted so output is deterministic.
    pub fn map(self: *Writer, m: props.Props) anyerror!void {
        try self.objectValue();
        var keys: std.ArrayList([]const u8) = .empty;
        defer keys.deinit(self.gpa);
        var it = m.iterator();
        while (it.next()) |e| try keys.append(self.gpa, e.key_ptr.*);
        std.mem.sort([]const u8, keys.items, {}, struct {
            fn lt(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.lessThan(u8, a, b);
            }
        }.lt);
        for (keys.items) |k| {
            try self.key(k);
            try self.value(m.get(k).?);
        }
        try self.endObject();
    }

    fn writeString(self: *Writer, s: []const u8) !void {
        try self.buf.append(self.gpa, '"');
        for (s) |c| switch (c) {
            '"' => try self.buf.appendSlice(self.gpa, "\\\""),
            '\\' => try self.buf.appendSlice(self.gpa, "\\\\"),
            '\n' => try self.buf.appendSlice(self.gpa, "\\n"),
            '\r' => try self.buf.appendSlice(self.gpa, "\\r"),
            '\t' => try self.buf.appendSlice(self.gpa, "\\t"),
            0...8, 11, 12, 14...31 => try self.buf.print(self.gpa, "\\u{x:0>4}", .{c}),
            else => try self.buf.append(self.gpa, c),
        };
        try self.buf.append(self.gpa, '"');
    }
};

test "nested output, commas and escaping" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var w = Writer.init(arena.allocator());
    try w.beginObject();
    try w.key("a");
    try w.int(1);
    try w.key("b");
    try w.arrayValue();
    try w.string("x\"y");
    try w.boolean(true);
    try w.endArray();
    try w.key("c");
    try w.objectValue();
    try w.key("d");
    try w.nullValue();
    try w.endObject();
    try w.endObject();
    try std.testing.expectEqualStrings("{\"a\":1,\"b\":[\"x\\\"y\",true],\"c\":{\"d\":null}}", w.bytes());
}

//! The backend-neutral pipeline description: a tree of `Node`s, each a type
//! name plus a plain string-keyed property map. Built from a parsed config
//! (`props.Value`), consumed by a factory. Nothing here knows about
//! GStreamer or the browser.

const std = @import("std");
const Allocator = std.mem.Allocator;
const props = @import("props.zig");
const registry_mod = @import("registry.zig");
const Props = props.Props;
const Value = props.Value;

pub const Node = struct {
    type_name: []const u8,
    /// Unique instance name, e.g. `tee_0`, `opt_conv_1`.
    name: []const u8,
    props: Props,
    /// Only for MuxElement: each branch is a chain ending in a sink.
    branches: []const []const Node = &.{},
};

pub const Graph = struct {
    /// The `pipeline:` section (type, port, mount_point, metadata_file, ...).
    pipeline: Props,
    nodes: []const Node,
};

pub const Error = error{
    MissingElements,
    ElementMissingType,
    UnknownElement,
    MissingRequiredOption,
    InvalidBranches,
    OutOfMemory,
};

pub const Diagnostic = struct {
    message: []const u8 = "",
};

const Counters = std.StringHashMapUnmanaged(u32);

const Builder = struct {
    gpa: Allocator,
    registry: *const registry_mod.Registry,
    counters: Counters = .empty,
    diag: ?*Diagnostic,

    fn fail(self: *Builder, comptime fmt: []const u8, args: anytype, e: Error) Error {
        if (self.diag) |d| d.message = std.fmt.allocPrint(self.gpa, fmt, args) catch "out of memory";
        return e;
    }

    fn nextName(self: *Builder, prefix: []const u8) ![]const u8 {
        const gop = try self.counters.getOrPut(self.gpa, prefix);
        if (!gop.found_existing) gop.value_ptr.* = 0;
        const n = gop.value_ptr.*;
        gop.value_ptr.* += 1;
        return std.fmt.allocPrint(self.gpa, "{s}_{d}", .{ prefix, n });
    }

    fn buildNode(self: *Builder, v: Value) Error!Node {
        const m = v.asMap() orelse return self.fail("element entry is not a mapping", .{}, error.ElementMissingType);
        const type_name = props.getString(m, "type") orelse
            return self.fail("element is missing the 'type' field", .{}, error.ElementMissingType);
        const spec = self.registry.find(type_name) orelse
            return self.fail("unknown element type '{s}'", .{type_name}, error.UnknownElement);

        for (spec.required) |req| {
            if (!m.contains(req))
                return self.fail("{s}: '{s}' is required", .{ type_name, req }, error.MissingRequiredOption);
        }

        // Pre-order naming: a mux is named before the elements of its branches,
        // matching construction order in the C++ registry lambdas.
        const name = blk: {
            if (std.mem.eql(u8, type_name, "InferServerElement"))
                break :blk try std.fmt.allocPrint(self.gpa, "infer_server_{s}", .{props.getString(m, "config_file").?});
            if (std.mem.eql(u8, type_name, "GstElement"))
                break :blk props.getString(m, "element").?;
            break :blk try self.nextName(spec.prefix);
        };

        var node: Node = .{ .type_name = type_name, .name = name, .props = m };

        if (spec.role == .mux) {
            const raw = props.getList(m, "branches") orelse &[_]Value{};
            const branches = try self.gpa.alloc([]const Node, raw.len);
            for (raw, 0..) |b, i| {
                const chain = b.asList() orelse
                    return self.fail("{s}: each branch must be a list of elements", .{name}, error.InvalidBranches);
                branches[i] = try self.buildChain(chain);
            }
            node.branches = branches;
        }
        return node;
    }

    fn buildChain(self: *Builder, list: []const Value) Error![]const Node {
        const out = try self.gpa.alloc(Node, list.len);
        for (list, 0..) |item, i| out[i] = try self.buildNode(item);
        return out;
    }
};

/// Build a graph from a parsed document. Allocate from an arena.
pub fn fromValue(gpa: Allocator, registry: *const registry_mod.Registry, root: Value, diag: ?*Diagnostic) Error!Graph {
    var b: Builder = .{ .gpa = gpa, .registry = registry, .diag = diag };
    const root_map = root.asMap() orelse props.Props.empty;

    const list = props.getList(root_map, "elements") orelse
        return b.fail("'elements' list is missing or not a sequence", .{}, error.MissingElements);

    return .{
        .pipeline = props.getMap(root_map, "pipeline") orelse .empty,
        .nodes = try b.buildChain(list),
    };
}

test "names follow construction order, mux first" {
    const yaml = @import("yaml.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const doc = try yaml.parseFirst(a,
        \\elements:
        \\  - type: CustomSrcElement
        \\  - type: MuxElement
        \\    branches:
        \\      - - type: OptimizedConverter
        \\        - type: MuxElement
        \\          branches:
        \\            - - type: MJPEGPublisher
        \\      - - type: NVUnixFDPublisher
    , null);
    var reg = try registry_mod.Registry.init(std.testing.allocator);
    defer reg.deinit();
    const g = try fromValue(a, &reg, doc, null);
    try std.testing.expectEqualStrings("custom_src_0", g.nodes[0].name);
    try std.testing.expectEqualStrings("tee_0", g.nodes[1].name);
    try std.testing.expectEqualStrings("opt_conv_0", g.nodes[1].branches[0][0].name);
    try std.testing.expectEqualStrings("tee_1", g.nodes[1].branches[0][1].name);
    try std.testing.expectEqualStrings("mjpeg_pub_0", g.nodes[1].branches[0][1].branches[0][0].name);
    try std.testing.expectEqualStrings("nv_unixfd_0", g.nodes[1].branches[1][0].name);
}

test "errors name the problem" {
    const yaml = @import("yaml.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var d: Diagnostic = .{};
    var reg = try registry_mod.Registry.init(std.testing.allocator);
    defer reg.deinit();

    const unknown = try yaml.parseFirst(a, "elements:\n  - type: Nope\n", null);
    try std.testing.expectError(error.UnknownElement, fromValue(a, &reg, unknown, &d));
    try std.testing.expectEqualStrings("unknown element type 'Nope'", d.message);

    const missing = try yaml.parseFirst(a, "elements:\n  - type: RTSPSourceElement\n", null);
    try std.testing.expectError(error.MissingRequiredOption, fromValue(a, &reg, missing, &d));
    try std.testing.expectEqualStrings("RTSPSourceElement: 'url' is required", d.message);
}

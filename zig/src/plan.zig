//! Backend-independent resolution. `resolve` walks a `Graph`, threading caps
//! from element to element and asking a factory to lower each node. It owns
//! everything that is the same for every backend: chain traversal, the
//! "what does the next element prefer" lookahead, nested-mux recursion, and
//! the shared scratchpad. It does not know what a gst string or a canvas is.
//!
//! A factory `F` is any type providing (duck-typed, checked at compile time):
//!
//!   fn preferredInputs(self: *F, node: *const Node) []const PixelFormat
//!   fn lower(self: *F, ctx: *LowerCtx, node: *const Node,
//!            branches: []const []const Segment) anyerror!Lowered

const std = @import("std");
const Allocator = std.mem.Allocator;
const caps_mod = @import("caps.zig");
const graph_mod = @import("graph.zig");
const props = @import("props.zig");
const Caps = caps_mod.Caps;
const PixelFormat = caps_mod.PixelFormat;
const Node = graph_mod.Node;
const Graph = graph_mod.Graph;

/// Generic key -> value store shared by every element in a pipeline (port of
/// the C++ `Scratchpad`; values are `props.Value`s instead of opaque strings).
pub const Scratchpad = std.StringHashMapUnmanaged(props.Value);

pub const Lowered = struct {
    /// Backend payload for this node. Empty means "contributes nothing"
    /// (a passthrough), exactly like an empty `gst_string` in the C++ core.
    text: []const u8 = "",
    out: Caps,
    /// Input caps to record; defaults to whatever arrived.
    in: ?Caps = null,
};

pub const Segment = struct {
    node: *const Node,
    text: []const u8,
    in: Caps,
    out: Caps,
    /// Resolved branches, for MuxElement.
    branches: []const []const Segment = &.{},
};

/// A named attachment point that application code can bind to at runtime
/// (the successor of `getGstElement(name)` + `setup()`).
pub const HandleKind = enum { frame_source, frame_sink, topic_writer, topic_reader, element };

pub const Handle = struct {
    name: []const u8,
    kind: HandleKind,
    type_name: []const u8,
};

/// A file the backend wants materialised next to the pipeline (e.g. the
/// nvdewarper config). Returned as data so factories stay free of I/O.
pub const SideFile = struct {
    path: []const u8,
    contents: []const u8,
};

pub const Plan = struct {
    chain: []const Segment,
    handles: []const Handle,
    side_files: []const SideFile,
    warnings: []const []const u8,
    notes: []const []const u8,
};

pub const Collector = struct {
    gpa: Allocator,
    handles: std.ArrayList(Handle) = .empty,
    side_files: std.ArrayList(SideFile) = .empty,
    warnings: std.ArrayList([]const u8) = .empty,
    notes: std.ArrayList([]const u8) = .empty,

    pub fn handle(self: *Collector, name: []const u8, kind: HandleKind, type_name: []const u8) !void {
        try self.handles.append(self.gpa, .{ .name = name, .kind = kind, .type_name = type_name });
    }
    pub fn sideFile(self: *Collector, path: []const u8, contents: []const u8) !void {
        try self.side_files.append(self.gpa, .{ .path = path, .contents = contents });
    }
    pub fn warn(self: *Collector, comptime fmt: []const u8, args: anytype) !void {
        try self.warnings.append(self.gpa, try std.fmt.allocPrint(self.gpa, fmt, args));
    }
    pub fn note(self: *Collector, comptime fmt: []const u8, args: anytype) !void {
        try self.notes.append(self.gpa, try std.fmt.allocPrint(self.gpa, fmt, args));
    }
};

pub const LowerCtx = struct {
    gpa: Allocator,
    /// What the previous segment outputs.
    upstream: Caps,
    /// What the next element prefers (ordered, first = most preferred).
    downstream_prefs: []const PixelFormat,
    scratchpad: *Scratchpad,
    pipeline: props.Props,
    out: *Collector,
};

pub fn resolve(
    comptime F: type,
    f: *F,
    gpa: Allocator,
    graph: Graph,
    scratchpad: *Scratchpad,
) !Plan {
    var out: Collector = .{ .gpa = gpa };
    const base: LowerCtx = .{
        .gpa = gpa,
        .upstream = Caps.any,
        .downstream_prefs = &.{},
        .scratchpad = scratchpad,
        .pipeline = graph.pipeline,
        .out = &out,
    };
    const chain = try resolveChain(F, f, graph.nodes, base);
    return .{
        .chain = chain,
        .handles = out.handles.items,
        .side_files = out.side_files.items,
        .warnings = out.warnings.items,
        .notes = out.notes.items,
    };
}

fn resolveChain(
    comptime F: type,
    f: *F,
    nodes: []const Node,
    base: LowerCtx,
) anyerror![]const Segment {
    var ctx = base;
    var segs: std.ArrayList(Segment) = .empty;

    for (nodes, 0..) |*node, i| {
        ctx.downstream_prefs = if (i + 1 < nodes.len) f.preferredInputs(&nodes[i + 1]) else &.{};

        // Branch chains start from the caps that reach the mux.
        var branches: []const []const Segment = &.{};
        if (node.branches.len > 0) {
            const bs = try ctx.gpa.alloc([]const Segment, node.branches.len);
            for (node.branches, 0..) |branch, bi| bs[bi] = try resolveChain(F, f, branch, ctx);
            branches = bs;
        }

        const lowered: Lowered = try f.lower(&ctx, node, branches);
        const in = lowered.in orelse ctx.upstream;

        try segs.append(ctx.gpa, .{
            .node = node,
            .text = lowered.text,
            .in = in,
            .out = lowered.out,
            .branches = branches,
        });
        ctx.upstream = lowered.out;
    }
    return segs.items;
}

test "resolve threads caps and supplies lookahead (toy factory)" {
    const yaml = @import("yaml.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const Toy = struct {
        seen_prefs: std.ArrayList([]const u8) = .empty,
        pub fn preferredInputs(_: *@This(), node: *const Node) []const PixelFormat {
            if (std.mem.eql(u8, node.type_name, "MJPEGPublisher")) return &.{.mjpeg};
            return &.{};
        }
        pub fn lower(self: *@This(), ctx: *LowerCtx, node: *const Node, _: []const []const Segment) anyerror!Lowered {
            try self.seen_prefs.append(ctx.gpa, if (ctx.downstream_prefs.len > 0) ctx.downstream_prefs[0].name() else "-");
            if (std.mem.eql(u8, node.type_name, "CustomSrcElement")) return .{ .text = "src", .out = Caps.ofFormat(.yuyv) };
            if (std.mem.eql(u8, node.type_name, "OptimizedConverter")) return .{ .text = "enc", .out = Caps.ofFormat(.mjpeg) };
            return .{ .text = "sink", .out = ctx.upstream };
        }
    };

    const doc = try yaml.parseFirst(a,
        \\elements:
        \\  - type: CustomSrcElement
        \\  - type: OptimizedConverter
        \\  - type: MJPEGPublisher
    , null);
    const g = try graph_mod.fromValue(a, doc, null);
    var toy: Toy = .{};
    var sp: Scratchpad = .empty;
    const plan = try resolve(Toy, &toy, a, g, &sp);

    try std.testing.expectEqual(@as(usize, 3), plan.chain.len);
    try std.testing.expectEqual(PixelFormat.mjpeg, plan.chain[2].in.format);
    try std.testing.expectEqualStrings("-", toy.seen_prefs.items[0]); // src: next (conv) has no prefs
    try std.testing.expectEqualStrings("MJPEG", toy.seen_prefs.items[1]); // conv: next prefers MJPEG
    try std.testing.expectEqual(@as(usize, 0), plan.warnings.len);
}

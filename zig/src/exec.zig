//! Runs a lowered plan through a runner, driving plugin element lifecycle
//! hooks around it so every runner (built in or plugin) behaves the same.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const abi = @import("plugin_abi.zig");
const graph_mod = @import("graph.zig");
const plan_mod = @import("plan.zig");
const registry_mod = @import("registry.zig");
const runner_mod = @import("runner.zig");

pub const Options = struct {
    /// Write the plan's side files (nvdewarper configs etc.) before building.
    write_side_files: bool = true,
    io: ?Io = null,
};

pub const Outcome = union(enum) {
    /// Pipeline ran to EOS (or was stopped).
    finished,
    /// Something failed; `message` says what.
    failed: []const u8,
};

const Instance = struct {
    def: *const abi.ElementDef,
    self: ?*anyopaque,
};

/// All plugin elements in the plan (depth-first, including mux branches).
fn collectPluginNodes(
    gpa: Allocator,
    reg: *const registry_mod.Registry,
    segs: []const plan_mod.Segment,
    out: *std.ArrayList(*const graph_mod.Node),
) !void {
    for (segs) |s| {
        if (reg.find(s.node.type_name)) |e| switch (e.kind) {
            .native => try out.append(gpa, s.node),
            else => {},
        };
        for (s.branches) |b| try collectPluginNodes(gpa, reg, b, out);
    }
}

fn cNodeFor(gpa: Allocator, n: *const graph_mod.Node) !abi.Node {
    return .{
        .type_name = (try gpa.dupeSentinel(u8, n.type_name, 0)).ptr,
        .name = (try gpa.dupeSentinel(u8, n.name, 0)).ptr,
        .props = @ptrCast(&n.props),
    };
}

pub fn execute(
    gpa: Allocator,
    reg: *const registry_mod.Registry,
    runner: runner_mod.Runner,
    view: runner_mod.PlanView,
    chain: []const plan_mod.Segment,
    opts: Options,
) !Outcome {
    if (opts.write_side_files) {
        const io = opts.io orelse return .{ .failed = "execute: side files need an Io; pass Options.io or disable write_side_files" };
        for (view.side_files) |f| {
            Io.Dir.cwd().writeFile(io, .{ .sub_path = f.path, .data = f.contents }) catch |e|
                return .{ .failed = try std.fmt.allocPrint(gpa, "cannot write {s}: {s}", .{ f.path, @errorName(e) }) };
        }
    }

    var build_err: []const u8 = "runner build failed";
    const session = runner.vtable.build(runner.ptr, gpa, view, &build_err) orelse
        return .{ .failed = build_err };
    defer runner.vtable.destroy(session);

    // Create and set up plugin elements against the runner's native pipeline.
    var nodes: std.ArrayList(*const graph_mod.Node) = .empty;
    try collectPluginNodes(gpa, reg, chain, &nodes);

    var live: std.ArrayList(Instance) = .empty;
    const native_handle = runner.vtable.nativeHandle(session);
    defer {
        var i = live.items.len;
        while (i > 0) : (i -= 1) {
            const inst = live.items[i - 1];
            if (inst.def.bringdown) |f| f(inst.self, native_handle);
            if (inst.def.destroy) |f| f(inst.self);
        }
    }

    for (nodes.items) |n| {
        const def = reg.find(n.type_name).?.kind.native;
        var cn = try cNodeFor(gpa, n);
        const self: ?*anyopaque = if (def.create) |f| f(&cn) else null;
        try live.append(gpa, .{ .def = def, .self = self });
        if (def.setup) |f| {
            if (f(self, native_handle) != 0)
                return .{ .failed = try std.fmt.allocPrint(gpa, "{s}: plugin setup failed", .{n.name}) };
        }
    }

    if (!runner.vtable.play(session))
        return .{ .failed = runner.vtable.errorText(session) orelse "runner could not start the pipeline" };
    const clean = runner.vtable.wait(session);
    runner.vtable.stop(session);

    if (!clean) {
        return .{ .failed = runner.vtable.errorText(session) orelse "pipeline reported an error" };
    }
    return .finished;
}

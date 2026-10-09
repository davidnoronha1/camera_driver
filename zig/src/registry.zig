//! Runtime element and runner registry. Replaces the closed comptime table:
//! built-in elements are entries like any other, and plugins add more.
//!
//! A registry entry says *what an element is* (type name, name prefix, role,
//! required options, which backends it supports) and carries a `Kind`
//! describing how it is lowered:
//!
//!   builtin   handled by code in gst_factory.zig / web_factory.zig
//!   manifest  declarative: a gst template with `{option}` substitution and a
//!             web `impl` id. Pure data, works on every platform incl. wasm.
//!   native    a C-ABI plugin element (see include/camera_driver_plugin.h);
//!             desktop only.

const std = @import("std");
const Allocator = std.mem.Allocator;
const abi = @import("plugin_abi.zig");
const caps_mod = @import("caps.zig");
const elements = @import("elements.zig");
const plan_mod = @import("plan.zig");
const props = @import("props.zig");
const runner_mod = @import("runner.zig");

pub const Role = elements.Role;
const PixelFormat = caps_mod.PixelFormat;

pub const ManifestHandle = struct {
    /// May contain `{name}`.
    name: []const u8,
    kind: plan_mod.HandleKind,
};

pub const Manifest = struct {
    /// GStreamer fragment; `{name}` is the instance name, `{opt}` / `{opt|default}`
    /// an option. null = no gst lowering.
    gst_template: ?[]const u8 = null,
    /// Format leaving the element; .unknown = same as what arrived.
    out_format: PixelFormat = .unknown,
    handles: []const ManifestHandle = &.{},
    preferred_inputs: []const PixelFormat = &.{},
    /// Browser `impl` id, or null with `web_reason`.
    web_impl: ?[]const u8 = null,
    web_reason: ?[]const u8 = null,
};

pub const Kind = union(enum) {
    builtin,
    manifest: Manifest,
    native: *const abi.ElementDef,
};

pub const Entry = struct {
    type_name: []const u8,
    prefix: []const u8,
    role: Role,
    required: []const []const u8 = &.{},
    backends: u32 = abi.backend_gst | abi.backend_web,
    kind: Kind = .builtin,
    /// Name of the plugin that registered it ("builtin" for the core).
    origin: []const u8 = "builtin",
};

pub const Registry = struct {
    /// Owns every string and copy stored in the registry.
    arena: std.heap.ArenaAllocator,
    elements: std.StringHashMapUnmanaged(Entry) = .empty,
    runners: std.StringHashMapUnmanaged(runner_mod.Runner) = .empty,
    /// Opaque handles to loaded native libraries, closed on deinit.
    libs: std.ArrayList(LoadedLib) = .empty,

    pub const LoadedLib = struct {
        path: []const u8,
        close: *const fn (handle: *anyopaque) void,
        handle: *anyopaque,
    };

    pub fn init(gpa: Allocator) !Registry {
        var r: Registry = .{ .arena = std.heap.ArenaAllocator.init(gpa) };
        errdefer r.arena.deinit();
        const a = r.arena.allocator();
        for (elements.all) |s| {
            try r.elements.put(a, s.type_name, .{
                .type_name = s.type_name,
                .prefix = s.prefix,
                .role = s.role,
                .required = s.required,
            });
        }
        try r.runners.put(a, "dry-run", runner_mod.DryRun.runner());
        return r;
    }

    pub fn deinit(self: *Registry) void {
        for (self.libs.items) |l| l.close(l.handle);
        self.arena.deinit();
    }

    pub fn alloc(self: *Registry) Allocator {
        return self.arena.allocator();
    }

    pub fn find(self: *const Registry, type_name: []const u8) ?*const Entry {
        return self.elements.getPtr(type_name);
    }

    pub fn findRunner(self: *const Registry, name: []const u8) ?runner_mod.Runner {
        return self.runners.get(name);
    }

    pub const AddError = error{ DuplicateElement, OutOfMemory };

    /// Add an element. Plugins may not replace an existing type.
    pub fn addElement(self: *Registry, entry: Entry) AddError!void {
        const a = self.alloc();
        if (self.elements.contains(entry.type_name)) return error.DuplicateElement;
        try self.elements.put(a, entry.type_name, entry);
    }

    pub fn addRunner(self: *Registry, r: runner_mod.Runner) !void {
        try self.runners.put(self.alloc(), r.name, r);
    }

    /// Sorted type names, for listing.
    pub fn typeNames(self: *const Registry, gpa: Allocator) ![]const []const u8 {
        var out: std.ArrayList([]const u8) = .empty;
        var it = self.elements.keyIterator();
        while (it.next()) |k| try out.append(gpa, k.*);
        std.mem.sort([]const u8, out.items, {}, struct {
            fn lt(_: void, x: []const u8, y: []const u8) bool {
                return std.mem.lessThan(u8, x, y);
            }
        }.lt);
        return out.items;
    }
};

// ── template rendering (manifest elements) ───────────────────────────────

pub const TemplateError = error{ MissingOption, BadTemplate, OutOfMemory };

/// Substitute `{name}`, `{opt}` and `{opt|default}`; `{{` and `}}` are literal braces.
pub fn renderTemplate(
    gpa: Allocator,
    template: []const u8,
    node_name: []const u8,
    node_props: props.Props,
    missing: *[]const u8,
) TemplateError![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    var i: usize = 0;
    while (i < template.len) {
        const c = template[i];
        if (c == '{' and i + 1 < template.len and template[i + 1] == '{') {
            try out.append(gpa, '{');
            i += 2;
        } else if (c == '}' and i + 1 < template.len and template[i + 1] == '}') {
            try out.append(gpa, '}');
            i += 2;
        } else if (c == '{') {
            const end = std.mem.indexOfScalarPos(u8, template, i, '}') orelse return error.BadTemplate;
            const spec = template[i + 1 .. end];
            const bar = std.mem.indexOfScalar(u8, spec, '|');
            const key = if (bar) |b| spec[0..b] else spec;
            const default = if (bar) |b| spec[b + 1 ..] else null;

            if (std.mem.eql(u8, key, "name")) {
                try out.appendSlice(gpa, node_name);
            } else if (node_props.get(key)) |v| {
                switch (v) {
                    .string => |s| try out.appendSlice(gpa, s),
                    .int => |n| try out.print(gpa, "{d}", .{n}),
                    .float => |f| try out.print(gpa, "{d}", .{f}),
                    .bool => |b| try out.appendSlice(gpa, if (b) "true" else "false"),
                    else => return error.BadTemplate,
                }
            } else if (default) |d| {
                try out.appendSlice(gpa, d);
            } else {
                missing.* = key;
                return error.MissingOption;
            }
            i = end + 1;
        } else {
            try out.append(gpa, c);
            i += 1;
        }
    }
    return out.items;
}

test "template substitution" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var p: props.Props = .empty;
    try p.put(a, "location", .{ .string = "/tmp/x.mcap" });
    try p.put(a, "port", .{ .int = 8080 });
    var missing: []const u8 = "";
    const s = try renderTemplate(a, "mcapsink name={name} location={location} p={port} q={quality|85} {{lit}}", "mcap_0", p, &missing);
    try std.testing.expectEqualStrings("mcapsink name=mcap_0 location=/tmp/x.mcap p=8080 q=85 {lit}", s);
    try std.testing.expectError(error.MissingOption, renderTemplate(a, "x={nope}", "n", p, &missing));
    try std.testing.expectEqualStrings("nope", missing);
}

test "builtins are registered; duplicates are refused" {
    var r = try Registry.init(std.testing.allocator);
    defer r.deinit();
    try std.testing.expect(r.find("OptimizedConverter") != null);
    try std.testing.expect(r.find("Nope") == null);
    try std.testing.expectError(error.DuplicateElement, r.addElement(.{ .type_name = "MuxElement", .prefix = "x", .role = .mux }));
    try r.addElement(.{ .type_name = "Extra", .prefix = "extra", .role = .sink });
    try std.testing.expect(r.findRunner("dry-run") != null);
}

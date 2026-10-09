//! The web plugin backend: declarative plugin manifests.
//!
//! A manifest is JSON that adds element types to a registry without any
//! native code, so it works on every platform including wasm. The same file
//! can carry a gst template (desktop lowering) and a browser `impl` id (web
//! lowering); the matching JS implementation is registered separately with
//! `registerImpl()` in web/host.js.
//!
//!   {
//!     "plugin": "mcap",
//!     "elements": [{
//!       "type": "McapSinkElement2",
//!       "prefix": "mcap_sink",
//!       "role": "sink",
//!       "required": ["location"],
//!       "preferred_inputs": ["H264", "I420"],
//!       "gst": {
//!         "template": "mcapsink location={location} name={name}_sink",
//!         "out": "same",
//!         "handles": [{"name": "{name}_sink", "kind": "topic_writer"}]
//!       },
//!       "web": {"impl": "record-mcap"}          // or {"unsupported": "reason"}
//!     }]
//!   }

const std = @import("std");
const Allocator = std.mem.Allocator;
const abi = @import("plugin_abi.zig");
const caps_mod = @import("caps.zig");
const plan_mod = @import("plan.zig");
const registry_mod = @import("registry.zig");

const PixelFormat = caps_mod.PixelFormat;
const Json = std.json.Value;

pub const Error = error{ BadManifest, DuplicateElement, OutOfMemory };

pub const Diagnostic = struct { message: []const u8 = "" };

fn fail(a: Allocator, diag: ?*Diagnostic, comptime fmt: []const u8, args: anytype) Error {
    if (diag) |d| d.message = std.fmt.allocPrint(a, fmt, args) catch "bad manifest";
    return error.BadManifest;
}

fn str(v: ?Json) ?[]const u8 {
    const j = v orelse return null;
    return if (j == .string) j.string else null;
}

fn parseRole(s: []const u8) ?registry_mod.Role {
    if (std.mem.eql(u8, s, "source")) return .source;
    if (std.mem.eql(u8, s, "transform")) return .transform;
    if (std.mem.eql(u8, s, "mux")) return .mux;
    if (std.mem.eql(u8, s, "sink")) return .sink;
    return null;
}

fn parseKind(s: []const u8) ?plan_mod.HandleKind {
    return std.meta.stringToEnum(plan_mod.HandleKind, s);
}

/// Register every element in `text`. Returns the plugin name.
pub fn register(reg: *registry_mod.Registry, text: []const u8, diag: ?*Diagnostic) Error![]const u8 {
    const a = reg.alloc();
    const parsed = std.json.parseFromSliceLeaky(Json, a, text, .{}) catch
        return fail(a, diag, "manifest is not valid JSON", .{});
    if (parsed != .object) return fail(a, diag, "manifest must be a JSON object", .{});
    const root = parsed.object;

    const plugin = str(root.get("plugin")) orelse "manifest";
    const list = root.get("elements") orelse return fail(a, diag, "manifest has no 'elements' array", .{});
    if (list != .array) return fail(a, diag, "'elements' must be an array", .{});

    for (list.array.items, 0..) |item, idx| {
        if (item != .object) return fail(a, diag, "elements[{d}] must be an object", .{idx});
        const o = item.object;

        const type_name = str(o.get("type")) orelse return fail(a, diag, "elements[{d}] needs 'type'", .{idx});
        const prefix = str(o.get("prefix")) orelse return fail(a, diag, "{s}: needs 'prefix'", .{type_name});
        const role = parseRole(str(o.get("role")) orelse "") orelse
            return fail(a, diag, "{s}: 'role' must be source|transform|mux|sink", .{type_name});

        var required: std.ArrayList([]const u8) = .empty;
        if (o.get("required")) |r| if (r == .array) for (r.array.items) |n| {
            if (n == .string) try required.append(a, n.string);
        };

        var m: registry_mod.Manifest = .{};
        var backends: u32 = 0;

        if (o.get("preferred_inputs")) |p| if (p == .array) {
            var fmts: std.ArrayList(PixelFormat) = .empty;
            for (p.array.items) |n| if (n == .string) {
                const f = PixelFormat.parse(n.string);
                if (f == .unknown) return fail(a, diag, "{s}: unknown pixel format '{s}'", .{ type_name, n.string });
                try fmts.append(a, f);
            };
            m.preferred_inputs = fmts.items;
        };

        if (o.get("gst")) |g| {
            if (g != .object) return fail(a, diag, "{s}: 'gst' must be an object", .{type_name});
            m.gst_template = str(g.object.get("template"));
            if (m.gst_template != null) backends |= abi.backend_gst;
            if (str(g.object.get("out"))) |out| {
                if (!std.mem.eql(u8, out, "same")) {
                    m.out_format = PixelFormat.parse(out);
                    if (m.out_format == .unknown) return fail(a, diag, "{s}: unknown 'out' format '{s}'", .{ type_name, out });
                }
            }
            if (g.object.get("handles")) |hs| if (hs == .array) {
                var handles: std.ArrayList(registry_mod.ManifestHandle) = .empty;
                for (hs.array.items) |h| {
                    if (h != .object) continue;
                    const hn = str(h.object.get("name")) orelse return fail(a, diag, "{s}: handle needs 'name'", .{type_name});
                    const hk = parseKind(str(h.object.get("kind")) orelse "") orelse
                        return fail(a, diag, "{s}: handle '{s}' has an unknown 'kind'", .{ type_name, hn });
                    try handles.append(a, .{ .name = hn, .kind = hk });
                }
                m.handles = handles.items;
            };
        }

        if (o.get("web")) |w| {
            if (w != .object) return fail(a, diag, "{s}: 'web' must be an object", .{type_name});
            m.web_impl = str(w.object.get("impl"));
            m.web_reason = str(w.object.get("unsupported"));
            if (m.web_impl != null) backends |= abi.backend_web;
        }
        if (backends == 0) backends = 0; // declared but lowers nowhere; planning will say so

        reg.addElement(.{
            .type_name = type_name,
            .prefix = prefix,
            .role = role,
            .required = required.items,
            .backends = backends,
            .kind = .{ .manifest = m },
            .origin = plugin,
        }) catch |e| switch (e) {
            error.DuplicateElement => return fail(a, diag, "{s}: element type is already registered", .{type_name}),
            error.OutOfMemory => return error.OutOfMemory,
        };
    }
    return plugin;
}

test "manifest registers a declarative element for both backends" {
    var reg = try registry_mod.Registry.init(std.testing.allocator);
    defer reg.deinit();
    var d: Diagnostic = .{};
    const name = try register(&reg,
        \\{"plugin":"demo","elements":[{
        \\  "type":"TimestampOverlay","prefix":"ts","role":"transform","required":["font"],
        \\  "preferred_inputs":["I420"],
        \\  "gst":{"template":"timeoverlay name={name} font-desc={font}","out":"same"},
        \\  "web":{"impl":"overlay-time"}
        \\}]}
    , &d);
    try std.testing.expectEqualStrings("demo", name);
    const e = reg.find("TimestampOverlay").?;
    try std.testing.expectEqualStrings("demo", e.origin);
    try std.testing.expect(e.backends == (abi.backend_gst | abi.backend_web));
    try std.testing.expectEqualStrings("font", e.required[0]);
}

test "manifest errors are specific" {
    var reg = try registry_mod.Registry.init(std.testing.allocator);
    defer reg.deinit();
    var d: Diagnostic = .{};
    try std.testing.expectError(error.BadManifest, register(&reg, "{\"elements\":[{\"type\":\"X\",\"prefix\":\"x\",\"role\":\"nope\"}]}", &d));
    try std.testing.expectEqualStrings("X: 'role' must be source|transform|mux|sink", d.message);
    try std.testing.expectError(error.BadManifest, register(&reg, "not json", &d));
    try std.testing.expectError(error.BadManifest, register(&reg,
        \\{"elements":[{"type":"MuxElement","prefix":"t","role":"mux"}]}
    , &d));
    try std.testing.expectEqualStrings("MuxElement: element type is already registered", d.message);
}

//! camera-driver-plan: turn a pipeline config into a plan without running it.
//!
//!   camera-driver-plan config.yaml                     # print the gst launch string
//!   camera-driver-plan config.yaml --doc 2             # pick a `---` document
//!   camera-driver-plan config.yaml --hw nvh264enc,x264enc,jpegenc
//!   camera-driver-plan config.yaml --backend web       # JSON plan for web/host.js
//!   camera-driver-plan config.yaml --json              # full JSON for either backend
//!   camera-driver-plan config.yaml --plugin libmcap.so   # load a native plugin
//!   camera-driver-plan config.yaml --manifest extra.json # declarative plugin
//!   camera-driver-plan config.yaml --run                 # plan, then execute it
//!   camera-driver-plan --list-elements | --list-runners
//!
//! The launch string can be run as-is: `gst-launch-1.0 -e "$(camera-driver-plan cfg.yaml)"`.

const std = @import("std");
const Io = std.Io;
const cd = @import("camera_driver");

const usage =
    \\usage: camera-driver-plan <config.yaml> [options]
    \\       camera-driver-plan --list-elements
    \\
    \\  --backend gst|web     target backend (default gst)
    \\  --doc N               which `---` document to plan (default 0)
    \\  --hw a,b,c            GStreamer elements that exist, e.g. nvh264enc,x264enc,jpegenc
    \\                        (default: x264enc,jpegenc,avdec_h264)
    \\  --web-caps a,b,c      browser capabilities (default: all); see web_factory.WebCaps
    \\  --device PATH         V4L2 device to assume (default /dev/video0)
    \\  --metadata FILE       camera_info / metadata YAML to seed the scratchpad
    \\  --json                print the full JSON result
    \\  --write-side-files    write files the plan asks for (e.g. nvdewarper configs)
    \\  --plugin PATH.so      load a native plugin (repeatable)
    \\  --manifest FILE.json  load a declarative plugin manifest (repeatable)
    \\  --run                 execute the plan with a runner (gst backend)
    \\  --runner NAME         which runner (default: gst); see --list-runners
    \\  --list-elements       list element types and where they came from
    \\  --list-runners        list available runners
    \\
;

fn readFile(ctx: ?*anyopaque, gpa: std.mem.Allocator, path: []const u8) ?[]const u8 {
    const io: *const Io = @ptrCast(@alignCast(ctx.?));
    return std.Io.Dir.cwd().readFileAlloc(io.*, path, gpa, .limited(16 * 1024 * 1024)) catch null;
}

fn exitWith(out: *Io.Writer, err: *Io.Writer, code: u8) noreturn {
    out.flush() catch {};
    err.flush() catch {};
    std.process.exit(code);
}

fn onSigint(_: std.posix.SIG) callconv(.c) void {
    cd.runner.interrupt_requested.store(true, .release);
}

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    const io = init.io;
    const args = try init.minimal.args.toSlice(arena);

    var out_buf: [4096]u8 = undefined;
    var out_w: Io.File.Writer = .init(.stdout(), io, &out_buf);
    const out = &out_w.interface;
    var err_buf: [1024]u8 = undefined;
    var err_w: Io.File.Writer = .init(.stderr(), io, &err_buf);
    const err = &err_w.interface;
    defer out.flush() catch {};
    defer err.flush() catch {};

    var registry = try cd.registry.Registry.init(init.gpa);
    defer registry.deinit();
    try registry.addRunner(cd.runner_gst.runner());

    var config_path: ?[]const u8 = null;
    var req: cd.session.Request = .{ .registry = &registry, .source = "" };
    var want_json = false;
    var write_side = false;
    var metadata_path: ?[]const u8 = null;
    var list_elements = false;
    var list_runners = false;
    var do_run = false;
    var runner_name: []const u8 = "gst";
    var plugins: std.ArrayList([]const u8) = .empty;
    var manifests: std.ArrayList([]const u8) = .empty;

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (std.mem.eql(u8, a, "--json")) {
            want_json = true;
        } else if (std.mem.eql(u8, a, "--write-side-files")) {
            write_side = true;
        } else if (std.mem.eql(u8, a, "--list-elements")) {
            list_elements = true;
        } else if (std.mem.eql(u8, a, "--list-runners")) {
            list_runners = true;
        } else if (std.mem.eql(u8, a, "--run")) {
            do_run = true;
        } else if (std.mem.eql(u8, a, "--backend") or std.mem.eql(u8, a, "--doc") or std.mem.eql(u8, a, "--hw") or
            std.mem.eql(u8, a, "--web-caps") or std.mem.eql(u8, a, "--device") or std.mem.eql(u8, a, "--metadata") or
            std.mem.eql(u8, a, "--plugin") or std.mem.eql(u8, a, "--manifest") or std.mem.eql(u8, a, "--runner"))
        {
            i += 1;
            if (i >= args.len) {
                try err.print("{s} needs a value\n{s}", .{ a, usage });
                exitWith(out, err, 2);
            }
            const v = args[i];
            if (std.mem.eql(u8, a, "--backend")) {
                req.backend = if (std.mem.eql(u8, v, "web")) .web else if (std.mem.eql(u8, v, "gst")) .gst else {
                    try err.print("unknown backend '{s}'\n", .{v});
                    exitWith(out, err, 2);
                };
            } else if (std.mem.eql(u8, a, "--doc")) {
                req.doc_index = std.fmt.parseInt(usize, v, 10) catch {
                    try err.print("--doc expects a number\n", .{});
                    exitWith(out, err, 2);
                };
            } else if (std.mem.eql(u8, a, "--hw")) {
                req.hw = try cd.hw.HwCaps.fromCsv(arena, v);
            } else if (std.mem.eql(u8, a, "--web-caps")) {
                req.web_caps = cd.web_factory.WebCaps.fromCsv(v);
            } else if (std.mem.eql(u8, a, "--device")) {
                req.device = v;
            } else if (std.mem.eql(u8, a, "--plugin")) {
                try plugins.append(arena, v);
            } else if (std.mem.eql(u8, a, "--manifest")) {
                try manifests.append(arena, v);
            } else if (std.mem.eql(u8, a, "--runner")) {
                runner_name = v;
            } else {
                metadata_path = v;
            }
        } else if (std.mem.startsWith(u8, a, "--")) {
            try err.print("unknown option {s}\n{s}", .{ a, usage });
            exitWith(out, err, 2);
        } else {
            config_path = a;
        }
    }

    // Plugins first: they extend the catalogue the config is checked against.
    for (plugins.items) |path| {
        var d: cd.plugin_desktop.Diagnostic = .{};
        cd.plugin_desktop.load(&registry, path, &d) catch |e| {
            try err.print("plugin error: {s} ({s})\n", .{ if (d.message.len > 0) d.message else path, @errorName(e) });
            exitWith(out, err, 1);
        };
    }
    for (manifests.items) |path| {
        const text = std.Io.Dir.cwd().readFileAlloc(io, path, arena, .limited(4 * 1024 * 1024)) catch |e| {
            try err.print("cannot read {s}: {s}\n", .{ path, @errorName(e) });
            exitWith(out, err, 1);
        };
        var d: cd.manifest.Diagnostic = .{};
        _ = cd.manifest.register(&registry, text, &d) catch {
            try err.print("manifest error in {s}: {s}\n", .{ path, d.message });
            exitWith(out, err, 1);
        };
    }

    if (list_elements) {
        for (try registry.typeNames(arena)) |name| {
            const e = registry.find(name).?;
            try out.print("{s:<28} {s:<10} {s}\n", .{ name, @tagName(e.role), e.origin });
        }
        return;
    }
    if (list_runners) {
        var it = registry.runners.iterator();
        while (it.next()) |e| try out.print("{s:<14} {s}\n", .{ e.key_ptr.*, @tagName(e.value_ptr.backend) });
        return;
    }
    const path = config_path orelse {
        try err.print("{s}", .{usage});
        exitWith(out, err, 2);
    };

    req.source = std.Io.Dir.cwd().readFileAlloc(io, path, arena, .limited(16 * 1024 * 1024)) catch |e| {
        try err.print("cannot read {s}: {s}\n", .{ path, @errorName(e) });
        exitWith(out, err, 1);
    };
    if (metadata_path) |mp| {
        req.metadata = std.Io.Dir.cwd().readFileAlloc(io, mp, arena, .limited(16 * 1024 * 1024)) catch |e| {
            try err.print("cannot read {s}: {s}\n", .{ mp, @errorName(e) });
            exitWith(out, err, 1);
        };
    }
    const io_ptr: *const Io = &io;
    req.files = .{ .ctx = @ptrCast(@constCast(io_ptr)), .read = readFile };

    const result = cd.session.run(arena, req);

    if (result == .failure) {
        const f = result.failure;
        try out.print("{s}\n", .{try cd.session.toJson(arena, result)});
        try err.print("error ({s}{s}): {s}\n", .{
            @tagName(f.stage),
            if (f.line > 0) try std.fmt.allocPrint(arena, ", line {d}", .{f.line}) else "",
            f.message,
        });
        exitWith(out, err, 1);
    }

    if (do_run) {
        if (result != .gst) {
            try err.print("--run needs the gst backend; the web backend runs in a browser (see web/host.js)\n", .{});
            exitWith(out, err, 2);
        }
        const g = result.gst;
        const runner = registry.findRunner(runner_name) orelse {
            try err.print("no runner named '{s}' (see --list-runners)\n", .{runner_name});
            exitWith(out, err, 2);
        };
        for (g.out.plan.warnings) |w| try err.print("warning: {s}\n", .{w});
        try err.flush();

        const act: std.posix.Sigaction = .{
            .handler = .{ .handler = onSigint },
            .mask = std.posix.sigemptyset(),
            .flags = 0,
        };
        std.posix.sigaction(.INT, &act, null);
        std.posix.sigaction(.TERM, &act, null);

        const view: cd.runner.PlanView = .{
            .backend = .gst,
            .text = g.out.launch,
            .handles = g.out.plan.handles,
            .side_files = g.out.plan.side_files,
        };
        const outcome = try cd.exec.execute(arena, &registry, runner, view, g.out.plan.chain, .{ .io = io });
        switch (outcome) {
            .finished => return,
            .failed => |m| {
                try err.print("error (run): {s}\n", .{m});
                exitWith(out, err, 1);
            },
        }
    }

    if (want_json or req.backend == .web) {
        try out.print("{s}\n", .{try cd.session.toJson(arena, result)});
    }
    switch (result) {
        .web => |w| if (!w.out.ok) exitWith(out, err, 3),
        .gst => |g| {
            if (!want_json) try out.print("{s}\n", .{g.out.launch});
            for (g.out.plan.warnings) |w| try err.print("warning: {s}\n", .{w});
            for (g.out.plan.side_files) |sf| {
                if (write_side) {
                    std.Io.Dir.cwd().writeFile(io, .{ .sub_path = sf.path, .data = sf.contents }) catch |e|
                        try err.print("cannot write {s}: {s}\n", .{ sf.path, @errorName(e) });
                } else {
                    try err.print("note: plan wants side file {s} (use --write-side-files)\n", .{sf.path});
                }
            }
        },
        .failure => unreachable,
    }
}

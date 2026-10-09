//! camera-driver-plan: turn a pipeline config into a plan without running it.
//!
//!   camera-driver-plan config.yaml                     # print the gst launch string
//!   camera-driver-plan config.yaml --doc 2             # pick a `---` document
//!   camera-driver-plan config.yaml --hw nvh264enc,x264enc,jpegenc
//!   camera-driver-plan config.yaml --backend web       # JSON plan for web/host.js
//!   camera-driver-plan config.yaml --json              # full JSON for either backend
//!   camera-driver-plan --list-elements
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

    var config_path: ?[]const u8 = null;
    var req: cd.session.Request = .{ .source = "" };
    var want_json = false;
    var write_side = false;
    var metadata_path: ?[]const u8 = null;
    var list = false;

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (std.mem.eql(u8, a, "--json")) {
            want_json = true;
        } else if (std.mem.eql(u8, a, "--write-side-files")) {
            write_side = true;
        } else if (std.mem.eql(u8, a, "--list-elements")) {
            list = true;
        } else if (std.mem.eql(u8, a, "--backend") or std.mem.eql(u8, a, "--doc") or std.mem.eql(u8, a, "--hw") or
            std.mem.eql(u8, a, "--web-caps") or std.mem.eql(u8, a, "--device") or std.mem.eql(u8, a, "--metadata"))
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

    if (list) {
        for (cd.elements.all) |s| try out.print("{s:<28} {s}\n", .{ s.type_name, @tagName(s.role) });
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

    if (want_json or req.backend == .web or result == .failure) {
        try out.print("{s}\n", .{try cd.session.toJson(arena, result)});
        switch (result) {
            .failure => |f| {
                try err.print("error ({s}{s}): {s}\n", .{
                    @tagName(f.stage),
                    if (f.line > 0) try std.fmt.allocPrint(arena, ", line {d}", .{f.line}) else "",
                    f.message,
                });
                exitWith(out, err, 1);
            },
            .web => |w| if (!w.out.ok) exitWith(out, err, 3),
            .gst => {},
        }
        if (!(want_json or req.backend == .web)) return;
    }

    switch (result) {
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
        else => {},
    }
}

//! The "finalizer": what takes a lowered plan and actually executes it.
//!
//! A runner is a swappable plugin kind. The desktop default (runner_gst.zig)
//! builds a GStreamer pipeline from the launch string and plays it; the web
//! default is `Runtime` in web/host.js. Others can be supplied by native
//! plugins through the C ABI (`cd_runner_def`) or built in (`dry-run`).
//!
//! Lifecycle, orchestrated by exec.zig so every runner behaves the same:
//!
//!     session = build(plan)            runner creates the native pipeline
//!     for plugin elements: create(); setup(native_handle)
//!     play(); wait()                   blocks until EOS / error / stop
//!     for plugin elements (reverse): bringdown(); destroy()
//!     destroy(session)

const std = @import("std");
const Allocator = std.mem.Allocator;
const abi = @import("plugin_abi.zig");
const plan_mod = @import("plan.zig");

pub const Backend = enum { gst, web };

/// What a runner is handed. All slices are owned by the caller and outlive
/// the session.
pub const PlanView = struct {
    backend: Backend,
    /// gst: the launch string. web: the JSON plan.
    text: []const u8,
    handles: []const plan_mod.Handle = &.{},
    side_files: []const plan_mod.SideFile = &.{},
};

pub const Error = error{ BuildFailed, PlayFailed, PipelineError, OutOfMemory };

pub const Runner = struct {
    name: []const u8,
    backend: Backend,
    ptr: ?*anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Returns a session pointer; on failure writes a message into `err`.
        build: *const fn (ptr: ?*anyopaque, gpa: Allocator, plan: PlanView, err: *[]const u8) ?*anyopaque,
        nativeHandle: *const fn (session: *anyopaque) ?*anyopaque,
        findElement: *const fn (session: *anyopaque, name: [:0]const u8) ?*anyopaque,
        play: *const fn (session: *anyopaque) bool,
        /// Blocks. Returns true on a clean end (EOS), false on error.
        wait: *const fn (session: *anyopaque) bool,
        stop: *const fn (session: *anyopaque) void,
        destroy: *const fn (session: *anyopaque) void,
        /// Why the last `wait` returned false, if known.
        errorText: *const fn (session: *anyopaque) ?[]const u8,
    };
};

/// Set by a signal handler (or another thread); runners poll it in `wait`.
pub var interrupt_requested: std.atomic.Value(bool) = .init(false);

// ── built-in "dry-run" runner ────────────────────────────────────────────
// Executes nothing; records what it was given. Useful for debugging a plan
// and as the reference implementation of the contract.

pub const DryRun = struct {
    pub const Session = struct {
        launch: []const u8,
        played: bool = false,
        stopped: bool = false,
    };

    pub fn runner() Runner {
        return .{ .name = "dry-run", .backend = .gst, .ptr = null, .vtable = &vtable };
    }

    const vtable: Runner.VTable = .{
        .build = build,
        .nativeHandle = nativeHandle,
        .findElement = findElement,
        .play = play,
        .wait = wait,
        .stop = stop,
        .destroy = destroy,
        .errorText = errorText,
    };

    fn errorText(_: *anyopaque) ?[]const u8 {
        return null;
    }

    fn build(_: ?*anyopaque, gpa: Allocator, plan: PlanView, _: *[]const u8) ?*anyopaque {
        const s = gpa.create(Session) catch return null;
        s.* = .{ .launch = gpa.dupe(u8, plan.text) catch return null };
        return s;
    }
    fn nativeHandle(_: *anyopaque) ?*anyopaque {
        return null;
    }
    fn findElement(_: *anyopaque, _: [:0]const u8) ?*anyopaque {
        return null;
    }
    fn play(s: *anyopaque) bool {
        @as(*Session, @ptrCast(@alignCast(s))).played = true;
        return true;
    }
    fn wait(_: *anyopaque) bool {
        return true;
    }
    fn stop(s: *anyopaque) void {
        @as(*Session, @ptrCast(@alignCast(s))).stopped = true;
    }
    fn destroy(_: *anyopaque) void {}
};

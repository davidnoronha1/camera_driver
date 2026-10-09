//! A small YAML-subset parser that produces `props.Value` trees directly.
//!
//! Supported (everything the pipeline configs use):
//!   - block mappings and block sequences, arbitrarily nested, including
//!     `- - type: X` and `- key: v` with continuation keys
//!   - flow sequences `[a, "b"]` and flow mappings `{k: v}` on one line
//!   - plain / 'single' / "double" quoted scalars, with bool/int/float/null typing
//!   - `#` comments and multiple documents separated by `---`
//! Not supported: anchors/aliases, tags, block scalars (`|`, `>`), multi-line
//! flow collections, complex keys. These raise `error.YamlSyntax` with a
//! line number in the optional `Diagnostic` rather than being misparsed.

const std = @import("std");
const Allocator = std.mem.Allocator;
const props = @import("props.zig");
const Value = props.Value;

pub const Error = error{ YamlSyntax, OutOfMemory };

pub const Diagnostic = struct {
    line: usize = 0,
    message: []const u8 = "",
};

const Line = struct {
    indent: usize,
    text: []const u8,
    number: usize,
};

/// Parse all documents in `source`. Allocations come from `gpa` (use an arena).
pub fn parse(gpa: Allocator, source: []const u8, diag: ?*Diagnostic) Error![]const Value {
    var docs: std.ArrayList(Value) = .empty;
    var lines: std.ArrayList(Line) = .empty;

    var number: usize = 0;
    // A flow collection may span physical lines (ROS camera_info files wrap
    // long `data: [...]` lists). Accumulate until the brackets balance and
    // hand the parser one logical line.
    var acc: std.ArrayList(u8) = .empty;
    var acc_depth: i32 = 0;
    var acc_start: Line = undefined;
    var it = std.mem.splitScalar(u8, source, '\n');
    while (it.next()) |raw| {
        number += 1;
        const no_cr = std.mem.trimEnd(u8, raw, "\r");
        const stripped = std.mem.trimEnd(u8, stripComment(no_cr), " \t");
        if (stripped.len == 0) continue;

        if (acc_depth > 0) {
            try acc.append(gpa, ' ');
            try acc.appendSlice(gpa, std.mem.trim(u8, stripped, " \t"));
            acc_depth += bracketDelta(stripped);
            if (acc_depth <= 0) {
                acc_start.text = acc.items;
                try lines.append(gpa, acc_start);
                acc_depth = 0;
            }
            continue;
        }

        if (std.mem.eql(u8, stripped, "---") or std.mem.startsWith(u8, stripped, "--- ")) {
            if (lines.items.len > 0) {
                try docs.append(gpa, try parseDocument(gpa, lines.items, diag));
                lines.clearRetainingCapacity();
            }
            continue;
        }

        var indent: usize = 0;
        while (indent < stripped.len and stripped[indent] == ' ') indent += 1;
        if (indent < stripped.len and stripped[indent] == '\t')
            return fail(diag, number, "tab character used for indentation");

        const line: Line = .{ .indent = indent, .text = stripped[indent..], .number = number };
        const depth = bracketDelta(line.text);
        if (depth > 0) {
            acc.clearRetainingCapacity();
            try acc.appendSlice(gpa, line.text);
            acc_depth = depth;
            acc_start = line;
        } else {
            try lines.append(gpa, line);
        }
    }
    if (acc_depth > 0) return fail(diag, acc_start.number, "unterminated flow collection");
    if (lines.items.len > 0)
        try docs.append(gpa, try parseDocument(gpa, lines.items, diag));
    return docs.items;
}

/// Parse a single document (the first), like yaml-cpp's `LoadFile`.
pub fn parseFirst(gpa: Allocator, source: []const u8, diag: ?*Diagnostic) Error!Value {
    const docs = try parse(gpa, source, diag);
    if (docs.len == 0) return .null;
    return docs[0];
}

fn parseDocument(gpa: Allocator, lines: []Line, diag: ?*Diagnostic) Error!Value {
    var p = Parser{ .gpa = gpa, .lines = lines, .diag = diag };
    const v = try p.parseBlock();
    if (p.pos < lines.len)
        return fail(diag, lines[p.pos].number, "unexpected content (bad indentation?)");
    return v;
}

fn fail(diag: ?*Diagnostic, line: usize, msg: []const u8) Error {
    if (diag) |d| d.* = .{ .line = line, .message = msg };
    return error.YamlSyntax;
}

fn stripComment(line: []const u8) []const u8 {
    var quote: u8 = 0;
    var prev_sig: u8 = 0; // previous non-space char, 0 = none
    var i: usize = 0;
    while (i < line.len) : (i += 1) {
        const c = line[i];
        if (quote != 0) {
            if (c == '\\' and quote == '"') {
                i += 1;
            } else if (c == quote) quote = 0;
            continue;
        }
        const opens_quote = prev_sig == 0 or prev_sig == ':' or prev_sig == '-' or
            prev_sig == '[' or prev_sig == '{' or prev_sig == ',';
        if ((c == '"' or c == '\'') and opens_quote) {
            quote = c;
            prev_sig = c;
            continue;
        }
        if (c == '#' and (i == 0 or line[i - 1] == ' ' or line[i - 1] == '\t')) return line[0..i];
        if (c != ' ' and c != '\t') prev_sig = c;
    }
    return line;
}

/// Net `[`/`{` opened minus closed, ignoring quoted text.
fn bracketDelta(text: []const u8) i32 {
    var depth: i32 = 0;
    var quote: u8 = 0;
    var prev_sig: u8 = 0;
    var i: usize = 0;
    while (i < text.len) : (i += 1) {
        const c = text[i];
        if (quote != 0) {
            if (c == '\\' and quote == '"') {
                i += 1;
            } else if (c == quote) quote = 0;
            continue;
        }
        const opens_quote = prev_sig == 0 or prev_sig == ':' or prev_sig == '-' or
            prev_sig == '[' or prev_sig == '{' or prev_sig == ',';
        if ((c == '"' or c == '\'') and opens_quote) {
            quote = c;
        } else if (c == '[' or c == '{') {
            // Only count brackets that start a flow value, not ones inside plain text like `a[0]`.
            if (opens_quote) depth += 1;
        } else if (c == ']' or c == '}') {
            depth -= 1;
        }
        if (c != ' ' and c != '\t') prev_sig = c;
    }
    return depth;
}

fn isSeqItem(text: []const u8) bool {
    return std.mem.eql(u8, text, "-") or std.mem.startsWith(u8, text, "- ");
}

/// Index of the `:` that separates a mapping key from its value, if `text`
/// is a `key: value` / `key:` line.
fn findKeyColon(text: []const u8) ?usize {
    if (text.len == 0 or text[0] == '[' or text[0] == '{') return null;
    var quote: u8 = 0;
    var i: usize = 0;
    while (i < text.len) : (i += 1) {
        const c = text[i];
        if (quote != 0) {
            if (c == '\\' and quote == '"') {
                i += 1;
            } else if (c == quote) quote = 0;
            continue;
        }
        if ((c == '"' or c == '\'') and i == 0) {
            quote = c;
            continue;
        }
        if (c == ':' and (i + 1 == text.len or text[i + 1] == ' ')) return i;
    }
    return null;
}

const Parser = struct {
    gpa: Allocator,
    lines: []Line,
    pos: usize = 0,
    diag: ?*Diagnostic,

    fn err(self: *Parser, msg: []const u8) Error {
        const n = if (self.pos < self.lines.len) self.lines[self.pos].number else 0;
        return fail(self.diag, n, msg);
    }

    fn parseBlock(self: *Parser) Error!Value {
        if (self.pos >= self.lines.len) return .null;
        const line = self.lines[self.pos];
        if (isSeqItem(line.text)) return self.parseSeq(line.indent);
        if (findKeyColon(line.text) != null) return self.parseMap(line.indent);
        self.pos += 1;
        return self.parseInlineValue(line.text, line.number);
    }

    fn parseSeq(self: *Parser, indent: usize) Error!Value {
        var items: std.ArrayList(Value) = .empty;
        while (self.pos < self.lines.len) {
            const line = self.lines[self.pos];
            if (line.indent != indent or !isSeqItem(line.text)) break;

            const after = line.text[1..];
            const trimmed = std.mem.trimStart(u8, after, " ");
            if (trimmed.len == 0) {
                self.pos += 1;
                if (self.pos < self.lines.len and self.lines[self.pos].indent > indent) {
                    try items.append(self.gpa, try self.parseBlock());
                } else {
                    try items.append(self.gpa, .null);
                }
            } else {
                // Re-read this line as if the item content were its own line
                // at the column where it starts. This handles `- key: v`
                // (continuation keys sit at that column) and `- - nested`.
                self.lines[self.pos] = .{
                    .indent = indent + 1 + (after.len - trimmed.len),
                    .text = trimmed,
                    .number = line.number,
                };
                try items.append(self.gpa, try self.parseBlock());
            }
        }
        return .{ .list = items.items };
    }

    fn parseMap(self: *Parser, indent: usize) Error!Value {
        var map: props.Props = .empty;
        while (self.pos < self.lines.len) {
            const line = self.lines[self.pos];
            if (line.indent < indent) break;
            if (line.indent > indent) return self.err("unexpected indentation");
            if (isSeqItem(line.text)) break;

            const colon = findKeyColon(line.text) orelse
                return self.err("expected 'key: value'");
            const key = try self.parseKey(line.text[0..colon], line.number);
            const rest = std.mem.trim(u8, line.text[colon + 1 ..], " ");

            var value: Value = .null;
            if (rest.len == 0) {
                self.pos += 1;
                if (self.pos < self.lines.len) {
                    const next = self.lines[self.pos];
                    if (next.indent > indent) {
                        value = try self.parseBlock();
                    } else if (next.indent == indent and isSeqItem(next.text)) {
                        value = try self.parseSeq(indent);
                    }
                }
            } else {
                self.pos += 1;
                value = try self.parseInlineValue(rest, line.number);
            }
            try map.put(self.gpa, key, value);
        }
        return .{ .map = map };
    }

    fn parseKey(self: *Parser, raw: []const u8, number: usize) Error![]const u8 {
        const k = std.mem.trim(u8, raw, " ");
        if (k.len >= 2 and (k[0] == '"' or k[0] == '\'')) {
            var i: usize = 0;
            const s = try self.parseQuoted(k, &i, number);
            return s;
        }
        return try self.gpa.dupe(u8, k);
    }

    /// A value that fits on one line: flow collection, quoted or plain scalar.
    fn parseInlineValue(self: *Parser, text: []const u8, number: usize) Error!Value {
        var i: usize = 0;
        const v = try self.parseFlow(text, &i, number, false);
        i = skipSpaces(text, i);
        if (i != text.len) return fail(self.diag, number, "trailing characters after value");
        return v;
    }

    fn parseFlow(self: *Parser, text: []const u8, i: *usize, number: usize, in_flow: bool) Error!Value {
        i.* = skipSpaces(text, i.*);
        if (i.* >= text.len) return .null;
        switch (text[i.*]) {
            '[' => {
                i.* += 1;
                var items: std.ArrayList(Value) = .empty;
                while (true) {
                    i.* = skipSpaces(text, i.*);
                    if (i.* >= text.len) return fail(self.diag, number, "unterminated flow sequence");
                    if (text[i.*] == ']') {
                        i.* += 1;
                        break;
                    }
                    try items.append(self.gpa, try self.parseFlow(text, i, number, true));
                    i.* = skipSpaces(text, i.*);
                    if (i.* < text.len and text[i.*] == ',') i.* += 1;
                }
                return .{ .list = items.items };
            },
            '{' => {
                i.* += 1;
                var map: props.Props = .empty;
                while (true) {
                    i.* = skipSpaces(text, i.*);
                    if (i.* >= text.len) return fail(self.diag, number, "unterminated flow mapping");
                    if (text[i.*] == '}') {
                        i.* += 1;
                        break;
                    }
                    const key_v = try self.parseFlow(text, i, number, true);
                    i.* = skipSpaces(text, i.*);
                    if (i.* >= text.len or text[i.*] != ':')
                        return fail(self.diag, number, "expected ':' in flow mapping");
                    i.* += 1;
                    const val = try self.parseFlow(text, i, number, true);
                    const key = key_v.asString() orelse
                        return fail(self.diag, number, "flow mapping keys must be strings");
                    try map.put(self.gpa, key, val);
                    i.* = skipSpaces(text, i.*);
                    if (i.* < text.len and text[i.*] == ',') i.* += 1;
                }
                return .{ .map = map };
            },
            '"', '\'' => return .{ .string = try self.parseQuoted(text, i, number) },
            else => {
                const start = i.*;
                if (in_flow) {
                    while (i.* < text.len and text[i.*] != ',' and text[i.*] != ']' and
                        text[i.*] != '}' and !(text[i.*] == ':' and (i.* + 1 >= text.len or text[i.* + 1] == ' '))) i.* += 1;
                } else {
                    i.* = text.len;
                }
                const slice = std.mem.trim(u8, text[start..i.*], " ");
                return self.plainScalar(slice);
            },
        }
    }

    fn parseQuoted(self: *Parser, text: []const u8, i: *usize, number: usize) Error![]const u8 {
        const q = text[i.*];
        i.* += 1;
        var out: std.ArrayList(u8) = .empty;
        while (i.* < text.len) : (i.* += 1) {
            const c = text[i.*];
            if (q == '"' and c == '\\' and i.* + 1 < text.len) {
                i.* += 1;
                try out.append(self.gpa, switch (text[i.*]) {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    '0' => 0,
                    else => |e| e,
                });
            } else if (c == q) {
                if (q == '\'' and i.* + 1 < text.len and text[i.* + 1] == '\'') {
                    try out.append(self.gpa, '\'');
                    i.* += 1;
                    continue;
                }
                i.* += 1;
                return out.items;
            } else {
                try out.append(self.gpa, c);
            }
        }
        return fail(self.diag, number, "unterminated quoted string");
    }

    fn plainScalar(self: *Parser, s: []const u8) Error!Value {
        if (s.len == 0 or std.mem.eql(u8, s, "~") or std.mem.eql(u8, s, "null")) return .null;
        if (std.mem.eql(u8, s, "true") or std.mem.eql(u8, s, "True")) return .{ .bool = true };
        if (std.mem.eql(u8, s, "false") or std.mem.eql(u8, s, "False")) return .{ .bool = false };

        const c = s[0];
        if (std.ascii.isDigit(c) or c == '-' or c == '+' or c == '.') {
            if (std.fmt.parseInt(i64, s, 0)) |i| return .{ .int = i } else |_| {}
            if (std.fmt.parseFloat(f64, s)) |f| {
                if (std.ascii.isDigit(s[s.len - 1]) or s[s.len - 1] == '.') return .{ .float = f };
            } else |_| {}
        }
        return .{ .string = try self.gpa.dupe(u8, s) };
    }
};

fn skipSpaces(text: []const u8, from: usize) usize {
    var i = from;
    while (i < text.len and (text[i] == ' ' or text[i] == '\t')) i += 1;
    return i;
}

// ─── tests ───────────────────────────────────────────────────────────────────

fn testParse(arena: Allocator, src: []const u8) !Value {
    var d: Diagnostic = .{};
    return parseFirst(arena, src, &d) catch |e| {
        std.debug.print("yaml error line {d}: {s}\n", .{ d.line, d.message });
        return e;
    };
}

test "scalars are typed" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const v = try testParse(arena.allocator(),
        \\a: 12
        \\b: 1.5
        \\c: true
        \\d: hello world
        \\e: "123"
        \\f: ~
        \\g: /tmp/camera_nv.sock
        \\h: rtsp://host:554/x
    );
    const m = v.asMap().?;
    try std.testing.expectEqual(@as(i64, 12), props.getInt(m, "a", 0));
    try std.testing.expectEqual(@as(f64, 1.5), props.getFloat(m, "b", 0));
    try std.testing.expect(props.getBool(m, "c", false));
    try std.testing.expectEqualStrings("hello world", props.getString(m, "d").?);
    try std.testing.expectEqualStrings("123", props.getString(m, "e").?);
    try std.testing.expect(m.get("f").? == .null);
    try std.testing.expectEqualStrings("/tmp/camera_nv.sock", props.getString(m, "g").?);
    try std.testing.expectEqualStrings("rtsp://host:554/x", props.getString(m, "h").?);
}

test "sequences of maps, nested seqs, flow lists, comments" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const v = try testParse(arena.allocator(),
        \\pipeline:
        \\  type: Pipeline   # trailing comment
        \\elements:
        \\  - type: MuxElement
        \\    branches:
        \\      - - type: OptimizedConverter
        \\          outputs: [MJPEG, H264]
        \\        - type: MJPEGPublisher
        \\          port: 8080
        \\      - - type: NVUnixFDPublisher
        \\          socket: /tmp/x.sock
        \\  - type: DisplayPublisher
    );
    const m = v.asMap().?;
    try std.testing.expectEqualStrings("Pipeline", props.getString(props.getMap(m, "pipeline").?, "type").?);
    const els = props.getList(m, "elements").?;
    try std.testing.expectEqual(@as(usize, 2), els.len);
    const branches = props.getList(els[0].asMap().?, "branches").?;
    try std.testing.expectEqual(@as(usize, 2), branches.len);
    const b0 = branches[0].asList().?;
    try std.testing.expectEqual(@as(usize, 2), b0.len);
    const conv = b0[0].asMap().?;
    const outs = props.getList(conv, "outputs").?;
    try std.testing.expectEqualStrings("MJPEG", outs[0].asString().?);
    try std.testing.expectEqual(@as(i64, 8080), props.getInt(b0[1].asMap().?, "port", 0));
}

test "multiple documents" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const docs = try parse(arena.allocator(),
        \\a: 1
        \\---
        \\b: 2
        \\---
        \\c: 3
    , null);
    try std.testing.expectEqual(@as(usize, 3), docs.len);
}

test "sequence at the same indent as its key" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const v = try testParse(arena.allocator(),
        \\selection:
        \\- mode: serial
        \\  serial: "ABC"
        \\- mode: interactive
    );
    const sel = props.getList(v.asMap().?, "selection").?;
    try std.testing.expectEqual(@as(usize, 2), sel.len);
    try std.testing.expectEqualStrings("ABC", props.getString(sel[0].asMap().?, "serial").?);
}

test "syntax errors carry a line number" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var d: Diagnostic = .{};
    try std.testing.expectError(error.YamlSyntax, parseFirst(arena.allocator(), "a: [1, 2\n", &d));
    try std.testing.expectEqual(@as(usize, 1), d.line);
}

test "flow collections can wrap across lines" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const v = try testParse(arena.allocator(),
        \\camera_matrix:
        \\  rows: 3
        \\  data: [500.0, 0.0, 320.0,
        \\         0.0, 500.0, 240.0,   # principal point row
        \\         0.0, 0.0, 1.0]
        \\after: 1
    );
    const m = v.asMap().?;
    const data = props.getList(props.getMap(m, "camera_matrix").?, "data").?;
    try std.testing.expectEqual(@as(usize, 9), data.len);
    try std.testing.expectEqual(@as(f64, 240.0), data[5].asFloat().?);
    try std.testing.expectEqual(@as(i64, 1), props.getInt(m, "after", 0));
}

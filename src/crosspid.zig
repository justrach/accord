//! Graff presence_chan JSONL vs Accord Unix 0600 vs in-process Mailbox.
//! Same n=400 median-of-3 shape the codegraff agent ran. Does not patch graff.
//!
//!   zig build cross

const std = @import("std");
const accord = @import("accord");
const Io = std.Io;
const net = std.Io.net;

const sock_path = "accord-cross.sock";
const jsonl_name = "accord-cross.jsonl";
const n400: usize = 400;
const n2000: usize = 2000;
const jsonl_cap: usize = 256 * 1024;
const trials: usize = 3;

const JsonlMsg = struct {
    from_pid: i32 = 0,
    from_start: u64 = 0,
    from_session: []const u8 = "",
    from_goal: []const u8 = "",
    to: []const u8 = "",
    ts_ms: i64 = 0,
    text: []const u8 = "",
    from_user: bool = false,
    kind: []const u8 = "message",
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;

    var payload: [64]u8 = undefined;
    @memset(&payload, 'x');

    std.debug.print("Accord × graff channel  ({s})\n", .{@tagName(@import("builtin").mode)});
    std.debug.print("JSONL clones presence_chan (positional append, 256KiB drain cap)\n", .{});
    std.debug.print("Unix is listenUnix chmod 0600; Mailbox is same-process only\n\n", .{});

    try printEnvelope(gpa, &payload);
    try printSocketMode(io);

    var pair: Pair = undefined;
    try pair.init(io, gpa);
    defer pair.deinit();

    std.debug.print("== n={d} median-of-{d}  (64B text) ==\n", .{ n400, trials });
    std.debug.print("{s:<18} {s:>12} {s:>12} {s:>8}\n", .{ "path", "chatty ms", "batch ms", "heard" });

    const mb_c = try medianMs(trials, benchMailboxChatty, io, gpa, payload[0..], n400);
    const mb_b = try medianHeard(trials, benchMailboxBatch, io, gpa, payload[0..], n400);
    printRow("mailbox", mb_c, mb_b.ms, mb_b.heard);

    const ux_c = try medianMsPair(trials, benchUnixChattyOn, &pair, payload[0..], n400);
    const ux_b = try medianHeardPair(trials, benchUnixBatch, &pair, payload[0..], n400);
    printRow("unix 0600", ux_c, ux_b.ms, ux_b.heard);

    const js_c = try medianMs(trials, benchJsonlChatty, io, gpa, payload[0..], n400);
    const js_b = try medianHeard(trials, benchJsonlBatch, io, gpa, payload[0..], n400);
    printRow("jsonl (graff)", js_c, js_b.ms, js_b.heard);

    std.debug.print("\n== n={d} batch (JSONL 256KiB cap) ==\n", .{n2000});
    const mb2 = try benchMailboxBatch(io, gpa, payload[0..], n2000);
    const ux2 = try benchUnixBatch(&pair, payload[0..], n2000);
    const js2 = try benchJsonlBatch(io, gpa, payload[0..], n2000);
    std.debug.print("{s:<18} {s:>12} {s:>8}\n", .{ "path", "batch ms", "heard" });
    printRow2("mailbox", mb2.ms, mb2.heard);
    printRow2("unix 0600", ux2.ms, ux2.heard);
    printRow2("jsonl (graff)", js2.ms, js2.heard);

    std.debug.print("\nMailbox is in-process queues. Unix 0600 is the cross-PID live link.\n", .{});
    std.debug.print("JSONL is the durable room; drain of a file >256KiB returns 0 (graff cap).\n", .{});
}

fn printRow(name: []const u8, chatty_ms: f64, batch_ms: f64, heard: usize) void {
    std.debug.print("{s:<18} {d:>12.2} {d:>12.2} {d:>8}\n", .{ name, chatty_ms, batch_ms, heard });
}

fn printRow2(name: []const u8, batch_ms: f64, heard: usize) void {
    std.debug.print("{s:<18} {d:>12.2} {d:>8}\n", .{ name, batch_ms, heard });
}

fn printEnvelope(gpa: std.mem.Allocator, payload: []const u8) !void {
    var aw: std.Io.Writer.Allocating = .init(gpa);
    defer aw.deinit();
    var s: std.json.Stringify = .{ .writer = &aw.writer };
    const msg: JsonlMsg = .{
        .from_pid = 88548,
        .from_start = 1,
        .from_session = "planner",
        .from_goal = "agent-inbox redesign",
        .to = "researcher",
        .ts_ms = 1_700_000_000_000,
        .text = payload,
        .from_user = false,
        .kind = "message",
    };
    try s.write(msg);
    try aw.writer.writeByte('\n');
    const line = aw.writer.buffered();
    const extra = @as(i64, @intCast(line.len)) - @as(i64, @intCast(payload.len));
    std.debug.print("== extra bytes / 64B text ==\n", .{});
    std.debug.print("accord frame     +{d}   (header; preface 8B once)\n", .{accord.header_size});
    std.debug.print("jsonl envelope   +{d}   (presence_chan fields + newline, line {d}B)\n\n", .{ extra, line.len });
}

fn printSocketMode(io: Io) !void {
    var listener = try accord.listenUnix(io, sock_path);
    defer {
        listener.deinit(io);
        Io.Dir.cwd().deleteFile(io, sock_path) catch {};
    }
    var tmp: [107:0]u8 = undefined;
    if (sock_path.len > tmp.len) return;
    @memcpy(tmp[0..sock_path.len], sock_path);
    tmp[sock_path.len] = 0;
    var st: std.c.Stat = undefined;
    if (std.c.stat(tmp[0..sock_path.len :0], &st) != 0) {
        std.debug.print("unix socket created (mode check skipped)\n\n", .{});
        return;
    }
    std.debug.print("unix socket mode {o}  (want 600)\n\n", .{st.mode & 0o777});
}

const Heard = struct { ms: f64, heard: usize };

fn nowNs(io: Io) i128 {
    return Io.Timestamp.now(io, .awake).nanoseconds;
}

fn msSince(io: Io, t0: i128) f64 {
    return @as(f64, @floatFromInt(nowNs(io) - t0)) / 1e6;
}

fn median3(a: f64, b: f64, c: f64) f64 {
    const lo = @min(a, @min(b, c));
    const hi = @max(a, @max(b, c));
    return a + b + c - lo - hi;
}

fn medianMs(
    comptime n: usize,
    bench: *const fn (Io, std.mem.Allocator, []const u8, usize) anyerror!f64,
    io: Io,
    gpa: std.mem.Allocator,
    payload: []const u8,
    count: usize,
) !f64 {
    _ = n;
    const a = try bench(io, gpa, payload, count);
    const b = try bench(io, gpa, payload, count);
    const c = try bench(io, gpa, payload, count);
    return median3(a, b, c);
}

fn medianHeard(
    comptime n: usize,
    bench: *const fn (Io, std.mem.Allocator, []const u8, usize) anyerror!Heard,
    io: Io,
    gpa: std.mem.Allocator,
    payload: []const u8,
    count: usize,
) !Heard {
    _ = n;
    const a = try bench(io, gpa, payload, count);
    const b = try bench(io, gpa, payload, count);
    const c = try bench(io, gpa, payload, count);
    return .{ .ms = median3(a.ms, b.ms, c.ms), .heard = b.heard };
}

fn medianHeardPair(
    comptime n: usize,
    bench: *const fn (*Pair, []const u8, usize) anyerror!Heard,
    pair: *Pair,
    payload: []const u8,
    count: usize,
) !Heard {
    _ = n;
    const a = try bench(pair, payload, count);
    const b = try bench(pair, payload, count);
    const c = try bench(pair, payload, count);
    return .{ .ms = median3(a.ms, b.ms, c.ms), .heard = b.heard };
}

fn medianMsPair(
    comptime n: usize,
    bench: *const fn (*Pair, []const u8, usize) anyerror!f64,
    pair: *Pair,
    payload: []const u8,
    count: usize,
) !f64 {
    _ = n;
    const a = try bench(pair, payload, count);
    const b = try bench(pair, payload, count);
    const c = try bench(pair, payload, count);
    return median3(a, b, c);
}

fn benchMailboxChatty(io: Io, gpa: std.mem.Allocator, payload: []const u8, count: usize) !f64 {
    var box = accord.Mailbox{ .gpa = gpa };
    defer box.deinit();
    const t0 = nowNs(io);
    var i: usize = 0;
    while (i < count) : (i += 1) {
        try box.post(.{ .kind = .msg, .seq = 1, .payload = payload, .stream = 1 });
        const f = box.take() orelse return error.Empty;
        f.deinit(gpa);
    }
    return msSince(io, t0);
}

fn benchMailboxBatch(io: Io, gpa: std.mem.Allocator, payload: []const u8, count: usize) !Heard {
    var box = accord.Mailbox{ .gpa = gpa };
    defer box.deinit();
    const t0 = nowNs(io);
    var i: usize = 0;
    while (i < count) : (i += 1) {
        try box.post(.{ .kind = .msg, .seq = 1, .payload = payload, .stream = 1 });
    }
    var heard: usize = 0;
    while (box.take()) |f| {
        f.deinit(gpa);
        heard += 1;
    }
    return .{ .ms = msSince(io, t0), .heard = heard };
}

fn benchUnixChattyOn(p: *Pair, payload: []const u8, count: usize) !f64 {
    const t0 = nowNs(p.io);
    var i: usize = 0;
    while (i < count) : (i += 1) {
        try p.client.send(1, .msg, .none, payload);
        const got = try p.server.recv(1);
        defer got.deinit(p.gpa);
        if (got.payload.len != payload.len) return error.BadPayload;
    }
    return msSince(p.io, t0);
}

fn benchUnixBatch(p: *Pair, payload: []const u8, count: usize) !Heard {
    const t0 = nowNs(p.io);
    // Inbox is 8 frames/stream; drain concurrently or a 400-deep push deadlocks.
    var rf = try Io.concurrent(p.io, recvN, .{ p, count });
    var i: usize = 0;
    while (i < count) : (i += 1) {
        try p.client.push(1, .msg, .none, payload);
    }
    try p.client.flush();
    const heard = try rf.await(p.io);
    return .{ .ms = msSince(p.io, t0), .heard = heard };
}

fn recvN(p: *Pair, count: usize) !usize {
    var heard: usize = 0;
    while (heard < count) : (heard += 1) {
        const got = try p.server.recv(1);
        got.deinit(p.gpa);
    }
    return heard;
}

const Pair = struct {
    io: Io,
    gpa: std.mem.Allocator,
    listener: net.Server,
    server: accord.Session,
    client: accord.Session,

    fn init(p: *Pair, io: Io, gpa: std.mem.Allocator) !void {
        Io.Dir.cwd().deleteFile(io, sock_path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        var listener = try accord.listenUnix(io, sock_path);
        errdefer listener.deinit(io);
        var cfut = try Io.concurrent(io, connectPath, .{ io, sock_path });
        const server_sock = try listener.accept(io);
        const client_sock = try cfut.await(io);
        p.* = .{
            .io = io,
            .gpa = gpa,
            .listener = listener,
            .server = .{ .io = io, .gpa = gpa, .role = .server, .stream = server_sock },
            .client = .{ .io = io, .gpa = gpa, .role = .client, .stream = client_sock },
        };
        var cstart = try Io.concurrent(io, startSess, .{&p.client});
        try p.server.start();
        try cstart.await(io);
    }

    fn deinit(p: *Pair) void {
        p.client.shutdown();
        p.server.shutdown();
        p.listener.deinit(p.io);
        Io.Dir.cwd().deleteFile(p.io, sock_path) catch {};
    }
};

fn connectPath(io: Io, path: []const u8) !net.Stream {
    const addr = try net.UnixAddress.init(path);
    return addr.connect(io);
}

fn startSess(s: *accord.Session) !void {
    try s.start();
}

fn sampleMsg(payload: []const u8) JsonlMsg {
    return .{
        .from_pid = 88548,
        .from_start = 1,
        .from_session = "planner",
        .from_goal = "agent-inbox redesign",
        .to = "researcher",
        .ts_ms = 1_700_000_000_000,
        .text = payload,
        .from_user = false,
        .kind = "message",
    };
}

fn jsonlPost(io: Io, arena: std.mem.Allocator, dir: Io.Dir, msg: JsonlMsg) bool {
    var aw: std.Io.Writer.Allocating = .init(arena);
    var s: std.json.Stringify = .{ .writer = &aw.writer };
    s.write(msg) catch return false;
    aw.writer.writeAll("\n") catch return false;
    const f = dir.createFile(io, jsonl_name, .{ .truncate = false, .read = true }) catch return false;
    defer f.close(io);
    const end: u64 = if (f.stat(io)) |st| st.size else |_| blk: {
        const st = dir.statFile(io, jsonl_name, .{}) catch return false;
        break :blk st.size;
    };
    f.writePositionalAll(io, aw.writer.buffered(), end) catch return false;
    return true;
}

fn jsonlDrain(io: Io, arena: std.mem.Allocator, dir: Io.Dir, offset: *u64) []const JsonlMsg {
    const text = dir.readFileAlloc(io, jsonl_name, arena, .limited(jsonl_cap)) catch return &.{};
    if (text.len < offset.*) offset.* = 0;
    if (text.len == offset.*) return &.{};
    var msgs: std.ArrayList(JsonlMsg) = .empty;
    var pos: usize = @intCast(offset.*);
    while (std.mem.indexOfScalarPos(u8, text, pos, '\n')) |nl| {
        const line = text[pos..nl];
        pos = nl + 1;
        const m = std.json.parseFromSliceLeaky(JsonlMsg, arena, line, .{ .ignore_unknown_fields = true }) catch continue;
        if (m.from_pid == 0 or m.text.len == 0) continue;
        msgs.append(arena, m) catch break;
    }
    offset.* = @intCast(pos);
    return msgs.items;
}

fn resetJsonl(io: Io) void {
    Io.Dir.cwd().deleteFile(io, jsonl_name) catch {};
}

fn benchJsonlChatty(io: Io, gpa: std.mem.Allocator, payload: []const u8, count: usize) !f64 {
    resetJsonl(io);
    defer resetJsonl(io);
    const dir = Io.Dir.cwd();
    const msg = sampleMsg(payload);
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const t0 = nowNs(io);
    var off: u64 = 0;
    var i: usize = 0;
    while (i < count) : (i += 1) {
        _ = arena_state.reset(.retain_capacity);
        const arena = arena_state.allocator();
        if (!jsonlPost(io, arena, dir, msg)) return error.JsonlPost;
        const got = jsonlDrain(io, arena, dir, &off);
        if (got.len != 1) return error.JsonlMiss;
    }
    return msSince(io, t0);
}

fn benchJsonlBatch(io: Io, gpa: std.mem.Allocator, payload: []const u8, count: usize) !Heard {
    resetJsonl(io);
    defer resetJsonl(io);
    const dir = Io.Dir.cwd();
    const msg = sampleMsg(payload);
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const t0 = nowNs(io);
    var i: usize = 0;
    while (i < count) : (i += 1) {
        _ = arena_state.reset(.retain_capacity);
        if (!jsonlPost(io, arena_state.allocator(), dir, msg)) return error.JsonlPost;
    }
    _ = arena_state.reset(.retain_capacity);
    var off: u64 = 0;
    const got = jsonlDrain(io, arena_state.allocator(), dir, &off);
    return .{ .ms = msSince(io, t0), .heard = got.len };
}


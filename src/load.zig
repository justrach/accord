//! Concurrent-request load + RSS.
//!   zig build load

const std = @import("std");
const builtin = @import("builtin");
const accord = @import("accord");
const Io = std.Io;
const net = std.Io.net;

const sock_path = "accord-load.sock";

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;

    const rss0 = currentRss();
    std.debug.print("Accord load + RSS  ({s})\n", .{@tagName(builtin.mode)});
    std.debug.print("baseline RSS {d:.1} MB  maxrss {d:.1} MB\n\n", .{ mb(rss0), mb(maxRss()) });
    printBudget();

    std.debug.print("== Concurrent connections (sessions held live, then 64B req/resp) ==\n", .{});
    std.debug.print("{s:>6} {s:>8} {s:>8} {s:>10} {s:>10} {s:>10} {s:>10}\n", .{
        "conns", "ms", "p99 us", "RSS MB", "dRSS MB", "per-conn", "vs struct",
    });

    const sess_sz = @sizeOf(accord.Session);
    var last_conns: ConnResult = .{ .ns = 0, .p99 = 0, .rss_live = rss0, .rss_delta = 0 };
    for ([_]usize{ 1, 8, 32, 64, 128 }) |n| {
        const r = benchConns(io, gpa, n) catch |err| {
            std.debug.print("{d:>6}  failed: {s}\n", .{ n, @errorName(err) });
            continue;
        };
        last_conns = r;
        const per = if (n == 0) 0 else r.rss_delta / n;
        const est = sess_sz * n * 2; // client+server
        std.debug.print("{d:>6} {d:>8.1} {d:>8.0} {d:>10.1} {d:>10.2} {d:>10} {d:>9.1}x\n", .{
            n,
            @as(f64, @floatFromInt(r.ns)) / 1e6,
            @as(f64, @floatFromInt(r.p99)) / 1e3,
            mb(r.rss_live),
            mb(r.rss_delta),
            per,
            @as(f64, @floatFromInt(r.rss_delta)) / @as(f64, @floatFromInt(est)),
        });
    }

    std.debug.print("\n== 8000 units, three shapes ==\n", .{});
    std.debug.print("{s:<22} {s:>8} {s:>10} {s:>10} {s:>12}\n", .{ "shape", "ms", "RSS MB", "dRSS MB", "notes" });

    const pipe = try benchPipeline(io, 8000);
    std.debug.print("{s:<22} {d:>8.1} {d:>10.1} {d:>10.2} {s:>12}\n", .{
        "pipeline 1 conn",
        @as(f64, @floatFromInt(pipe.ns)) / 1e6,
        mb(pipe.rss_live),
        mb(pipe.rss_delta),
        "7.4M/s class",
    });

    const box = try benchMailbox(io, gpa, 8000);
    std.debug.print("{s:<22} {d:>8.1} {d:>10.1} {d:>10.2} {s:>12}\n", .{
        "mailbox 8000 posts",
        @as(f64, @floatFromInt(box.ns)) / 1e6,
        mb(box.rss_live),
        mb(box.rss_delta),
        "no socket",
    });

    std.debug.print("{s:<22} {s:>8} {d:>10.1} {s:>10} {s:>12}\n", .{
        "128 live sessions",
        "—",
        mb(last_conns.rss_live),
        "meas.",
        "held conns",
    });

    std.debug.print("\n== Compare @ 8000 concurrent connections (extrapolated) ==\n", .{});
    const per128 = if (last_conns.rss_delta > 0)
        last_conns.rss_delta / 128
    else
        @sizeOf(accord.Session) * 2;
    const rss_8k = per128 * 8000;
    std.debug.print("measured dRSS/conn (from 128): {d} bytes\n", .{per128});
    std.debug.print("struct-only estimate 8000x2:   {d:.1} MB\n", .{mb(@sizeOf(accord.Session) * 8000 * 2)});
    std.debug.print("RSS-scaled 8000 conns:         {d:.1} MB  (includes thread stacks)\n", .{mb(rss_8k)});
    std.debug.print("pipeline 8000 msgs dRSS:       {d:.2} MB\n", .{mb(pipe.rss_delta)});
    std.debug.print("mailbox 8000 dRSS:             {d:.2} MB\n", .{mb(box.rss_delta)});
    std.debug.print("process maxrss:                {d:.1} MB\n", .{mb(maxRss())});
    std.debug.print("\nThread stacks dominate Session structs once Io.concurrent=thread.\n", .{});
}

fn printBudget() void {
    std.debug.print("sizeof(Session) = {d} B\n", .{@sizeOf(accord.Session)});
    std.debug.print("max_streams={d}  inbox=8  fd ulimit~10240\n\n", .{accord.max_streams});
}

fn mb(n: u64) f64 {
    return @as(f64, @floatFromInt(n)) / (1024.0 * 1024.0);
}

fn currentRss() u64 {
    return switch (builtin.os.tag) {
        .macos, .ios, .tvos, .watchos, .visionos => darwinRss(),
        .linux => linuxRss(),
        else => maxRss(),
    };
}

fn maxRss() u64 {
    const u = std.posix.getrusage(0);
    const v: u64 = @intCast(@max(u.maxrss, 0));
    return switch (builtin.os.tag) {
        .linux => v * 1024,
        else => v,
    };
}

const MachBasicInfo = extern struct {
    virtual_size: u64,
    resident_size: u64,
    user_seconds: i32,
    user_microseconds: i32,
    system_seconds: i32,
    system_microseconds: i32,
    policy: i32,
    suspend_count: u64,
};

extern "c" fn task_info(target: c_uint, flavor: c_int, info: *MachBasicInfo, cnt: *c_uint) c_int;
extern "c" var mach_task_self_: c_uint;

fn darwinRss() u64 {
    var info: MachBasicInfo = undefined;
    var count: c_uint = @intCast(@sizeOf(MachBasicInfo) / @sizeOf(c_int));
    const kr = task_info(mach_task_self_, 20, &info, &count);
    if (kr != 0) return 0;
    return info.resident_size;
}

fn linuxRss() u64 {
    const f = std.fs.openFileAbsolute("/proc/self/statm", .{}) catch return 0;
    defer f.close();
    var buf: [64]u8 = undefined;
    const n = f.read(&buf) catch return 0;
    var it = std.mem.tokenizeScalar(u8, buf[0..n], ' ');
    _ = it.next();
    const pages = std.fmt.parseInt(u64, it.next() orelse "0", 10) catch 0;
    const page = std.heap.pageSize();
    return pages * page;
}

fn nowNs(io: Io) i64 {
    return @intCast(Io.Timestamp.now(io, .awake).nanoseconds);
}

const ConnResult = struct { ns: i64, p99: i64, rss_live: u64, rss_delta: u64 };
const Hold = struct {
    ready: std.atomic.Value(u32) = .init(0),
    go: std.atomic.Value(u32) = .init(0),
};

fn benchConns(io: Io, gpa: std.mem.Allocator, n: usize) !ConnResult {
    const rss_before = currentRss();
    var listener = try accord.listenUnix(io, sock_path);
    defer {
        listener.deinit(io);
        Io.Dir.cwd().deleteFile(io, sock_path) catch {};
    }

    const lat = try gpa.alloc(i64, n);
    defer gpa.free(lat);
    var hold: Hold = .{};

    var clients: std.ArrayList(Io.Future(anyerror!void)) = .empty;
    defer clients.deinit(gpa);
    try clients.ensureTotalCapacity(gpa, n);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        clients.appendAssumeCapacity(try Io.concurrent(io, oneClient, .{ io, gpa, sock_path, &lat[i], &hold }));
    }

    var servers: std.ArrayList(Io.Future(anyerror!void)) = .empty;
    defer servers.deinit(gpa);
    try servers.ensureTotalCapacity(gpa, n);
    i = 0;
    while (i < n) : (i += 1) {
        const sock = try listener.accept(io);
        servers.appendAssumeCapacity(try Io.concurrent(io, oneServer, .{ io, gpa, sock }));
    }

    while (hold.ready.load(.acquire) < n) {
        Io.sleep(io, Io.Duration.fromMicroseconds(50), .awake) catch {};
    }
    const rss_live = currentRss();
    hold.go.store(1, .release);

    const t0 = nowNs(io);
    for (clients.items) |*f| try f.await(io);
    const t1 = nowNs(io);
    for (servers.items) |*f| try f.await(io);

    std.sort.heap(i64, lat, {}, ascI64);
    const delta = if (rss_live > rss_before) rss_live - rss_before else 0;
    return .{ .ns = t1 - t0, .p99 = lat[lat.len * 99 / 100], .rss_live = rss_live, .rss_delta = delta };
}

fn oneClient(io: Io, gpa: std.mem.Allocator, path: []const u8, lat: *i64, hold: *Hold) anyerror!void {
    const addr = try net.UnixAddress.init(path);
    const sock = try addr.connect(io);
    var sess: accord.Session = .{ .io = io, .gpa = gpa, .role = .client, .stream = sock };
    try sess.start();
    defer sess.shutdown();
    const sid = try sess.open();
    _ = hold.ready.fetchAdd(1, .acq_rel);
    while (hold.go.load(.acquire) == 0) {
        Io.sleep(io, Io.Duration.fromMicroseconds(50), .awake) catch {};
    }
    var payload: [64]u8 = undefined;
    @memset(&payload, 'x');
    const t0 = nowNs(io);
    try sess.send(sid, .msg, .none, &payload);
    const got = try sess.recv(sid);
    defer got.deinit(gpa);
    lat.* = nowNs(io) - t0;
}

fn oneServer(io: Io, gpa: std.mem.Allocator, sock: net.Stream) anyerror!void {
    var sess: accord.Session = .{ .io = io, .gpa = gpa, .role = .server, .stream = sock };
    try sess.start();
    defer sess.shutdown();
    const got = try sess.recv(1);
    defer got.deinit(gpa);
    try sess.send(1, .msg, .none, got.payload);
}

const MemRun = struct { ns: i64, rss_live: u64, rss_delta: u64 };

fn benchPipeline(io: Io, n: usize) !MemRun {
    const rss_before = currentRss();
    var listener = try accord.listenUnix(io, sock_path);
    defer {
        listener.deinit(io);
        Io.Dir.cwd().deleteFile(io, sock_path) catch {};
    }

    var client_fut = try Io.concurrent(io, pipeClient, .{ io, n });
    const sock = try listener.accept(io);

    var rbuf: [8192]u8 = undefined;
    var wbuf: [8192]u8 = undefined;
    var reader = sock.reader(io, &rbuf);
    var writer = sock.writer(io, &wbuf);

    var pre: [accord.preface_size]u8 = undefined;
    accord.encodePreface(&pre);
    try writer.interface.writeAll(&pre);
    try writer.interface.flush();
    const gotp = try reader.interface.takeArray(accord.preface_size);
    _ = try accord.decodePreface(gotp);

    var body: [64]u8 = undefined;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        _ = try readAccord(&reader.interface, &body);
    }
    const rss_live = currentRss();
    try accord.writeFrame(&writer.interface, 1, .ack, .none, "ok");
    try writer.interface.flush();
    const ns = try client_fut.await(io);
    sock.close(io);
    const delta = if (rss_live > rss_before) rss_live - rss_before else 0;
    return .{ .ns = ns, .rss_live = rss_live, .rss_delta = delta };
}

fn pipeClient(io: Io, n: usize) anyerror!i64 {
    const addr = try net.UnixAddress.init(sock_path);
    const sock = try addr.connect(io);
    defer sock.close(io);
    var rbuf: [8192]u8 = undefined;
    var wbuf: [8192]u8 = undefined;
    var reader = sock.reader(io, &rbuf);
    var writer = sock.writer(io, &wbuf);
    var pre: [accord.preface_size]u8 = undefined;
    accord.encodePreface(&pre);
    try writer.interface.writeAll(&pre);
    try writer.interface.flush();
    const got = try reader.interface.takeArray(accord.preface_size);
    _ = try accord.decodePreface(got);
    var payload: [64]u8 = undefined;
    @memset(&payload, 'x');
    const t0 = nowNs(io);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        try accord.writeFrame(&writer.interface, 1, .msg, .none, &payload);
    }
    try writer.interface.flush();
    _ = try readAccord(&reader.interface, &payload);
    return nowNs(io) - t0;
}

fn benchMailbox(io: Io, gpa: std.mem.Allocator, n: usize) !MemRun {
    const rss_before = currentRss();
    var box: accord.Mailbox = .{ .gpa = gpa, .max_frames = n + 8 };
    defer box.deinit();
    var payload: [64]u8 = undefined;
    @memset(&payload, 'x');
    const t0 = nowNs(io);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        try box.post(.{ .kind = .msg, .seq = @truncate(i), .payload = &payload });
    }
    const rss_live = currentRss();
    const t1 = nowNs(io);
    while (box.take()) |f| f.deinit(gpa);
    const delta = if (rss_live > rss_before) rss_live - rss_before else 0;
    return .{ .ns = t1 - t0, .rss_live = rss_live, .rss_delta = delta };
}

fn readAccord(r: *Io.Reader, body: []u8) !accord.Kind {
    const hdr = try r.takeArray(accord.header_size);
    const h = try accord.decodeHeader(hdr);
    if (h.len > body.len) return error.BufferTooSmall;
    if (h.len > 0) try r.readSliceAll(body[0..h.len]);
    return h.kind;
}

fn ascI64(_: void, a: i64, b: i64) bool {
    return a < b;
}

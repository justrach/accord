const std = @import("std");
const accord = @import("accord");
const Io = std.Io;
const net = std.Io.net;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;

    try demoMailbox(gpa);
    try demoLive(io, gpa);
}

fn demoMailbox(gpa: std.mem.Allocator) !void {
    var box: accord.Mailbox = .{ .gpa = gpa };
    defer box.deinit();

    try box.post(.{ .kind = .progress, .flags = .replaceable, .seq = 1, .payload = "thinking" });
    try box.post(.{ .kind = .progress, .flags = .replaceable, .seq = 2, .payload = "writing" });
    try box.post(.{ .kind = .msg, .seq = 3, .payload = "landed" });
    try box.post(.{ .kind = .stop, .flags = .urgent, .seq = 4, .payload = &.{} });

    std.debug.print("mailbox (in-process):\n", .{});
    while (box.take()) |frame| {
        defer frame.deinit(gpa);
        std.debug.print("  {s} seq={d} payload=\"{s}\"\n", .{
            @tagName(frame.kind),
            frame.seq,
            frame.payload,
        });
    }
}

fn demoLive(io: Io, gpa: std.mem.Allocator) !void {
    const path = "accord-lite.sock";
    var listener = try accord.listenUnix(io, path);
    defer {
        listener.deinit(io);
        Io.Dir.cwd().deleteFile(io, path) catch {};
    }

    var client_fut = try Io.concurrent(io, liveClient, .{ io, gpa, path });
    const sock = try listener.accept(io);

    var sess: accord.Session = .{ .io = io, .gpa = gpa, .role = .server, .stream = sock };
    try sess.start();
    defer sess.shutdown();

    std.debug.print("live session (always-on, bidi):\n", .{});

    try sess.send(1, .msg, .none, "from-server");
    const first = try sess.recv(1);
    defer first.deinit(gpa);
    std.debug.print("  stream {d} recv \"{s}\"\n", .{ first.stream, first.payload });

    try sess.ping();
    const burst_n: usize = 32;
    var i: usize = 0;
    while (i < burst_n) : (i += 1) {
        const got = try sess.recv(1);
        defer got.deinit(gpa);
    }
    std.debug.print("  keepalive ping + {d} pipelined msgs on stream 1\n", .{burst_n});

    const on_three = try sess.recv(3);
    defer on_three.deinit(gpa);
    std.debug.print("  stream {d} recv \"{s}\" (same connection)\n", .{ on_three.stream, on_three.payload });
    try sess.send(3, .msg, .none, "still-here");

    try client_fut.await(io);
    std.debug.print("  server flushes={d} frames_out={d}\n", .{
        sess.flushes.load(.monotonic),
        sess.frames_out.load(.monotonic),
    });
}

fn liveClient(io: Io, gpa: std.mem.Allocator, path: []const u8) !void {
    const addr = try net.UnixAddress.init(path);
    const sock = try addr.connect(io);
    var sess: accord.Session = .{ .io = io, .gpa = gpa, .role = .client, .stream = sock };
    try sess.start();
    defer sess.shutdown();

    const one = try sess.open();
    try sess.send(one, .msg, .none, "from-client");
    const reply = try sess.recv(one);
    defer reply.deinit(gpa);

    try sess.ping();
    var i: usize = 0;
    while (i < 32) : (i += 1) {
        try sess.push(one, .msg, .none, "x");
    }
    try sess.flush();

    const three = try sess.open();
    try sess.send(three, .msg, .none, "stream-3");
    const reused = try sess.recv(three);
    defer reused.deinit(gpa);

    std.debug.print("  client flushes={d} frames_out={d} (batching: fewer flushes than frames)\n", .{
        sess.flushes.load(.monotonic),
        sess.frames_out.load(.monotonic),
    });
}

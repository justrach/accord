//! Head-to-head evals on one Unix socket, same payload, same process.
//!
//! Local Unix bakeoff against a gRPC DATA-path stand-in (9+5+kind, no HPACK,
//! no WINDOW_UPDATE). WAN/TLS/HPACK would dominate on the real internet.
//! Pipeline trials are interleaved medians of 5. Duplex flood is both peers
//! writing and reading at once — the realtime-link case.
//!
//! Protocols:
//!   accord        4-byte frame (this repo)
//!   len32         u32 length + kind + payload
//!   json          u32 length + {"k":N,"p":"..."}
//!   http11        persistent HTTP/1.1 Content-Length
//!   grpc-stream   HTTP/2 DATA (9) + gRPC prefix (5)  [no per-msg HEADERS]
//!   grpc-unary    HEADERS + DATA + HEADERS per message
//!
//! Run: `zig build eval`

const std = @import("std");
const accord = @import("accord");
const Io = std.Io;
const net = std.Io.net;

const sock_path = "accord-eval.sock";
const warmup_n: usize = 200;
const pipe_n: usize = 20_000;
const duplex_n: usize = 10_000;
const ping_n: usize = 2_000;
const flood_n: usize = 2_000;
const payload_sizes = [_]usize{ 64, 1024 };

const Proto = enum {
    accord,
    len32,
    json,
    http11,
    grpc_stream,
    grpc_unary,

    fn name(p: Proto) []const u8 {
        return switch (p) {
            .accord => "accord",
            .len32 => "len32",
            .json => "json",
            .http11 => "http/1.1",
            .grpc_stream => "grpc-stream",
            .grpc_unary => "grpc-unary",
        };
    }
};

const all_protos = [_]Proto{ .accord, .len32, .json, .http11, .grpc_stream, .grpc_unary };

const kind_msg: u8 = 2;
const kind_ack: u8 = 3;
const kind_progress: u8 = 4;
const kind_stop: u8 = 5;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;

    std.debug.print("Accord evals  (unix socket, same process, {s})\n", .{@tagName(builtinMode())});
    std.debug.print("guarantee: persistent stream, in-order delivery, no durability\n\n", .{});

    try printWireTable();
    printAnatomy();
    try benchPipeline(io, gpa);
    try benchDuplex(io, gpa);
    try benchPingPong(io, gpa);
    try benchStopUnderLoad(io, gpa);
    try benchCoalesce(io, gpa);
    try benchMailbox(io, gpa);

    std.debug.print("\nNotes:\n", .{});
    std.debug.print("- grpc-stream is the bidi DATA path only (no HPACK HEADERS per message).\n", .{});
    std.debug.print("- grpc-unary adds empty HEADERS+trailers per message (closer to unary RPC).\n", .{});
    std.debug.print("- http/1.1 is persistent; a new TCP handshake per RPC would be much slower.\n", .{});
    std.debug.print("- WAN/TLS/QUIC not measured; they sit under this framing.\n", .{});
    std.debug.print("- Native Accord is not a drop-in for a gRPC stub: both ends run this codec,\n", .{});
    std.debug.print("  or one end is a gateway that translates.\n", .{});
}

fn builtinMode() std.builtin.OptimizeMode {
    return @import("builtin").mode;
}

fn nowNs(io: Io) i64 {
    return @intCast(Io.Timestamp.now(io, .awake).nanoseconds);
}

fn fillPayload(buf: []u8) void {
    @memset(buf, 'x');
}

fn printWireTable() !void {
    std.debug.print("== Wire size (1000 messages) ==\n", .{});
    std.debug.print("{s:<14} {s:>8} {s:>10} {s:>10} {s:>12}\n", .{ "protocol", "pay", "bytes", "overhd/msg", "ratio" });

    var payload: [1024]u8 = undefined;
    fillPayload(&payload);
    const counts = [_]usize{ 64, 1024 };
    for (counts) |n| {
        for (all_protos) |p| {
            const bytes = wireBytes(p, kind_msg, payload[0..n]) * 1000;
            const total_payload = 1000 * n;
            const over: i64 = @as(i64, @intCast(bytes)) - @as(i64, @intCast(total_payload));
            const per_msg = @divTrunc(over, 1000);
            const ratio = @as(f64, @floatFromInt(bytes)) / @as(f64, @floatFromInt(total_payload));
            std.debug.print("{s:<14} {d:>8} {d:>10} {d:>10} {d:>11.2}\n", .{
                p.name(), n, bytes, per_msg, ratio,
            });
        }
    }
    std.debug.print("\n", .{});
}

fn printAnatomy() void {
    std.debug.print("== Where the extra bytes are (64B payload) ==\n", .{});
    std.debug.print("accord       4B  [len:2][stream:1][kind:4b flags:4b]          +64 = 68\n", .{});
    std.debug.print("             seq is NOT on the wire (TCP/unix already orders)\n", .{});
    std.debug.print("len32        5B  [len:4][kind:1]                             +64 = 69\n", .{});
    std.debug.print("json        16B  [len:4] + {{\"k\":N,\"p\":\"...\"}} wrapper       +64 = 80\n", .{});
    std.debug.print("grpc-stream 15B  [HTTP/2 DATA:9][grpc prefix:5][kind:1]      +64 = 79\n", .{});
    std.debug.print("grpc-unary  33B  [HEADERS:9][DATA:9][trailers:9][grpc:5][k:1]+64 = 97\n", .{});
    std.debug.print("http/1.1    50B  ASCII request line + Content-Length + X-Kind +64 = 114\n", .{});
    std.debug.print("\nBoth peers on a native socket must speak Accord (same as gRPC).\n", .{});
    std.debug.print("HTTP/MCP clients go through a gateway; Mailbox is in-process only.\n\n", .{});
}

fn wireBytes(p: Proto, kind: u8, payload: []const u8) u64 {
    return switch (p) {
        .accord => accord.header_size + payload.len,
        .len32 => 4 + 1 + payload.len,
        .json => 4 + jsonLen(kind, payload),
        .http11 => httpHeaderLen(kind, payload.len) + payload.len,
        .grpc_stream => 9 + 5 + 1 + payload.len,
        .grpc_unary => 9 + 9 + 9 + 5 + 1 + payload.len,
    };
}

fn jsonLen(kind: u8, payload: []const u8) u64 {
    _ = kind;
    // {"k":N,"p":"..."}
    return 10 + payload.len + 2;
}

fn httpHeaderLen(kind: u8, payload_len: usize) u64 {
    var buf: [96]u8 = undefined;
    const h = std.fmt.bufPrint(&buf, "POST / HTTP/1.1\r\nContent-Length: {d}\r\nX-Kind: {d}\r\n\r\n", .{
        payload_len, kind,
    }) catch unreachable;
    return h.len;
}

fn writeFrame(w: *Io.Writer, p: Proto, kind: u8, payload: []const u8) !void {
    switch (p) {
        .accord => {
            const k: accord.Kind = @enumFromInt(kind);
            const flags: accord.Flags = if (kind == kind_stop) .urgent else .none;
            try accord.writeFrame(w, 1, k, flags, payload);
        },
        .len32 => {
            var lenb: [4]u8 = undefined;
            std.mem.writeInt(u32, &lenb, @intCast(1 + payload.len), .little);
            try w.writeAll(&lenb);
            try w.writeByte(kind);
            try w.writeAll(payload);
        },
        .json => {
            var jbuf: [accord.max_payload + 32]u8 = undefined;
            const json = try std.fmt.bufPrint(&jbuf, "{{\"k\":{d},\"p\":\"{s}\"}}", .{ kind, payload });
            var lenb: [4]u8 = undefined;
            std.mem.writeInt(u32, &lenb, @intCast(json.len), .little);
            try w.writeAll(&lenb);
            try w.writeAll(json);
        },
        .http11 => {
            var hbuf: [96]u8 = undefined;
            const h = try std.fmt.bufPrint(&hbuf, "POST / HTTP/1.1\r\nContent-Length: {d}\r\nX-Kind: {d}\r\n\r\n", .{
                payload.len, kind,
            });
            try w.writeAll(h);
            try w.writeAll(payload);
        },
        .grpc_stream => {
            try writeH2Data(w, 1, kind, payload);
        },
        .grpc_unary => {
            try writeH2Headers(w, 1, false);
            try writeH2Data(w, 1, kind, payload);
            try writeH2Headers(w, 1, true);
        },
    }
}

fn writeH2Headers(w: *Io.Writer, stream_id: u32, end: bool) !void {
    var hdr: [9]u8 = .{ 0, 0, 0, 1, 0, 0, 0, 0, 0 };
    hdr[4] = if (end) 5 else 4; // END_HEADERS [| END_STREAM]
    std.mem.writeInt(u32, hdr[5..9], stream_id, .big);
    try w.writeAll(&hdr);
}

fn writeH2Data(w: *Io.Writer, stream_id: u32, kind: u8, payload: []const u8) !void {
    const msg_len: u32 = @intCast(1 + payload.len);
    const inner: u32 = 5 + msg_len;
    const total = 9 + 5 + 1 + payload.len;
    if (total <= w.buffer.len) {
        const dest = try w.writableSlice(total);
        dest[0] = @intCast((inner >> 16) & 0xff);
        dest[1] = @intCast((inner >> 8) & 0xff);
        dest[2] = @intCast(inner & 0xff);
        dest[3] = 0;
        dest[4] = 0;
        std.mem.writeInt(u32, dest[5..9], stream_id, .big);
        dest[9] = 0;
        std.mem.writeInt(u32, dest[10..14], msg_len, .big);
        dest[14] = kind;
        if (payload.len != 0) @memcpy(dest[15..], payload);
        return;
    }
    var hdr: [9]u8 = undefined;
    hdr[0] = @intCast((inner >> 16) & 0xff);
    hdr[1] = @intCast((inner >> 8) & 0xff);
    hdr[2] = @intCast(inner & 0xff);
    hdr[3] = 0;
    hdr[4] = 0;
    std.mem.writeInt(u32, hdr[5..9], stream_id, .big);
    var grpc: [5]u8 = undefined;
    grpc[0] = 0;
    std.mem.writeInt(u32, grpc[1..5], msg_len, .big);
    try w.writeAll(&hdr);
    try w.writeAll(&grpc);
    try w.writeByte(kind);
    try w.writeAll(payload);
}

fn readFrame(r: *Io.Reader, p: Proto, payload_out: []u8) !u8 {
    switch (p) {
        .accord => {
            const h = try accord.readFrame(r, payload_out);
            return @intFromEnum(h.kind);
        },
        .len32 => {
            const lb = try r.takeArray(4);
            const n = std.mem.readInt(u32, lb, .little);
            if (n == 0 or n - 1 > payload_out.len) return error.BufferTooSmall;
            const kind = try r.takeByte();
            try r.readSliceAll(payload_out[0 .. n - 1]);
            return kind;
        },
        .json => {
            const lb = try r.takeArray(4);
            const n = std.mem.readInt(u32, lb, .little);
            var jbuf: [accord.max_payload + 32]u8 = undefined;
            if (n > jbuf.len) return error.BufferTooSmall;
            try r.readSliceAll(jbuf[0..n]);
            if (n < 6) return error.BadJson;
            return jbuf[5] - '0';
        },
        .http11 => {
            var hdr: [256]u8 = undefined;
            var i: usize = 0;
            while (i < hdr.len) {
                hdr[i] = try r.takeByte();
                i += 1;
                if (i >= 4 and std.mem.eql(u8, hdr[i - 4 .. i], "\r\n\r\n")) break;
            }
            const kind = parseXKind(hdr[0..i]) orelse return error.BadHttp;
            const clen = parseContentLength(hdr[0..i]) orelse return error.BadHttp;
            if (clen > payload_out.len) return error.BufferTooSmall;
            if (clen > 0) try r.readSliceAll(payload_out[0..clen]);
            return kind;
        },
        .grpc_stream => return readH2Data(r, payload_out),
        .grpc_unary => {
            _ = try r.takeArray(9);
            const kind = try readH2Data(r, payload_out);
            _ = try r.takeArray(9);
            return kind;
        },
    }
}

fn readH2Data(r: *Io.Reader, payload_out: []u8) !u8 {
    const hdr = try r.takeArray(9);
    const inner: u32 = (@as(u32, hdr[0]) << 16) | (@as(u32, hdr[1]) << 8) | hdr[2];
    if (inner < 5) return error.BadGrpc;
    const grpc = try r.takeArray(5);
    const plen = std.mem.readInt(u32, grpc[1..5], .big);
    if (plen == 0) return error.BadGrpc;
    const kind = try r.takeByte();
    const body = plen - 1;
    if (body > payload_out.len) return error.BufferTooSmall;
    if (body > 0) try r.readSliceAll(payload_out[0..body]);
    return kind;
}

fn parseContentLength(hdr: []const u8) ?usize {
    const key = "Content-Length: ";
    const at = std.mem.indexOf(u8, hdr, key) orelse return null;
    const rest = hdr[at + key.len ..];
    const end = std.mem.indexOfScalar(u8, rest, '\r') orelse return null;
    return std.fmt.parseInt(usize, rest[0..end], 10) catch null;
}

fn parseXKind(hdr: []const u8) ?u8 {
    const key = "X-Kind: ";
    const at = std.mem.indexOf(u8, hdr, key) orelse return null;
    return hdr[at + key.len] - '0';
}

const Pair = struct {
    io: Io,
    listener: net.Server,
    client: net.Stream,
    server: net.Stream,

    fn deinit(p: *Pair) void {
        p.client.close(p.io);
        p.server.close(p.io);
        p.listener.deinit(p.io);
        Io.Dir.cwd().deleteFile(p.io, sock_path) catch {};
    }
};

fn connectPath(io: Io, path: []const u8) !net.Stream {
    const addr = try net.UnixAddress.init(path);
    return addr.connect(io);
}

fn openPair(io: Io) !Pair {
    Io.Dir.cwd().deleteFile(io, sock_path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    const addr = try net.UnixAddress.init(sock_path);
    var listener = try addr.listen(io, .{});
    errdefer listener.deinit(io);
    var cfut = try Io.concurrent(io, connectPath, .{ io, sock_path });
    const server = try listener.accept(io);
    const client = try cfut.await(io);
    return .{ .io = io, .listener = listener, .client = client, .server = server };
}

fn benchPipeline(io: Io, gpa: std.mem.Allocator) !void {
    _ = gpa;
    std.debug.print("== Pipeline ({d} msgs, one ack) ==\n", .{pipe_n});
    std.debug.print("{s:<14} {s:>8} {s:>10} {s:>12} {s:>10}\n", .{ "protocol", "pay", "ms", "msgs/s", "MB/s" });

    var payload_store: [1024]u8 = undefined;
    fillPayload(&payload_store);

    for (payload_sizes) |psz| {
        const payload = payload_store[0..psz];
        for (all_protos) |p| {
            _ = try runOnce(io, p, warmup_n, payload, false);
        }
        var samples: [all_protos.len][5]i64 = undefined;
        var trial: usize = 0;
        while (trial < 5) : (trial += 1) {
            for (all_protos, 0..) |p, i| {
                samples[i][trial] = try runOnce(io, p, pipe_n, payload, false);
            }
        }
        for (all_protos, 0..) |p, i| {
            std.sort.heap(i64, &samples[i], {}, ascI64);
            const ns = samples[i][2];
            const ms = @as(f64, @floatFromInt(ns)) / 1_000_000.0;
            const msgs = @as(f64, @floatFromInt(pipe_n)) / (ms / 1000.0);
            const bytes = wireBytes(p, kind_msg, payload) * pipe_n;
            const mbs = @as(f64, @floatFromInt(bytes)) / (ms / 1000.0) / (1024.0 * 1024.0);
            std.debug.print("{s:<14} {d:>8} {d:>10.2} {d:>12.0} {d:>10.1}\n", .{
                p.name(), psz, ms, msgs, mbs,
            });
        }
    }
    std.debug.print("\n", .{});
}

fn benchDuplex(io: Io, gpa: std.mem.Allocator) !void {
    _ = gpa;
    std.debug.print("== Duplex flood ({d} msgs each way, 64B) ==\n", .{duplex_n});
    std.debug.print("{s:<14} {s:>10} {s:>12} {s:>10}\n", .{ "protocol", "ms", "msgs/s", "MB/s" });

    var payload: [64]u8 = undefined;
    fillPayload(&payload);

    for (all_protos) |p| {
        _ = try runDuplex(io, p, 200, &payload);
        var samples: [3]i64 = undefined;
        for (&samples) |*s| s.* = try runDuplex(io, p, duplex_n, &payload);
        std.sort.heap(i64, &samples, {}, ascI64);
        const ns = samples[1];
        const ms = @as(f64, @floatFromInt(ns)) / 1_000_000.0;
        const msgs = @as(f64, @floatFromInt(duplex_n * 2)) / (ms / 1000.0);
        const bytes = wireBytes(p, kind_msg, &payload) * duplex_n * 2;
        const mbs = @as(f64, @floatFromInt(bytes)) / (ms / 1000.0) / (1024.0 * 1024.0);
        std.debug.print("{s:<14} {d:>10.2} {d:>12.0} {d:>10.1}\n", .{
            p.name(), ms, msgs, mbs,
        });
    }
    std.debug.print("\n", .{});
}

fn runDuplex(io: Io, p: Proto, n: usize, payload: []const u8) !i64 {
    var pair = try openPair(io);
    defer pair.deinit();
    var server_fut = try Io.concurrent(io, duplexPeer, .{ io, pair.server, p, n, payload });
    const t0 = nowNs(io);
    try duplexPeer(io, pair.client, p, n, payload);
    const dt = nowNs(io) - t0;
    try server_fut.await(io);
    return dt;
}

fn duplexPeer(io: Io, stream: net.Stream, p: Proto, n: usize, payload: []const u8) !void {
    var rbuf: [64 * 1024]u8 = undefined;
    var wbuf: [64 * 1024]u8 = undefined;
    var reader = stream.reader(io, &rbuf);
    var writer = stream.writer(io, &wbuf);
    var scratch: [2048]u8 = undefined;
    var read_fut = try Io.concurrent(io, duplexRead, .{ &reader.interface, p, n, &scratch });
    try writeMany(&writer.interface, p, n, payload);
    try writer.interface.flush();
    try read_fut.await(io);
}

fn duplexRead(r: *Io.Reader, p: Proto, n: usize, scratch: *[2048]u8) !void {
    try readMany(r, p, n, scratch);
}

fn writeMany(w: *Io.Writer, p: Proto, n: usize, payload: []const u8) !void {
    switch (p) {
        .accord => {
            var i: usize = 0;
            while (i < n) : (i += 1) {
                try accord.writeFrame(w, 1, .msg, .none, payload);
            }
        },
        .grpc_stream => {
            var i: usize = 0;
            while (i < n) : (i += 1) {
                try writeH2Data(w, 1, kind_msg, payload);
            }
        },
        else => {
            var i: usize = 0;
            while (i < n) : (i += 1) {
                try writeFrame(w, p, kind_msg, payload);
            }
        },
    }
}

fn readMany(r: *Io.Reader, p: Proto, n: usize, scratch: []u8) !void {
    switch (p) {
        .accord => {
            var i: usize = 0;
            while (i < n) : (i += 1) {
                _ = try accord.readFrame(r, scratch);
            }
        },
        .grpc_stream => {
            var i: usize = 0;
            while (i < n) : (i += 1) {
                _ = try readH2Data(r, scratch);
            }
        },
        else => {
            var i: usize = 0;
            while (i < n) : (i += 1) {
                _ = try readFrame(r, p, scratch);
            }
        },
    }
}

fn runOnce(io: Io, p: Proto, n: usize, payload: []const u8, pingpong: bool) !i64 {
    var pair = try openPair(io);
    defer pair.deinit();

    var server_fut = try Io.concurrent(io, pipelineServer, .{ io, pair.server, p, n, pingpong });

    var rbuf: [64 * 1024]u8 = undefined;
    var wbuf: [64 * 1024]u8 = undefined;
    var reader = pair.client.reader(io, &rbuf);
    var writer = pair.client.writer(io, &wbuf);
    var scratch: [2048]u8 = undefined;

    const t0 = nowNs(io);
    var i: usize = 0;
    if (pingpong) {
        while (i < n) : (i += 1) {
            try writeFrame(&writer.interface, p, kind_msg, payload);
            try writer.interface.flush();
            _ = try readFrame(&reader.interface, p, &scratch);
        }
    } else {
        try writeMany(&writer.interface, p, n, payload);
        try writer.interface.flush();
        _ = try readFrame(&reader.interface, p, &scratch);
    }
    const t1 = nowNs(io);
    try server_fut.await(io);
    return t1 - t0;
}

fn pipelineServer(io: Io, stream: net.Stream, p: Proto, n: usize, pingpong: bool) !void {
    var rbuf: [64 * 1024]u8 = undefined;
    var wbuf: [64 * 1024]u8 = undefined;
    var reader = stream.reader(io, &rbuf);
    var writer = stream.writer(io, &wbuf);
    var scratch: [2048]u8 = undefined;
    var i: usize = 0;
    if (pingpong) {
        while (i < n) : (i += 1) {
            _ = try readFrame(&reader.interface, p, &scratch);
            try writeFrame(&writer.interface, p, kind_ack, "ok");
            try writer.interface.flush();
        }
    } else {
        try readMany(&reader.interface, p, n, &scratch);
        i = n;
    }
    if (!pingpong) {
        try writeFrame(&writer.interface, p, kind_ack, "ok");
        try writer.interface.flush();
    }
}

fn benchPingPong(io: Io, gpa: std.mem.Allocator) !void {
    _ = gpa;
    std.debug.print("== Ping-pong RTT ({d} round-trips, 64B) ==\n", .{ping_n});
    std.debug.print("{s:<14} {s:>10} {s:>10} {s:>10}\n", .{ "protocol", "p50 us", "p99 us", "msgs/s" });

    var payload: [64]u8 = undefined;
    fillPayload(&payload);

    var lat: [ping_n]i64 = undefined;
    for (all_protos) |p| {
        _ = try runOnce(io, p, 100, &payload, true);
        try runPingLatencies(io, p, &payload, &lat);
        std.sort.heap(i64, &lat, {}, ascI64);
        const p50 = lat[lat.len * 50 / 100];
        const p99 = lat[lat.len * 99 / 100];
        var sum: i64 = 0;
        for (lat) |x| sum += x;
        const mean_s = @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(lat.len)) / 1e9;
        const msgs = 1.0 / mean_s;
        std.debug.print("{s:<14} {d:>10.1} {d:>10.1} {d:>10.0}\n", .{
            p.name(),
            @as(f64, @floatFromInt(p50)) / 1000.0,
            @as(f64, @floatFromInt(p99)) / 1000.0,
            msgs,
        });
    }
    std.debug.print("\n", .{});
}

fn runPingLatencies(io: Io, p: Proto, payload: []const u8, lat: []i64) !void {
    var pair = try openPair(io);
    defer pair.deinit();
    var server_fut = try Io.concurrent(io, pipelineServer, .{ io, pair.server, p, lat.len, true });

    var rbuf: [64 * 1024]u8 = undefined;
    var wbuf: [64 * 1024]u8 = undefined;
    var reader = pair.client.reader(io, &rbuf);
    var writer = pair.client.writer(io, &wbuf);
    var scratch: [2048]u8 = undefined;

    for (lat) |*slot| {
        const t0 = nowNs(io);
        try writeFrame(&writer.interface, p, kind_msg, payload);
        try writer.interface.flush();
        _ = try readFrame(&reader.interface, p, &scratch);
        slot.* = nowNs(io) - t0;
    }
    try server_fut.await(io);
}

fn benchStopUnderLoad(io: Io, gpa: std.mem.Allocator) !void {
    _ = gpa;
    std.debug.print("== Stop-under-load ({d} progress then stop, 64B) ==\n", .{flood_n});
    std.debug.print("{s:<14} {s:>10} {s:>10} {s:>12}\n", .{ "protocol", "p50 us", "p99 us", "bytes-before" });

    var payload: [64]u8 = undefined;
    fillPayload(&payload);
    const trials: usize = 40;
    var lat: [trials]i64 = undefined;

    for (all_protos) |p| {
        const before = wireBytes(p, kind_progress, &payload) * flood_n;
        var t: usize = 0;
        while (t < trials) : (t += 1) {
            lat[t] = try runStopTrial(io, p, &payload);
        }
        std.sort.heap(i64, &lat, {}, ascI64);
        const p50 = lat[lat.len * 50 / 100];
        const p99 = lat[lat.len * 99 / 100];
        std.debug.print("{s:<14} {d:>10.1} {d:>10.1} {d:>12}\n", .{
            p.name(),
            @as(f64, @floatFromInt(p50)) / 1000.0,
            @as(f64, @floatFromInt(p99)) / 1000.0,
            before,
        });
    }
    std.debug.print("\n", .{});
}

fn runStopTrial(io: Io, p: Proto, payload: []const u8) !i64 {
    var pair = try openPair(io);
    defer pair.deinit();
    var server_fut = try Io.concurrent(io, stopServer, .{ io, pair.server, p });

    var rbuf: [64 * 1024]u8 = undefined;
    var wbuf: [64 * 1024]u8 = undefined;
    var reader = pair.client.reader(io, &rbuf);
    var writer = pair.client.writer(io, &wbuf);
    var scratch: [2048]u8 = undefined;

    var i: usize = 0;
    while (i < flood_n) : (i += 1) {
        try writeFrame(&writer.interface, p, kind_progress, payload);
    }
    const t0 = nowNs(io);
    try writeFrame(&writer.interface, p, kind_stop, &.{});
    try writer.interface.flush();
    _ = try readFrame(&reader.interface, p, &scratch);
    const dt = nowNs(io) - t0;
    try server_fut.await(io);
    return dt;
}

fn stopServer(io: Io, stream: net.Stream, p: Proto) !void {
    var rbuf: [64 * 1024]u8 = undefined;
    var wbuf: [64 * 1024]u8 = undefined;
    var reader = stream.reader(io, &rbuf);
    var writer = stream.writer(io, &wbuf);
    var scratch: [2048]u8 = undefined;
    while (true) {
        const k = try readFrame(&reader.interface, p, &scratch);
        if (k == kind_stop) {
            try writeFrame(&writer.interface, p, kind_ack, "ok");
            try writer.interface.flush();
            return;
        }
    }
}

fn benchCoalesce(io: Io, gpa: std.mem.Allocator) !void {
    std.debug.print("== Coalesced progress (mailbox, {d} replaceable + stop) ==\n", .{flood_n});
    var box: accord.Mailbox = .{ .gpa = gpa, .max_frames = flood_n + 8 };
    defer box.deinit();

    const t0 = nowNs(io);
    var i: u16 = 1;
    while (i <= flood_n) : (i += 1) {
        try box.post(.{
            .kind = .progress,
            .flags = .replaceable,
            .seq = i,
            .payload = "working",
        });
    }
    try box.post(.{ .kind = .stop, .flags = .urgent, .seq = i, .payload = &.{} });
    const t1 = nowNs(io);

    var n: usize = 0;
    while (box.take()) |f| {
        n += 1;
        f.deinit(gpa);
    }
    std.debug.print("accord mailbox delivered {d} frames (expect 2) in {d}us\n", .{
        n, @divTrunc(t1 - t0, 1000),
    });
    std.debug.print("json/http/grpc without replace would deliver {d} frames\n\n", .{flood_n + 1});
}

fn benchMailbox(io: Io, gpa: std.mem.Allocator) !void {
    std.debug.print("== In-process mailbox ({d} durable msgs, 64B) ==\n", .{pipe_n});
    var box: accord.Mailbox = .{ .gpa = gpa, .max_frames = pipe_n + 8 };
    defer box.deinit();
    var payload: [64]u8 = undefined;
    fillPayload(&payload);

    const t0 = nowNs(io);
    var i: usize = 0;
    while (i < pipe_n) : (i += 1) {
        try box.post(.{ .kind = .msg, .seq = @truncate(i), .payload = &payload });
    }
    var n: usize = 0;
    while (box.take()) |f| {
        n += 1;
        f.deinit(gpa);
    }
    const t1 = nowNs(io);
    const ms: f64 = @as(f64, @floatFromInt(t1 - t0)) / 1_000_000.0;
    const msgs = @as(f64, @floatFromInt(n)) / (ms / 1000.0);
    std.debug.print("delivered {d} in {d:.2}ms ({d:.0} msgs/s) — no syscall, no framing\n", .{ n, ms, msgs });
}

fn ascI64(_: void, a: i64, b: i64) bool {
    return a < b;
}

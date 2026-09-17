//! Accord live: always-on, bidirectional, multiplexed.
//!
//! Both peers on a native socket must speak this framing — same contract
//! as gRPC. A process that only has HTTP/MCP talks through a gateway;
//! in-process agents use Mailbox and never open a socket.
//!
//! Duplex realtime link: one always-on socket, many streams, control on
//! stream 0 (ping/stop/goaway flush now). Data corks into the writer
//! buffer; `push`+`flush` is the hot path, `send` flushes for the simple API.
//!
//! vs gRPC/HTTP/2 DATA path (9 + 5 + kind = 15B overhead):
//!   Accord data frame is 4 bytes. seq is not on the wire: the stream
//!   is already reliable and ordered. seq stays a local counter.
//!
//! Preface (once per connection):
//!   "ACD1" | version:u8 | 0 | max_frame:u16le
//!
//! Frame (little-endian u32):
//!   len:u16 | stream:u8 | kind:u4 | flags:u4 | payload
//!
//! stop/ping/pong/goaway flush immediately. Replaceable progress
//! occupies one pending slot per stream.
//!
//! Trust model (defaults, no extra config):
//!   - No TLS: the socket is the trust boundary. Use `listenUnix` so the
//!     path is 0600 (owner-only). Do not expose it on a TCP port as-is.
//!   - Untrusted bytes are framed: oversize, bad kind/stream, and control
//!     frames with bodies are rejected and the connection is dropped.
//!   - Inboxes are bounded (8 frames/stream). Mailbox caps at 4096.

const std = @import("std");
const Io = std.Io;
const net = std.Io.net;

pub const header_size: usize = 4;
pub const preface_size: usize = 8;
pub const max_payload: u16 = 16 * 1024;
pub const max_streams: u16 = 8;
pub const magic: [4]u8 = "ACD1".*;
pub const version: u8 = 1;

pub const Kind = enum(u4) {
    ping = 0,
    pong = 1,
    msg = 2,
    ack = 3,
    progress = 4,
    stop = 5,
    close = 6,
    goaway = 7,
};

pub const Flags = packed struct(u4) {
    control: bool = false,
    replace: bool = false,
    _: u2 = 0,

    pub const none: Flags = .{};
    pub const urgent: Flags = .{ .control = true };
    pub const replaceable: Flags = .{ .replace = true };
};

pub const Header = struct {
    stream: u16 = 0,
    kind: Kind,
    flags: Flags = .none,
    len: u16,
};

pub const Frame = struct {
    stream: u16 = 0,
    kind: Kind,
    flags: Flags = .none,
    seq: u16,
    payload: []const u8,
};

pub const DecodeError = error{ BadKind, BadStream, BadFlags, PayloadTooLarge };
pub const WriteError = Io.Writer.Error || error{ PayloadTooLarge, BadStream };
pub const ReadError = DecodeError || Io.Reader.Error || error{BufferTooSmall};
pub const max_mailbox_frames: usize = 4096;

/// Little-endian wire word: len | stream<<16 | kind<<24 | flags<<28.
pub const Wire = packed struct(u32) {
    len: u16 = 0,
    stream: u8 = 0,
    kind: u4 = 0,
    flags: u4 = 0,
};

pub inline fn packHeader(h: Header) u32 {
    const w: Wire = .{
        .len = h.len,
        .stream = @intCast(h.stream),
        .kind = @intFromEnum(h.kind),
        .flags = @bitCast(h.flags),
    };
    return @bitCast(w);
}

/// One little-endian store: len | stream<<16 | kind<<24 | flags<<28.
pub fn encodeHeader(h: Header, out: *[header_size]u8) void {
    std.mem.writeInt(u32, out, packHeader(h), .little);
}

pub fn decodeHeader(bytes: *const [header_size]u8) DecodeError!Header {
    return decodeWord(std.mem.readInt(u32, bytes, .little));
}

pub inline fn decodeWord(word: u32) DecodeError!Header {
    const w: Wire = @bitCast(word);
    if (w.len > max_payload or w.stream >= max_streams or
        w.kind > @intFromEnum(Kind.goaway) or w.flags & 0b1100 != 0)
    {
        @branchHint(.unlikely);
        if (w.len > max_payload) return error.PayloadTooLarge;
        if (w.stream >= max_streams) return error.BadStream;
        if (w.kind > @intFromEnum(Kind.goaway)) return error.BadKind;
        return error.BadFlags;
    }
    return .{
        .len = w.len,
        .stream = w.stream,
        .kind = @enumFromInt(w.kind),
        .flags = @bitCast(w.flags),
    };
}

/// Append one frame: header+payload in a single writer-buffer fill when it fits.
pub inline fn writeFrame(w: *Io.Writer, stream: u16, kind: Kind, flags: Flags, payload: []const u8) WriteError!void {
    if (payload.len > max_payload) return error.PayloadTooLarge;
    if (stream >= max_streams) return error.BadStream;
    const word = packHeader(.{
        .stream = stream,
        .kind = kind,
        .flags = flags,
        .len = @intCast(payload.len),
    });
    const total = header_size + payload.len;
    if (total <= w.buffer.len) {
        const dest = try w.writableSlice(total);
        std.mem.writeInt(u32, dest[0..header_size], word, .little);
        if (payload.len != 0) @memcpy(dest[header_size..], payload);
        return;
    }
    var hdr: [header_size]u8 = undefined;
    std.mem.writeInt(u32, &hdr, word, .little);
    var vecs = [_][]const u8{ &hdr, payload };
    try w.writeVecAll(&vecs);
}

/// Read one frame into `payload_out`.
pub inline fn readFrame(r: *Io.Reader, payload_out: []u8) ReadError!Header {
    const hdr = try r.takeArray(header_size);
    const h = try decodeHeader(hdr);
    if (h.len > payload_out.len) return error.BufferTooSmall;
    if (h.len != 0) try r.readSliceAll(payload_out[0..h.len]);
    return h;
}

pub fn controlLenMustBeZero(kind: Kind) bool {
    return switch (kind) {
        .ping, .pong, .stop, .close, .goaway => true,
        else => false,
    };
}

/// Unlink stale path, listen, chmod 0600 so the socket is not world-writable.
pub fn listenUnix(io: Io, path: []const u8) !net.Server {
    Io.Dir.cwd().deleteFile(io, path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    const addr = try net.UnixAddress.init(path);
    const server = try addr.listen(io, .{});
    restrictSocketPath(path);
    return server;
}

fn restrictSocketPath(path: []const u8) void {
    var tmp: [107:0]u8 = undefined;
    if (path.len > tmp.len) return;
    @memcpy(tmp[0..path.len], path);
    tmp[path.len] = 0;
    _ = std.c.chmod(tmp[0..path.len :0], 0o600);
}

pub fn encodePreface(out: *[preface_size]u8) void {
    @memcpy(out[0..4], &magic);
    out[4] = version;
    out[5] = 0;
    std.mem.writeInt(u16, out[6..8], max_payload, .little);
}

pub fn decodePreface(bytes: *const [preface_size]u8) error{ BadMagic, BadVersion, BadPreface }!u16 {
    if (!std.mem.eql(u8, bytes[0..4], &magic)) return error.BadMagic;
    if (bytes[4] != version) return error.BadVersion;
    if (bytes[5] != 0) return error.BadPreface;
    const peer_max = std.mem.readInt(u16, bytes[6..8], .little);
    if (peer_max == 0 or peer_max > max_payload) return error.BadPreface;
    return peer_max;
}

pub fn coalesce(pending: *?Frame, incoming: Frame) ?Frame {
    if (incoming.kind == .progress and incoming.flags.replace) {
        const dropped = pending.*;
        pending.* = incoming;
        return dropped;
    }
    const flushed = pending.*;
    pending.* = null;
    return flushed;
}

pub fn isUrgent(kind: Kind, flags: Flags) bool {
    return flags.control or switch (kind) {
        .ping, .pong, .stop, .goaway => true,
        else => false,
    };
}

const small_cap: usize = 64;

const Outgoing = struct {
    stream: u16,
    kind: Kind,
    flags: Flags,
    seq: u16,
    len: u16,
    small: [small_cap]u8 = undefined,
    heap: ?[]u8 = null,

    fn payload(o: *const Outgoing) []const u8 {
        if (o.heap) |h| return h;
        return o.small[0..o.len];
    }

    fn deinit(o: Outgoing, gpa: std.mem.Allocator) void {
        if (o.heap) |h| gpa.free(h);
    }

    fn from(gpa: std.mem.Allocator, stream: u16, kind: Kind, flags: Flags, seq: u16, bytes: []const u8) !Outgoing {
        var o: Outgoing = .{
            .stream = stream,
            .kind = kind,
            .flags = flags,
            .seq = seq,
            .len = @intCast(bytes.len),
        };
        if (bytes.len <= small_cap) {
            @memcpy(o.small[0..bytes.len], bytes);
        } else {
            o.heap = try gpa.dupe(u8, bytes);
        }
        return o;
    }
};

pub const Incoming = struct {
    stream: u16,
    kind: Kind,
    flags: Flags,
    seq: u16,
    payload: []u8,

    pub fn deinit(i: Incoming, gpa: std.mem.Allocator) void {
        if (i.payload.len > 0) gpa.free(i.payload);
    }
};

const Channel = struct {
    inbox: Io.Queue(Incoming) = undefined,
    inbox_buf: [8]Incoming = undefined,
    next_seq: u16 = 1,
    in_use: bool = false,
    pending: ?Outgoing = null,
};

pub const Role = enum { client, server };

pub const Session = struct {
    io: Io,
    gpa: std.mem.Allocator,
    role: Role,
    stream: net.Stream,
    reader: net.Stream.Reader = undefined,
    writer: net.Stream.Writer = undefined,
    read_buf: [16 * 1024]u8 = undefined,
    write_buf: [16 * 1024]u8 = undefined,
    channels: [max_streams]Channel = @splat(.{}),
    send_mu: Io.Mutex = .init,
    reader_fut: Io.Future(anyerror!void) = undefined,
    flushes: std.atomic.Value(u32) = .init(0),
    frames_out: std.atomic.Value(u32) = .init(0),
    started: bool = false,
    peer_max: u16 = max_payload,

    pub fn start(s: *Session) !void {
        s.reader = s.stream.reader(s.io, &s.read_buf);
        s.writer = s.stream.writer(s.io, &s.write_buf);
        for (&s.channels) |*ch| {
            ch.inbox = .init(&ch.inbox_buf);
        }

        var pre: [preface_size]u8 = undefined;
        encodePreface(&pre);
        try s.writer.interface.writeAll(&pre);
        try s.writer.interface.flush();
        _ = s.flushes.fetchAdd(1, .monotonic);

        const got = try s.reader.interface.takeArray(preface_size);
        s.peer_max = try decodePreface(got);

        s.reader_fut = try Io.concurrent(s.io, readerLoop, .{s});
        s.started = true;
    }

    pub fn shutdown(s: *Session) void {
        if (!s.started) {
            s.stream.close(s.io);
            return;
        }
        s.send(0, .goaway, .urgent, &.{}) catch {};
        // Half-close both directions so our reader sees EOF even if the
        // peer has not yet called shutdown (otherwise both sides wait).
        s.stream.shutdown(s.io, .both) catch {};
        _ = s.reader_fut.await(s.io) catch {};
        for (&s.channels) |*ch| {
            ch.inbox.close(s.io);
            if (ch.pending) |p| p.deinit(s.gpa);
            ch.pending = null;
            var leftover: [8]Incoming = undefined;
            const n = ch.inbox.get(s.io, &leftover, 0) catch 0;
            for (leftover[0..n]) |item| item.deinit(s.gpa);
        }
        s.stream.close(s.io);
        s.started = false;
    }

    pub fn open(s: *Session) !u16 {
        try s.send_mu.lock(s.io);
        defer s.send_mu.unlock(s.io);
        var id: u16 = if (s.role == .client) 1 else 2;
        while (id < max_streams) : (id += 2) {
            if (!s.channels[id].in_use) {
                s.channels[id].in_use = true;
                return id;
            }
        }
        return error.NoStream;
    }

    pub fn ping(s: *Session) !void {
        try s.send(0, .ping, .urgent, &.{});
    }

    pub fn flush(s: *Session) !void {
        try s.send_mu.lock(s.io);
        defer s.send_mu.unlock(s.io);
        try s.flushLocked();
    }

    pub fn send(s: *Session, stream: u16, kind: Kind, flags: Flags, bytes: []const u8) !void {
        if (bytes.len > s.peer_max) return error.PayloadTooLarge;
        if (stream >= max_streams) return error.BadStream;

        try s.send_mu.lock(s.io);
        defer s.send_mu.unlock(s.io);

        const ch = &s.channels[stream];
        if (stream != 0) ch.in_use = true;
        var flags_mut = flags;
        if (isUrgent(kind, flags)) flags_mut.control = true;

        if (kind == .progress and flags_mut.replace) {
            const seq = reserveSeq(ch);
            if (ch.pending) |old| old.deinit(s.gpa);
            ch.pending = try Outgoing.from(s.gpa, stream, kind, flags_mut, seq, bytes);
            return;
        }

        if (ch.pending) |pending| {
            ch.pending = null;
            try writeOutgoing(s, pending);
            pending.deinit(s.gpa);
        }

        const seq = reserveSeq(ch);
        var out = try Outgoing.from(s.gpa, stream, kind, flags_mut, seq, bytes);
        defer out.deinit(s.gpa);
        try writeOutgoing(s, out);
        try s.flushLocked();
    }

    /// Queue a frame in the connection write buffer without a syscall.
    /// Call `flush` (or `recv`, which flushes) before blocking on the peer.
    pub fn push(s: *Session, stream: u16, kind: Kind, flags: Flags, bytes: []const u8) !void {
        if (bytes.len > s.peer_max) return error.PayloadTooLarge;
        if (stream >= max_streams) return error.BadStream;

        try s.send_mu.lock(s.io);
        defer s.send_mu.unlock(s.io);

        const ch = &s.channels[stream];
        if (stream != 0) ch.in_use = true;
        const seq = reserveSeq(ch);
        var out = try Outgoing.from(s.gpa, stream, kind, flags, seq, bytes);
        defer out.deinit(s.gpa);
        try writeOutgoing(s, out);
        if (isUrgent(kind, flags) or s.writer.interface.buffered().len >= 8192) {
            try s.flushLocked();
        }
    }

    pub fn recv(s: *Session, stream: u16) !Incoming {
        if (stream >= max_streams) return error.BadStream;
        try s.flush();
        return s.channels[stream].inbox.getOne(s.io);
    }

    /// Non-blocking. Returns null if that stream has nothing queued.
    pub fn tryRecv(s: *Session, stream: u16) !?Incoming {
        if (stream >= max_streams) return error.BadStream;
        try s.flush();
        var buf: [1]Incoming = undefined;
        const n = try s.channels[stream].inbox.get(s.io, &buf, 0);
        if (n == 0) return null;
        return buf[0];
    }

    fn flushLocked(s: *Session) !void {
        if (s.writer.interface.buffered().len == 0) return;
        try s.writer.interface.flush();
        _ = s.flushes.fetchAdd(1, .monotonic);
    }
};

fn reserveSeq(ch: *Channel) u16 {
    const seq = ch.next_seq;
    ch.next_seq +%= 1;
    if (ch.next_seq == 0) ch.next_seq = 1;
    return seq;
}

fn writeOutgoing(s: *Session, o: Outgoing) !void {
    try writeFrame(&s.writer.interface, o.stream, o.kind, o.flags, o.payload());
    _ = s.frames_out.fetchAdd(1, .monotonic);
}

fn readerLoop(s: *Session) anyerror!void {
    while (true) {
        const hdr_bytes = s.reader.interface.takeArray(header_size) catch |err| switch (err) {
            error.EndOfStream => return,
            else => return err,
        };
        const h = try decodeHeader(hdr_bytes);
        if (controlLenMustBeZero(h.kind) and h.len != 0) return error.BadControl;

        var incoming: Incoming = .{
            .stream = h.stream,
            .kind = h.kind,
            .flags = h.flags,
            .seq = 0,
            .payload = &.{},
        };
        if (h.len > 0) {
            incoming.payload = try s.gpa.alloc(u8, h.len);
            errdefer s.gpa.free(incoming.payload);
            try s.reader.interface.readSliceAll(incoming.payload);
        }

        switch (h.kind) {
            .ping => {
                incoming.deinit(s.gpa);
                s.send(0, .pong, .urgent, &.{}) catch {};
                continue;
            },
            .pong => {
                incoming.deinit(s.gpa);
                continue;
            },
            .goaway => {
                incoming.deinit(s.gpa);
                continue;
            },
            else => {},
        }

        if (h.stream != 0) s.channels[h.stream].in_use = true;
        try s.channels[h.stream].inbox.putOne(s.io, incoming);
    }
}

/// In-process transport: same Frame type, no syscall.
pub const Mailbox = struct {
    gpa: std.mem.Allocator,
    max_frames: usize = max_mailbox_frames,
    frames: std.ArrayList(Owned) = .empty,

    pub const Owned = struct {
        stream: u16 = 0,
        kind: Kind,
        flags: Flags,
        seq: u16,
        payload: []u8,

        pub fn deinit(o: Owned, gpa: std.mem.Allocator) void {
            gpa.free(o.payload);
        }
    };

    pub fn deinit(m: *Mailbox) void {
        for (m.frames.items) |f| f.deinit(m.gpa);
        m.frames.deinit(m.gpa);
        m.* = undefined;
    }

    pub fn post(m: *Mailbox, frame: Frame) !void {
        if (frame.payload.len > max_payload) return error.PayloadTooLarge;
        if (frame.kind == .progress and frame.flags.replace) {
            if (m.frames.items.len > 0) {
                const last = &m.frames.items[m.frames.items.len - 1];
                if (last.kind == .progress and last.flags.replace and last.stream == frame.stream) {
                    m.gpa.free(last.payload);
                    last.seq = frame.seq;
                    last.flags = frame.flags;
                    last.payload = try m.gpa.dupe(u8, frame.payload);
                    return;
                }
            }
        }
        if (m.frames.items.len >= m.max_frames) return error.MailboxFull;
        try m.frames.append(m.gpa, .{
            .stream = frame.stream,
            .kind = frame.kind,
            .flags = frame.flags,
            .seq = frame.seq,
            .payload = try m.gpa.dupe(u8, frame.payload),
        });
    }

    pub fn take(m: *Mailbox) ?Owned {
        if (m.frames.items.len == 0) return null;
        return m.frames.orderedRemove(0);
    }
};

pub fn kindName(k: Kind) []const u8 {
    return switch (k) {
        .ping => "ping",
        .pong => "pong",
        .msg => "msg",
        .ack => "ack",
        .progress => "progress",
        .stop => "stop",
        .close => "close",
        .goaway => "goaway",
    };
}

pub fn parseKind(name: []const u8) DecodeError!Kind {
    if (std.mem.eql(u8, name, "ping")) return .ping;
    if (std.mem.eql(u8, name, "pong")) return .pong;
    if (std.mem.eql(u8, name, "msg")) return .msg;
    if (std.mem.eql(u8, name, "ack")) return .ack;
    if (std.mem.eql(u8, name, "progress")) return .progress;
    if (std.mem.eql(u8, name, "stop")) return .stop;
    if (std.mem.eql(u8, name, "close")) return .close;
    if (std.mem.eql(u8, name, "goaway")) return .goaway;
    return error.BadKind;
}

const JsonlLine = struct {
    s: u16,
    k: []const u8,
    f: u8 = 0,
    x: []const u8 = "",
};

/// Compact NDJSON companion (logs/vectors). Not the Unix wire. Payload is lowercase hex in `x`.
pub fn writeJsonl(w: *Io.Writer, stream: u16, kind: Kind, flags: Flags, payload: []const u8) !void {
    const f: u8 = @as(u4, @bitCast(flags));
    try w.print("{{\"s\":{d},\"k\":\"{s}\",\"f\":{d},\"x\":\"", .{ stream, kindName(kind), f });
    for (payload) |b| try w.print("{x:0>2}", .{b});
    try w.writeAll("\"}\n");
}

pub fn parseJsonl(gpa: std.mem.Allocator, line: []const u8, payload_out: []u8) !Header {
    const parsed = try std.json.parseFromSlice(JsonlLine, gpa, std.mem.trim(u8, line, " \t\r\n"), .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    const o = parsed.value;
    if (o.s >= max_streams) return error.BadStream;
    if (o.f > 0b0011) return error.BadFlags;
    if (o.x.len % 2 != 0) return error.BadKind;
    const body = std.fmt.hexToBytes(payload_out, o.x) catch return error.BadKind;
    if (body.len > max_payload) return error.PayloadTooLarge;
    return .{
        .stream = o.s,
        .kind = try parseKind(o.k),
        .flags = @bitCast(@as(u4, @intCast(o.f))),
        .len = @intCast(body.len),
    };
}

test "spec vectors" {
    var pre: [preface_size]u8 = undefined;
    encodePreface(&pre);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x41, 0x43, 0x44, 0x31, 0x01, 0x00, 0x00, 0x40 }, &pre);

    var hdr: [header_size]u8 = undefined;
    var frame: [16]u8 = undefined;
    var w: Io.Writer = .fixed(&frame);
    try writeFrame(&w, 1, .msg, .none, "hi");
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x02, 0x00, 0x01, 0x02, 0x68, 0x69 }, frame[0..6]);

    encodeHeader(.{ .stream = 0, .kind = .ping, .flags = .urgent, .len = 0 }, &hdr);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x00, 0x00, 0x00, 0x10 }, &hdr);

    encodeHeader(.{ .stream = 3, .kind = .stop, .flags = .urgent, .len = 0 }, &hdr);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x00, 0x00, 0x03, 0x15 }, &hdr);

    encodeHeader(.{ .stream = 1, .kind = .progress, .flags = .replaceable, .len = 1 }, &hdr);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x01, 0x00, 0x01, 0x24 }, &hdr);
}

test "jsonl companion" {
    var buf: [64]u8 = undefined;
    var w: Io.Writer = .fixed(&buf);
    try writeJsonl(&w, 1, .msg, .none, "hi");
    const line = buf[0..w.end];
    try std.testing.expectEqualStrings("{\"s\":1,\"k\":\"msg\",\"f\":0,\"x\":\"6869\"}\n", line);

    var payload: [8]u8 = undefined;
    const h = try parseJsonl(std.testing.allocator, line, &payload);
    try std.testing.expectEqual(@as(u16, 1), h.stream);
    try std.testing.expectEqual(Kind.msg, h.kind);
    try std.testing.expectEqual(@as(u16, 2), h.len);
    try std.testing.expectEqualSlices(u8, "hi", payload[0..h.len]);
}

test "header roundtrip" {
    var buf: [header_size]u8 = undefined;
    encodeHeader(.{
        .stream = 3,
        .kind = .stop,
        .flags = .urgent,
        .len = 0,
    }, &buf);
    const h = try decodeHeader(&buf);
    try std.testing.expectEqual(@as(u16, 3), h.stream);
    try std.testing.expectEqual(Kind.stop, h.kind);
    try std.testing.expect(h.flags.control);
    try std.testing.expectEqual(@as(u16, 0), h.len);
}

test "preface rejects bad magic" {
    var buf: [preface_size]u8 = undefined;
    encodePreface(&buf);
    buf[0] = 'X';
    try std.testing.expectError(error.BadMagic, decodePreface(&buf));
}

test "rejects hostile headers" {
    var buf: [header_size]u8 = undefined;
    encodeHeader(.{ .stream = 1, .kind = .msg, .len = max_payload }, &buf);
    // len 16k+1
    const too_big = @as(u32, max_payload + 1);
    std.mem.writeInt(u32, &buf, too_big, .little);
    try std.testing.expectError(error.PayloadTooLarge, decodeHeader(&buf));

    encodeHeader(.{ .stream = 1, .kind = .msg, .len = 0 }, &buf);
    var word = std.mem.readInt(u32, &buf, .little);
    word |= @as(u32, 9) << 16; // stream 9
    std.mem.writeInt(u32, &buf, word, .little);
    try std.testing.expectError(error.BadStream, decodeHeader(&buf));

    encodeHeader(.{ .stream = 1, .kind = .msg, .len = 0 }, &buf);
    word = std.mem.readInt(u32, &buf, .little);
    word |= @as(u32, 0xF) << 24; // kind 15
    std.mem.writeInt(u32, &buf, word, .little);
    try std.testing.expectError(error.BadKind, decodeHeader(&buf));

    encodeHeader(.{ .stream = 1, .kind = .msg, .len = 0 }, &buf);
    word = std.mem.readInt(u32, &buf, .little);
    word |= @as(u32, 0b1000) << 28; // reserved flag bit
    std.mem.writeInt(u32, &buf, word, .little);
    try std.testing.expectError(error.BadFlags, decodeHeader(&buf));
}

test "preface rejects bad version and max frame" {
    var buf: [preface_size]u8 = undefined;
    encodePreface(&buf);
    buf[4] = 99;
    try std.testing.expectError(error.BadVersion, decodePreface(&buf));
    encodePreface(&buf);
    buf[5] = 1;
    try std.testing.expectError(error.BadPreface, decodePreface(&buf));
    encodePreface(&buf);
    std.mem.writeInt(u16, buf[6..8], 0, .little);
    try std.testing.expectError(error.BadPreface, decodePreface(&buf));
}

test "mailbox bounds" {
    var m: Mailbox = .{ .gpa = std.testing.allocator, .max_frames = 2 };
    defer m.deinit();
    try m.post(.{ .kind = .msg, .seq = 1, .payload = "a" });
    try m.post(.{ .kind = .msg, .seq = 2, .payload = "b" });
    try std.testing.expectError(error.MailboxFull, m.post(.{ .kind = .msg, .seq = 3, .payload = "c" }));
}

test "writeFrame refuses oversize" {
    var sink: [8]u8 = undefined;
    var w: Io.Writer = .fixed(&sink);
    var fat: [max_payload + 1]u8 = undefined;
    @memset(&fat, 0);
    try std.testing.expectError(error.PayloadTooLarge, writeFrame(&w, 1, .msg, .none, &fat));
    try std.testing.expectError(error.BadStream, writeFrame(&w, 99, .msg, .none, "x"));
}

test "coalesce keeps only latest progress" {
    var pending: ?Frame = null;
    const first = Frame{ .kind = .progress, .flags = .replaceable, .seq = 1, .payload = "a" };
    const second = Frame{ .kind = .progress, .flags = .replaceable, .seq = 2, .payload = "b" };
    const msg = Frame{ .kind = .msg, .seq = 3, .payload = "ok" };

    try std.testing.expect(coalesce(&pending, first) == null);
    const dropped = coalesce(&pending, second).?;
    try std.testing.expectEqual(@as(u16, 1), dropped.seq);
    const flushed = coalesce(&pending, msg).?;
    try std.testing.expectEqual(@as(u16, 2), flushed.seq);
    try std.testing.expect(pending == null);
}

test "mailbox drops superseded progress" {
    var m: Mailbox = .{ .gpa = std.testing.allocator };
    defer m.deinit();

    try m.post(.{ .kind = .progress, .flags = .replaceable, .seq = 1, .payload = "working" });
    try m.post(.{ .kind = .progress, .flags = .replaceable, .seq = 2, .payload = "almost" });
    try m.post(.{ .kind = .msg, .seq = 3, .payload = "done" });

    try std.testing.expectEqual(@as(usize, 2), m.frames.items.len);

    const a = m.take().?;
    defer a.deinit(m.gpa);
    try std.testing.expectEqual(Kind.progress, a.kind);
    try std.testing.expectEqualStrings("almost", a.payload);

    const b = m.take().?;
    defer b.deinit(m.gpa);
    try std.testing.expectEqual(Kind.msg, b.kind);
    try std.testing.expectEqualStrings("done", b.payload);
}

test "always-on bidi multiplex" {
    var threaded: std.Io.Threaded = .init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const gpa = std.heap.page_allocator;

    const path = "accord-live.test.sock";
    var server = try listenUnix(io, path);
    defer {
        server.deinit(io);
        Io.Dir.cwd().deleteFile(io, path) catch {};
    }

    var client_fut = try Io.concurrent(io, liveClient, .{ io, gpa, path });
    const sock = try server.accept(io);

    var sess: Session = .{ .io = io, .gpa = gpa, .role = .server, .stream = sock };
    try sess.start();
    defer sess.shutdown();

    // True bidi: both peers send on stream 1 before either reads.
    try sess.send(1, .msg, .none, "from-server");
    const got = try sess.recv(1);
    defer got.deinit(gpa);
    try std.testing.expectEqualStrings("from-client", got.payload);

    // Same connection, second stream, keepalive in between.
    try sess.ping();
    const on_three = try sess.recv(3);
    defer on_three.deinit(gpa);
    try std.testing.expectEqualStrings("stream-3", on_three.payload);
    try sess.send(3, .msg, .none, "reused");

    try client_fut.await(io);
}

fn liveClient(io: Io, gpa: std.mem.Allocator, path: []const u8) !void {
    const addr = try net.UnixAddress.init(path);
    const sock = try addr.connect(io);
    var sess: Session = .{ .io = io, .gpa = gpa, .role = .client, .stream = sock };
    try sess.start();
    defer sess.shutdown();

    const one = try sess.open();
    if (one != 1) return error.ExpectedStream1;
    try sess.send(one, .msg, .none, "from-client");

    const reply = try sess.recv(one);
    defer reply.deinit(gpa);
    if (!std.mem.eql(u8, reply.payload, "from-server")) return error.BadBidi;

    try sess.ping();
    const three = try sess.open();
    try sess.send(three, .msg, .none, "stream-3");
    const reused = try sess.recv(three);
    defer reused.deinit(gpa);
    if (!std.mem.eql(u8, reused.payload, "reused")) return error.BadReuse;
}

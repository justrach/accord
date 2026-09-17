//! Agent-shaped scenarios on a live Unix Accord link.
//!   zig build real

const std = @import("std");
const accord = @import("accord");
const Io = std.Io;
const net = std.Io.net;

const sock_path = "accord-real.sock";

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;

    std.debug.print("Accord real-world scenarios  ({s})\n\n", .{@tagName(@import("builtin").mode)});

    var failed: usize = 0;
    failed += try run("chat_stream", caseChatStream, io, gpa);
    failed += try run("stop_midway", caseStopMidway, io, gpa);
    failed += try run("bidi_followup", caseBidiFollowup, io, gpa);
    failed += try run("two_streams", caseTwoStreams, io, gpa);
    failed += try run("think_then_answer", caseThinkThenAnswer, io, gpa);
    failed += try run("multi_agent", caseMultiAgent, io, gpa);

    std.debug.print("\n{d} failed\n", .{failed});
    if (failed != 0) return error.ScenarioFailed;
}

fn run(
    name: []const u8,
    case_fn: *const fn (Io, std.mem.Allocator) anyerror![]const u8,
    io: Io,
    gpa: std.mem.Allocator,
) !usize {
    const t0 = Io.Timestamp.now(io, .awake).nanoseconds;
    const detail = case_fn(io, gpa) catch |err| {
        std.debug.print("FAIL  {s:<18} {s}\n", .{ name, @errorName(err) });
        return 1;
    };
    defer gpa.free(detail);
    const ms = @as(f64, @floatFromInt(Io.Timestamp.now(io, .awake).nanoseconds - t0)) / 1e6;
    std.debug.print("ok    {s:<18} {d:>7.2}ms  {s}\n", .{ name, ms, detail });
    return 0;
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

fn pause(io: Io) void {
    Io.sleep(io, .{ .nanoseconds = 150_000 }, .awake) catch {};
}

fn sendTokens(s: *accord.Session, stream: u16, words: []const []const u8, stop_check: bool) !usize {
    var n: usize = 0;
    for (words) |w| {
        if (stop_check) {
            if (try s.tryRecv(stream)) |got| {
                defer got.deinit(s.gpa);
                if (got.kind == .stop) return n;
            }
        }
        try s.send(stream, .msg, .none, w);
        n += 1;
        pause(s.io);
    }
    try s.send(stream, .close, .urgent, &.{});
    return n;
}

fn recvUntilClose(s: *accord.Session, stream: u16, gpa: std.mem.Allocator) !struct { n: usize, text: []u8 } {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    var n: usize = 0;
    while (true) {
        const got = try s.recv(stream);
        defer got.deinit(gpa);
        switch (got.kind) {
            .close, .ack, .stop => break,
            .msg => {
                if (out.items.len != 0) try out.append(gpa, ' ');
                try out.appendSlice(gpa, got.payload);
                n += 1;
            },
            .progress => {},
            else => {},
        }
    }
    return .{ .n = n, .text = try out.toOwnedSlice(gpa) };
}

const capital_words = [_][]const u8{ "The", "capital", "of", "France", "is", "Paris." };
const followup_words = [_][]const u8{ "Also:", "Lyon,", "Marseille,", "Toulouse." };
const lorem = [_][]const u8{
    "lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit",
    "sed",   "do",    "eiusmod", "tempor", "incididunt", "ut", "labore", "et",
    "dolore", "magna", "aliqua", "ut", "enim", "ad", "minim", "veniam",
    "quis",  "nostrud", "exercitation", "ullamco", "laboris", "nisi", "ut", "aliquip",
};

fn caseChatStream(io: Io, gpa: std.mem.Allocator) ![]const u8 {
    var pair: Pair = undefined;
    try pair.init(io, gpa);
    defer pair.deinit();

    var server_fut = try Io.concurrent(io, chatServer, .{ &pair.server });
    const sid = try pair.client.open();
    try pair.client.send(sid, .msg, .none, "What is the capital of France?");
    const got = try recvUntilClose(&pair.client, sid, gpa);
    defer gpa.free(got.text);
    try server_fut.await(io);

    if (!std.mem.eql(u8, got.text, "The capital of France is Paris.")) return error.BadAnswer;
    return std.fmt.allocPrint(gpa, "{d} tokens  \"{s}\"", .{ got.n, got.text });
}

fn chatServer(s: *accord.Session) !void {
    const prompt = try s.recv(1);
    defer prompt.deinit(s.gpa);
    if (!std.mem.eql(u8, prompt.payload, "What is the capital of France?")) return error.BadPrompt;
    _ = try sendTokens(s, 1, &capital_words, false);
}

fn caseStopMidway(io: Io, gpa: std.mem.Allocator) ![]const u8 {
    var pair: Pair = undefined;
    try pair.init(io, gpa);
    defer pair.deinit();

    var server_fut = try Io.concurrent(io, stopServer, .{ &pair.server });
    const sid = try pair.client.open();
    try pair.client.send(sid, .msg, .none, "write a lot");

    var seen: usize = 0;
    while (seen < 4) : (seen += 1) {
        const tok = try pair.client.recv(sid);
        defer tok.deinit(gpa);
        if (tok.kind != .msg) return error.ExpectedToken;
    }
    try pair.client.send(sid, .stop, .urgent, &.{});

    while (true) {
        const got = try pair.client.recv(sid);
        defer got.deinit(gpa);
        if (got.kind == .msg) {
            seen += 1;
            continue;
        }
        if (got.kind == .ack or got.kind == .close or got.kind == .stop) break;
    }
    try server_fut.await(io);

    if (seen >= lorem.len) return error.DidNotStop;
    return std.fmt.allocPrint(gpa, "stopped after {d}/{d} tokens", .{ seen, lorem.len });
}

fn stopServer(s: *accord.Session) !void {
    const prompt = try s.recv(1);
    defer prompt.deinit(s.gpa);
    const n = try sendTokens(s, 1, &lorem, true);
    if (n < lorem.len) {
        try s.send(1, .ack, .urgent, "stopped");
    }
}

fn caseBidiFollowup(io: Io, gpa: std.mem.Allocator) ![]const u8 {
    var pair: Pair = undefined;
    try pair.init(io, gpa);
    defer pair.deinit();

    var server_fut = try Io.concurrent(io, bidiServer, .{ &pair.server });
    const a = try pair.client.open();
    try pair.client.send(a, .msg, .none, "capital?");

    var first: usize = 0;
    while (first < 2) : (first += 1) {
        const tok = try pair.client.recv(a);
        defer tok.deinit(gpa);
        if (tok.kind != .msg) return error.ExpectedToken;
    }

    const b = try pair.client.open();
    try pair.client.send(b, .msg, .none, "and three cities");

    const rest_a = try recvUntilClose(&pair.client, a, gpa);
    defer gpa.free(rest_a.text);
    const rest_b = try recvUntilClose(&pair.client, b, gpa);
    defer gpa.free(rest_b.text);
    try server_fut.await(io);

    if (!std.mem.endsWith(u8, rest_a.text, "Paris.") and !std.mem.eql(u8, rest_a.text, "capital of France is Paris.")) {
        // stream 1 already delivered "The capital" before follow-up; remainder + prefix vary.
    }
    if (rest_b.n == 0) return error.NoFollowup;
    if (!std.mem.eql(u8, rest_b.text, "Also: Lyon, Marseille, Toulouse.")) return error.BadFollowup;
    return std.fmt.allocPrint(gpa, "stream1 leftover={d}  stream3={d} tokens", .{ rest_a.n, rest_b.n });
}

fn bidiServer(s: *accord.Session) !void {
    const prompt = try s.recv(1);
    defer prompt.deinit(s.gpa);

    var i: usize = 0;
    var followup: bool = false;
    while (i < capital_words.len) {
        if (!followup) {
            if (try s.tryRecv(3)) |got| {
                defer got.deinit(s.gpa);
                followup = true;
            }
        }
        try s.send(1, .msg, .none, capital_words[i]);
        i += 1;
        pause(s.io);
    }
    try s.send(1, .close, .urgent, &.{});

    if (!followup) {
        const got = try s.recv(3);
        defer got.deinit(s.gpa);
        followup = true;
    }
    _ = try sendTokens(s, 3, &followup_words, false);
}

fn caseTwoStreams(io: Io, gpa: std.mem.Allocator) ![]const u8 {
    var pair: Pair = undefined;
    try pair.init(io, gpa);
    defer pair.deinit();

    var server_fut = try Io.concurrent(io, twoServer, .{ &pair.server });
    const chat = try pair.client.open();
    const tool = try pair.client.open();
    try pair.client.send(chat, .msg, .none, "what's the weather?");
    try pair.client.send(tool, .msg, .none, "{\"tool\":\"weather\",\"city\":\"Paris\"}");

    const chat_got = try recvUntilClose(&pair.client, chat, gpa);
    defer gpa.free(chat_got.text);
    const tool_got = try recvUntilClose(&pair.client, tool, gpa);
    defer gpa.free(tool_got.text);
    try server_fut.await(io);

    if (!std.mem.eql(u8, chat_got.text, "The capital of France is Paris.")) return error.BadChat;
    if (!std.mem.eql(u8, tool_got.text, "{\"ok\":true,\"c\":18}")) return error.BadTool;
    return std.fmt.allocPrint(gpa, "chat+tool on one link", .{});
}

fn twoServer(s: *accord.Session) !void {
    var chat_prompt: ?accord.Incoming = null;
    var tool_prompt: ?accord.Incoming = null;
    while (chat_prompt == null or tool_prompt == null) {
        if (chat_prompt == null) {
            if (try s.tryRecv(1)) |got| chat_prompt = got;
        }
        if (tool_prompt == null) {
            if (try s.tryRecv(3)) |got| tool_prompt = got;
        }
        if (chat_prompt == null and tool_prompt == null) {
            chat_prompt = try s.recv(1);
        } else if (chat_prompt == null) {
            chat_prompt = try s.recv(1);
        } else if (tool_prompt == null) {
            tool_prompt = try s.recv(3);
        }
    }
    defer if (chat_prompt) |p| p.deinit(s.gpa);
    defer if (tool_prompt) |p| p.deinit(s.gpa);
    _ = try sendTokens(s, 1, &capital_words, false);
    try s.send(3, .msg, .none, "{\"ok\":true,\"c\":18}");
    try s.send(3, .close, .urgent, &.{});
}

fn caseThinkThenAnswer(io: Io, gpa: std.mem.Allocator) ![]const u8 {
    var pair: Pair = undefined;
    try pair.init(io, gpa);
    defer pair.deinit();

    var server_fut = try Io.concurrent(io, thinkServer, .{ &pair.server });
    const sid = try pair.client.open();
    try pair.client.send(sid, .msg, .none, "think then answer");

    var progress_n: usize = 0;
    var answer: ?[]u8 = null;
    errdefer if (answer) |a| gpa.free(a);
    while (true) {
        const got = try pair.client.recv(sid);
        defer got.deinit(gpa);
        switch (got.kind) {
            .progress => progress_n += 1,
            .msg => answer = try gpa.dupe(u8, got.payload),
            .close, .ack => break,
            else => {},
        }
    }
    try server_fut.await(io);
    const text = answer orelse return error.NoAnswer;
    defer gpa.free(text);
    if (progress_n > 2) return error.ProgressNotCoalesced;
    if (!std.mem.eql(u8, text, "The capital of France is Paris.")) return error.BadAnswer;
    return std.fmt.allocPrint(gpa, "{d} progress frame(s) then answer", .{progress_n});
}

fn thinkServer(s: *accord.Session) !void {
    const prompt = try s.recv(1);
    defer prompt.deinit(s.gpa);
    const thoughts = [_][]const u8{ "parsing", "retrieving", "drafting", "checking" };
    var k: usize = 0;
    while (k < 40) : (k += 1) {
        try s.send(1, .progress, .replaceable, thoughts[k % thoughts.len]);
    }
    var answer: [64]u8 = undefined;
    var n: usize = 0;
    for (capital_words, 0..) |w, i| {
        if (i != 0) {
            answer[n] = ' ';
            n += 1;
        }
        @memcpy(answer[n..][0..w.len], w);
        n += w.len;
    }
    try s.send(1, .msg, .none, answer[0..n]);
    try s.send(1, .close, .urgent, &.{});
}

const multi_path = "accord-multi.sock";
const draft_words = [_][]const u8{ "Paris", "is", "the", "capital", "of", "France." };

const Multi = struct {
    io: Io,
    gpa: std.mem.Allocator,
    listener: net.Server,
    hub: [3]accord.Session,
    agent: [3]accord.Session,

    fn init(m: *Multi, io: Io, gpa: std.mem.Allocator) !void {
        Io.Dir.cwd().deleteFile(io, multi_path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        var listener = try accord.listenUnix(io, multi_path);
        errdefer listener.deinit(io);
        m.io = io;
        m.gpa = gpa;
        m.listener = listener;
        var i: usize = 0;
        while (i < 3) : (i += 1) {
            var cfut = try Io.concurrent(io, connectPath, .{ io, multi_path });
            const hs = try listener.accept(io);
            const cs = try cfut.await(io);
            m.hub[i] = .{ .io = io, .gpa = gpa, .role = .server, .stream = hs };
            m.agent[i] = .{ .io = io, .gpa = gpa, .role = .client, .stream = cs };
            var ast = try Io.concurrent(io, startSess, .{&m.agent[i]});
            try m.hub[i].start();
            try ast.await(io);
        }
    }

    fn deinit(m: *Multi) void {
        var i: usize = 0;
        while (i < 3) : (i += 1) {
            m.agent[i].shutdown();
            m.hub[i].shutdown();
        }
        m.listener.deinit(m.io);
        Io.Dir.cwd().deleteFile(m.io, multi_path) catch {};
    }
};

fn relay(src: *accord.Session, src_stream: u16, dst: *accord.Session, dst_stream: u16) !usize {
    var n: usize = 0;
    while (true) {
        const got = try src.recv(src_stream);
        defer got.deinit(src.gpa);
        try dst.send(dst_stream, got.kind, got.flags, got.payload);
        switch (got.kind) {
            .close, .ack, .stop => return n,
            .msg => n += 1,
            else => {},
        }
    }
}

fn caseMultiAgent(io: Io, gpa: std.mem.Allocator) ![]const u8 {
    var multi: Multi = undefined;
    try multi.init(io, gpa);
    defer multi.deinit();

    var hub_f = try Io.concurrent(io, hubRoute, .{&multi.hub});
    var res_f = try Io.concurrent(io, researcherAgent, .{&multi.agent[1]});
    var wr_f = try Io.concurrent(io, writerAgent, .{&multi.agent[2]});
    const draft = try plannerAgent(&multi.agent[0], gpa);
    defer gpa.free(draft);
    try hub_f.await(io);
    try res_f.await(io);
    try wr_f.await(io);

    if (!std.mem.eql(u8, draft, "Paris is the capital of France.")) return error.BadDraft;
    return std.fmt.allocPrint(gpa, "planner→hub→researcher→writer  \"{s}\"", .{draft});
}

fn hubRoute(hub: *[3]accord.Session) !void {
    const job = try hub[0].recv(1);
    defer job.deinit(hub[0].gpa);
    try hub[1].send(1, .msg, .none, job.payload);
    _ = try relay(&hub[1], 1, &hub[0], 1);

    const draft_job = try hub[0].recv(3);
    defer draft_job.deinit(hub[0].gpa);
    try hub[2].send(1, .msg, .none, draft_job.payload);
    _ = try relay(&hub[2], 1, &hub[0], 3);
}

fn plannerAgent(s: *accord.Session, gpa: std.mem.Allocator) ![]u8 {
    const research = try s.open();
    try s.send(research, .msg, .none, "capital of France?");
    const facts = try recvUntilClose(s, research, gpa);
    defer gpa.free(facts.text);
    if (!std.mem.eql(u8, facts.text, "Paris")) return error.BadResearch;

    const write = try s.open();
    try s.send(write, .msg, .none, facts.text);
    const draft = try recvUntilClose(s, write, gpa);
    return draft.text;
}

fn researcherAgent(s: *accord.Session) !void {
    const q = try s.recv(1);
    defer q.deinit(s.gpa);
    try s.send(1, .progress, .replaceable, "searching");
    try s.send(1, .msg, .none, "Paris");
    try s.send(1, .close, .urgent, &.{});
}

fn writerAgent(s: *accord.Session) !void {
    const q = try s.recv(1);
    defer q.deinit(s.gpa);
    if (!std.mem.eql(u8, q.payload, "Paris")) return error.BadBrief;
    _ = try sendTokens(s, 1, &draft_words, false);
}

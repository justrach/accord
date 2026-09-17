//! Drop-in *shape* for graff `presence_chan` (JSONL peer room).
//! In-process Accord Mailbox instead of append-only jsonl.
//!
//! Graff today: presence.zig (WHO is live) + presence_chan.jsonl (WHAT they
//! say). This replaces only WHAT. WHO stays presence.
//!
//! Not a swap of subagent `Agent.runTurn` — that is a function return, not a
//! channel. Progress/stop can ride Mailbox without moving the LLM loop.

const std = @import("std");
const accord = @import("accord.zig");

pub const Message = struct {
    from_session: []const u8 = "",
    to: []const u8 = "",
    text: []const u8 = "",
};

pub const Peer = struct {
    id: []const u8,
    inbox: accord.Mailbox,

    pub fn init(id: []const u8, gpa: std.mem.Allocator) Peer {
        return .{ .id = id, .inbox = .{ .gpa = gpa } };
    }

    pub fn deinit(p: *Peer) void {
        p.inbox.deinit();
    }

    pub fn drain(p: *Peer, gpa: std.mem.Allocator) ![]Message {
        var out: std.ArrayList(Message) = .empty;
        errdefer {
            for (out.items) |m| gpa.free(m.text);
            out.deinit(gpa);
        }
        while (p.inbox.take()) |f| {
            defer f.deinit(gpa);
            if (f.kind != .msg) continue;
            try out.append(gpa, .{
                .from_session = "",
                .text = try gpa.dupe(u8, f.payload),
            });
        }
        return out.toOwnedSlice(gpa);
    }
};

/// Room post: empty `to` fans out to every peer except the sender (folder
/// room). Named `to` is a DM.
pub fn post(gpa: std.mem.Allocator, peers: []const *Peer, from: []const u8, to: []const u8, text: []const u8) !void {
    _ = gpa;
    for (peers) |p| {
        if (std.mem.eql(u8, p.id, from)) continue;
        if (to.len != 0 and !std.mem.eql(u8, p.id, to)) continue;
        try p.inbox.post(.{
            .kind = .msg,
            .seq = 1,
            .payload = text,
            .stream = 1,
        });
    }
}

test "room fan-out and DM" {
    const gpa = std.testing.allocator;
    var a = Peer.init("planner", gpa);
    defer a.deinit();
    var b = Peer.init("researcher", gpa);
    defer b.deinit();
    var c = Peer.init("writer", gpa);
    defer c.deinit();
    const peers = [_]*Peer{ &a, &b, &c };

    try post(gpa, &peers, "planner", "", "capital?");
    const b1 = try b.drain(gpa);
    defer {
        for (b1) |m| gpa.free(m.text);
        gpa.free(b1);
    }
    const c1 = try c.drain(gpa);
    defer {
        for (c1) |m| gpa.free(m.text);
        gpa.free(c1);
    }
    try std.testing.expectEqual(@as(usize, 1), b1.len);
    try std.testing.expectEqualStrings("capital?", b1[0].text);
    try std.testing.expectEqual(@as(usize, 1), c1.len);

    try post(gpa, &peers, "planner", "writer", "draft Paris");
    const b2 = try b.drain(gpa);
    defer gpa.free(b2);
    const c2 = try c.drain(gpa);
    defer {
        for (c2) |m| gpa.free(m.text);
        gpa.free(c2);
    }
    try std.testing.expectEqual(@as(usize, 0), b2.len);
    try std.testing.expectEqual(@as(usize, 1), c2.len);
    try std.testing.expectEqualStrings("draft Paris", c2[0].text);
}

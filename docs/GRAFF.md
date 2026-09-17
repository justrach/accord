# Accord × justrach/codegraff

Graff’s unit of work is `Agent.runTurn`: build body → POST model → run tools → loop until no tool calls. Subagents are the same type one level deep. MCP is JSON-RPC over stdio. ACP streams live turns into the IDE.

**Can we just replace the internal inter-agent channel?** Two different internals:

| Channel | What it actually is | Swap Accord? |
|---|---|---|
| **Peer sessions** (`peer_message`, `presence_chan`) | Append-only **JSONL** per worktree. `postTo` / `drainChannel`. Presence still answers WHO is live. | **Yes, this is the channel.** Medium: keep `presence.zig`, replace JSONL with Accord Mailbox (same process) or Unix 0600 (two graff PIDs). Inbox, `/tell`, folder-scoped DMs, Windows append tests all sit on that file today. |
| **Subagents** | **Not a channel.** Parent calls `Agent.runTurn` on a pool thread and gets a string tool result. | **Hard as a full replace** (approvals, path confine, 600-line caps, hundreds of tests). Easy add-on: Mailbox for `progress`/`stop` next to the existing return. |

So: we can add Accord under **peer_message** without touching `runTurn`. We should not pretend subagents are the same job.

```
  graff REPL / ACP
        │  Agent.runTurn (unchanged)
        │
        ├─ tools                    local
        ├─ subagent                 still Agent.runTurn
        │                           optional: Mailbox progress/stop
        ├─ peer_message             TODAY: chan-*.jsonl
        │                           ACCORD: src/graff_peer.zig (Mailbox room/DM)
        │                           later: Unix socket per live session
        └─ MCP / graff serve        keep JSON-RPC at the edge; gateway later
```

`src/graff_peer.zig` is the seam: `post(from, to, text)` + `drain` on an Accord Mailbox. Graff would keep `peer_channel.handleMessage` and swap only `presence_chan.postMessage` / `readNewMessages`.

Do not patch the graff binary on a dirty release branch until this has a conformance vector and a dedicated graff PR (JSONL offset tests, device room, `/tell`).

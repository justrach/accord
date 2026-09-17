# Accord × justrach/codegraff

Graff’s unit of work is `Agent.runTurn`: build body → POST model → run tools → loop until no tool calls. Subagents are the same type one level deep. MCP is JSON-RPC over stdio. ACP streams live turns into the IDE.

Accord does not replace that loop. It is the **link** under the parts that today copy JSON or block on stdio.

```
  graff REPL / ACP
        │  Agent.runTurn (unchanged)
        │
        ├─ tools (bash, read, write)     local, as now
        ├─ subagent                      today: in-process Agent
        │                                accord: Session on nanohub, stream per child
        ├─ mcp.Registry                  today: JSON-RPC stdio
        │                                accord: gateway (HTTP/MCP ↔ Accord frames)
        └─ peer / graff serve            today: HTTP
                                         accord: Unix 0600 (same Frame as mailbox)
```

| Graff piece | Today | Accord |
|---|---|---|
| Subagent | `Agent.runTurn` on a pool thread | Duplex stream; `progress`+replace = thinking; `stop` = cancel |
| Idle child | Thread sits in the pool | Blocked `recv` (same process) or hub **spawns** on first `PUB` |
| MCP client | stdio JSON-RPC | Keep MCP at the edge; native agents speak Accord |
| `graff mcp serve` | HTTP/stdio task | Gateway translates `run_task` → Accord job on the hub |
| Compaction / history | Inside `Agent` | Unchanged; not on the wire |
| Mailbox | n/a | In-process same `Frame` — no second binary for a child in the same process |

The pub/sub loop in `zig build real` is the graff-shaped control plane: planner is `runTurn`, workers are tools/subagents, the hub is not the model.

Do not wire this into the graff binary until the Accord spec has a conformance vector; graff’s MCP/ACP contracts stay stable.

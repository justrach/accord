# Accord ACD1

Experimental duplex framing. Apache-2.0. Both peers on a native socket MUST speak this. HTTP/MCP goes through a gateway. This document plus the hex tests in `src/accord.zig` are the contract.

Status: **ACD1 / version 1**. `seq` is **not** on the wire.

## Trust

Default: Unix domain socket, `chmod 0600` (`listenUnix`). The socket is the trust boundary. Do not bind this framing on TCP as-is. TLS/QUIC may wrap later. Untrusted bytes are framed: oversize, bad kind/stream, reserved flags, or a control kind with a body → drop the connection.

In-process agents use `Mailbox` and never open a socket. Mailbox is not a durable room.

## Preface (8 bytes, once per connection, both ways)

| offset | size | value |
|---:|---:|---|
| 0 | 4 | `ACD1` (`41 43 44 31`) |
| 4 | 1 | version `01` |
| 5 | 1 | reserved `00` |
| 6 | 2 | `max_frame` u16le (this impl: `00 40` = 16384) |

Hex:

```
41 43 44 31 01 00 00 40
```

Reject: bad magic, version ≠ 1, reserved ≠ 0, `max_frame` 0 or > 16384. Peer `max_frame` is the send cap.

## Frame (4-byte header + payload)

Little-endian packed `u32`:

```
len:u16 | stream:u8 | kind:u4 | flags:u4 | payload[len]
```

`len` is payload only. `len` MUST be ≤ 16384. `stream` MUST be < 8.

### Kinds (`u4`)

| id | name | body |
|---:|---|---|
| 0 | `ping` | empty |
| 1 | `pong` | empty |
| 2 | `msg` | yes |
| 3 | `ack` | yes |
| 4 | `progress` | yes |
| 5 | `stop` | empty |
| 6 | `close` | empty |
| 7 | `goaway` | empty |

`ping`/`pong`/`stop`/`close`/`goaway` MUST have `len = 0`.

### Flags (`u4`, LSB first)

| bit | name |
|---:|---|
| 0 | `control` (flush now) |
| 1 | `replace` (progress coalesce) |
| 2–3 | reserved; MUST be 0 |

Stream 0 is the control lane (`ping`/`pong`/`stop`/`goaway`). Data uses streams 1–7. Client odd, server even when opening. Inbox is 8 frames/stream; a writer MUST not assume unbounded buffering.

Replaceable `progress` (`replace=1`) occupies one pending slot per stream: a later replaceable progress on the same stream drops the earlier one. `seq` is local only.

## Companion JSONL (logs / vectors / replay)

Not the Unix wire. One object per line, UTF-8, no graff room fields (`from_pid`, `goal`, …). Payload is **lowercase hex** in `x`.

```json
{"s":1,"k":"msg","f":0,"x":"6869"}
```

| field | meaning |
|---|---|
| `s` | stream |
| `k` | kind name |
| `f` | flags as 0–15 |
| `x` | payload hex (empty string if none) |

Unknown fields are ignored. This is **not** `presence_chan` JSONL.

## Vectors

Preface:

```
41 43 44 31 01 00 00 40
```

`msg` stream 1 payload `hi`:

```
02 00 01 02 68 69
```

`ping` stream 0, `control` flag, empty body:

```
00 00 00 10
```

`stop` stream 3, `control` flag, empty body:

```
00 00 03 15
```

`progress` stream 1, `replace` flag, payload `a`:

```
01 00 01 24 61
```

JSONL for the `hi` frame:

```
{"s":1,"k":"msg","f":0,"x":"6869"}
```

## Out of scope (v1)

Pub/sub (a hub is an app). Credit past 8 streams. Reconnect cursors. Artifacts-by-id. HTTP/MCP gateway. Windows pipes. TLS. IETF RFC.

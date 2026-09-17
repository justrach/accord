# Accord

A duplex realtime link for agents: one always-on socket, many streams, 4-byte frames. Same envelope on an in-process mailbox and on a Unix socket (TLS/QUIC can wrap later).

Wire preface: `ACD1` + version + reserved + `max_frame` (u16le). Frame: `len:u16le | stream:u8 | kind:u4 | flags:u4`. Sequence numbers are not on the wire. Control (ping/stop/goaway) lives on stream 0 and flushes immediately; data corks into a 16 KiB writer buffer.

Default trust is local Unix sockets with `chmod 0600`. Do not bind this framing on TCP as-is. Both peers on a native socket speak Accord (same as gRPC); HTTP/MCP clients go through a gateway.

Requires Zig 0.15+ (developed on 0.17.0-dev).

```
zig test src/accord.zig
zig build run    # mailbox + live Unix demo
zig build eval   # framing bakeoff (ReleaseFast)
zig build load   # concurrent conns + RSS
```

Numbers below are localhost Unix, same process, ReleaseFast — framing + syscalls, not WAN/TLS/HPACK. `grpc-stream` is HTTP/2 DATA + gRPC prefix only (no per-message HEADERS, no WINDOW_UPDATE). Pipeline trials are interleaved medians of 5.

## Duplex realtime

This is the shape Accord is for: both peers writing and reading at once, no RPC turn.

![Duplex flood](docs/duplex.svg)

Stop-under-load (2000 progress then stop): Accord 13 µs p50 vs grpc-stream 23 µs. Replaceable progress still collapses 2001 frames to 2 on the mailbox.

![Coalesced progress](docs/coalesce.svg)

## Extra bytes

![Extra bytes per 64-byte payload](docs/overhead.svg)

| protocol | extra / msg | 64 B on the wire |
|---|---:|---:|
| accord | +4 | 68 |
| len32 | +5 | 69 |
| grpc-stream | +15 | 79 |
| json | +16 | 80 |
| grpc-unary | +33 | 97 |
| http/1.1 | +50 | 114 |

## Pipeline

Unidirectional flood, one ack. At 64 B Accord edges the gRPC DATA path; at 1 KiB everyone clusters on memcpy.

![Pipeline throughput](docs/pipeline.svg)

Ping-pong RTT is ~4.2 µs p50 for every binary codec (syscall floor).

## RSS at many connections

8 000 inflight is not 8 000 `Session`s — a handful of duplex links × many streams. Holding connections is what costs RSS (`Io.concurrent` reader stacks).

![RSS vs live connections](docs/rss.svg)

| shape | time | ΔRSS |
|---|---:|---:|
| pipeline 8000 msgs, 1 conn | 0.5 ms | ~0 |
| mailbox 8000 posts | 0.1 ms | 0.81 MB |
| 128 live sessions | — | 13.1 MB (28.4 MB RSS) |
| 8000 conns (scaled from 128) | — | ~816 MB |

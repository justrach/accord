# Accord

A small framed, multiplexed agent-comms protocol. Same envelope on an in-process mailbox and on a Unix socket (TLS/QUIC can wrap later).

Wire preface: `ACD1` + version + reserved + `max_frame` (u16le). Frames are 4 bytes: `len:u16le | stream:u8 | kind:u4 | flags:u4`. Sequence numbers are not on the wire.

Default trust is local Unix sockets with `chmod 0600`. Do not bind this framing on TCP as-is. Both peers on a native socket speak Accord (same as gRPC); HTTP/MCP clients go through a gateway.

Requires Zig 0.15+ (developed on 0.17.0-dev).

```
zig test src/accord.zig
zig build run    # mailbox + live Unix demo
zig build eval   # framing bakeoff (ReleaseFast)
zig build load   # concurrent conns + RSS
```

Numbers below are one localhost Unix run, same process, ReleaseFast — framing + syscalls, not WAN/TLS/HPACK.

## Extra bytes

Accord’s win is the header, not memcpy. On a 64 B payload the extra bytes are:

![Extra bytes per 64-byte payload](docs/overhead.svg)

| protocol | extra / msg | 64 B on the wire |
|---|---:|---:|
| accord | +4 | 68 |
| len32 | +5 | 69 |
| grpc-stream | +15 | 79 |
| json | +16 | 80 |
| grpc-unary | +33 | 97 |
| http/1.1 | +50 | 114 |

Those +4 bytes are `[len:2][stream:1][kind:4b flags:4b]`. grpc-stream is HTTP/2 DATA (9) + gRPC prefix (5) + kind (1). grpc-unary adds HEADERS + trailers per message.

## Pipeline

20 000 messages, one ack. Binary framings cluster; this is not “faster than gRPC” on the internet.

![Pipeline throughput](docs/pipeline.svg)

Ping-pong RTT on the same socket is ~4.2 µs p50 for every binary codec (syscall floor).

## Coalesce

Replaceable progress keeps one pending slot per stream, so a thinking flood does not become a thinking flood on the wire.

![Coalesced progress](docs/coalesce.svg)

## RSS at many connections

8 000 inflight is not 8 000 `Session`s — it is a handful of connections × many streams. Holding connections is what costs RSS (`Io.concurrent` reader stacks, ~86 KB/conn at 128).

![RSS vs live connections](docs/rss.svg)

| shape | time | ΔRSS |
|---|---:|---:|
| pipeline 8000 msgs, 1 conn | 0.5 ms | ~0 |
| mailbox 8000 posts | 0.1 ms | 0.80 MB |
| 128 live sessions | — | 10.8 MB (24 MB RSS) |
| 8000 conns (scaled from 128) | — | ~676 MB |

# Accord

A small framed, multiplexed agent-comms protocol. Same envelope on an in-process mailbox and on a Unix socket (TLS/QUIC can wrap later).

Wire preface: `ACD1` + version + reserved + `max_frame` (u16le). Frames are 4 bytes: `len:u16le | stream:u8 | kind:u4 | flags:u4`. Sequence numbers are not on the wire.

Requires Zig 0.15+ (developed on 0.17.0-dev).

```
zig test src/accord.zig
zig build run    # mailbox + live Unix demo
zig build eval   # framing bakeoff (ReleaseFast)
zig build load   # concurrent conns + RSS
```

Default trust is local Unix sockets with `chmod 0600`. Do not bind this framing on TCP as-is.

# Architecture

`omq.rs` is split into a sans-I/O protocol crate, an async backend, and
compatibility layers. The protocol crate owns ZMTP correctness. The backend
owns tasks, transports, queues, reconnect, and socket semantics.

```text
user API: omq-tokio, omq-libzmq, pyomq
        |
        v
omq-tokio backend: SocketDriver, ConnectionDriver, transports, routing
        |
        v
omq-proto core: Connection, frames, mechanisms, messages, options
```

## Crates

`omq-proto` is pure protocol code. It has no file descriptors and no async
runtime. `Connection::handle_input` consumes bytes, `poll_event` emits decoded
events, `send_message` queues frames, and `poll_transmit` exposes wire bytes.

`omq-tokio` is the default runtime backend. It owns TCP, IPC, inproc, UDP,
WS/WSS, reconnect supervisors, monitor events, socket actors, connection
drivers, and hot-path send/recv shortcuts.

`omq-libzmq` exposes a libzmq-compatible C ABI. `bindings/pyomq` exposes sync
and asyncio Python APIs through PyO3. `yring` and `blume` provide hot-path
queues used by inproc and routing.

## Socket Model

Every socket has one logical inbound queue and one logical outbound routing
surface. Connected peers attach driver tasks to those surfaces.

```text
Socket::send -> SendSubmitter / SocketDriver -> per-peer send pipe
per-peer driver -> recv_tx / SocketDriver -> Socket::recv
```

`SocketDriver` owns state that must be serialized: peer table, bind/connect
lifecycles, reconnect timers, monitor events, ROUTER identities, XPUB
subscriptions, DISH groups, and REQ/REP type state. It is not on every hot
message path.

Stateless sends bypass the actor through `SendSubmitter`. REQ/REP still check
shared type state before submit. Plain recv paths bypass the actor when no
identity, group, or subscription post-processing is needed.

## Messages

`Payload` and `Message` are small-value enums optimized for common single-part
traffic.

```rust
enum PayloadInner {
    Empty,
    Inline { len: u8, data: [u8; 62] },
    Single(Bytes),
}

enum MessageInner {
    Empty,
    Inline { len: u8, data: [u8; 71] },
    Single(Payload),
    Multi(Vec<Payload>),
}
```

`Payload` is 64 B and stores up to 62 B inline. `Message` is 80 B and stores up
to 71 B inline, so 64 B user messages avoid heap allocation and refcount
traffic. Larger single-part messages use `Bytes`; multipart messages use
`Vec<Payload>`.

## Encoding

`FrameBuffer` is the outbound framing buffer: a 256 KiB arena plus an entry list.
Frame headers always go into the arena. Small messages below `ARENA_THRESHOLD`
encode header and payload contiguously into the arena. Large messages write
the header into the arena and keep payload `Bytes` as external entries for
gather write.

`PeerTransmitSlot` wraps `FrameBuffer` in a short-held `std::sync::Mutex`, capped
at 512 KiB (close to the kernel TCP send buffer). Socket handles encode into
the slot. `ConnectionDriver` owns the writer and flushes from its `data_ready`
select branch. Producer-to-consumer signaling uses `DataSignal`: an atomic
flag plus `Notify` that coalesces wakes so only the `false`-to-`true`
transition fires `notify_one`. The consumer clears the flag before draining,
then calls `rearm_if_nonempty` to self-wake if data remains. For
budget-interrupted drains, `reschedule` fires unconditionally.

CURVE and BLAKE3ZMQ keep per-connection nonce state, so encrypted traffic uses
per-connection ordered transforms. LZ4 fan-out may encode once at socket level
when wire bytes are identical for all matched subscribers.

## Routing

Round-robin sockets (`PUSH`, `DEALER`, `REQ`, `CLIENT`, `SCATTER`) use per-peer
`yring` send pipes for both byte-stream and inproc peers. The submitter scans
active pipes from a moving cursor. If every active pipe is full, async send
waits on a rotating peer and `try_send` reports HWM backpressure.

`FallbackQueue` remains only as the no-peer/pre-connect fallback. Peer tasks drain
it before newer pipe-fed sends, so messages queued before handshake are not
overtaken.

Fan-out sockets (`PUB`, `XPUB`, `RADIO`) encode once and distribute matching
wire bytes. Wide multi-thread fan-out may use shard workers. Each shard owns
its peer filter state, pushes encoded items into peer rings, then signals
touched peers once per batch. Fan-out sockets drop on mute; `OnMute::Block`
does not make `PUB` or `XPUB` wait. `xpub_nodrop` stays on the direct
backpressure path.

Identity-routed sockets (`ROUTER`, `REP`, `SERVER`, `PEER`) route by peer
identity. Exclusive sockets (`PAIR`, `CHANNEL`) target one peer. Fair-queue
recv preserves per-peer ordering while rotating across peers.

## Inproc

Inproc bypasses ZMTP framing and kernel I/O. Cross-thread peers use `yring`
send pipes and deliver `InboundFrame::Message` through `inproc_peer_driver`.
Same-thread paths use `blume` batching where applicable. Public semantics
remain the same: HWM backpressure, round-robin fairness, and
connect-before-bind.

## Transports

| URI | implementation |
| --- | --- |
| `tcp://host:port` | TCP with `TCP_NODELAY` |
| `ipc:///path` | Unix stream or Windows named pipe |
| `inproc://name` | in-process channel |
| `udp://host:port` | datagrams for RADIO/DISH |
| `lz4+tcp://host:port` | TCP plus LZ4 transform |
| `ws://...`, `wss://...` | ZWS over WebSocket, optional TLS |

Reconnect supervisors replay subscriptions and groups after reconnect. Handles
with no active peer fall back to bounded pre-connect queues.

## Mechanisms And Monitoring

Mechanisms live under `omq-proto/src/proto/mechanism/`: NULL is always on;
PLAIN, CURVE, LZ4, WS, and BLAKE3ZMQ are feature-gated.

`Socket::monitor()` returns a `Stream<Item = MonitorEvent>`. Events carry owned
`PeerInfo` snapshots for listening, accept/connect, delayed connect,
handshake, disconnect, peer command, and close.

## Source Map

Protocol: `omq-proto/src/message.rs`, `frame_buffer.rs`, `flow.rs`,
`routing.rs`, `subscription.rs`, `proto/connection/`, `proto/frame.rs`.

Backend: `omq-tokio/src/socket/actor/`, `socket/handle.rs`,
`engine/driver.rs`, `engine/send_pipe.rs`, `engine/transmit_slot.rs`,
`engine/signal.rs`, `routing/`, and `transport/`.

To add a socket type, extend `omq_proto::proto::SocketType`, compatibility
checks, protocol routing, and the matching backend strategy. To add a
transport or mechanism, extend endpoint/mechanism parsing first, then add the
backend module and integration tests.

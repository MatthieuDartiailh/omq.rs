# Architecture

A high-level tour of the codebase: which crate does what, how a message
travels from `Socket::send` to the wire and back, and where the two
runtime backends differ. Detail lives in [`compio.md`](compio.md) and
[`tokio.md`](tokio.md). The optimization history lives in
[`performance.md`](performance.md).

## Three-layer split

```text
+------------------------------------------------------------------+
|  user code                                                       |
|  depends directly on one backend crate:                          |
|         omq-tokio   (default, multi-thread, mio/epoll/kqueue)    |
|         omq-compio  (single-thread io_uring/IOCP)                |
+------------------------------------------------------------------+
        |  Socket::send / Socket::recv / connect / bind / monitor
        v
+------------------------------------------------------------------+
|  runtime backend  (omq-tokio or omq-compio)                      |
|  ----------------------------------------                        |
|  Socket actor / direct-IO state                                  |
|  per-connection driver tasks                                     |
|  transports: TCP / IPC / inproc / UDP                            |
|  monitor: lifecycle event Stream                                 |
+------------------------------------------------------------------+
        |  Connection::handle_input(bytes)        (in)
        |  Connection::poll_event() -> Event      (out)
        |  Connection::send_message(msg)          (in)
        |  Connection::poll_transmit / advance    (out)
        v
+------------------------------------------------------------------+
|  omq-proto  (sans-I/O ZMTP core)                                 |
|  ----------------------------------------                        |
|  Connection      ZMTP 3.x codec + state machine                  |
|  Greeting        version negotiation                             |
|  Mechanism       NULL / PLAIN / CURVE / BLAKE3ZMQ                |
|  Transform       lz4 encode/decode                               |
|  Endpoint        URI parsing                                     |
|  Subscription    patricia-trie matcher                           |
|  Payload/Message multi-chunk Bytes types                         |
+------------------------------------------------------------------+
```

`omq-proto` never touches a file descriptor. Bytes go in via
`handle_input`, events come out via `poll_event`, outbound frames
accumulate via `send_message`/`send_command` and are read via
`poll_transmit`/`advance_transmit`. The runtime backends own the I/O
loop and call those methods.

The shape mirrors `rustls::ConnectionCommon` and `quinn-proto`: codec
state isolated from runtime so the same protocol code drives every
backend.

## Two-queue socket model

Each socket has exactly two queues, regardless of how many peers are
connected:

```text
                    Socket::recv
                         ^
                         |
              +----------+-----------+
              |   socket-wide        |   <- single queue per socket
              |   inbound queue      |      (no per-peer recv queues)
              |   (InboundFrame)     |
              +----------------------+
                  ^       ^       ^
                  |       |       |     drivers push decoded
                  |       |       |     messages straight in
        +---------+--+ +--+----+ +-+-----+
        | driver A   | | drv B | | drv C |   per-connection
        | peer A     | | peer B| | peer C|   driver tasks
        +----+-------+ +-+-----+ +-+-----+
             |           |         |
             v           v         v          write_vectored
            TCP / IPC / inproc / UDP           to file descriptors
             ^           ^         ^
             |           |         |     work-stealing: any
        +----+-----------+---------+----+  idle driver takes
        | socket-wide outbound queue    |  the next message
        | (bounded by send_hwm)         |
        +----------------+--------------+
                         ^
                         |
                    Socket::send
```

Slow peers do not corner the socket. A blocked driver leaves messages
in the shared outbound queue; faster drivers steal them. The shared
queue's bound is the socket's HWM. Backpressure is a single cap, not a
per-peer matrix.

This is the core simplification over libzmq's pipe-per-peer plus
dedicated I/O thread model. It also avoids head-of-line blocking
patterns where a single non-draining peer freezes the socket.

Round-robin patterns (`PUSH`, `DEALER`, `REQ`, `CLIENT`, `SCATTER`)
use this shape directly. Exclusive patterns (`PAIR`, `CHANNEL`) have
at most one peer and skip the shared queue entirely. Fan-out patterns
(`PUB`, `XPUB`, `RADIO`) fan from one outbound queue into per-peer
subscription filters. Identity-routed patterns (`ROUTER`, `REP`,
`SERVER`, `PEER`) look up the peer by destination identity and bypass
the shared queue.

## Message and Payload types

Both are custom enums (not SmallVecs) tuned for the common decode
path. Each is exactly 64 bytes (one cache line):

```rust
// Payload: 64 bytes. One decoded ZMTP frame.
enum PayloadInner {
    Empty,
    Inline { len: u8, data: [u8; 62] },  // no heap, no Arc
    Single(Bytes),                         // one owned chunk
}

// Message: 64 bytes. One or more frames (parts).
enum MessageInner {
    Empty,
    Inline { len: u8, data: [u8; 55] },  // single-frame <= 55 B
    Single(Payload),
    Multi(Vec<Payload>),
}
```

Inline variants cover payloads up to 62 bytes and single-frame
messages up to 55 bytes with zero refcounting overhead. The codec's
fast path (`try_advance_ready`) constructs `MessageInner::Inline`
directly from the input buffer, skipping the intermediate `Payload`.

Layers prepend static prefixes (sentinels, identities, ZMTP frame
headers) by pushing extra `Bytes` chunks onto a `Payload`, never by
copying the payload itself. At write time the codec flattens chunks
into a `Vec<IoSlice>` and the kernel stitches them via `writev` /
`sendmsg`.

`Payload` is `pub` in `omq-proto` but not re-exported from the
backends. Users see only `Message`. Public API: `Deref<[u8]>`
(single-part only), `From<Message> for Bytes`, `msg.iter()`,
`msg.pop_front()`, `msg.part_bytes(idx)`, `Message::with_prefix()`.

## Backends compared

| | omq-tokio | omq-compio |
|---|---|---|
| Runtime | Multi-thread, work-stealing | Single-thread, cooperative |
| Linux I/O | epoll (mio) | io_uring |
| Other platforms | macOS/BSD kqueue, Windows IOCP | macOS kqueue, Windows IOCP |
| Hot-path send | Per-peer `PeerWireSlot` (`EncodedQueue` under `std::sync::Mutex`); driver flushes via `data_ready` select arm | Per-peer `EncodedQueue` under sync `try_lock` |
| Hot-path recv | Connection driver pushes straight into user `recv_tx` | `RecvMulti` (multi-shot recv from io_uring `BUF_RING`) fed to codec inline |
| Fan-in scaling | Free across cores via runtime | One runtime per worker thread (manual) |
| Strengths | Multi-peer fan-in, no per-thread setup, ecosystem fit | Small-message wire throughput, low syscall cost, low jitter |

Both expose an identical public `Socket` API. Anything on one that is
not on the other is a bug. Verified by
`omq-{compio,tokio}/tests/coverage_matrix.rs` (every socket-type x
transport cell on each backend) and `omq-tokio/tests/interop_compio.rs`
(cross-runtime ZMTP wire compatibility).

## Transports

| URI scheme | Transport | Backends |
|---|---|---|
| `tcp://host:port` | TCP, `TCP_NODELAY` set on accept/connect | both |
| `ipc:///path` | IPC: Unix domain stream (Linux/macOS/BSD), named pipes (Windows) | both |
| `inproc://name` | In-process channel; bypasses ZMTP codec entirely | both |
| `udp://host:port` | UDP datagram (`RADIO` / `DISH` only) | both |
| `lz4+tcp://host:port` | TCP + LZ4 transform | both, feature `lz4` |
| `ws://host:port/path` | ZeroMQ over WebSocket (ZWS/2.0, RFC 45) | both, feature `ws` |
| `wss://host:port/path` | ZeroMQ over WebSocket with TLS | both, feature `ws` |

Compression-style schemes are runtime layers stacked on top of the TCP
transport, not separate transports. The encoder/decoder live in
`omq-proto` and are wired in by the backend after the ZMTP handshake.

WebSocket (`ws://`, `wss://`) is a separate transport with its own
driver loop (`ws_driver`). ZWS replaces ZMTP's byte-stream framing
with WebSocket binary messages: each message is one ZMTP frame
prefixed by a 1-byte flag (`0x00` final, `0x01` more, `0x02`
command). The 64-byte ZMTP greeting is skipped; mechanism negotiation
happens via `Sec-WebSocket-Protocol` header during the HTTP upgrade.

## Transport type dispatch

Each backend needs a single concrete type for a peer's I/O channel
that works for both TCP and IPC without threading a type parameter
through every struct above it. Two approaches exist.

`Box<dyn Trait>` erases the stream type at runtime: a fat pointer
whose vtable is resolved on every read or write. No generics propagate
upward, but every frame operation pays an indirect call that blocks
inlining and cannot be predicted by the CPU branch predictor.

omq uses a closed enum instead:

```rust
enum WireReader { Tcp(AsyncFd<TcpStream>), Ipc(AsyncFd<UnixStream>) }
enum WireWriter { Tcp(OwnedWriteHalf<TcpStream>), Ipc(OwnedWriteHalf<UnixStream>) }
```

The enum erases the transport type for the same structural reason:
`PeerIo` holds a `WireReader` with no type parameter. The compiler
resolves the match statically and inlines through each arm.
Hot-path reads and writes are direct calls at zero indirection cost.

`Box<dyn>` would only be warranted if transports were user-extensible
at runtime. The transport set is fixed at build time, so the enum is
strictly better: same ergonomics, no vtable.

## Mechanisms

| Name | Cargo feature | Notes |
|---|---|---|
| `NULL` | always available | RFC 23 |
| `PLAIN` | `plain` | RFC 24, username/password with authenticator callback |
| `CURVE` | `curve` | RFC 26, X25519 + ChaCha20Poly1305 |
| `BLAKE3ZMQ` | `blake3zmq` | OMQ-native, Noise XX + BLAKE3 + ChaCha20-BLAKE3, [RFC](https://github.com/paddor/omq-blake3zmq/blob/main/RFC.md), unaudited |

All mechanisms are sans-I/O state machines under
`omq-proto/src/proto/mechanism/`. The greeting state machine selects
the mechanism from the wire and routes incoming frames to the right
handshake.

## Monitor

`Socket::monitor()` returns a `Stream<Item = MonitorEvent>` shaped like
a socket. Each event carries an owned `PeerInfo` snapshot
(connection ID, endpoint, identity, type, mechanism). Events:

- `Listening`, `Accepted` (server side)
- `Connected`, `ConnectDelayed` (client side)
- `HandshakeSucceeded`, `Disconnected`
- `PeerCommand` (incoming `SUBSCRIBE` / `JOIN` / etc.)
- `Closed`

Multiple subscribers per socket; each gets an independent stream with
its own lag counter (returned as `Err(Lagged(n))` if the receiver fell
behind).

## Source file map

### omq-proto

| file | what |
|------|------|
| `src/message.rs` | `Payload` + `Message` enums, inline/single/multi variants |
| `src/proto/connection/` | `Connection` -- the sans-I/O ZMTP codec + state machine (inbound, outbound, mod) |
| `src/proto/frame.rs` | ZMTP frame encoding/decoding, `encode_message_flat`, `write_frame_header` |
| `src/proto/greeting.rs` | ZMTP greeting state machine |
| `src/proto/command.rs` | ZMTP commands (SUBSCRIBE, PING, etc.) |
| `src/proto/chunked_buf.rs` | `ChunkedInputBuf` -- zero-copy multi-chunk input buffer |
| `src/proto/mechanism/` | Mechanism dispatch + handshakes (NULL / PLAIN / CURVE / BLAKE3ZMQ) |
| `src/proto/transform/` | LZ4 per-part encoder/decoder |
| `src/proto/zws.rs` | ZWS/2.0 frame codec (feature `ws`) |
| `src/endpoint.rs` | URI parsing (`tcp://`, `ipc://`, `lz4+tcp://`, `ws://`, etc.) |
| `src/options.rs` | `Options` builder (HWM, identity, keepalive, mechanism) |
| `src/encoded_queue.rs` | `EncodedQueue` -- arena + entry-based gather-write encoder (used by both backends) |
| `src/routing.rs` | Socket-type-to-routing-strategy categorization (`SendCategory`, `RecvCategory`) |
| `src/subscription.rs` | Patricia-trie prefix matcher for SUB/XSUB |

### omq-compio

| file | what |
|------|------|
| `src/socket/handle.rs` | Public `Socket` handle -- send/recv/connect/bind/close |
| `src/socket/inner.rs` | `SocketInner` -- shared socket state, peer slots |
| `src/socket/direct_io.rs` | `DirectIoState` -- per-wire-peer fast-path state (Cell-based fields, EncodedQueueCell) |
| `src/socket/send.rs` | Send strategies (round-robin, fan-out, identity) |
| `src/socket/recv.rs` | Recv path, direct-recv claim arbitration |
| `src/socket/dial.rs` | TCP/IPC dial supervisors with reconnect |
| `src/socket/install.rs` | Peer slot installation, wire driver spawning |
| `src/transport/driver.rs` | Per-connection driver loop (`run_connection`) |
| `src/transport/peer_io.rs` | `PeerIo`, `WireReader`/`WireWriter`, `RecvStream` |
| `src/transport/tcp.rs` | TCP bind/connect/accept |
| `src/transport/ipc.rs` | IPC bind/connect |
| `src/transport/inproc.rs` | In-process transport (no ZMTP; blume MPSC for fan-in, yring SPSC for eligible cross-thread pairs) |
| `src/transport/ws.rs` | WS bind/connect/accept (feature `ws`) |
| `src/transport/ws_driver.rs` | WS connection driver (feature `ws`) |
| `src/monitor.rs` | `MonitorPublisher` + `MonitorStream` |

### omq-tokio

| file | what |
|------|------|
| `src/socket/actor.rs` | `SocketDriver` actor -- peer table, type state, routing |
| `src/socket/handle.rs` | Public `Socket` handle |
| `src/socket/dispatch.rs` | Send-side dispatch (actor bypass for non-REQ/REP) |
| `src/engine/driver.rs` | Per-connection `ConnectionDriver` |
| `src/engine/wire_slot.rs` | `PeerWireSlot` -- per-peer wire buffer; driver flushes via `data_ready` |
| `src/routing/mod.rs` | `SendStrategy`/`RecvStrategy` dispatch |
| `src/routing/round_robin.rs` | Round-robin submitter |
| `src/routing/exclusive.rs` | PAIR/CHANNEL single-peer submitter |
| `src/routing/fan_out.rs` | PUB/XPUB/RADIO fan-out with subscription filter |
| `src/routing/identity.rs` | ROUTER/REP/SERVER identity routing |
| `src/routing/peer_send.rs` | `PeerSend` enum -- unified per-peer send dispatch (`Wire`/`Inbox`) |
| `src/routing/fair_queue.rs` | PULL/SUB fair-queue recv |
| `src/transport/tcp.rs` | TCP bind/connect with reconnect |
| `src/transport/ipc.rs` | IPC bind/connect |
| `src/transport/inproc.rs` | In-process transport (yring SPSC for eligible cross-thread pairs) |
| `src/transport/udp.rs` | UDP datagram transport (RADIO/DISH) |
| `src/transport/ws.rs` | WS bind/connect/accept + WS connection driver (feature `ws`) |

## Adding a new socket type / transport / mechanism

**Socket type.** Add the variant to `omq_proto::proto::SocketType` and
to `is_compatible`. Add it to `send_category` and `recv_category` in
`omq-proto/src/routing.rs`. Wire send/recv strategy in both backends'
`routing/` (tokio) or socket actor (compio). The two backends must
stay in lockstep.

**Transport.** Add an `Endpoint` variant and parser in
`omq-proto/src/endpoint.rs`. Add `transport/<name>.rs` in each backend.
Compression-style transports (`lz4+tcp`) are implemented as
`MessageEncoder` / `MessageDecoder` layers on top of `tcp`, not as
separate transports.

**Mechanism.** Add a module under `omq-proto/src/proto/mechanism/`.
Feature-gate it. Register it with the greeting/handshake state machine.
Add an integration test in **both** backends'
`tests/<mechanism>.rs`.

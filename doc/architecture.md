# Architecture

A high-level tour of the codebase: which crate does what, how a message
travels from `Socket::send` to the wire and back, and where the two
runtime backends differ. Detail lives in [`compio.md`](compio.md),
[`tokio.md`](tokio.md), and [`performance.md`](performance.md).

## Three-layer split

```
+------------------------------------------------------------------+
|  user code                                                       |
|  +-> omq (facade, picks one backend at build time):              |
|         omq-compio  (default, single-thread io_uring/IOCP)       |
|         omq-tokio   (multi-thread, mio/epoll/kqueue)             |
+------------------------------------------------------------------+
        |  Socket::send / Socket::recv / connect / bind / monitor
        v
+------------------------------------------------------------------+
|  runtime backend  (omq-compio or omq-tokio)                      |
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
|  Mechanism       NULL / CURVE / BLAKE3ZMQ                        |
|  Transform       lz4 / zstd encode/decode                        |
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

```
                    Socket::recv
                         ^
                         |
              +----------+-----------+
              |   socket-wide        |   <- single queue per socket
              |   inbound queue      |      (no per-peer recv queues)
              |   (InprocFrame)      |
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

Round-robin patterns (`PUSH`, `DEALER`, `REQ`, `PAIR`, `CLIENT`,
`CHANNEL`, `SCATTER`) use this shape directly. Fan-out patterns (`PUB`,
`XPUB`, `RADIO`) fan from one outbound queue into per-peer subscription
filters. Identity-routed patterns (`ROUTER`, `REP`, `SERVER`, `PEER`)
look up the peer by destination identity and bypass the shared queue.

The optional `priority` Cargo feature swaps round-robin work-stealing
for strict per-pipe priority tiers (nanomsg-style 1..=255, lower =
higher priority). Higher tiers ship first; lower tiers run only when
higher are blocked or disconnected.

## Multi-chunk frame payloads

```rust
type Payload = SmallVec<[Bytes; 2]>;    // 2 chunks inline
type Message = SmallVec<[Payload; 3]>;  // 3 frames inline
```

`Bytes::clone` is one atomic increment, never a memcpy. Layers prepend
their static prefixes (sentinels, identities, ZMTP frame headers) by
pushing extra `Bytes` onto a `Payload`, never by copying the payload.
At write time the codec emits a `Vec<IoSlice>` and the kernel stitches
the chunks via `writev` / `sendmsg`. Inline storage covers the common
shapes -- single-frame payloads, REQ/REP three-part envelopes --
without heap allocation.

The codec exposes accessors (`Payload::as_bytes`, `as_slice`,
`is_contiguous`) so callers inspect single-chunk payloads without
coalescing.

## Backends compared

| | omq-compio | omq-tokio |
|---|---|---|
| Runtime | Single-thread, cooperative | Multi-thread, work-stealing |
| Linux I/O | io_uring | epoll (mio) |
| Other platforms | macOS kqueue, Windows IOCP | macOS/BSD kqueue, Windows IOCP |
| Hot-path send | Per-peer `EncodedQueue` under sync `try_lock` | Direct push into per-driver flume queue |
| Hot-path recv | Inline `read` on the FD via `PollFd::read_ready` | Connection driver pushes straight into user `recv_tx` |
| Fan-in scaling | One runtime per worker thread (manual) | Free across cores via runtime |
| Strengths | Small-message wire throughput, low syscall cost, low jitter | Multi-peer fan-in, no per-thread setup, ecosystem fit |

Both expose an identical public `Socket` API. Anything on one that is
not on the other is a bug. Verified by
`omq-{compio,tokio}/tests/coverage_matrix.rs` (every socket-type x
transport cell on each backend) and `omq-tokio/tests/interop_compio.rs`
(cross-runtime ZMTP wire compatibility).

## Transports

| URI scheme | Transport | Backends |
|---|---|---|
| `tcp://host:port` | TCP, `TCP_NODELAY` set on accept/connect | both |
| `ipc:///path` | Unix domain stream | both |
| `inproc://name` | In-process channel; bypasses ZMTP codec entirely | both |
| `udp://host:port` | UDP datagram (`RADIO` / `DISH` only) | both |
| `lz4+tcp://host:port` | TCP + LZ4 transform | both, feature `lz4` |
| `zstd+tcp://host:port` | TCP + zstd transform with optional dict training | both, feature `zstd` |

Compression-style schemes are runtime layers stacked on top of the TCP
transport, not separate transports. The encoder/decoder live in
`omq-proto` and are wired in by the backend after the ZMTP handshake.

## Mechanisms

| Name | Cargo feature | Notes |
|---|---|---|
| `NULL` | always available | RFC 23 |
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

## Adding a new socket type / transport / mechanism

**Socket type.** Add the variant to `omq_proto::proto::SocketType` and
to `is_compatible`. Wire send/recv strategy in both backends'
`routing/` (tokio) or socket actor (compio). The two backends must
stay in lockstep.

**Transport.** Add an `Endpoint` variant and parser in
`omq-proto/src/endpoint.rs`. Add `transport/<name>.rs` in each backend.
Compression-style transports (`lz4+tcp`, `zstd+tcp`) are implemented as
`MessageEncoder` / `MessageDecoder` layers on top of `tcp`, not as
separate transports.

**Mechanism.** Add a module under `omq-proto/src/proto/mechanism/`.
Feature-gate it. Register it with the greeting/handshake state machine.
Add an integration test in **both** backends'
`tests/<mechanism>.rs`.

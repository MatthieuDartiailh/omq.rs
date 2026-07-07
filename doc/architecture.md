# Architecture

A high-level tour of the codebase: which crate does what, how a message
travels from `Socket::send` to the wire and back, and where protocol
logic stops and runtime code starts.

## Three-layer Split

```text
+------------------------------------------------------------------+
| user code                                                        |
| depends on omq-tokio, omq-libzmq, or pyomq                       |
+------------------------------------------------------------------+
        | Socket::send / Socket::recv / connect / bind / monitor
        v
+------------------------------------------------------------------+
| omq-tokio runtime backend                                        |
| SocketDriver actor, routing strategies, connection drivers        |
| transports: TCP / IPC / inproc / UDP / WS / WSS                  |
| monitor: lifecycle event Stream                                  |
+------------------------------------------------------------------+
        | Connection::handle_input(bytes)        (in)
        | Connection::poll_event() -> Event      (out)
        | Connection::send_message(msg)          (in)
        | Connection::poll_transmit / advance    (out)
        v
+------------------------------------------------------------------+
| omq-proto sans-I/O ZMTP core                                     |
| Connection, greeting, mechanisms, transforms, endpoints, messages |
+------------------------------------------------------------------+
```

`omq-proto` never touches a file descriptor. Bytes go in via
`handle_input`, events come out via `poll_event`, outbound frames
accumulate via `send_message`/`send_command` and are read via
`poll_transmit`/`advance_transmit`. `omq-tokio` owns the I/O loop and
calls those methods.

The shape mirrors `rustls::ConnectionCommon` and `quinn-proto`: codec
state is isolated from runtime code.

## Two-queue Socket Model

Each socket has exactly two logical queues, regardless of how many peers
are connected:

```text
                    Socket::recv
                         ^
                         |
              +----------+-----------+
              | socket-wide inbound  |
              | queue / recv channel |
              +----------------------+
                  ^       ^       ^
                  |       |       |
        +---------+--+ +--+----+ +-+-----+
        | driver A   | | drv B | | drv C |   per-connection tasks
        +----+-------+ +-+-----+ +-+-----+
             |           |         |
             v           v         v
            TCP / IPC / inproc / UDP / WS
             ^           ^         ^
             |           |         |
        +----+-----------+---------+----+
        | socket-wide outbound routing  |
        | strategy, bounded by send_hwm |
        +----------------+--------------+
                         ^
                         |
                    Socket::send
```

Round-robin patterns (`PUSH`, `DEALER`, `REQ`, `CLIENT`, `SCATTER`)
use active per-peer pipes plus a shared fallback queue. Exclusive
patterns (`PAIR`, `CHANNEL`) have at most one peer. Fan-out patterns
(`PUB`, `XPUB`, `RADIO`) fan from one outbound message into matching
per-peer targets. Identity-routed patterns (`ROUTER`, `REP`, `SERVER`,
`PEER`) look up the destination identity and target that peer.

## Message And Payload Types

`Payload` and `Message` are custom enums tuned for the common decode
path. Each is exactly 64 bytes:

```rust
enum PayloadInner {
    Empty,
    Inline { len: u8, data: [u8; 62] },
    Single(Bytes),
}

enum MessageInner {
    Empty,
    Inline { len: u8, data: [u8; 55] },
    Single(Payload),
    Multi(Vec<Payload>),
}
```

Inline variants cover payloads up to 62 bytes and single-frame messages
up to 55 bytes with no heap allocation. Larger payloads are `Bytes`
chunks. Layers prepend static prefixes by pushing extra chunks, not by
copying the payload itself.

## Transports

| URI scheme | Transport |
|---|---|
| `tcp://host:port` | TCP, `TCP_NODELAY` set on accept/connect |
| `ipc:///path` | Unix domain stream on Unix, named pipes on Windows |
| `inproc://name` | In-process channel; bypasses ZMTP codec entirely |
| `udp://host:port` | UDP datagram (`RADIO` / `DISH` only) |
| `lz4+tcp://host:port` | TCP + LZ4 transform, feature `lz4` |
| `ws://host:port/path` | ZeroMQ over WebSocket, feature `ws` |
| `wss://host:port/path` | ZeroMQ over WebSocket with TLS, feature `ws` |

Compression-style schemes are runtime layers stacked on top of TCP, not
separate transports. The encoder/decoder live in `omq-proto` and are
wired in by `omq-tokio` after the ZMTP handshake.

## Transport Type Dispatch

`omq-tokio` uses closed enums instead of `Box<dyn Trait>` for hot
transport operations:

```rust
enum WireReader { Tcp(AsyncReadHalf<TcpStream>), Ipc(OwnedReadHalf) }
enum WireWriter { Tcp(OwnedWriteHalf<TcpStream>), Ipc(OwnedWriteHalf) }
```

The enum erases the transport type without a vtable. The compiler
resolves the match statically and inlines through each arm.

## Tokio Runtime Backend

`omq-tokio` is the runtime backend. Tokio is preemptive and
work-stealing across cores. Per-connection driver tasks push into one
socket-wide inbound queue. Send-side routing depends on socket type:
single-peer round-robin uses a direct wire slot, multi-peer round-robin
uses active per-peer pipes, fan-out, identity, and exclusive routes use
per-peer targets, and a shared fallback queue remains for no-peer and
inproc paths.

```text
                    Socket::recv
                         ^
                         |    (recv_tx is async_channel; bounded by recv_hwm)
                         |
              +----------+-----------+
              |   user-facing        |
              |   recv channel       |
              +----------------------+
                  ^       ^       ^
                  | direct push    |
        +---------+--+ +--+----+ +-+-----+
        | conn drv A | | drv B | | inproc|   ConnectionDriver tasks
        | TCP/IPC    | | TCP   | | peer  |   one per peer
        +----+-------+ +-+-----+ +-+-----+
             ^           ^         |
             | wire      | wire    |    PeerWireSlot: handle encodes,
             | slot A    | slot B  |    driver flushes via data_ready
        +----+-----------+---------+----+
        |     SocketDriver actor        |   <- owns peer table, type
        |   (cmd_tx in, peer_out in)    |      state, routing strategy
        +----------------+--------------+
                         ^
                         |    Socket::send routes here only when actor
                         |    state must mutate. Other sends bypass to
                         |    SendSubmitter.
                         |
                    Socket::send
```

### SocketDriver Actor

`SocketDriver` owns mutable state that must be serialized:

- `HashMap<PeerId, PeerInfo>` for connected TCP, IPC, inproc, UDP, WS,
  and WSS peers.
- `TypeState` for REQ/REP alternation, ROUTER identity prefixes, DISH
  groups, XPUB subscriptions, and conflate state.
- `SendStrategy` and `RecvStrategy` for round-robin, fan-out,
  identity-route, exclusive, and fair-queue policy.
- Bind, connect, disconnect, listener, dialer, and reconnect timers.

The actor receives user commands through `cmd_tx` and driver events
through `peer_out`. REQ and REP sends still go through actor-owned
state because `pre_send` mutates the alternation state. ROUTER, REP,
SERVER, PEER, DISH, and XPUB recv paths also go through the actor
because they need identity, group, or subscription post-processing.

This actor is the serialization point for rare stateful events. It is
not the hot path for message flow when no actor state changes.

### Send Bypass

`Inner` holds a cloneable `SendSubmitter` built from `SendStrategy`
before the driver starts. `Socket::send` validates frame counts and
pushes directly into the submitter for PUSH, DEALER, PUB, PAIR, CLIENT,
SCATTER, CHANNEL, and similar stateless send paths.

REQ and REP lock a shared `Arc<Mutex<TypeState>>`, call `pre_send`
inline, then push the transformed message through the submitter. The
actor uses the same `TypeState` for `post_recv` and peer disconnect
handling. REQ/REP alternation prevents normal send/recv contention.

### Recv Bypass

For socket types whose recv path is plain fair-queue delivery, each
`ConnectionDriver` pushes `Event::Message` straight into
`recv_tx: async_channel::Sender<Message>`. This skips `peer_out` and
the actor loop.

Per-peer ordering is preserved because a single driver task delivers in
TCP order. Backpressure still works because `recv_tx` is bounded by
`recv_hwm`; a full channel blocks the driver read loop and halts reads.

| Bypassed recv | Actor recv | Reason |
|---|---|---|
| PULL, DEALER, REQ, SUB, XSUB, PAIR, CLIENT, CHANNEL, GATHER | REP, ROUTER, SERVER, PEER | Identity-prefix prepending |
| | DISH | Group membership filter |
| | XPUB | Subscribe-as-message parsing |

REQ is special: the driver pushes raw envelope-wrapped messages through
the direct path, and `Socket::recv` strips the empty delimiter inline.

### Round-robin Active Pipes

Round-robin sockets (`PUSH`, `DEALER`, `REQ`, `CLIENT`, `SCATTER`) have
two hot paths:

- Single byte-stream peer: `Socket::send` encodes directly into that
  peer's `PeerWireSlot`. The driver owns the writer and flushes the
  encoded chunks from its `data_ready` select arm.
- Multiple byte-stream peers: each peer registers an active `blume`
  pipe. The socket-side submitter scans active pipes from a moving
  cursor and `try_send`s into the first pipe with capacity. If every
  active pipe is full, async `send` waits on one rotating pipe and
  `try_send` reports HWM backpressure.

The shared `DropQueue` is the fallback for no connected peer yet, inproc
peers, and mixed byte-stream plus inproc round-robin sets. Byte-stream
drivers still poll the shared queue so messages queued before a peer
becomes active drain in order before new pipe traffic.

### Arena Encoding

The send path uses `EncodedQueue` with a contiguous `arena: BytesMut`
and an `entries: VecDeque<Entry>`. Frame headers are always written into
the arena. Messages below `ARENA_THRESHOLD` are encoded contiguously:
header and payload land in one arena range, so a batch of small messages
produces one iovec instead of two per message. Larger messages use the
gather path: the header goes into the arena, and payload `Bytes` chunks
are tracked as external entries for zero-copy gather-write.

The arena path is disabled when CURVE or BLAKE3ZMQ is active because the
per-connection transform owns nonce state and must encrypt frames in
strict wire order. LZ4 does not have this constraint: its
`MessageEncoder` lives outside the codec, holds no per-frame sequence
state, and produces wire-ready bytes independently.

### PeerWireSlot

Each wire peer gets a `PeerWireSlot` containing an `EncodedQueue` behind
a short-held `std::sync::Mutex`. `Socket::send` encodes ZMTP frames into
the slot. The driver flushes them through a dedicated `data_ready`
select arm and retains exclusive ownership of the write half.

The slot replaces the old direct writer lock pattern. The handle never
touches the writer, the mutex hold time is encode-only, and the driver
does all I/O. A coalescing `pending: AtomicBool` gates
`data_ready.notify_one()`, so many rapid encodes produce one wake.

`PeerWireSlot` is used by round-robin single-peer, exclusive, fan-out,
and identity routes. Inproc peers have no slot and use the
`PeerSend::Inbox` variant.

### PUB/RADIO Fan-Out Shards

Normal lossy `PUB`, `XPUB`, and `RADIO` sends bypass the actor. On a
multi-thread Tokio runtime, `FanOutSend` creates shard workers for wide
wire fan-out; the shard count follows `available_parallelism`, matching
Tokio's default worker count. Sharding activates at four wire peers.
Earlier peers, inproc peers, and `xpub_nodrop` sockets stay on the
direct `PeerWireSlot` path; `xpub_nodrop` keeps direct dispatch so it can
preserve send backpressure.

When a wire peer becomes sharded, `PeerWireSlot::new` returns the slot
and its `yring::Producer<WireSlotItem>` separately. The shard worker owns
that producer, so per-peer fan-out pushes do not take the slot mutex.
New sharded peers are assigned to the shard with the smallest live-peer
load. Shard ring capacity is `send_hwm`.

The hot send path encodes each outbound `Message` once in the caller
thread, then takes one mutex over shard endpoints and pushes a
`Dispatch` command into each nonempty shard ring. Small encoded messages
use inline `WireSlotItem`s; larger or chunked messages use shared
`Arc<[Bytes]>` chunks. A full shard dispatch ring drops that message for
that shard, matching lossy PUB semantics. Control commands
(`AddPeer`, `RemovePeer`, `Subscribe`, `Cancel`, `Join`, `Leave`, and
`Shutdown`) are enqueued reliably and may wait for ring space.

For `lz4+tcp://`, the fan-out path owns one socket-level
`MessageEncoder`. Static dictionaries and auto-trained dictionaries are
installed on that encoder, so each outbound user message is compressed
once for the socket fan-out. The first emitted `LZ4D` dictionary shipment
is stored beside the fan-out encoder. Each `PeerWireSlot` tracks a
per-connection `fanout_dict_shipped` bit; direct fallback peers and shard
workers push the stored dictionary before the first dictionary-compressed
payload for that connection. Late subscribers therefore receive the same
single PUB socket dictionary once after subscribing, without rotating or
training per-peer dictionaries.

Messages at or above `compression_offload_threshold` activate a temporary
deferred fan-out gate. The caller snapshots the matched fallback peers and
sharded peer ids, enqueues a `DeferredFanOutMsg` into a bounded
`blume::Sender`, and returns. While the gate is active, later messages
take the same bounded hop so they cannot overtake the large message. The
single deferred worker drains in FIFO order, moves the socket
`MessageEncoder` into `spawn_blocking` for each queued message, restores
it after compression, then pushes the encoded batch to the captured route.
When the `blume` queue and pending sender count reach zero, the gate
becomes idle and callers resume the direct shard path.

Each shard worker owns its peers' subscription prefixes or RADIO groups,
filters locally, pushes matching wire items lock-free into peer rings,
then flushes and signals touched peers once per batch. This keeps
per-subscriber ordering on one path and moves wide fan-out work off the
caller after the single encoded dispatch.

### Reconnect, Monitor, And Concurrency

Dial supervisor tasks own handles that are `None` while reconnect is in
flight. Round-robin sends fall back to the shared queue bounded by
`send_hwm` while no active peer pipe exists and drain through the new
driver after handshake. Subscriptions and group joins are replayed.

Within a tokio runtime, multiple `Socket` clones can call `send` and
`recv` concurrently from different worker threads. The round-robin
active-pipe table is protected by a short `std::sync::Mutex` around the
peer vector and cursor. The recv-side `async_channel::Sender` is
multi-producer. The actor remains the serialization point for state that
must be observed atomically.

### Windows IPC

On Windows, IPC uses named pipes instead of Unix domain sockets. The
application still uses `ipc://` endpoints; `ipc:///my-pipe` maps to
`\\.\pipe\my-pipe` internally.

| Aspect | Unix | Windows |
|--------|------|---------|
| Bind path syntax | `/tmp/socket.sock` or `@abstract-ns` | `my-pipe` |
| Connection type | Stream socket | Named pipe |
| Buffer management | `SO_SNDBUF`/`SO_RCVBUF` tuning | Windows pipe defaults |
| Type dispatch | `type IpcStream = UnixStream` | `enum IpcStream { Server, Client }` |

## Mechanisms

| Name | Cargo feature | Notes |
|---|---|---|
| `NULL` | always available | RFC 23 |
| `PLAIN` | `plain` | RFC 24, username/password authenticator |
| `CURVE` | `curve` | RFC 26, X25519 + ChaCha20Poly1305 |
| `BLAKE3ZMQ` | `blake3zmq` | OMQ-native, Noise XX + BLAKE3 + ChaCha20-BLAKE3, unaudited |

All mechanisms are sans-I/O state machines under
`omq-proto/src/proto/mechanism/`.

## Monitor

`Socket::monitor()` returns a `Stream<Item = MonitorEvent>`. Each event
carries an owned `PeerInfo` snapshot. Events include `Listening`,
`Accepted`, `Connected`, `ConnectDelayed`, `HandshakeSucceeded`,
`Disconnected`, `PeerCommand`, and `Closed`.

## Source File Map

### omq-proto

| file | what |
|------|------|
| `src/message.rs` | `Payload` + `Message` enums |
| `src/proto/connection/` | sans-I/O ZMTP codec + state machine |
| `src/proto/frame.rs` | ZMTP frame encoding/decoding |
| `src/proto/greeting.rs` | ZMTP greeting state machine |
| `src/proto/command.rs` | ZMTP commands |
| `src/proto/chunked_buf.rs` | zero-copy multi-chunk input buffer |
| `src/proto/mechanism/` | NULL / PLAIN / CURVE / BLAKE3ZMQ |
| `src/proto/transform/` | LZ4 per-part encoder/decoder |
| `src/proto/zws.rs` | ZWS/2.0 frame codec |
| `src/endpoint.rs` | URI parsing |
| `src/options.rs` | `Options` builder |
| `src/encoded_queue.rs` | arena + entry-based gather-write encoder |
| `src/routing.rs` | socket-type-to-routing-strategy categorization |
| `src/subscription.rs` | Patricia-trie prefix matcher |

### omq-tokio

| file | what |
|------|------|
| `src/socket/actor/` | `SocketDriver` actor: peer table, type state, lifecycle |
| `src/socket/handle.rs` | public `Socket` handle |
| `src/socket/dispatch.rs` | send-side dispatch and actor bypass |
| `src/engine/driver.rs` | per-connection `ConnectionDriver` |
| `src/engine/wire_slot.rs` | per-peer encoded send slot |
| `src/routing/` | round-robin, fan-out, identity, exclusive, fair-queue |
| `src/transport/tcp.rs` | TCP bind/connect with reconnect |
| `src/transport/ipc.rs` | IPC bind/connect |
| `src/transport/inproc.rs` | in-process transport |
| `src/transport/udp.rs` | UDP datagram transport |
| `src/transport/ws.rs` | WS/WSS transport |

## Adding A New Socket Type / Transport / Mechanism

**Socket type.** Add the variant to `omq_proto::proto::SocketType` and
`is_compatible`. Add it to `send_category` and `recv_category` in
`omq-proto/src/routing.rs`. Wire send/recv strategy in
`omq-tokio/src/routing/` and add integration tests.

**Transport.** Add an `Endpoint` variant and parser in
`omq-proto/src/endpoint.rs`. Add a `transport/<name>.rs` module in
`omq-tokio`.

**Mechanism.** Add a module under `omq-proto/src/proto/mechanism/`,
feature-gate it, register it with the greeting/handshake state machine,
and add integration tests.

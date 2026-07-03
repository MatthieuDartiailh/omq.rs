# omq-tokio internals

A tour of `omq-tokio`: the actor-shaped multi-thread backend, its
hot-path bypass, and the routing strategies that sit on top. Cross-
cutting basics (three-layer split, two-queue model, multi-chunk
payloads) live in [`architecture.md`](architecture.md). The hot-path
techniques shared with compio (arena encoding, work-stealing,
header scratch) are described from the compio side in
[`compio.md`](compio.md).

`omq-tokio` is the multi-thread backend. Its structure differs from
compio's because tokio's runtime is preemptive and work-stealing across
cores. The same high-level shape applies: per-connection driver tasks
push into one socket-wide inbound queue. Send-side routing depends on
socket type: single-peer round-robin uses a direct wire slot, multi-peer
round-robin uses active per-peer pipes, fan-out/identity/exclusive use
per-peer targets, and a shared fallback queue remains for no-peer and
inproc paths.

## Top-level shape

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
        | conn drv A | | drv B | | drv C |   ConnectionDriver tasks
        | TCP/IPC    | | TCP   | | inproc|   one per peer
        +----+-------+ +-+-----+ +-+-----+
             ^           ^         |
             |           |         |    PeerWireSlot: handle encodes,
             | wire      | wire    |    driver flushes via data_ready
             | slot A    | slot B  |
        +----+-----------+---------+----+
        |     SocketDriver actor        |   <- owns peer table, type
        |   (cmd_tx in, peer_out in)    |      state, routing strategy
        +----------------+--------------+
                         ^
                         |    Socket::send routes here only when
                         |    actor state must mutate (e.g. REQ/REP);
                         |    everything else bypasses straight to
                         |    the SendSubmitter (see below).
                         |
                    Socket::send
```

The `SocketDriver` is a textbook actor in the sense that it owns
mutable state nobody else can touch and the outside world communicates
with it via channels. The bypass paths described below carry the
common message flow around it.

## State the actor owns

- `HashMap<PeerId, PeerInfo>` -- every connected peer (TCP/IPC/inproc/
  UDP), including each peer's driver handle, monitor handle, codec
  config.
- `TypeState` -- REQ/REP alternation flag, ROUTER identity-prefix
  table, DISH group memberships, XPUB subscription trie, conflate flag.
- `SendStrategy` + `RecvStrategy` -- round-robin, fan-out,
  identity-route, fair-queue policy.
- bind/connect/disconnect bookkeeping -- listener tasks, dialer tasks,
  reconnect timers.

## Channels into the actor

- `cmd_tx: mpsc::Sender<SocketCommand>` from user handles. Carries
  `Bind`, `Connect`, `Send`, `Subscribe`, etc. REQ/REP `Send` keeps
  going through here because `pre_send` flips the alternation bit --
  real per-message state mutation that must be serialized against
  concurrent `Socket` clones.
- `peer_out: mpsc::Sender<(PeerId, PeerOut)>` from connection drivers.
  Carries `Connected`, `Disconnected`, `Event(msg)`. Recv types that
  need post-processing send `Event` here; bypass-eligible types skip
  this hop.

This is the same pattern `tokio-tungstenite`, `redis-rs`, and `quinn`
use: a single task serializes mutation of state that has many
concurrent sources of input. It's the right pattern for **rare,
stateful, multi-source events** -- bind, connect, subscribe,
identity-route lookups, monitor fan-out, HWM accounting, conflate.

It is **not** the right pattern for the per-message hot path when no
actor state actually mutates per-message.

## Send bypass (`Socket::send`)

For PUSH/DEALER/PUB/PAIR/CLIENT/SCATTER/CHANNEL send, `TypeState::pre_
send` is identity or a stateless frame-count assert. Routing those
messages through the actor would mean `cmd_tx.send(...).await` +
per-message `tokio::spawn` + oneshot ack + queue push (~3 context
switches) just to deliver a message the actor will only forward
unchanged.

`Inner` holds a `SendSubmitter` clone built from the `SendStrategy`
before the driver is spawned. `Socket::send` matches on socket type:

- REQ / REP -- lock a shared `Arc<Mutex<TypeState>>`, call `pre_send`
  inline (alternation flip + envelope framing), push the transformed
  message through the submitter. The same `TypeState` is shared with
  the actor, which locks it for `post_recv` and `on_peer_disconnected`.
  Contention is zero: REQ/REP alternation guarantees send and recv
  never overlap.
- everything else -- inline-validate frame count and push straight into
  the submitter.

`SendSubmitter` is cloneable and safe for concurrent cloned `Socket`
handles. The exact data structure depends on the routing strategy:
round-robin uses a shared active-pipe table plus per-peer MPSC pipes,
fan-out and identity route directly to peer targets, and the fallback
queue uses a bounded concurrent queue.

## Recv bypass (`ConnectionDriver`)

For socket types whose recv path is plain fair-queue delivery, the
connection driver gets a clone of the user-facing
`recv_tx: async_channel::Sender<Message>` and pushes `Event::Message`
straight into it, skipping `peer_out` and the actor's event loop.

Per-peer ordering is preserved because a single driver task delivers in
TCP order. Backpressure still works because `recv_tx` is bounded
(`recv_hwm`); a full channel blocks the driver's read loop, halting
TCP reads.

| Bypassed (recv) | Through actor (recv) | Reason |
|---|---|---|
| Pull, Dealer, Req, Sub, XSub, Pair, Client, Channel, Gather | Rep, Router, Server, Peer | Identity-prefix prepending |
|  | Dish | Group membership filter |
|  | XPub | Subscribe-as-message (0x01/0x00) parsing |

REQ is a special case: the driver pushes raw (envelope-wrapped) messages
via `recv_direct`, and `Socket::recv` strips the empty delimiter inline
via `TypeState::post_recv_req_direct`. This variant skips the
`req_awaiting_reply` flag check to avoid a race with
`on_peer_disconnected` in the actor.

## Round-robin active pipes

Round-robin sockets (`PUSH`, `DEALER`, `REQ`, `CLIENT`, `SCATTER`) have
two hot paths:

- **Single byte-stream peer**: `Socket::send` encodes directly into
  that peer's `PeerWireSlot`. The driver owns the writer and flushes the
  already-encoded chunks from its `data_ready` select arm.
- **Multiple byte-stream peers**: each peer registers an active
  per-peer `blume` pipe. The socket-side submitter scans active pipes
  from a moving cursor and `try_send`s into the first pipe with capacity.
  Full pipes are skipped for that attempt; if every active pipe is full,
  async `send` waits on one rotating pipe and `try_send` reports HWM
  backpressure.

The active pipe is `blume::Sender<Message>` on the socket side and a
single `blume::Receiver<Message>` owned by the `ConnectionDriver`.
`blume` is MPSC, not MPMC: senders are cloneable, but there is exactly
one receiver. That matches the tokio socket API, where cloned socket
handles may send concurrently while each peer has one driver task.

The driver polls its pipe in a dedicated `select!` arm after the ZMTP
handshake. `recv_batch_mut()` drains all currently queued messages in
one swap, then the driver batch-encodes and flushes locally. Encoding
happens on the connection driver's runtime worker instead of on the
application task, so multi-peer PUSH can use the driver workers instead
of serializing all per-peer encode work at the caller.

The old shared `DropQueue` is still present, but it is now the fallback
path:

- no connected peer yet, so sends need to survive connect-before-bind
  and drain after handshake;
- inproc peers, which have no byte-stream driver pipe;
- mixed byte-stream + inproc round-robin sets, where using only active
  byte-stream pipes would starve inproc.

Inproc fallback still uses a pump task from the shared queue to the
peer inbox. Byte-stream drivers still poll `shared_msg_rx` so messages
queued before a peer becomes active drain in order before new pipe
traffic. Fan-out and identity sockets do not use pump tasks: they send
via `PeerSend`, which routes directly to the per-peer `PeerWireSlot`
(wire) or driver inbox (inproc).

## Arena encoding (`ARENA_THRESHOLD` = 96 KiB)

Both backends use `EncodedQueue` with a contiguous `arena: BytesMut`
(256 KiB initial capacity) and an `entries: VecDeque<Entry>` where
each entry is either an arena range or an external `Bytes`. Frame
headers (2-9 bytes) are always written into the arena. Messages
below `ARENA_THRESHOLD` (96 KiB) are encoded contiguously into the
arena via `encode_arena`: header + payload land in one region, so N
small messages produce one iovec for the batch instead of 2N.
Messages at or above the threshold use the gather path
(`encode_gather`): header goes into the arena, payload `Bytes` are
tracked as `Entry::External` (zero-copy, no memcpy). At drain time,
arena ranges are frozen into `Bytes::slice()` sharing one backing
allocation.

The gather functions (`encode_message_gather`,
`encode_message_prefixed_gather`) moved from `frame.rs` into
`EncodedQueue` methods, which write frame headers directly into the
arena and track payloads as external entries. The per-frame
`scratch: BytesMut` is eliminated.

The arena path is disabled when CURVE or BLAKE3ZMQ is active.
These mechanisms hold per-connection symmetric keys and a nonce
counter inside the codec's `FrameTransform`. The nonce must advance
in strict wire order per frame, so encryption is coupled to the
codec's `send_message`/`poll_transmit` sequencing. The arena bypass
skips `Connection::send_message` entirely, so there is no point at
which the transform can encrypt. LZ4 does not have this constraint:
its `MessageEncoder` lives outside the codec, holds no per-frame
sequence state, and produces wire-ready bytes independently.
`Connection` exposes `take_transform()` / `restore_transform()` /
`emit_encrypted_frames()` and `FrameTransform` exposes
`encrypt_message()` as infrastructure for future per-peer encryption
offloading, but the routing strategies do not wire this up yet.

## 128 KiB read buffer

The connection driver reads into a `BytesMut` (128 KiB initial
capacity) via `read_buf`. After each read, `buf.split().freeze()`
hands the codec a zero-copy `Bytes`; no allocation or memcpy per
syscall. `BytesMut` reuses its backing allocation across reads.

## Routing strategies

`omq-tokio/src/routing/` factors the per-message dispatch logic out of
the actor:

| Strategy | Used by | Shape |
|---|---|---|
| `round_robin` | PUSH / DEALER / REQ / CLIENT / SCATTER | Single-peer `PeerWireSlot`; multi-peer active per-peer MPSC pipes; shared fallback queue for no-peer/inproc |
| `exclusive` | PAIR / CHANNEL | Single-peer slot; awaits peer-ready on send-before-connect |
| `fan_out` | PUB / XPUB / RADIO | Per-peer `PeerWireSlot`; subscription/group filter; conflate applies here |
| `identity` | ROUTER / REP / SERVER / PEER | First frame is destination identity; lookup in identity table; per-peer `PeerWireSlot` |
| `fair_queue` | PULL / SUB / XSUB / GATHER / DISH | Recv-only; round-robin across peer drivers |
| `drop_queue` | (HWM behavior) | Bounded queue with drop-on-full when `send_hwm` reached |
| `pump` | inproc peers | Per-peer pump task between shared queue and inbox |
| `peer_send` | (shared type) | `PeerSend` enum (`Wire`/`Inbox`): unified per-peer send dispatch used by fan-out, identity, and exclusive strategies |

## PeerWireSlot: per-peer send bypass

Each wire peer gets a `PeerWireSlot` containing an `EncodedQueue`
behind a `std::sync::Mutex`. `Socket::send` encodes ZMTP frames into
the slot, and the driver flushes them to the wire via a dedicated
`data_ready` select arm. The handle never touches the writer.
Messages of any size are accepted; small messages (<96 KiB) are
arena-encoded, larger ones use zero-copy gather-write. The driver
drain arm writes drained chunks directly to the socket via
`write_chunks`, bypassing the driver's local `EncodedQueue`.

The slot replaces the earlier `DirectIo` pattern where the handle
locked an `Arc<Mutex<Writer>>` to write directly. `PeerWireSlot`
is simpler: the Mutex hold time is nanoseconds (encode only, no I/O),
there is no continuation loop, no `SharedWriter`, and the driver
retains exclusive ownership of the write half.

Slot lifecycle:

- Created per peer at connection setup, stored on `DriverHandle`.
- `handshake_done` set by the driver after ZMTP handshake completes
  (disabled for CURVE/BLAKE3ZMQ frame transforms).
- `mark_dead` called on EOF or cancel; pending bytes are flushed.
- Re-enabled after peer churn (N to 1 transition) for round-robin
  sockets.

Signal coalescing: a `pending: AtomicBool` flag gates
`data_ready.notify_one()`. The sender only notifies on
false-to-true transitions, so N rapid encodes produce one wake.
The driver drain arm loops until the slot is empty (or
`max_batch_bytes` reached), so messages that arrive during
`write_vectored` are flushed without re-entering `select!`.

`PeerWireSlot` is used by most send strategies for wire peers:

- **RoundRobin**: single-peer fast path via `try_encode`. Multi-peer
  round-robin uses active raw-message pipes instead, so the peer driver
  does encoding locally.
- **Exclusive**: PAIR/CHANNEL direct to slot.
- **FanOut**: message encoded once via `pre_encode()`, shared chunks
  pushed into each matching peer's slot via `try_push_encoded`.
- **Identity**: lookup by routing identity, then `try_encode` on
  the target peer's slot.

Inproc peers have no slot (`PeerSend::Inbox` variant) and fall back
to the driver's `mpsc` inbox.

Cap: `WIRE_SLOT_CAP` (2 MiB total bytes in the slot's `EncodedQueue`).
When the slot is full, the sender waits on `space_available` until the
driver drains enough bytes.

## Reconnect and monitor

Both follow the same shape as compio (see [`compio.md`](compio.md) for
detail): a dial supervisor task owns a handle that is `None` while
reconnect is in flight. Round-robin sends fall back to the shared queue
(bounded by `send_hwm`) while no active peer pipe exists and drain
through the new driver after handshake. Subscriptions and group joins
are replayed.

`Socket::monitor()` returns the same `Stream<Item = MonitorEvent>`
shape on both backends; events carry an owned `PeerInfo` snapshot.

## Concurrency model

Within a tokio runtime, multiple `Socket` clones can call `send` /
`recv` concurrently from different worker threads. The round-robin
active-pipe table is protected by a short `std::sync::Mutex` around the
peer vector and cursor; message payloads are moved into per-peer MPSC
pipes outside actor control. The recv-side `async_channel::Sender` is
multi-producer. The actor remains the serialization point for any state
that must be observed atomically (REQ alternation, ROUTER identity
table, XPUB subscription trie), which is why those paths still go
through `cmd_tx`.

## Benchmark note

The fan-out comparison benchmark keeps `omq-tokio-mt` fan-out puller
processes on current-thread runtimes while the pusher uses the
multi-thread runtime. The benchmark is measuring the PUSH socket's
multi-peer send path; giving every PULL process a full multi-thread
worker pool oversubscribes the machine and can make peer fairness look
broken even when the pusher distributes evenly.

## Windows IPC: Named Pipes

On Windows, the IPC transport uses named pipes instead of Unix domain
sockets. The implementation is transparent to the application layer:
`ipc:///my-pipe` automatically routes to `\\.\pipe\my-pipe` internally.
All 20 socket types work identically across all platforms.

### Platform-Specific Behavior

| Aspect | Unix | Windows |
|--------|------|---------|
| Bind path syntax | `/tmp/socket.sock` or `@abstract-ns` | `my-pipe` (auto-prefixed to `\\.\pipe\my-pipe`) |
| Connection type | Stream socket (full-duplex) | Named pipe (full-duplex) |
| Buffer management | `SO_SNDBUF`/`SO_RCVBUF` tuning | Automatic (Windows pipes) |
| Type dispatch | `type IpcStream = UnixStream` | `enum IpcStream { Server(...), Client(...) }` |

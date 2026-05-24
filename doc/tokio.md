# omq-tokio internals

A tour of `omq-tokio`: the actor-shaped multi-thread backend, its
hot-path bypass, and the routing strategies that sit on top. Cross-
cutting basics (three-layer split, two-queue model, multi-chunk
payloads) live in [`architecture.md`](architecture.md). The hot-path
techniques shared with compio (flat-buf encoding, work-stealing,
header scratch) are described from the compio side in
[`compio.md`](compio.md).

`omq-tokio` is the multi-thread backend. Its structure differs from
compio's because tokio's runtime is preemptive and work-stealing across
cores. The same high-level shape applies: per-connection driver tasks
push into one socket-wide inbound queue and pull from one socket-wide
outbound queue.

## Top-level shape

```
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
             |           |         |
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
  UDP), including each peer's outbound flume `Sender`, monitor handle,
  codec config.
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
identity-route lookups, monitor fan-out, HWM accounting, conflate,
priority tiers.

It is **not** the right pattern for the per-message hot path when no
actor state actually mutates per-message.

## Send bypass (`Socket::send`)

For PUSH/DEALER/PUB/PAIR/CLIENT/SCATTER/CHANNEL send, `TypeState::pre_
send` is identity or a stateless frame-count assert. Routing those
messages through the actor would mean `cmd_tx.send(...).await` +
per-message `tokio::spawn` + oneshot ack + flume push (~3 context
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

`SendSubmitter` is lock-free MPMC over flume, so concurrent cloned
`Socket` handles are safe.

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

## Direct shared-queue arm; pump-task elimination

An earlier shape kept the shared `DropQueue` receiver in the
`RoundRobin` routing strategy and spawned a pump task per peer: pump
raced `shared_rx`, forwarded one message at a time to the driver's
inbox. Three task hops end-to-end.

Now each `ConnectionDriver` holds
`shared_msg_rx: Option<flume::Receiver<Message>>` for byte-stream
(TCP/IPC) connections and polls it in a dedicated `select!` arm. The
arm greedily drains up to 256 messages / 512 KiB per wakeup, encodes
them all, then flushes with a tight `write_all` + `write_vectored`
loop. Result: **one task hop** for byte-stream sockets.

Pump tasks are still spawned for inproc peers, which use a per-peer
inbox channel rather than a shared receiver.

## Flat-buf encoding (`FLAT_THRESHOLD` = 48 KiB)

The compio driver uses `EncodedQueue` (a shared struct between sender
and driver task). The tokio `ConnectionDriver` owns a local
`flat_buf: BytesMut` and calls `Connection::send_message_flat` for each
sub-threshold message. `send_message_flat` encodes ZMTP header +
payload bytes contiguously into the caller-supplied `BytesMut` without
touching the codec's transmit queue. At the end of a batch the driver
issues one `write_all(&flat_buf.split())` covering all flat messages,
then a `write_vectored` for any large-message chunks from the codec's
normal transmit path.

The two backends use **different thresholds**: tokio at 48 KiB, compio
at 32 KiB. Tokio's per-iovec overhead on the multi-thread runtime is
higher (more scheduler and task-wake cost per syscall), so the break-
even vs. a contiguous memcpy sits further out -- a 32-64 KiB sweep
peaked at 48 KiB, where 32 KiB messages jump from ~3.4 to ~5.0 GB/s
without dragging the 64 KiB cell back into the flat path. compio's
cooperative scheduler has lower per-iovec cost, and 32 KiB is where
the memcpy/arc-bump crossover lands on the single-thread path.

The flat path is disabled when a frame transform (CURVE, BLAKE3ZMQ) is
active, since those require the codec's encrypt-in-place flow via
`send_message`.

## 128 KiB read buffer

The connection driver reads into a `BytesMut` (128 KiB initial
capacity) via `read_buf`. After each read, `buf.split().freeze()`
hands the codec a zero-copy `Bytes` — no allocation or memcpy per
syscall. `BytesMut` reuses its backing allocation across reads.

## Routing strategies

`omq-tokio/src/routing/` factors the per-message dispatch logic out of
the actor:

| Strategy | Used by | Shape |
|---|---|---|
| `round_robin` | PUSH / DEALER / REQ / PAIR / CLIENT / CHANNEL / SCATTER | One shared send queue + work-stealing send pumps; per-socket HWM |
| `fan_out` | PUB / XPUB / RADIO | Per-connection queues; subscription/group filter; conflate applies here |
| `identity` | ROUTER / REP / SERVER / PEER | First frame is destination identity; lookup in identity table |
| `fair_queue` | PULL / SUB / XSUB / GATHER / DISH | Recv-only; round-robin across peer drivers |
| `drop_queue` | (HWM behaviour) | Bounded queue with drop-on-full when `send_hwm` reached |
| `pump` | inproc peers | Per-peer pump task between shared queue and inbox |

The `priority` Cargo feature swaps `round_robin` for strict per-pipe
priority tiers (nanomsg-style 1..=255). Round-robin send always
prefers the highest-priority alive peer; lower tiers run only when
higher tiers are blocked or disconnected. This trades work-stealing
for per-peer queues -- relevant for ordering, not for raw throughput.

## Reconnect and monitor

Both follow the same shape as compio (see [`compio.md`](compio.md) for
detail): a dial supervisor task owns a handle that is `None` while
reconnect is in flight; sends fall back to the shared queue (bounded by
`send_hwm`) and drain through the new driver after handshake.
Subscriptions and group joins are replayed.

`Socket::monitor()` returns the same `Stream<Item = MonitorEvent>`
shape on both backends; events carry an owned `PeerInfo` snapshot.

## Concurrency model

Within a tokio runtime, multiple `Socket` clones can call `send` /
`recv` concurrently from different worker threads. The hot-path
`SendSubmitter` is a lock-free MPMC channel; the recv-side
`async_channel::Sender` is also multi-producer. The actor remains the
serialization point for any state that must be observed atomically
(REQ alternation, ROUTER identity table, XPUB subscription trie),
which is why those paths still go through `cmd_tx`.

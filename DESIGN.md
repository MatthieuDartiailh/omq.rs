# omq-compio Design Document

This document is a human-readable tour of `omq-compio`'s internals: what every
type does, how they relate, how a message travels from `socket.send(msg)` to the
wire and back, and which techniques keep the hot path fast.

---

## Three-layer split

```
┌─────────────────────────────────────────────────────────┐
│  User code  (depends on omq or omq-compio directly)     │
└────────────────────────┬────────────────────────────────┘
                         │  Socket API (send / recv / …)
┌────────────────────────▼────────────────────────────────┐
│  omq-compio  (runtime / I/O layer)                      │
│  - Socket, SocketInner, PeerSlot                        │
│  - Connection drivers (run_connection)                  │
│  - EncodedQueue fast path                               │
│  - Transport glue: TCP, IPC, inproc, UDP                │
└────────────────────────┬────────────────────────────────┘
                         │  handle_input / poll_event / poll_transmit
┌────────────────────────▼────────────────────────────────┐
│  omq-proto  (sans-I/O core)                             │
│  - Connection (ZMTP codec + state machine)              │
│  - Greeting, mechanism handshake (NULL / CURVE / BLAKE3)│
│  - MessageEncoder / MessageDecoder (lz4 / zstd)          │
│  - Message, Payload, Endpoint, Options, SocketType      │
└─────────────────────────────────────────────────────────┘
```

`omq-proto` never touches a file descriptor. Bytes go in via `handle_input`,
events come out via `poll_event`, outbound frames accumulate via
`send_message`/`send_command` and are read via `poll_transmit`/`advance_transmit`.
The `omq-compio` layer owns the I/O loop and calls those methods.

---

## Key types at a glance

| Type | File | Role |
|------|------|------|
| `Socket` | `socket/handle.rs` | Public handle; `Clone + Send + Sync`; all `&self` methods |
| `SocketInner` | `socket/inner.rs` | Arc'd shared state: peers, recv queue, send queue, monitor |
| `PeerSlot` | `socket/inner.rs` | One connected peer: outbound channel, DirectIoState, info |
| `PeerOut` | `socket/inner.rs` | `Inproc { sender, identity }` or `Wire(WirePeerHandle)` |
| `DirectIoState` | `socket/inner.rs` | Per-wire-peer I/O machinery: codec, writer, fast-path state |
| `EncodedQueue` | `socket/inner.rs` | Zero-copy ZMTP encoder; bypasses codec mutex on hot path |
| `PeerIo` | `transport/peer_io.rs` | Codec + decoder + reader behind one async mutex |
| `WireReader/WireWriter` | `transport/peer_io.rs` | Enum over TCP / IPC; static dispatch, no `Box<dyn>` |
| `run_connection` | `transport/driver.rs` | Per-peer driver loop; `select_biased!` over read/send/hb |
| `MonitorPublisher` | `monitor.rs` | Publishes lifecycle events to subscribers |
| `MonitorStream` | `monitor.rs` | Per-subscriber event stream with lag counter |

---

## `SocketInner` — shared socket state

Every `Socket` clone holds `Arc<SocketInner>`. All mutation goes through
`RwLock` / `Mutex` / atomics so the handle is freely cloneable across tasks.

```
SocketInner {
  socket_type: SocketType,
  options: Options,

  // Outbound
  out_peers: RwLock<Vec<PeerSlot>>,          // all connected/bound peers
  rr_index: AtomicUsize,                     // round-robin cursor
  shared_send_tx/rx: Option<flume::channel>, // one shared queue for RR types
  identity_to_slot: RwLock<HashMap<Bytes,usize>>, // ROUTER lookup

  // Inbound
  in_tx/in_rx: flume::channel<InprocFrame>,  // socket-wide receive queue
  on_peer_ready: Event,                      // notified on peer add/handshake

  // Subscription / group state (SUB / XSUB / DISH)
  subscriptions: RwLock<SubscriptionSet>,    // local filter
  our_subs: RwLock<Vec<Bytes>>,              // replayed to each new pub

  // Infrastructure
  monitor: MonitorPublisher,
  listeners / dialers / udp_dialers: RwLock<Vec<...>>, // task handles (drop=cancel)
  closed: AtomicBool,
  next_connection_id: AtomicU64,
}
```

### `PeerSlot`

One entry per connected peer in `out_peers`.

```
PeerSlot {
  out: PeerOut,                              // how to send to this peer
  direct_io: Option<DirectIoHandle>,         // fast-path state (wire only)
  peer: Arc<RwLock<Option<InprocPeerSnapshot>>>,  // type + identity
  connection_id: u64,
  endpoint: Endpoint,
  info: Arc<RwLock<Option<PeerInfo>>>,       // post-handshake metadata
  peer_sub: Option<Arc<RwLock<SubscriptionSet>>>, // PUB fan-out filter
  peer_groups: Option<Arc<RwLock<HashSet<Bytes>>>>, // RADIO group filter
}
```

`direct_io` is swapped to `None` by the driver on exit and back to `Some(new_state)`
by the dial supervisor on reconnect. `Socket::send`'s fast path reads the inner
`Option`; `None` means reconnect is in flight → fall back to `cmd_tx`.

### `PeerOut`

```
enum PeerOut {
  Inproc { sender: flume::Sender<InprocFrame>, our_identity: Bytes },
  Wire(WirePeerHandle),  // Arc<RwLock<flume::Sender<DriverCommand>>>
}
```

Inproc peers receive a frame directly in the peer's shared `in_tx`. Wire peers
go through a per-peer command channel to the driver task.

---

## `DirectIoState` — per-wire-peer fast-path state

Shared between the driver task and the `Socket::send` / `Socket::recv` callers.

```
DirectIoState {
  // Codec + read side
  peer_io: SharedPeerIo,           // Arc<async_lock::Mutex<PeerIo>>

  // Write side (separate from codec so codec lock can be dropped first)
  writer: async_lock::Mutex<WireWriter>,

  // Fast-path send bypass (NULL mechanism, and also transform path)
  encoded_queue: Mutex<EncodedQueue>,
  encoder: async_lock::Mutex<Option<MessageEncoder>>,  // lz4 / zstd send side
  has_transform: bool,             // selects encoder path over passthrough
  transform_passthrough: Option<(Bytes, usize)>,  // sentinel + threshold for bypass
  driver_in_select: AtomicBool,    // driver is parked; notify to wake
  transmit_ready: Event,           // sender → driver wakeup signal

  // Recv-direct arbitration
  recv_claim: AtomicU8,            // 0=driver reads, 1=recv() owns reads
  recv_state_changed: Event,       // claim flip → driver re-evaluates
  eof_signal: Event,               // recv() signals EOF to driver

  // Misc
  poll_fd: Arc<PollFd<socket2::Socket>>,  // cancel-safe readiness probe
  handshake_done: AtomicBool,
  last_input_nanos: AtomicU64,     // heartbeat input timestamp
  hb_epoch: Instant,               // monotonic origin for hb math
}
```

The **writer** lives separately from `PeerIo` so the driver can release the
codec lock before calling `write_vectored`, opening a window for the sender to
encode the next message while I/O is in flight.

The **encoder** lives separately from `PeerIo` so the sender can acquire it
independently of the driver's `peer_io` lock (which is held during reads). On
compio's cooperative single-threaded runtime `encoder.try_lock()` always succeeds
when called from the sender, since no other task runs concurrently. The encoded
output goes into `EncodedQueue` — the same flush path as the NULL passthrough —
so the transform path benefits from drain-vec reuse and flat-buf batching just
like uncompressed messages.

---

## `EncodedQueue` — the direct-encode bypass

When `has_transform == false` (NULL mechanism, no compression), `Socket::send`
encodes ZMTP frames directly into an `EncodedQueue` under a **sync** `Mutex`.
The driver drains the queue and calls `write_vectored` (or `write_all` for the
flat region) in step 3b.

```
EncodedQueue {
  chunks: VecDeque<Bytes>,   // large-message chunks: header + Arc-bumped payload
  flat_buf: BytesMut,        // contiguous backing for small messages (< FLAT_THRESHOLD)
  total_bytes: usize,        // for cap detection (512 KB default)
  scratch: BytesMut,         // reused header buffer — zero alloc post-warmup
}

const FLAT_THRESHOLD: usize = 32 * 1024;  // both backends; see §omq-tokio for tokio differences
```

Two encoding paths, chosen per message:

**Small messages (total bytes < `FLAT_THRESHOLD`)** — `encode_flat`:
1. Writes `[flags, size]` header + all payload bytes contiguously into `flat_buf`.
2. No `Bytes` allocation; no Arc bump. N small messages land in one contiguous region.
3. At flush, `flat_buf.split().freeze()` produces one `Bytes` covering all N messages →
   **1 iovec** for N messages (vs. 2N for the large-message path).

**Large messages (total bytes ≥ `FLAT_THRESHOLD`)** — `encode_and_push`:
1. `flat_buf` is first flushed to `chunks` (one `split().freeze()`) to maintain wire order.
2. Writes header into `scratch`, calls `scratch.split().freeze()` → owned `Bytes`.
3. For every payload `Bytes` chunk: `clone()` (one atomic increment, no copy).
4. Header + payload chunks appended to `chunks`; kernel gathers via `writev`.

The driver flushes via `drain_into_vec(&mut reused_vec)` (same `Vec<Bytes>` reused
across iterations) → `write_vectored`. On partial write, `put_back_unwritten` slices
the last partially-written `Bytes` and prepends unwritten chunks to the front.

**Why this matters for small messages:** the sync `Mutex::try_lock` is much
cheaper than an async mutex acquisition; frame encoding is inlined without going
through the codec's transmit buffer (no `clone_transmit_chunks` + `advance_transmit`);
and packing N small frames into one `BytesMut` region cuts the iovec count from 2N
to 1, reducing `writev` overhead and improving kernel batching.

---

## `PeerIo` — codec, transform, and reader

```
PeerIo {
  codec: Connection,               // omq-proto ZMTP codec
  decoder: Option<MessageDecoder>, // lz4 or zstd receive-side decompressor
  reader: WireReader,              // TCP or IPC read half
  handshake_done: bool,
}

type SharedPeerIo = Arc<async_lock::Mutex<PeerIo>>;
```

Lock discipline: the `PeerIo` mutex is **never held across an `.await`**. It is
acquired, used, and dropped in a single step so that the sender's `try_lock`
succeeds between driver iterations.

The send-side encoder (`MessageEncoder`) was deliberately removed from `PeerIo`
and placed in `DirectIoState::encoder` so sender and driver can encode / decode
concurrently without contending on the same mutex.

### `WireReader` / `WireWriter` — static dispatch

```
enum WireReader { Tcp(OwnedReadHalf<TcpStream>), Ipc(OwnedReadHalf<UnixStream>) }
enum WireWriter { Tcp(OwnedWriteHalf<TcpStream>), Ipc(OwnedWriteHalf<UnixStream>) }
```

An enum over the small set of supported stream transports lets the compiler emit
a static `match` at the call site instead of a virtual dispatch through
`Box<dyn Trait>`. The original `Box<dyn DynWriter>` shape allocated once per
send and once per read on the hot path; that allocation alone dominated
throughput at 128 B message sizes.

### `MessageEncoder` / `MessageDecoder`

```
enum MessageEncoder {
  Lz4(Lz4Encoder),     // feature = "lz4"
  Zstd(ZstdEncoder),   // feature = "zstd"
}

enum MessageDecoder {
  Lz4(Lz4Decoder),     // feature = "lz4"
  Zstd(ZstdDecoder),   // feature = "zstd"
}
```

`MessageEncoder::for_endpoint` constructs a matched `(MessageEncoder, MessageDecoder)`
pair for compression transports (`lz4+tcp://`, `zstd+tcp://`). The encoder lives in
`DirectIoState::encoder`; the decoder lives in `PeerIo::decoder`. They hold
independent state (compression context, dictionary) so each can be locked
separately — the sender and driver never contend on the same mutex for encode vs.
decode.

On send: `encoder.encode(&msg)` → `TransformedOut` (a `SmallVec` of wire messages)
→ each pushed into `EncodedQueue`. On recv: `decoder.decode(wire_msg)` →
`Option<Message>` (`None` = dictionary shipment, silently consumed at transport).

Messages smaller than the compression threshold (512 B without a dictionary)
pass through as `SENTINEL_PLAIN | [0,0,0,0] | body` — no actual compression,
just a 4-byte prefix. `encoder.passthrough_info()` returns `(sentinel, threshold)`;
when set, `try_direct_encode` uses the `EncodedQueue` passthrough path directly
without locking the encoder at all.

---

## Driver loop — `run_connection`

One driver task per wire connection. Runs the `select_biased!` loop below.

```
loop {
  state.driver_in_select.store(false)

  // Graceful close check
  if closing && pending empty && codec empty && eq empty → return Ok(())

  // Step 1: drain codec.poll_event() [under peer_io lock — skipped if !codec_has_input]
  //   HandshakeSucceeded → set handshake_done, drain pending_cmds; codec_maybe_dirty = true
  //   Message → decoder.decode, send to peer_in_tx (socket inbound)
  //   Command → update peer_sub/peer_groups, surface to user if XPUB
  //   codec_has_input = false after drain (re-set by read_ready arm)

  // Step 2: dispatch drained events [OUTSIDE peer_io lock]
  //   (awaits here; must not hold lock)

  // Step 3a: flush codec transmit buffer [skipped if !codec_maybe_dirty]
  //   clone_transmit_chunks [under peer_io lock] → release lock
  //   writer.write_vectored [writer lock only] → release
  //   advance_transmit [peer_io lock] → release
  //   codec_maybe_dirty = false when confirmed empty
  //   if wrote: continue

  // Step 3b: flush EncodedQueue
  //   encoded_queue.drain_into_vec(&mut drain_buf) [sync Mutex]  ← same Vec reused
  //   writer.write_vectored [writer lock]
  //   if partial: put_back_unwritten
  //   if wrote: continue

  // Step 4: park
  state.driver_in_select.store(true)
  if !encoded_queue.is_empty() { continue }  // close the store/check race

  select_biased! {
    eof_signal     → return Ok(())
    timeout_fut    → return Err(HandshakeFailed)   // pre-handshake deadline
    hb_fut         → check liveness, send Ping
    read_ready     → read(buf) [peer_io lock], handle_input  // codec_has_input = codec_maybe_dirty = true
    cmd_inbox      → encode SendMessage / SendCommand into codec; codec_maybe_dirty = true
    shared_queue   → work-steal, encode into codec; codec_maybe_dirty = true
    transmit_ready → (sender woke us), loop to step 3b
  }
}
```

### Lock discipline during step 3a/3b

The codec lock is released **before** `write_vectored`. This lets the sender
encode the next message (via `EncodedQueue` or the codec path) while the I/O
syscall is in flight.

### Recv-direct claim arbitration

`recv_claim: AtomicU8` arbitrates whether the driver or a `recv()` caller owns
the read path at any moment.

- `0` → driver reads from the FD in the `read_ready` arm.
- `1` → a `try_direct_recv` caller has claimed it; driver parks on
  `recv_state_changed` instead of `poll_fd`.

The claim is a `compare_exchange(0 → 1)` protected by a RAII `ClaimGuard`.
On drop, `ClaimGuard` resets to 0 and notifies `recv_state_changed`. The driver
re-evaluates on its next iteration without holding any lock.

---

## Send paths

`Socket::send` dispatches by socket type:

| Socket types | Strategy | Key mechanism |
|---|---|---|
| PUSH / DEALER / REQ / PAIR / REP | Round-robin (or priority) | Single peer → `try_direct_encode`; multi-peer → shared queue |
| PUB / XPUB | Fan-out, subscription-filtered | Per-peer `SubscriptionSet` checked at send time |
| ROUTER / SERVER / PEER | Identity-routed | First frame = destination; lookup in `identity_to_slot` |
| RADIO | Fan-out to UDP dialers + ZMTP peers, group-filtered | `[group, body]` shape validated |
| XSUB | Pure fan-out | All peers |

### `try_direct_encode` (single wire peer, no transform)

```
1. Check handshake_done (atomic load, no lock)
2. encoded_queue.try_lock() — if busy (driver flushing), return false
3. Check total_bytes < 512 KB cap
4. if msg.byte_len() < FLAT_THRESHOLD: encode_flat(msg) → flat_buf (copy, 0 Arc bumps)
   else: encode_and_push(msg) → chunks (header Bytes + Arc-bump payload)
5. if driver_in_select: transmit_ready.notify(1)
6. Return true
```

If any check fails, falls back to `cmd_tx.send_async(DriverCommand::SendMessage(msg))`.
If `cmd_tx` is disconnected (peer dead / reconnecting), falls back to the shared queue.

### `try_direct_encode` (single wire peer, with transform)

```
1. encoder.try_lock() — if busy (driver is mid-handshake drain), return false
2. Check handshake_done (atomic load)
3. encoder.encode(msg) → TransformedOut (SmallVec of wire messages)
4. Drop encoder lock
5. encoded_queue.try_lock() — if busy, return false
6. Check total_bytes < 512 KB cap
7. For each wire message: encode_flat (< FLAT_THRESHOLD) or encode_and_push
8. Drop encoded_queue lock
9. if driver_in_select: transmit_ready.notify(1)
10. Return true
```

The encoder lock is dropped **before** acquiring `encoded_queue`, keeping the
critical section minimal. Because the encoder is separate from `PeerIo`, the
driver can be mid-read (holding `peer_io`) while the sender encodes simultaneously.

---

## Recv path

`Socket::recv` tries the **direct-recv fast path** first (for PULL / SUB / REP
/ PAIR / REQ sockets with a single wire peer) then falls back to the `in_rx`
channel loop.

### Direct-recv (`try_direct_recv`)

Saves ~12 µs per round-trip by reading the FD inline instead of waiting for
the driver to parse events and enqueue to `in_rx`.

```
1. Bail if in_rx is not empty (driver buffered something)
2. snapshot_direct_io_single_peer() → Some(state)?
3. recv_claim.compare_exchange(0 → 1)   [claim guard]
4. Bail if in_rx is not empty (race-safe recheck)
5. Loop:
   a. Drain codec.poll_event() [peer_io lock] → first Message wins
   b. Race: in_rx.recv_async() vs poll_fd.read_ready()
      - in_rx wins → drop claim, process inline, return
      - read_ready wins → continue
   c. reader.read(buf) [peer_io lock] → codec.handle_input
   d. Flush any codec transmit (e.g. auto-PONG) via writer lock
   e. Loop to 5a
```

### `in_rx` fallback loop

Processes `InprocFrame` variants from the socket-wide bounded channel:
- `SinglePart { peer_identity, body }` — hot path, ~72 B slot
- `Message(Box<InprocFullMessage>)` — multipart, boxed to keep slot small
- `Command(Command)` — XPUB-only: wrap as `\x01<topic>` or `\x00<topic>`

---

## Transports

### TCP / IPC

Both use the same `run_connection` driver. Only the `WireReader`/`WireWriter`
enum variant differs. TCP sets `TCP_NODELAY` after accept/connect.

`bind_tcp` spawns an accept loop; `connect_tcp_with_reconnect` spawns a dial
supervisor that handles exponential backoff reconnection.

### Inproc

No driver, no codec, no handshake. A global `REGISTRY` (`LazyLock<Mutex<HashMap<String, Sender<…>>>>`) maps names to `Sender<InprocConnectRequest>`. `bind` registers a name; `connect` sends a request.

Peers exchange an `InprocPeerSnapshot` (socket type + identity) synchronously.
Messages flow as `InprocFrame` through flume channels. The `InprocListener` drops
from the registry on `Drop`.

### UDP (RADIO / DISH)

No driver, no ZMTP codec, no handshake. `RADIO` stores `Arc<UdpSocket>` per
`connect()`; `send_radio` encodes `[group, body]` into a datagram and calls
`sock.send`. `DISH` `bind()` spawns a `recv_from` loop that decodes datagrams and
checks `joined_groups` locally.

### `lz4+tcp` / `zstd+tcp`

Dialed and accepted as plain TCP. After the TCP connection is up, a matched
`(MessageEncoder, MessageDecoder)` pair is constructed via
`MessageEncoder::for_endpoint`. The encoder is installed in
`DirectIoState::encoder`; the decoder in `PeerIo::decoder`. Every
post-handshake message passes through `encoder.encode` (sender) /
`decoder.decode` (receiver). Neither touches the ZMTP handshake.

---

## Monitor subsystem

`MonitorPublisher` (one per `SocketInner`) holds a `Vec<MonitorSink>`.

```
publish(event):
  lock sinks
  prune disconnected receivers
  for each sink: try_send(event) [non-blocking]
    if full: lag += 1 [atomic]
```

`Socket::monitor()` returns a `MonitorStream` (one per `subscribe()` call):

```
MonitorStream { rx: flume::Receiver<MonitorEvent>, lagged: Arc<AtomicU64> }

recv():
  if lagged.swap(0) > 0 → return Err(Lagged(n))
  else → rx.recv_async()
```

Events published: `Listening`, `Accepted`, `Connected`, `ConnectDelayed`,
`HandshakeSucceeded`, `Disconnected`, `PeerCommand`, `Closed`.

---

## Reconnect mechanism

The dial supervisor task owns the `DirectIoHandle`
(`Arc<RwLock<Option<Arc<DirectIoState>>>>`) and the `WirePeerHandle`.

On driver exit:
1. `direct_io_handle` is set to `None` → fast-path sends fall back to `cmd_tx`.
2. `cmd_tx` is disconnected (driver task dropped `Receiver`) → send falls back to
   shared queue.
3. Messages buffer in the shared queue (bounded by `send_hwm`).
4. Supervisor dials with exponential backoff.
5. New `DirectIoState` is built and installed; `direct_io_handle` is restored.
6. New driver drains the shared queue.

Subscriptions and group joins are replayed by the `snap_listener` task after
each handshake via `our_subs` and `joined_groups`.

---

## Memory and allocation model

### `Bytes`, `Payload`, `Message`

```
Payload = SmallVec<[Bytes; 2]>    — 2 chunks inline (sentinel + body, or just body)
Message = SmallVec<[Payload; 3]>  — 3 parts inline (covers REQ/REP envelopes)
```

`Bytes::clone()` is one atomic increment — no data copy. The codec, transforms,
and `EncodedQueue` all consume messages by cloning their `Bytes` chunks. The
kernel's `writev` then gathers the scattered pointers without further copying.

### `InprocFrame::SinglePart`

The single-part variant carries `Option<Bytes>` (identity) and `Bytes` (body)
inline (~72 B). The `Message` struct is ~624 B; wrapping it in a box for the
`Message` variant keeps the flume channel slot small on the hot path.

### Header scratch buffers

Two separate scratch buffers amortize frame-header allocations:

- `EncodedQueue::scratch: BytesMut` — used by the compio sender for large-message
  ZMTP headers. After warmup it is permanently allocated; `clear()` + `extend()` +
  `split().freeze()` produces an owned `Bytes` with zero allocator calls per header.
- `Connection::header_scratch: BytesMut` (in `omq-proto`) — used by the codec
  on the normal `send_message` path (transform transports, cmd encoding, CURVE).
  Holds up to 64 KiB; replaced when remaining capacity drops below 9 bytes.
  Roughly one allocation per ~7000 frames (64 KiB / 9 B max header size).

---

## Concurrency model

`omq-compio` is designed for **single-threaded compio runtimes**. Within one
runtime the scheduler is cooperative — no preemption, no context switch inside a
task. This means:

- `driver_in_select` is written and read without barriers in practice, though
  `Relaxed` atomics are used for correctness across executor cores.
- `Mutex::try_lock` on `encoded_queue` almost never fails on a single-core
  runtime because the driver cannot preempt the sender.
- All lock acquisitions are brief; none are held across yields.

For multi-core deployments, instantiate one `compio::runtime::Runtime` per
worker thread and pin it via `RuntimeBuilder::thread_affinity`. Cross-runtime
sends go through flume MPSC (thread-safe). This typically lifts wire throughput
by 20–40% for TCP/IPC small messages (sender and receiver overlap their I/O).

---

## omq-tokio hot-path

`omq-tokio` is the multi-thread backend. Its send path differs structurally
from compio's, but the same flat-buffer and direct-queue ideas apply.

### Flat-buf encoding (`FLAT_THRESHOLD` = 32 KiB)

The compio driver uses `EncodedQueue` (a shared struct between sender and driver
task). The tokio `ConnectionDriver` owns a local `flat_buf: BytesMut` and calls
`Connection::send_message_flat` for each sub-threshold message. `send_message_flat`
encodes ZMTP header + payload bytes contiguously into the caller-supplied `BytesMut`
without touching the codec's transmit queue. At the end of a batch the driver issues
one `write_all(&flat_buf.split())` covering all flat messages, then a
`write_vectored` for any large-message chunks from the codec's normal transmit path.

Both backends use **32 KiB**. This was established for tokio first: its
`write_vectored` overhead per-iovec is higher on the multi-thread runtime (more
scheduler and task-wake cost per syscall), so the break-even vs. a contiguous
memcpy sits around 32–64 KiB. Measurements showed 32 KiB fixed a catastrophic
2 KiB regression in tokio (35.9k → 405k msg/s). compio was originally 1 KiB;
raising it to 32 KiB produces no measurable regression (the single-thread
scheduler naturally batches, so per-iovec cost is lower, and the memcpy vs.
arc-bump crossover is in the same ballpark). Above 32 KiB the memcpy cost of
the flat path dominates and the arc-bump + `write_vectored` path wins again.

The flat path is disabled when a frame transform (CURVE, BLAKE3ZMQ) is active, since
those require the codec's encrypt-in-place flow via `send_message`.

### Direct shared-queue arm; pump-task elimination

Previously, the `RoundRobin` routing strategy kept a shared `DropQueue` receiver
and spawned a **pump task** per peer: pump raced `shared_rx` → forwarded one message
at a time → driver `inbox`. Three task hops end-to-end.

Now each `ConnectionDriver` holds `shared_msg_rx: Option<flume::Receiver<Message>>`
for byte-stream (TCP/IPC) connections and polls it in a dedicated `select!` arm.
The arm greedily drains up to 256 messages / 512 KiB per wakeup, encodes them all,
then flushes with a tight `write_all` + `write_vectored` loop. Result: **one task
hop** for byte-stream sockets. Pump tasks are still spawned for inproc peers (which
use a per-peer inbox channel, not a shared receiver).

### 64 KiB read buffer (both backends)

Both backends use a 64 KiB read buffer. With 8 KiB a 32 KiB message required 4
syscalls; with 64 KiB it fits in one. The filled buffer is consumed inline (no
`buf.clone()` that would memcpy the whole payload).

### `SocketDriver` actor + hot-path bypass

The tokio backend wraps each `Socket` in a `SocketDriver` task — an actor in
the textbook sense: it owns mutable state nobody else can touch, and the
outside world communicates with it via channels.

**State the actor owns:**
- `HashMap<PeerId, PeerInfo>` — every connected peer (TCP/IPC/inproc/UDP),
  including each peer's outbound flume `Sender`, monitor handle, codec config.
- `TypeState` — REQ/REP alternation flag, ROUTER identity-prefix table, DISH
  group memberships, XPUB subscription trie, conflate flag.
- `SendStrategy` + `RecvStrategy` — round-robin, fan-out, identity-route,
  fair-queue policy.
- bind/connect/disconnect bookkeeping — listener tasks, dialer tasks,
  reconnect timers.

**Channels in:** `cmd_tx: mpsc::Sender<SocketCommand>` (Bind, Connect, Send,
Subscribe, …) from user handles; `peer_out: mpsc::Sender<(PeerId, PeerOut)>`
(Connected, Disconnected, Event(msg)) from connection drivers.

This is the same shape `tokio-tungstenite`, `redis-rs`, and `quinn` use: a
single task serializes mutation of state that has many concurrent sources of
input. It's the right pattern for **rare, stateful, multi-source events** —
bind, connect, subscribe, identity-route lookups, monitor fan-out, HWM
accounting, conflate, priority tiers.

**It is not the right pattern for the per-message hot path** when no actor
state actually mutates per-message. For PUSH/DEALER/PUB/PAIR/CLIENT/SCATTER/
CHANNEL send, `TypeState::pre_send` is identity or a stateless frame-count
assert. For PULL/DEALER/SUB/XSUB/PAIR/CLIENT/CHANNEL/GATHER recv,
`TypeState::post_recv` is identity. Routing those messages through the actor
means `cmd_tx.send(...).await` + per-message `tokio::spawn` + oneshot ack
+ flume push (~3 context switches) on send, and an extra mpsc hop through
`peer_out` on recv — all to deliver a message the actor will only forward
unchanged.

**Send bypass (`Socket::send`).** For non-REQ/REP sockets, the handle reads
a `SendSubmitter` clone (lock-free MPMC over flume) directly out of `Inner`
and pushes the message in. Frame-count validation that lived inside
`pre_send` is mirrored inline so protocol errors (e.g. `CLIENT` with multipart
input) still surface. REQ/REP keep going through the actor because their
`pre_send` flips the alternation bit — real per-message state mutation that
must be serialized against concurrent `Socket` clones.

**Recv bypass (`ConnectionDriver`).** For socket types whose recv path is
plain fair-queue delivery, the connection driver gets a clone of the
user-facing `recv_tx: async_channel::Sender<Message>` and pushes
`Event::Message` straight into it, skipping `peer_out` and the actor's event
loop. Per-peer ordering is preserved because a single driver task delivers
in TCP order; backpressure still works because `recv_tx` is bounded
(`recv_hwm`) and a full channel blocks the driver's read loop, halting TCP
reads. Types that need post-processing keep going through the actor:

| Bypassed (recv) | Through actor (recv) | Reason |
|---|---|---|
| Pull, Dealer, Sub, XSub, Pair, Client, Channel, Gather | Rep, Router, Server, Peer | Identity-prefix prepending |
|  | Dish | Group membership filter |
|  | XPub | Subscribe-as-message (0x01/0x00) parsing |

**Result.** PUSH/PULL TCP loopback at 128 B: 84k → 4.0M msg/s (≈48× on the
hop count alone, before multi-core gains). The actor still owns peer-table
mutations and connection lifecycle — it's just no longer on the per-message
path for the common send/recv cases.

This is structurally why omq-tokio outperforms zmq.rs (also tokio-based): both
runtimes are work-stealing across all cores, but zmq.rs routes every message
through its socket actor's mpsc inbox; omq-tokio routes only the messages
that have actor state to mutate.

---

## Performance summary

| Technique | Where | Gain |
|-----------|-------|------|
| `EncodedQueue` (sync mutex, inline framing) | compio `socket/inner.rs` | Removes async-mutex round-trip per send; eliminates `clone_transmit_chunks` + N Arc bumps |
| `flat_buf` small-message packing | compio `socket/inner.rs`, tokio `engine/driver.rs` | 2N iovecs → 1 for N small messages; better kernel batching |
| `FLAT_THRESHOLD` = 32 KiB | both backends | break-even between memcpy and arc-bump+writev is ~32–64 KiB on both runtimes |
| Codec-skip guards (`codec_has_input`, `codec_maybe_dirty`) | compio `transport/driver.rs` | Skips `async PeerIo` mutex acquire on iterations where codec state didn't change |
| Drain-vec reuse | compio `transport/driver.rs` | Same `Vec<Bytes>` cleared + reused across flushes; no per-flush heap alloc |
| Direct shared-queue arm; pump-task elimination | tokio `engine/driver.rs`, `routing/round_robin.rs` | 3 task hops → 1 for TCP/IPC; pump still used for inproc |
| Actor bypass on send (non-REQ/REP) | tokio `socket/handle.rs` | `Socket::send` skips actor: ~3 context switches → 1 flume push |
| Actor bypass on recv (fair-queue types) | tokio `engine/driver.rs`, `socket/actor.rs` | `ConnectionDriver` pushes `recv_tx` directly; skips actor event loop |
| `WireReader`/`WireWriter` enums (static dispatch) | `transport/peer_io.rs` | Eliminates `Box<dyn>` heap alloc and vtable indirection per read/write |
| Writer lock separate from codec lock | compio `socket/inner.rs` | Encoder + I/O overlap; codec lock released before `write_vectored` |
| Encoder lock separate from `PeerIo` | compio `socket/inner.rs` | Sender encodes compress-transform messages concurrently with driver reads; no lock contention on the read/write boundary |
| `driver_in_select` flag | compio `transport/driver.rs` | Skips `transmit_ready.notify` when driver is actively looping |
| 64 KiB read buffer | both backends `transport/driver.rs` | One read per 32 KiB message; avoids 4 syscalls at 8 KiB |
| Batch drain (`max_batch_bytes` = 1 MiB) | `transport/driver.rs` | Amortises `write_vectored` syscall across back-to-back sends |
| Frame-header scratch (`Connection::header_scratch`) | `omq-proto` | ~1 alloc per ~7000 frames instead of per-frame `BytesMut`; eliminates malloc pressure at 80k+ msg/s |
| `SmallVec` inline storage | `omq-proto` | `Payload`/`Message` common cases live on stack |
| `InprocFrame::SinglePart` compact form | `transport/inproc.rs` | 72 B channel slot vs ~624 B for `Message` |
| `try_direct_recv` | compio `socket/handle.rs` | Saves one task-wake (~12 µs) on recv side |
| Work-stealing shared queue | compio `socket/inner.rs` | Multiple drivers race a shared send queue |
| Zero-copy `Bytes::clone()` | everywhere | No data copy on send; kernel gathers via `writev` |
| `PollFd::read_ready` (cancel-safe) | compio `transport/driver.rs` | io_uring `PollOnce` SQE; cancellable when another arm wins |

---

## Performance history: the road to beating libzmq

Beating libzmq at **small messages** over a real TCP socket turned out to be the
hardest part of the project. Large messages were comparatively easy: `writev`
batching multi-chunk frames in one syscall vs. libzmq's separate `send()` calls
for header + payload gave a 2–3× advantage above 2 KiB from early on. Small
messages (128–512 B) were a different story.

### Why libzmq is so hard to beat at 128 B

libzmq's architecture separates the application thread from a **dedicated I/O
thread** (`zmq_ctx_t` spawns one reaper + one I/O thread per context by default).
The app encodes and hands a message to the I/O thread via a pipe; the I/O thread
does the actual `send()`. This means the app loop and I/O are truly concurrent:
while the app is encoding message N+1, the I/O thread is writing message N to the
socket. At 128 B where encoding is fast and kernel round-trips dominate, this
overlap is the primary advantage.

omq-compio is **single-threaded**: both the sender hot loop and the driver run in
the same compio runtime. There is no independent I/O thread, so encoding and
`write_vectored` are sequential unless the sender finishes encoding before the
driver parks in `select_biased!`.

### Phase 1 — baseline (orig_impl3 era)

The initial implementation (commit `68771b7`) had:
- `Box<dyn DynWriter>` virtual dispatch on every write (one heap alloc per call).
- 8 KiB read buffer (4 syscalls for a 32 KiB message).
- Per-frame `BytesMut` allocation for the 1–9 byte frame header.
- Send path: `Socket::send` → codec async mutex → encode → driver task → `cmd_tx`
  → driver wakes → `clone_transmit_chunks` → N `Arc` bumps → `write_vectored`.

128 B TCP throughput: ~800k msg/s. libzmq: ~3M. Gap: ~3.7×.

### Phase 2 — read-path fixes (orig_impl4 / ee796a5)

- **8 KiB → 64 KiB read buffer**: 32 KiB messages now land in one read.
- **Drop `buf.clone()`** on every read: was memcpy-ing the entire filled buffer
  into a fresh allocation on each driver iteration.
- **max_batch_bytes 256 KiB → 1 MiB**: stops short-cutting `write_vectored`
  batches at messages ≥ 8 KiB.
- **`WireReader`/`WireWriter` enums** replacing `Box<dyn DynWriter>`: eliminates
  vtable indirection and per-call heap allocation on the hot read/write paths.

Result: large messages roughly doubled; 128 B unchanged (read-path work doesn't
help small-message throughput when the bottleneck is the send path).

### Phase 3 — frame-header scratch (`1c83ebc`)

`Connection` now holds a `header_scratch: BytesMut` (64 KiB cap). Each
`encode_frame_into` writes the header there and uses `split().freeze()` — roughly
one allocation per ~7000 frames. Before: one short-lived `BytesMut` per frame at
80k+ msg/s → measurable malloc / cfree pressure in the profile.

### Phase 4 — EncodedQueue send bypass (`c52db81`) — the big compio win

The most impactful single change. Before, every `Socket::send` had to:
1. Acquire the codec async mutex (contended with the driver).
2. Encode into the codec's internal transmit buffer.
3. Wait for the driver to notice, acquire the mutex again, call
   `clone_transmit_chunks` (N Arc increments), `write_vectored`, `advance_transmit`.

After: sender acquires a **sync** `Mutex::try_lock` on `EncodedQueue`, encodes
directly into `VecDeque<Bytes>`, and returns. The driver drains the queue in step
3b without touching the codec at all. For NULL mechanism sockets this eliminates:
- Async mutex contention between sender and driver.
- `clone_transmit_chunks` (N Arc bumps → chunks move by value).
- `advance_transmit` bookkeeping per flush.

Results vs. libzmq (two-process TCP, one core each):

| Size | omq (before) | omq (after) | libzmq |
|------|--------------|-------------|--------|
| 128 B | ~1.30M | 1.48M | ~2.96M |
| 512 B | — | 2.12M | 2.01M |
| 2 KiB | — | 1.44M | 679k |
| 8 KiB | — | 540k | 186k |

Still ~50% behind libzmq at 128 B. The gap comes almost entirely from the
single-thread penalty: libzmq's I/O thread overlaps app encoding with kernel
writes; omq must do them sequentially.

### Phase 5 — flat_buf + codec-skip guards (`0f2d36c`) — closing the small-message gap

Two insights:

**Insight 1 — iovec count matters at small sizes.** For N back-to-back 128 B
messages, the pre-flat path pushed 2 chunks per message to the iovec list (header
`Bytes` + payload `Bytes`) → 2N iovecs per `writev`. The kernel handles up to 1024
iovecs per `writev`, so at very high throughput the driver was issuing many
`writev` calls each with 1000+ tiny iovecs. Packing N small messages into one
contiguous `BytesMut` (`flat_buf`) collapses 2N iovecs to 1 for the whole batch —
the kernel sees one large write instead of many tiny scattered ones.

**Insight 2 — spurious codec lock acquires hurt at low contention.** Steps 1 and
3a of the driver loop each acquire the async `PeerIo` mutex. When the driver is
looping fast (no reads pending, no codec output), those acquires are free — but
not free enough. Two boolean guards (`codec_has_input`, `codec_maybe_dirty`) skip
both steps when nothing could have changed. On a pure-send benchmark with no
heartbeats the codec path is cold in every iteration.

Result: **128 B TCP reaches ~3.00M msg/s** (vs. 2.95M libzmq) — parity and a
small edge. The single-thread penalty is offset by the flat encoding eliminating
per-message iovec overhead that libzmq also pays (though libzmq's I/O thread
pipeline hides it differently).

### Phase 6 — tokio: direct shared-queue + FLAT_THRESHOLD 32 KiB (`6155402`, `bad6c7b`)

The same insight applied to `omq-tokio`:
- **Direct shared-queue arm** eliminates the pump task between the shared `DropQueue`
  and the `ConnectionDriver`. Before: 3 task hops (app → shared queue → pump →
  driver). After: 1 hop for TCP/IPC (driver polls `shared_msg_rx` directly in a
  `select!` arm, drains up to 256 messages per wakeup).
- **FLAT_THRESHOLD = 32 KiB** for both backends. Tokio's per-iovec overhead on
  the multi-thread scheduler is the primary driver: 32 KiB fixed a catastrophic
  2 KiB regression (35.9k → 405k msg/s) from the earlier 1 KiB threshold.
  compio was originally 1 KiB; raising it to 32 KiB is neutral (no measurable
  regression — the cooperative single-thread scheduler has lower per-iovec cost,
  but the memcpy vs. arc-bump crossover lands in the same ballpark). Below
  32 KiB, `write_all(flat_buf)` wins; above it, arc-bump + `write_vectored` wins.

### Phase 7 — `MessageEncoder` / `MessageDecoder` split; transform path via `EncodedQueue` (`4099155` era)

`MessageTransform` was a unified type holding both encode and decode state behind
the same `PeerIo` mutex. Under `lz4+tcp` / `zstd+tcp` the sender's
`try_direct_encode` had to race `peer_io.try_lock()` against the driver's read
loop. Because the driver holds `peer_io` for the entire `handle_input` call on
every received chunk, `try_lock` almost always lost — forcing every compressed
send through the slower `cmd_tx` path.

Fix: split into `MessageEncoder` (send state: compression context, dict, out_buf)
and `MessageDecoder` (recv state: dict, decompress context). The encoder moves into
`DirectIoState::encoder` (its own `async_lock::Mutex`); the decoder stays in
`PeerIo`. The sender locks the encoder, compresses, then pushes results into
`EncodedQueue` — the same flush path as uncompressed messages. The driver's
read-path lock on `PeerIo` no longer blocks the sender.

Additionally, `Payload` gained ergonomic zero-copy accessors (`as_bytes`,
`as_slice`, `is_contiguous`) so callers can inspect single-chunk payloads without
coalescing.

### Phase 8 — tokio: actor bypass on send + recv hot paths (`ebf2542`)

After Phase 6 stripped the pump-task hop, profiling the remaining tokio gap to
zmq.rs showed the per-message cost was now dominated by the **actor itself**:
`Socket::send` round-tripping through `cmd_tx → SocketCommand::Send → spawn →
flume push → oneshot ack` (~3 context switches), plus inbound messages going
`ConnectionDriver → peer_out → SocketDriver event loop → recv_tx` (~1 extra
hop). The actor existed to serialize state mutation, but PUSH/DEALER/PUB/PAIR/
CLIENT/SCATTER/CHANNEL `pre_send` and PULL/DEALER/SUB/XSUB/PAIR/CLIENT/CHANNEL/
GATHER `post_recv` are identity or stateless frame-count checks — nothing to
serialize.

**Send bypass.** `Inner` gained a `SendSubmitter` clone (built from the
`SendStrategy` before the driver is spawned). `Socket::send` matches on
socket type: REQ/REP go through `cmd_tx` as before (alternation bit
mutates); everything else inline-validates frame count and pushes straight
into the submitter. `SendSubmitter` is already lock-free MPMC over flume,
so concurrent cloned `Socket` handles are fine.

**Recv bypass.** `ConnectionDriver` gained `recv_direct:
Option<async_channel::Sender<Message>>`. When set (via
`with_recv_direct(recv_tx.clone())`), the event-drain loop pushes
`Event::Message` straight into the user-facing recv channel and skips
`peer_out`. The actor still receives `Connected`/`Disconnected` events on
`peer_out` so peer-table bookkeeping is unaffected. Types that need
post-processing (Rep/Router/Server/Peer identity prefix, Dish group filter,
XPub subscribe parsing) keep `recv_direct = None` and go through the actor.

**Numbers (TCP loopback, single PUSH/PULL):**

| Size | omq-tokio before | omq-tokio after | zmq.rs |
|------|------------------|-----------------|--------|
| 128 B | 84k msg/s | 4.03M msg/s | 304k |
| 2 KiB | 72k msg/s | 1.72M msg/s | 263k |

The 48× lift at 128 B is hop-count savings; the multi-core parallelism that
tokio always had is now exposed because the actor is no longer the
serialization bottleneck. zmq.rs runs the same tokio multi-thread runtime but
routes every message through its socket actor's mpsc inbox — that's why
omq-tokio (13.2×) widens versus zmq.rs even though omq-compio's single-core
io_uring path lands at 9.2× on the same wire.

### What was tried and abandoned

**Direct-write fast path (Stage 4, reverted).** An earlier experiment let
`Socket::send` encode + call `write_vectored` inline on the sender's task,
completely bypassing the driver. RTT dropped from ~165 µs to ~85 µs — a clean 2×
win on latency. **PUSH/PULL throughput collapsed by 4–7×** at 128 B (TCP:
~830k → ~115k msg/s). Cause: the pre-bypass driver batched N queued messages per
`select!` wakeup into one `writev`. The inline path did one `writev` per
`Socket::send` call. The lesson: a hop that looks like pure latency overhead may
be providing implicit batching that is critical for throughput. The recv-side
bypass (`try_direct_recv`) was kept because it doesn't change the send pipeline.

---

## Adding a new socket type / transport / mechanism

**New socket type**: add the variant to `omq_proto::proto::SocketType` and
`is_compatible`, then wire send/recv strategy in both backends' routing.

**New transport**: add an `Endpoint` variant and parser in
`omq-proto/src/endpoint.rs`, then bind/connect glue in
`omq-compio/src/transport/`. Compression-style transports are implemented as
`MessageEncoder` / `MessageDecoder` layers on top of TCP, not separate transport
variants.

**New mechanism**: add a module under `omq-proto/src/proto/mechanism/`,
feature-gate it, register with the greeting/handshake state machine, and add
integration tests for both backends.

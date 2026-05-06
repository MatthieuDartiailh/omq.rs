# omq-compio internals

A tour of `omq-compio`: what every type does, how they relate, how a
message travels from `socket.send(msg)` to the wire and back, and which
techniques keep the hot path fast. Cross-cutting basics
(three-layer split, two-queue model, multi-chunk payloads) live in
[`architecture.md`](architecture.md).

`omq-compio` is the single-thread backend. Within one runtime the
scheduler is cooperative -- no preemption, no context switch inside a
task -- and every type below is designed around that invariant.

## Key types at a glance

| Type | File | Role |
|------|------|------|
| `Socket` | `socket/handle.rs` | Public handle; `Clone + Send + Sync`; all `&self` methods |
| `SocketInner` | `socket/inner.rs` | Arc'd shared state: peers, recv queue, send queue, monitor |
| `PeerSlot` | `socket/inner.rs` | One connected peer: outbound channel, DirectIoState, info |
| `PeerOut` | `socket/inner.rs` | `Inproc { sender, identity }` or `Wire(WirePeerHandle)` |
| `DirectIoState` | `socket/inner.rs` | Per-wire-peer I/O machinery: codec, writer, fast-path state |
| `EncodedQueue` | `socket/inner.rs` | Zero-copy ZMTP encoder; bypasses codec mutex on hot path |
| `PeerIo` | `transport/peer_io.rs` | Codec + decoder + reader behind one sync mutex |
| `RecvStream` | `transport/peer_io.rs` | Pinned multi-shot recv stream yielding `BufferRef` |
| `WireReader` / `WireWriter` | `transport/peer_io.rs` | Enum over TCP / IPC; static dispatch, no `Box<dyn>` |
| `run_connection` | `transport/driver.rs` | Per-peer driver loop; `select_biased!` over stream/cmd/hb |
| `MonitorPublisher` | `monitor.rs` | Publishes lifecycle events to subscribers |
| `MonitorStream` | `monitor.rs` | Per-subscriber event stream with lag counter |

## `SocketInner` -- shared socket state

Every `Socket` clone holds `Arc<SocketInner>`. All mutation goes through
`RwLock` / `Mutex` / atomics so the handle is freely cloneable across
tasks.

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
  listeners / dialers / udp_dialers: RwLock<Vec<...>>, // task handles (drop = cancel)
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

`direct_io` is swapped to `None` by the driver on exit and back to
`Some(new_state)` by the dial supervisor on reconnect. `Socket::send`'s
fast path reads the inner `Option`; `None` means reconnect is in flight
-> fall back to `cmd_tx`.

### `PeerOut`

```rust
enum PeerOut {
  Inproc { sender: flume::Sender<InprocFrame>, our_identity: Bytes },
  Wire(WirePeerHandle),  // Arc<RwLock<flume::Sender<DriverCommand>>>
}
```

Inproc peers receive a frame directly in the peer's shared `in_tx`.
Wire peers go through a per-peer command channel to the driver task.

## `DirectIoState` -- per-wire-peer fast-path state

Shared between the driver task and the `Socket::send` / `Socket::recv`
callers.

```
DirectIoState {
  // Codec + read side
  peer_io: SharedPeerIo,           // Arc<std::sync::Mutex<PeerIo>>
  recv_stream: LocalStream,        // async_lock::Mutex<Option<RecvStream>>

  // Write side (separate from codec so codec lock can be dropped first)
  writer: async_lock::Mutex<WireWriter>,

  // Fast-path send bypass (NULL mechanism, and also transform path)
  encoded_queue: Mutex<EncodedQueue>,
  encoder: async_lock::Mutex<Option<MessageEncoder>>,  // lz4 / zstd send side
  has_transform: bool,             // selects encoder path over passthrough
  transform_passthrough: Option<(Bytes, usize)>,  // sentinel + threshold for bypass
  driver_in_select: AtomicBool,    // driver is parked; notify to wake
  transmit_ready: Event,           // sender -> driver wakeup signal

  // Recv-direct arbitration
  recv_claim: AtomicU8,            // 0 = driver reads, 1 = recv() owns reads
  recv_state_changed: Event,       // claim flip -> driver re-evaluates
  recv_codec_ready: Event,         // driver fed codec while claim=1 -> wake user
  eof_signal: Event,               // recv() signals EOF to driver

  // Misc
  handshake_done: AtomicBool,
  last_input_nanos: AtomicU64,     // heartbeat input timestamp
  hb_epoch: Instant,               // monotonic origin for hb math
}
```

`peer_io` is a **sync** mutex. The codec is driven from a single-thread
runtime and the lock is never held across `.await`, so `.lock()` cannot
block waiting on a parked holder. This is what makes the recv path
cancel-safe: there is no `.await` between pulling a `BufferRef` from
the multi-shot stream and feeding it to `handle_input`. A future drop
in that window is impossible.

`recv_stream` is wrapped in an `unsafe Send + Sync` `LocalStream`
because compio's `SubmitMultiManaged` is not `Send`, and `Arc` requires
`Sync`. The wrapper is sound only because the runtime is thread-pinned
-- the stream is never accessed from another thread.

The **writer** lives separately from `PeerIo` so the driver can release
the codec lock before calling `write_vectored`, opening a window for
the sender to encode the next message while I/O is in flight.

The **encoder** lives separately from `PeerIo` so the sender can
acquire it independently of the driver's `peer_io` lock (which is held
during reads). On compio's cooperative single-thread runtime
`encoder.try_lock()` always succeeds when called from the sender, since
no other task runs concurrently. The encoded output goes into
`EncodedQueue` -- the same flush path as the NULL passthrough -- so the
transform path benefits from drain-vec reuse and flat-buf batching just
like uncompressed messages.

## `EncodedQueue` -- the direct-encode bypass

When `has_transform == false` (NULL mechanism, no compression),
`Socket::send` encodes ZMTP frames directly into an `EncodedQueue`
under a **sync** `Mutex`. The driver drains the queue and calls
`write_vectored` (or `write_all` for the flat region) in step 3b.

```
EncodedQueue {
  chunks: VecDeque<Bytes>,   // large-message chunks: header + Arc-bumped payload
  flat_buf: BytesMut,        // contiguous backing for small messages (< FLAT_THRESHOLD)
  total_bytes: usize,        // for cap detection (512 KB default)
  scratch: BytesMut,         // reused header buffer -- zero alloc post-warmup
}

const FLAT_THRESHOLD: usize = 32 * 1024;
```

Two encoding paths, chosen per message:

**Small messages (total bytes < `FLAT_THRESHOLD`)** -- `encode_flat`:
1. Writes `[flags, size]` header + all payload bytes contiguously into
   `flat_buf`.
2. No `Bytes` allocation; no Arc bump. N small messages land in one
   contiguous region.
3. At flush, `flat_buf.split().freeze()` produces one `Bytes` covering
   all N messages -> **1 iovec** for N messages (vs. 2N for the
   large-message path).

**Large messages (total bytes >= `FLAT_THRESHOLD`)** --
`encode_and_push`:
1. `flat_buf` is first flushed to `chunks` (one `split().freeze()`) to
   maintain wire order.
2. Writes header into `scratch`, calls `scratch.split().freeze()` ->
   owned `Bytes`.
3. For every payload `Bytes` chunk: `clone()` (one atomic increment, no
   copy).
4. Header + payload chunks appended to `chunks`; kernel gathers via
   `writev`.

The driver flushes via `drain_into_vec(&mut reused_vec)` (same
`Vec<Bytes>` reused across iterations) -> `write_vectored`. On partial
write, `put_back_unwritten` slices the last partially-written `Bytes`
and prepends unwritten chunks to the front.

**Why this matters for small messages:** the sync `Mutex::try_lock` is
much cheaper than an async mutex acquisition; frame encoding is inlined
without going through the codec's transmit buffer (no
`clone_transmit_chunks` + `advance_transmit`); and packing N small
frames into one `BytesMut` region cuts the iovec count from 2N to 1,
reducing `writev` overhead and improving kernel batching.

## `PeerIo` -- codec, transform, and reader

```rust
PeerIo {
  codec: Connection,               // omq-proto ZMTP codec
  decoder: Option<MessageDecoder>, // lz4 or zstd receive-side decompressor
  reader: WireReader,              // TCP or IPC read half (kept for rearm)
  handshake_done: bool,
}

type SharedPeerIo = Arc<std::sync::Mutex<PeerIo>>;
```

Lock discipline: the `PeerIo` mutex is **never held across an
`.await`**. It is a sync mutex, so holding across `.await` would block
the runtime thread on the next `lock()` from any other task. Acquire,
use, drop -- in one synchronous step.

`reader` is retained on `PeerIo` (not just used at bring-up) so the
recv path can rebuild the multi-shot stream after kernel termination
(see `LocalStream::rearm`).

The send-side encoder (`MessageEncoder`) was deliberately removed from
`PeerIo` and placed in `DirectIoState::encoder` so sender and driver
can encode / decode concurrently without contending on the same mutex.

### `WireReader` / `WireWriter` -- static dispatch

```rust
enum WireReader { Tcp(AsyncFd<TcpStream>), Ipc(AsyncFd<UnixStream>) }
enum WireWriter { Tcp(OwnedWriteHalf<TcpStream>), Ipc(OwnedWriteHalf<UnixStream>) }
```

An enum over the small set of supported stream transports lets the
compiler emit a static `match` at the call site instead of a virtual
dispatch through `Box<dyn Trait>`. The original `Box<dyn DynWriter>`
shape allocated once per send and once per read on the hot path; that
allocation alone dominated throughput at 128 B message sizes.

The reader holds an `AsyncFd<T>` (not `OwnedReadHalf<T>`) because
compio's multi-shot recv API (`AsyncReadMulti`) is implemented for
`AsyncFd` only. The write half stays on `OwnedWriteHalf` -- no managed
write API is needed.

### `RecvStream` -- multi-shot recv

```rust
type RecvStream =
    Pin<Box<dyn Stream<Item = io::Result<BufferRef>> + 'static>>;

struct CancellableRecvStream {
    stream: RecvStream,
    cancel: compio::runtime::CancelToken,
}
```

Built once per connection via
`WireReader::build_recv_stream()` -> `RecvMulti` -> `submit_multi(op)
.into_managed(pool)`, paired with a fresh `CancelToken`. One
persistent SQE per connection: the kernel selects a buffer from the
runtime's `BUF_RING` only when bytes are ready, posts a CQE carrying
a `BufferRef`, and the stream yields it. Dropping the consumer
future of `.next()` does NOT cancel the SQE -- bytes accumulate in
the ring and are picked up by the next poll. Every `.next().await`
poll site wraps with `.with_cancel(cancel.clone())` so the
`SubmitMulti`'s first poll registers its op key with the token; the
large-frame switch can later call `cancel.clone().cancel()` to
submit an `IORING_OP_ASYNC_CANCEL` and drain pending CQEs
deterministically.

Stored in `DirectIoState::recv_stream: LocalStream`. `LocalStream::rearm`
rebuilds it after `ENOBUFS` (the kernel terminates the multi-shot SQE
when the pool is exhausted) -- a fresh stream gets a fresh token.

### `MessageEncoder` / `MessageDecoder`

```rust
enum MessageEncoder {
  Lz4(Lz4Encoder),     // feature = "lz4"
  Zstd(ZstdEncoder),   // feature = "zstd"
}

enum MessageDecoder {
  Lz4(Lz4Decoder),     // feature = "lz4"
  Zstd(ZstdDecoder),   // feature = "zstd"
}
```

`MessageEncoder::for_endpoint` constructs a matched `(MessageEncoder,
MessageDecoder)` pair for compression transports (`lz4+tcp://`,
`zstd+tcp://`). The encoder lives in `DirectIoState::encoder`; the
decoder lives in `PeerIo::decoder`. They hold independent state
(compression context, dictionary) so each can be locked separately --
the sender and driver never contend on the same mutex for encode vs.
decode.

On send: `encoder.encode(&msg)` -> `TransformedOut` (a `SmallVec` of
wire messages) -> each pushed into `EncodedQueue`. On recv:
`decoder.decode(wire_msg)` -> `Option<Message>` (`None` = dictionary
shipment, silently consumed at transport).

Messages smaller than the compression threshold (512 B without a
dictionary) pass through as `SENTINEL_PLAIN | [0,0,0,0] | body` -- no
actual compression, just a 4-byte prefix.
`encoder.passthrough_info()` returns `(sentinel, threshold)`; when set,
`try_direct_encode` uses the `EncodedQueue` passthrough path directly
without locking the encoder at all.

## Driver loop -- `run_connection`

One driver task per wire connection. Runs the `select_biased!` loop
below.

```
loop {
  state.driver_in_select.store(false)

  // Graceful close check
  if closing && pending empty && codec empty && eq empty -> return Ok(())

  // Step 1: drain codec.poll_event() [under sync peer_io lock]
  //   Skipped post-handshake when recv_claim == 1 (user owns the codec
  //   inline; double-draining would surface events out of FIFO order).
  //   Skipped post-handshake when !codec_has_input (nothing new).
  //   HandshakeSucceeded -> set handshake_done, drain pending_cmds; codec_maybe_dirty = true
  //   Message -> decoder.decode, send to peer_in_tx (socket inbound)
  //   Command -> update peer_sub/peer_groups, surface to user if XPUB
  //   codec_has_input = false after drain (re-set by stream_arm)

  // Step 2: dispatch drained events [OUTSIDE peer_io lock]
  //   (awaits here; sync peer_io is dropped before yielding)

  // Step 3a: flush codec transmit buffer [skipped if !codec_maybe_dirty]
  //   clone_transmit_chunks [sync peer_io lock] -> release
  //   writer.write_vectored [writer lock only] -> release
  //   advance_transmit [sync peer_io lock] -> release
  //   codec_maybe_dirty = false when confirmed empty
  //   if wrote: continue

  // Step 3b: flush EncodedQueue
  //   encoded_queue.drain_into_vec(&mut drain_buf) [sync Mutex]  <- same Vec reused
  //   writer.write_vectored [writer lock]
  //   if partial: put_back_unwritten
  //   if wrote: continue

  // Step 4: park
  state.driver_in_select.store(true)
  if !encoded_queue.is_empty() { continue }  // close the store/check race

  select_biased! {
    eof_signal     -> return Ok(())
    timeout_fut    -> return Err(HandshakeFailed)   // pre-handshake deadline
    hb_fut         -> check liveness, send Ping
    stream_arm     -> if recv_claim == 1: park on recv_state_changed.
                      else: lock recv_stream, race-recheck claim, pull
                      BufferRef, sync-lock peer_io, handle_input;
                      codec_has_input = true; if claim flipped to 1
                      while parked, notify recv_codec_ready.
                      ENOBUFS -> rearm and continue.
    cmd_inbox      -> encode SendMessage / SendCommand into codec; codec_maybe_dirty = true
    shared_queue   -> work-steal, encode into codec; codec_maybe_dirty = true
    transmit_ready -> (sender woke us), loop to step 3b
  }
}
```

### Lock discipline during step 3a/3b

The codec lock is released **before** `write_vectored`. This lets the
sender encode the next message (via `EncodedQueue` or the codec path)
while the I/O syscall is in flight.

### Recv-direct claim arbitration

`recv_claim: AtomicU8` arbitrates whether the driver or a `recv()`
caller owns the read path at any moment.

- `0` -> driver pulls `BufferRef`s from the multi-shot stream in the
  `stream_arm`.
- `1` -> a `try_direct_recv` caller has claimed it; driver's
  `stream_arm` parks on `recv_state_changed.listen()`. Step 1 (drain)
  is also skipped under the claim so events stay in the codec for the
  user to drain inline in FIFO order.

The claim is a `compare_exchange(0 -> 1)` protected by a RAII
`ClaimGuard`. On drop, `ClaimGuard` resets to 0 and notifies
`recv_state_changed`. The driver re-evaluates on its next iteration
without holding any lock.

Two race-recheck signals harden the boundary:

- Inside `stream_arm`, after locking `recv_stream`, the driver
  re-loads `recv_claim`. If it is now `1`, the driver releases the
  stream lock and parks. This catches user `claim 0 -> 1` flips that
  happen between the iteration-top sample and the lock acquire.
- After a successful `handle_input`, if `recv_claim == 1` the driver
  notifies `recv_codec_ready`. The user's `pull_and_feed` `select_biased!`
  races this signal: when fired, the user breaks out of `stream.next()`
  and re-drains the codec the driver just populated. Without this, the
  user could be parked on `stream.next()` waiting for bytes the kernel
  has already delivered to the driver's pull, while events sit in the
  codec.

## Send paths

`Socket::send` dispatches by socket type:

| Socket types | Strategy | Key mechanism |
|---|---|---|
| PUSH / DEALER / REQ / PAIR / REP | Round-robin (or priority) | Single peer -> `try_direct_encode`; multi-peer -> shared queue |
| PUB / XPUB | Fan-out, subscription-filtered | Per-peer `SubscriptionSet` checked at send time |
| ROUTER / SERVER / PEER | Identity-routed | First frame = destination; lookup in `identity_to_slot` |
| RADIO | Fan-out to UDP dialers + ZMTP peers, group-filtered | `[group, body]` shape validated |
| XSUB | Pure fan-out | All peers |

### `try_direct_encode` (single wire peer, no transform)

```
1. Check handshake_done (atomic load, no lock)
2. encoded_queue.try_lock() -- if busy (driver flushing), return false
3. Check total_bytes < 512 KB cap
4. if msg.byte_len() < FLAT_THRESHOLD: encode_flat(msg) -> flat_buf (copy, 0 Arc bumps)
   else: encode_and_push(msg) -> chunks (header Bytes + Arc-bump payload)
5. if driver_in_select: transmit_ready.notify(1)
6. Return true
```

If any check fails, falls back to
`cmd_tx.send_async(DriverCommand::SendMessage(msg))`. If `cmd_tx` is
disconnected (peer dead / reconnecting), falls back to the shared
queue.

### `try_direct_encode` (single wire peer, with transform)

```
1. encoder.try_lock() -- if busy (driver is mid-handshake drain), return false
2. Check handshake_done (atomic load)
3. encoder.encode(msg) -> TransformedOut (SmallVec of wire messages)
4. Drop encoder lock
5. encoded_queue.try_lock() -- if busy, return false
6. Check total_bytes < 512 KB cap
7. For each wire message: encode_flat (< FLAT_THRESHOLD) or encode_and_push
8. Drop encoded_queue lock
9. if driver_in_select: transmit_ready.notify(1)
10. Return true
```

The encoder lock is dropped **before** acquiring `encoded_queue`,
keeping the critical section minimal. Because the encoder is separate
from `PeerIo`, the driver can be mid-read (holding `peer_io`) while the
sender encodes simultaneously.

## Recv path

`Socket::recv` tries the **direct-recv fast path** first (for PULL /
SUB / REP / PAIR / REQ sockets with a single wire peer) then falls back
to the `in_rx` channel loop.

### Direct-recv (`try_direct_recv`)

Saves ~12 µs per round-trip by feeding the codec inline instead of
waiting for the driver to parse events and enqueue to `in_rx`.

```
1. Bail if in_rx is not empty (driver buffered something)
2. snapshot_direct_io_single_peer() -> Some(state)?
3. recv_claim.compare_exchange(0 -> 1)   [claim guard]
4. Bail if in_rx is not empty (race-safe recheck)
5. Loop:
   a. Drain codec.poll_event() [sync peer_io lock] -> first Message wins
   b. Race three arms:
      - in_rx.recv_async()           -> drop claim, process inline, return
      - recv_codec_ready.listen()    -> Fed (driver just fed events)
      - pull_and_feed:
          recv_stream.lock; stream.next() [BufferRef from BUF_RING];
          sync-lock peer_io; handle_input -> Fed
   c. After Fed, if in_rx is non-empty -> drop claim, return Ok(None)
      so the channel path drains older events first.
   d. ENOBUFS from stream -> rearm and continue.
   e. Flush any codec transmit (e.g. auto-PONG) via writer lock.
   f. Loop to 5a.
```

Cancel-safety: dropping the recv future at any `.await` is safe.
- At `recv_stream.lock().await` -> stream untouched.
- At `stream.next().await` -> multi-shot SQE persists, bytes stay in
  `BUF_RING`.
- After `stream.next()` returns `Some(Ok(buf))` there is NO `.await`
  before `handle_input` (sync `peer_io` lock + sync `handle_input`),
  so a drop in this window is impossible.

### `in_rx` fallback loop

Processes `InprocFrame` variants from the socket-wide bounded channel:
- `SinglePart { peer_identity, body }` -- hot path, ~72 B slot
- `Message(Box<InprocFullMessage>)` -- multipart, boxed to keep slot small
- `Command(Command)` -- XPUB-only: wrap as `\x01<topic>` or `\x00<topic>`

## Transports

### TCP / IPC

Both use the same `run_connection` driver. Only the
`WireReader`/`WireWriter` enum variant differs. TCP sets `TCP_NODELAY`
after accept/connect.

`bind_tcp` spawns an accept loop; `connect_tcp_with_reconnect` spawns a
dial supervisor that handles exponential backoff reconnection.

### Inproc

No driver, no codec, no handshake. A global `REGISTRY`
(`LazyLock<Mutex<HashMap<String, Sender<...>>>>`) maps names to
`Sender<InprocConnectRequest>`. `bind` registers a name; `connect`
sends a request.

Peers exchange an `InprocPeerSnapshot` (socket type + identity)
synchronously. Messages flow as `InprocFrame` through flume channels.
The `InprocListener` drops from the registry on `Drop`.

### UDP (RADIO / DISH)

No driver, no ZMTP codec, no handshake. `RADIO` stores `Arc<UdpSocket>`
per `connect()`; `send_radio` encodes `[group, body]` into a datagram
and calls `sock.send`. `DISH` `bind()` spawns a `recv_from` loop that
decodes datagrams and checks `joined_groups` locally.

### `lz4+tcp` / `zstd+tcp`

Dialed and accepted as plain TCP. After the TCP connection is up, a
matched `(MessageEncoder, MessageDecoder)` pair is constructed via
`MessageEncoder::for_endpoint`. The encoder is installed in
`DirectIoState::encoder`; the decoder in `PeerIo::decoder`. Every
post-handshake message passes through `encoder.encode` (sender) /
`decoder.decode` (receiver). Neither touches the ZMTP handshake.

## Monitor subsystem

`MonitorPublisher` (one per `SocketInner`) holds a `Vec<MonitorSink>`.

```
publish(event):
  lock sinks
  prune disconnected receivers
  for each sink: try_send(event) [non-blocking]
    if full: lag += 1 [atomic]
```

`Socket::monitor()` returns a `MonitorStream` (one per `subscribe()`
call):

```
MonitorStream { rx: flume::Receiver<MonitorEvent>, lagged: Arc<AtomicU64> }

recv():
  if lagged.swap(0) > 0 -> return Err(Lagged(n))
  else -> rx.recv_async()
```

Events published: `Listening`, `Accepted`, `Connected`,
`ConnectDelayed`, `HandshakeSucceeded`, `Disconnected`, `PeerCommand`,
`Closed`.

## Reconnect mechanism

The dial supervisor task owns the `DirectIoHandle`
(`Arc<RwLock<Option<Arc<DirectIoState>>>>`) and the `WirePeerHandle`.

On driver exit:
1. `direct_io_handle` is set to `None` -> fast-path sends fall back to
   `cmd_tx`.
2. `cmd_tx` is disconnected (driver task dropped `Receiver`) -> send
   falls back to shared queue.
3. Messages buffer in the shared queue (bounded by `send_hwm`).
4. Supervisor dials with exponential backoff.
5. New `DirectIoState` is built and installed; `direct_io_handle` is
   restored.
6. New driver drains the shared queue.

Subscriptions and group joins are replayed by the `snap_listener` task
after each handshake via `our_subs` and `joined_groups`.

## Memory and allocation model

### `InprocFrame::SinglePart`

The single-part variant carries `Option<Bytes>` (identity) and `Bytes`
(body) inline (~72 B). The `Message` struct is ~624 B; wrapping it in
a box for the multipart variant keeps the flume channel slot small on
the hot path.

### Header scratch buffers

Two separate scratch buffers amortize frame-header allocations:

- `EncodedQueue::scratch: BytesMut` -- used by the compio sender for
  large-message ZMTP headers. After warmup it is permanently allocated;
  `clear()` + `extend()` + `split().freeze()` produces an owned `Bytes`
  with zero allocator calls per header.
- `Connection::header_scratch: BytesMut` (in `omq-proto`) -- used by the
  codec on the normal `send_message` path (transform transports, cmd
  encoding, CURVE). Holds up to 64 KiB; replaced when remaining
  capacity drops below 9 bytes. Roughly one allocation per ~7000
  frames (64 KiB / 9 B max header size).

## Runtime configuration

The recv path uses `io_uring` provided buffers (`IORING_REGISTER_PBUF_RING`).
Each runtime owns one `BufferPool` from which the kernel selects a slot
on every multi-shot CQE. Pool size is set on the `ProactorBuilder`:

```rust
use omq_compio::runtime::ProactorBuilderExt;

let mut proactor = ProactorBuilder::new();
proactor.with_omq_buffer_pool();              // 128 x 32 KiB (4 MiB)
// or:
proactor.with_omq_buffer_pool_sized(256, 64 * 1024);

let rt = RuntimeBuilder::new().with_proactor(proactor).build()?;
```

`omq_compio::build_default_runtime()` is the convenience constructor;
bench harnesses, integration tests, and `pyomq` use it. External
callers building their own `Runtime` must call one of the helpers --
the default 8 x 8 KiB pool is too small for sustained delivery and
trips `ENOBUFS` rapidly. Each multi-shot CQE consumes one slot until
the consumer drops the `BufferRef`, so a single connection in a
gigabit burst can hold ~8 slots in flight.

`ENOBUFS` is recoverable. The driver and the user both detect the
errno on `stream.next()` and call `LocalStream::rearm` to rebuild the
`RecvStream` from `WireReader::build_recv_stream()`. Bytes in the
kernel socket buffer are picked up by the new SQE -- nothing is lost.

Linux >= 6.0 is required (multi-shot recv with provided buffers).

### Pool sizing recipe

The default 128 x 32 KiB pool (4 MiB per runtime) is tuned for the
common ZMQ workload: messages up to ~32 KiB. For larger payloads,
slot size matters: a message that does not fit in one slot is split
across N slots and each slot is fed to the codec separately, so per-
chunk overhead grows linearly in N. A bigger slot is the cheap fix.

| Peak msg size | Recommended pool | Pool RAM per runtime |
|---|---|---|
| ≤ 32 KiB | `with_omq_buffer_pool()` (default 128 x 32 KiB) | 4 MiB |
| ≤ 256 KiB | `with_omq_buffer_pool_sized(128, 256 * 1024)` | 32 MiB |
| ≤ 1 MiB | `with_omq_buffer_pool_sized(64, 1024 * 1024)` | 64 MiB |
| ≤ 16 MiB | `with_omq_buffer_pool_sized(16, 16 * 1024 * 1024)` | 256 MiB |
| ≥ 100 MiB (one-off snapshots) | `with_omq_buffer_pool_sized(8, slot)` with `slot ≥ peak` | depends |

Trade-offs:

- **Slot size** sets how much of one message fits in one buffer. Slots
  bigger than peak msg size waste bytes on every smaller message.
- **Slot count** sets how many slots can be in flight before `ENOBUFS`
  forces a rearm. More slots = better burst absorption but more pinned
  RAM. A single high-rate connection rarely needs more than `2 × peak
  inflight CQEs` worth of slots.
- For frames whose wire payload exceeds
  `Options::large_message_threshold` (default 128 KiB), the recv path
  switches to a cancel-and-drain + sized one-shot Recv, allocating one
  contiguous `BytesMut` for the whole payload. The pool slots are not
  used for that frame's tail, so very-large messages do not require a
  proportionally-large pool. The cancel-drain prefix is bounded by one
  or two pool slots; see "Zero-copy recv for large frames" in
  `performance.md`. Set `Options::disable_large_message_path()` (or
  `large_message_threshold(0)`) to keep every frame on the multi-shot
  path -- useful for low-latency profiles where the per-cancel SQE-
  rebuild cost (~tens of µs) is worse than the memcpy.

If you don't know your workload, start with the default; reads
exceeding the slot size still work (each chunk produces its own CQE),
just slower than a single-slot delivery would be.

## Concurrency model

`omq-compio` is designed for **single-threaded compio runtimes**.
Within one runtime the scheduler is cooperative -- no preemption, no
context switch inside a task. This means:

- `driver_in_select` is written and read without barriers in practice,
  though `Relaxed` atomics are used for correctness across executor
  cores.
- `Mutex::try_lock` on `encoded_queue` almost never fails on a
  single-core runtime because the driver cannot preempt the sender.
- All lock acquisitions are brief; none are held across yields.

For multi-core deployments, instantiate one `compio::runtime::Runtime`
per worker thread and pin it via `RuntimeBuilder::thread_affinity`.
Cross-runtime sends go through flume MPSC (thread-safe). This typically
lifts wire throughput by 20-40 % for TCP/IPC small messages (sender and
receiver overlap their I/O).

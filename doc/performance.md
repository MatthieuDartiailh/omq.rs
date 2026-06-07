# How to beat libzmq

Design choices and dead ends behind the throughput numbers in
[`../COMPARISONS.md`](../COMPARISONS.md).

For structure, see [`architecture.md`](architecture.md),
[`compio.md`](compio.md), [`tokio.md`](tokio.md).

## The problem

Beating libzmq at small messages over a real TCP socket is the
hardest part. Large messages are easy: `writev` batching
multi-chunk frames in one syscall vs. libzmq's separate `send()`
for header + payload gives 2-3x above 2 KiB from the start.

Small messages (8-128 B) are different. Encoding is cheap, kernel
round-trips dominate. libzmq separates the application thread from
a dedicated I/O thread: the app encodes and hands off via a pipe;
the I/O thread writes. At 128 B that overlap is the primary
advantage. A naive single-threaded ZMQ library cannot keep up.

## Starting point: pure Ruby

The two-queue architecture comes from a sister project,
[OMQ Ruby](https://github.com/paddor/omq) -- a pure-Ruby ZMTP
implementation. One inbound queue and one outbound queue per
socket, not per peer. Per-connection driver fibers push/pull
between the queues and the wire. On a 2018 Mac Mini in a Linux
VM, it pushes ~500k 128 B msg/s over TCP -- already faster than
zmq.rs on the same wire, despite Ruby.

omq.rs is the Rust port of that idea, first targeting OMQ Ruby's
throughput, then libzmq's.

## Sans-I/O codec

The full ZMTP state machine -- greeting, mechanism handshake
(NULL/CURVE/BLAKE3ZMQ), framing, compression transforms -- lives
in `omq-proto` and never touches an fd. Bytes in via
`Connection::handle_input`, events out via `poll_event`, outbound
via `poll_transmit`/`advance_transmit`. Backends own I/O.

This is the `rustls::ConnectionCommon`/`quinn-proto` shape. The
same crate drives both the single-thread io_uring backend and the
multi-thread tokio backend, byte-for-byte identical wire output.
Test surface stays small: greeting, handshake, framing,
subscription matching all run synchronously with no runtime.

## Multi-chunk frame payloads

Every layer prepends its prefix (sentinels, identities, ZMTP
headers) by pushing one more `Bytes` onto a `Payload` -- never
copying the payload. At write time the codec flattens chunks into
`Vec<IoSlice>` and the kernel stitches them via `writev`/`sendmsg`.

The 2-3x advantage over libzmq at >= 2 KiB comes almost entirely
from this.

## Zero-copy where it pays off

libzmq copies every message through at least two internal queues
(application -> I/O thread mailbox -> kernel). omq avoids userspace
copies on the hot path for medium and large messages:

**Send.** `Bytes` payloads are Arc-cloned (refcount bump, no data
copy) from `Socket::send` through frame encoding to the kernel
`writev`. `encode_message_gather` pushes the payload `Bytes`
reference directly into the iovec list; only the 2-9 byte frame
header is serialized. For small messages below `ARENA_THRESHOLD`
(32 KiB), contiguous encoding into the arena buffer is faster
than per-message gather-write.

**Recv.** For frames above `large_message_threshold` (128 KiB),
the compio backend reads the payload directly into a pre-allocated
`BytesMut` via a one-shot `read_until`, bypassing the BUF_RING
pool entirely. Small frames use multi-shot recv from the pool (one
memcpy per CQE). Net copy count: 0 extra copies for large
messages, 1 for small.

**Inproc.** Same-process transfers are `Arc<Bytes>` clones with no
serialization. Throughput is constant regardless of payload size.

The result is most visible at 32 KiB-128 KiB TCP, where omq
sustains 2x the throughput of libzmq: the saved copies keep the
data in L3 instead of flushing it to DRAM.

## First Rust attempt: pure tokio actor

Per-socket `SocketDriver` actor, per-peer `ConnectionDriver` via
flume. Every message round-trips through the actor.

Result: ~80k 128 B msg/s over TCP. zmq.rs: ~300k. libzmq: ~3M.

Three context switches per send (`cmd_tx.send` + `tokio::spawn` +
oneshot ack) plus a per-peer mpsc hop.

## Choosing an io_uring runtime

Tried `monoio` first. Working port, fast I/O, but the API was
difficult (buffer ownership, lifetimes, cancellation). `compio` had
better ergonomics (closer to tokio) and cross-platform support
(io_uring on Linux, IOCP on Windows, kqueue on macOS).

omq-tokio is maintained as a second backend because most Rust
applications use tokio.

## Even with io_uring, hops are the bottleneck

Naive io_uring port: throughput barely improved. io_uring's speed
shows up only when the hot path is short enough to expose it. Two
task hops + async-mutex per message means io_uring wins by a few
percent over epoll, not an order of magnitude.

## Eliminating task hops on send

For PUSH/DEALER/PUB/PAIR/CLIENT/SCATTER/CHANNEL, `pre_send` is
stateless. Routing through the actor mutates nothing.

Fix (tokio): `SendSubmitter` clone from the routing strategy.
`Socket::send` matches on type -- REQ/REP keep the actor (real
per-message state), everything else pushes straight into the
submitter.

Fix (recv): connection driver gets a clone of the user-facing
`recv_tx` and pushes directly, skipping the actor.

**128 B PUSH/PULL TCP on tokio: 84k -> 4.0M msg/s (48x).**

## Removing the pump task

Round-robin routing kept a shared `DropQueue` receiver with a
per-peer pump task. Three hops end-to-end.

Fix: each `ConnectionDriver` holds the shared receiver directly,
greedily drains up to 256 messages per wakeup, encodes, flushes
with one `write_vectored`. One hop for byte-stream sockets.

## Single-peer fast path (compio)

For sockets connected to one wire peer (PUSH/REQ/REP), even one
channel hop costs measurable latency.

`DirectIoState` contains an `EncodedQueue` behind a sync `Mutex`.
`Socket::send` does `try_lock` (sync, not async), encodes ZMTP
frames directly, returns. The driver drains and writes on its
next loop.

Sync `Mutex::try_lock` on a single-thread cooperative runtime
almost never fails. Fallback: channel path. Second fallback:
socket-wide shared queue.

**128 B TCP compio: ~1.30M -> 1.48M msg/s.**

## Direct-recv (compio)

Before: `kernel -> driver wakeup -> codec -> channel push ->
Socket::recv wakeup`. Two task hops.

Direct-recv: `Socket::recv` claims the fd via a one-byte atomic,
pulls bytes from multi-shot recv, feeds the codec, drains a
message -- all on the caller's task.

Cancel-safe by construction: multi-shot recv uses io_uring's
`BUF_RING`, dropping the future does not cancel the SQE. Bytes
accumulate as `BufferRef`s and are picked up by the next poll.

**REQ/REP IPC RTT at 32 B:**

| stage | p50 RTT |
|---|---|
| baseline | ~150 µs |
| send-side fast path | ~100 µs |
| + direct-recv | <60 µs |
| + later optimizations | ~20 µs |

## Iovec batching for small messages

At 128 B throughput peaks, the sender issued `writev` with 1000+
tiny iovecs (2 per message: header + payload). Kernel limit is
1024 per call.

Fix: `EncodedQueue` keeps an `arena: BytesMut` (256 KiB initial
capacity). Messages below `ARENA_THRESHOLD` (32 KiB) are written
contiguously into the arena. N small messages produce one iovec for
the whole batch. Above the threshold, the gather-write path
wins because memcpy of a large payload would dominate.

**128 B TCP compio: 1.48M -> ~3.00M msg/s.** Past libzmq's 2.95M.

## One alloc per 7000 frames

Per-frame `BytesMut::with_capacity(9)` for the 1-9 byte ZMTP
header showed up in `samply`. Fix: `Connection::header_scratch`
is a 64 KiB buffer, reused across messages. One allocation per
~7000 frames.

## Static dispatch on transports

Replaced `Box<dyn DynReader>`/`Box<dyn DynWriter>` with:

```rust
enum WireReader { Tcp(OwnedReadHalf<TcpStream>), Ipc(OwnedReadHalf<UnixStream>) }
enum WireWriter { Tcp(OwnedWriteHalf<TcpStream>), Ipc(OwnedWriteHalf<UnixStream>) }
```

No heap alloc, no vtable. The variant set is closed -- new wire
transports are rare.

## Compression split

`MessageTransform` held both encode and decode behind one
`PeerIo` mutex. The driver holds that mutex during `handle_input`,
so `try_lock` from the sender almost always lost.

Split: `MessageEncoder` in `DirectIoState::encoder` under its own
mutex. `MessageDecoder` stays in `PeerIo`. Sender no longer
blocked by the driver's read path.

## Inproc bypasses ZMTP

Same-process `inproc://` connections skip wire framing entirely.
Global name registry, direct `InboundFrame` exchange via channels.
Hot-path `SinglePart` variant is ~72 B. Throughput: ~3M msg/s
for any size below 32 KiB, >100 GB/s nominal at 32 KiB+ (no
kernel crossing).

## Large-frame recv: accumulation + OneShot

For large messages spanning multiple BUF_RING buffers, the codec's
`split_to` hit its slow path: allocate + copy everything
contiguous. Combined with the per-buffer copy on the way in: 2x
memcpy of the full payload. At 512 KiB, 96% of recv-side
instructions were in memcpy.

Fix: when the codec's head frame exceeds `large_message_threshold`
(default 128 KiB), the compio backend bypasses the codec's chunked
buffer and accumulates into a single pre-allocated `BytesMut`.

Two recv modes:

```
MultiShot --[ENOBUFS during accumulation]--> OneShot
    ^                                            |
    +------------[small frame]-------------------+
```

**MultiShot** (default): persistent multi-shot recv SQE from
BUF_RING pool. Small messages stay here permanently.

**OneShot**: a single `read_until` pulls the payload directly into
pre-allocated `BytesMut`. Triggered when message exceeds pool
capacity (kernel kills the multi-shot SQE with `ENOBUFS`).

Cancel-safe: the accumulation buffer lives in
`DirectIoState::pending_acc`, not in the future's locals. Drop
mid-accumulation -> next `recv()` resumes.

Copy count: small messages 1x memcpy. Large messages: ~1.25x at
256 KiB, ~1.03x at 2 MiB, ~1.00x at 32 MiB+.

Current large-message ratios vs libzmq (compio):

| size | TCP | IPC |
|---|---|---|
| 2 MiB | 1.4x | 2.0x |
| 8 MiB | 1.2x | 1.6x |
| 32 MiB | 1.01x | 1.9x |

Why not `CancelToken`: compio's `cancel_token` checks
`key.has_result()` and short-circuits for multi-shot keys that
already delivered CQEs. Five attempts deadlocked in release
builds. `ENOBUFS` sidesteps the problem -- the kernel terminates
the SQE itself.

## Send-path route caching

Profiling at 128 B TCP: `Socket::send` was 15% CPU, of which only
3% was encoding. The rest: four lock acquisitions and two
`Arc<DirectIoState>` refcount bumps per message for a peer set
that changes maybe once per benchmark.

Fix 1: fused peer selection. One `direct_io` read instead of
three. `peer_alive` eliminated.

Fix 2: generation-gated cache. `peers_gen: AtomicU64` increments
on any peer mutation. Cache hit skips the entire peer lookup.
Cost: one atomic load + one uncontended mutex.

**Inproc 128 B: 3.07M -> 3.42M msg/s (+11%).** TCP/IPC: ~3-5%
(kernel dominates).

Recv-side cache was tried and reverted -- cross-thread Mutex
contention was worse than the uncontended RwLock reads it replaced.

## Closing the small-message recv gap (8 B - 32 B)

At this point omq beat libzmq from 128 B up but trailed at 8 B
and 32 B IPC: ~3.8M vs ~8.4M msg/s (0.45x).

### Profile before (8 B IPC, PULL side)

| % | function |
|---|---|
| 20.4 | decode_assembled_frame |
| 18.3 | try_recv |
| 12.9 | memmove (Bytes::copy_from_slice) |
| 8.1 | shared_clone (Arc increment) |
| 7.8 | shared_drop (Arc decrement) |

Three areas: codec parsing (38%), Bytes refcounting (16%), cache
drain (18%).

### Round 1: recv cache + try_recv drain

Bench loop calls `recv()` then `while try_recv().is_ok() {}`
to drain the batch from cache instead of one async `recv()` per
message.

**8 B: 3.8M -> 6.9M msg/s.**

### Round 2: front_offset, inline Payload, PULL fast path

**`front_offset` in `ChunkedInputBuf`.** `advance(2)` (skip
header) used `Bytes::slice()` -- Arc clone + drop per frame.
New `front_offset: usize` field tracks consumed bytes. `advance`
bumps the offset. Front `Bytes` dropped only when fully consumed.

**Inline `Payload`.** Was `SmallVec<[Bytes; 1]>` (40 B). Now:

```rust
enum PayloadInner {
    Empty,
    Inline { len: u8, data: [u8; 62] },  // no heap, no Arc
    Single(Bytes),
}
```

64 bytes (one cache line). 62 B inline capacity covers most
small-message workloads. Per-frame cost: ~3 atomic ops -> ~0.

**PULL fast path in `try_recv`.** Three specialization levels:
REQ/REP/DISH lock per pop, SUB holds lock for filtering,
PULL/PAIR skips both entirely.

### Round 3: cross-crate inlining

After Rounds 1-2, numbers barely moved. Every hot-path function
was a separate symbol -- `split_to` alone was 11.9% self time.
`omq-proto` and `omq-compio` are separate crates; without LTO
the compiler cannot inline across the boundary.

Fix: `#[inline]` annotations on all hot-path functions in
`omq-proto`. After the Payload-skip fast path landed (Round 8),
the recv hot path no longer crosses crate boundaries --
`try_advance_ready` does everything inside `omq-proto`. LTO is
not needed; `#[inline]` annotations stay.

### Round 4: smaller Message, UnsafeCell recv_cache

**`Message` inline parts 3 -> 1.** Was `SmallVec<[Payload; 3]>`
= 128 B. Single-part PUSH/PULL: two dead slots copied per message.
Now `[Payload; 1]` = 64 B.

**`UnsafeCell` recv_cache.** On compio's single-threaded runtime,
recv_cache is never contended. Replaced `Mutex<VecDeque<Message>>`
with `UnsafeCell`-backed wrapper. Removes one atomic CAS per
try_recv (~8 ns).

### Profile after rounds 3-4 (8 B IPC)

| % | function |
|---|---|
| 66.3 | handle_input (all codec, inlined) |
| 15.8 | drain_remaining_user_events_into |
| 5.7 | bench_peer main |
| 2.0 | memmove |

memcpy: 24% -> 2%. try_recv overhead: gone.

### Round 5: codec-direct try_recv

Store `Arc<DirectIoState>` on `SocketInner`. PULL/PAIR's try_recv
locks `peer_io` directly, pops one event per call. Skips
`drain_remaining` entirely.

drain_remaining: 15.8% -> 0%. Replaced by
`drain_one_user_event` at 11.1%.

### Round 6: separate message queue, batch swap

**Separate `messages: VecDeque<Message>` in `Connection`.**
`absorb_data_frame` pushes directly instead of wrapping in
`Event::Message`. Data plane and control plane are separate
queues.

**Cache-first try_recv.** Check `recv_cache.pop_front()` before
touching `peer_io`. After `drain_and_swap` fills the cache,
~800 messages per batch pop with zero locking.

### Profile after round 6 (8 B TCP)

| % | function |
|---|---|
| 62.5 | handle_input |
| 16.8 | bench_peer main |
| 9.0 | VecDeque::push_back |
| 8.3 | memmove |

Async overhead dropped to noise. Codec + queue + memcpy: >80%.

### Round 7: Message enum, Payload internalized

Replaced `SmallVec<[Payload; 1]>` with a custom enum:

```rust
enum MessageInner {
    Empty,
    Inline { len: u8, data: [u8; 55] },
    Single(Payload),
    Multi(Vec<Payload>),
}
```

64 B (one cache line), covers up to 55 B inline.
`absorb_data_frame` constructs `Inline` directly. SmallVec::drop
disappeared from the profile (was 6%).

`Payload` removed from the public API. Users see only `Message`.

### Round 8: Payload-skip fast path in the codec

Before: `try_decode_frame` -> `Payload::inline` (copy N) ->
`absorb_data_frame` -> `Message::from_inline` (copy N). Two
copies per message.

Fix: `try_advance_ready` combines header peek, buffer read, and
Message construction in one step. For non-command, non-more,
inline-sized, no-crypto, no-multipart frames: one `read_into`
directly into `MessageInner::Inline`. One memcpy.

| path | copies | total bytes (32 B msg) |
|---|---|---|
| before: split_to -> Payload -> Message -> push_back | 3 | 112 B |
| after: read_into -> Message -> push_back | 2 | 96 B |
| libzmq: memcpy(msg_t) | 1 | 64 B |

Remaining gap: `VecDeque::push_back` copies 64 B per message.
libzmq's `yqueue_t` writes in-place (one pointer advance).

### Round 9: SmallVec for parts_payload()

`msg.parts_payload()` returned `Vec<Payload>` -- one malloc+free
per single-part send. Fix: `SmallVec<[Payload; 1]>`.

**8 B IPC: 7.26M -> 7.83M msg/s (+8%).**

### Dead end: arena recv (Bytes::slice)

Tried sharing the read buffer's Arc via `Bytes::slice` instead of
inline copy. Arc bump + drop (~10 ns for two atomics) cost the
same as the inline copy + zeroing. A microbenchmark confirmed:
inline wins or ties at every size up to 55 B. For payloads that
fit in a cache line, the atomic in an Arc bump is more expensive
than the copy.

### Net result

8 B TCP: 3.8M -> 8.2M msg/s (0.45x -> parity with libzmq).
32 B TCP: 3.7M -> 6.6M msg/s (0.45x -> 0.74x).

After rounds 1-9: 8 B TCP 8.72M (1.03x libzmq), 32 B TCP
7.13M (0.84x). The UnsafeCell bypass (below) closed the 32 B
gap entirely.

## Inproc cross-core: blume batching channel

After wire-transport work, inproc-mt at 32 B ran at 2.13M msg/s
-- 25% slower than TCP (2.86M). TCP, which encodes ZMTP frames
and crosses the kernel, was beating a direct in-process channel.

TCP's advantage: batching. Many small messages into one arena buffer,
one io_uring SQE. Two cross-core cache-line transfers for the
whole batch. Inproc used `flume::bounded` -- per-message atomics
and wakeups. Two cache-line round-trips per message (~40-80 ns
each).

### blume: batching MPSC channel

Produce one-at-a-time, consume in batches. Key ideas:

**Coalesced wake.** Notify only on empty-to-non-empty transitions.
N rapid sends -> one wake.

**Swap-drain.** Lock shared queue, `mem::swap` entire VecDeque
into local cache. O(1). Subsequent pops: zero shared-state access.

Microbench (cross-thread, bounded(1024)):

| mode | blume | flume | ratio |
|---|---|---|---|
| try (32 B) | 14.3M | 8.3M | 1.72x |
| async (32 B) | 16.1M | 4.7M | 3.45x |

### Result

| size | before (flume) | after (blume) | TCP |
|---|---|---|---|
| 32 B | 2.13M | 2.90M (+36%) | 2.86M |
| 128 B | 2.36M | 2.51M (+6%) | 2.63M |
| 512 B | 2.45M | 2.55M (+4%) | 2.19M |
| 2 KiB | 2.30M | 2.71M (+18%) | 1.31M |

At 32 B, inproc-mt went from 25% behind TCP to parity.

## Tokio inproc recv_direct

Wire connections got `recv_direct` (bypass actor). Inproc did not:
every message went through the actor. Fix: `spawn_inproc_peer`
checks `can_bypass_actor_recv` and passes `recv_tx` directly to
the inproc driver.

## Things tried and dropped

**Direct-write on send.** Sender does inline `write_vectored`,
skipping the driver. Latency: 165 µs -> 85 µs RTT. Throughput:
830k -> 115k msg/s (4-7x collapse). The driver's implicit
batching was critical -- per-call inline write means one syscall
per message instead of one per N. Reverted.

**TCP_CORK.** Two `setsockopt` syscalls per flush. Regressed
10-15%. The coalescing it provides already comes from
`write_vectored`. The `rzmq` project ships cork toggling inside
io_uring (queued SQE, not syscall) -- that's the only model where
cost-benefit flips.

**Sub-noise-floor alloc reductions.** `SmallVec<[IoSlice; 8]>` for
transmit_chunks, pre-sizing codec buffers. Provably correct, but
below bench noise floor. Kept on a side branch.

## Why the stacked optimizations work

libzmq's I/O thread overlaps encoding with kernel writes.
omq-compio is single-threaded: encoding and `write_vectored` run
sequentially. omq compensates by being shorter everywhere else:

- No actor hop on send for non-REQ/REP.
- No pump task hop for byte-stream peers.
- No async-mutex on encode.
- One iovec per N small messages, not 2N.
- One header alloc per ~7000 frames.
- No vtable/Box on the hot path.
- Encode/write pipelined via lock decomposition (writer mutex
  separate from codec mutex).

No separate I/O thread, but encode pipelines against write.

## Inproc per-peer ypipe: 3M -> 17M msg/s

Each SPSC-eligible inproc connection (PUSH/PULL, PAIR) gets a
dedicated `blume::spsc` ring (1024-slot, lock-free). Per-peer
rings replace the shared blume MPSC channel. The ring carries
`Message` directly (64 B by value); no `InboundFrame` wrapper, no
`Bytes` clone, no heap allocation for messages <=55 B.

Send fast path (PUSH/PAIR, single peer): one `UnsafeCell` access
to the per-peer producer, push, flush. No Mutex, no PeerOut clone,
no generation check.

Recv fair-queue: round-robin `prefetch_and_pop()` across per-peer
consumers. One message per `recv()` call; `try_recv()` also polls
consumers. PULL/PAIR skip `process_inbound_frame` entirely (no
identity routing, no subscription filtering).

Conditional notify via `inproc_parked: Arc<AtomicBool>`: recv sets
it before blocking in select, clears on wake. Producers skip
`Event::notify` when the consumer is actively draining. Under
sustained throughput the notify path is never hit.

Cross-thread only. Same-thread stays on blume: a bounded ring
with spin-on-full cannot coexist with same-thread sequential
send-all-then-recv-all patterns (no concurrent consumer to drain
the ring, so the sender deadlocks at capacity). A fallback to
blume on ring-full was tried but breaks FIFO ordering (messages
sent after the overflow go through the ring, arriving before the
overflow messages buffered in blume).

### Profile (8 B cross-thread)

| % | function |
|---|---|
| 3.2 | SPSC push+flush (the actual work) |
| 17.6 | send() routing |
| 8.4 | Event::notify |
| 6.9 | scoped_tls (compio TLS) |

Ring work is 3.2% of cycles. The rest is async runtime machinery.

### Result

| size | before | after (mt) | libzmq |
|---|---|---|---|
| 8 B | 3.1M | **16.8M** | 10.7M |
| 32 B | 3.1M | **15.2M** | 9.9M |
| 128 B | 3.1M | **12.2M** | 2.9M |

Cross-thread omq-compio: 1.6x libzmq at 8 B, 4x at 128 B.

## Wire send: UnsafeCell bypass (closing the 32 B gap)

Profile at 32 B TCP before this change:

| % | function |
|---|---|
| 13.5 | send_round_robin (Mutex + Arc clones) |
| 10.7 | try_direct_encode (actual encoding) |
| 9.5 | slow_round_robin (dispatch) |
| 5.3 | iter_parts (intermediate Payload copy) |
| 4.5 | memmove |

23% of cycles in routing overhead for a peer set that never
changes during the benchmark. Two fixes:

**`iter_slices` replaces `iter_parts`.** The old path constructed a
temporary `Payload` struct (64 B) for each part of an inline message,
copying the data in and then reading it back out. `iter_slices` yields
`&[u8]` directly from the message's inline storage. One fewer 32 B
memcpy per message on the arena-encode path.

**`direct_send_io: UnsafeCell<Option<(Arc<DirectIoState>, u64)>>`.**
Caches the `DirectIoState` reference with a generation stamp. The
fast path in `Socket::send` reads it unsafely (sound: compio is
single-threaded), checks one atomic (`peers_gen`), and calls
`try_direct_encode` directly. Skips: Mutex lock/unlock,
2× `Arc::clone`, `PeerOut` enum match, 2× `Arc::drop`.

Per-message atomic ops: 6 → 2 (one `peers_gen` load, one
`handshake_done` load inside try_direct_encode; `driver_in_select`
is usually false during sustained send so no notify).

| size | before | after | libzmq | ratio |
|---|---|---|---|---|
| 8 B | 8.72M | **15.2M** | 8.5M | **1.79x** |
| 32 B | 7.13M | **11.7M** | 8.3M | **1.40x** |
| 128 B | 3.00M | **7.1M** | 2.9M | **2.47x** |

The 32 B TCP gap flipped from 0.84x to 1.40x.

## Fan-in recv: unbox + skip identity clone

Profile at 16-peer fan-in (N PUSH → 1 PULL), 32 B TCP:

| % | function |
|---|---|
| 12.8 | memmove (80 B InboundFrame copies through blume) |
| 19.1 | blume total (try_send + try_recv + send_async) |
| 9.2 | Bytes refcount (identity clone + codec Bytes) |

Two fixes:

**Inline `InboundMessage` in `InboundFrame`.** Was
`Box<InboundMessage>` (heap alloc per message). Now inline (80 B
enum). The Box moved to the `Command` variant (cold path: handshake
only). Eliminates malloc+free per message on the hot delivery path.

**Skip `peer_identity.clone()` for non-identity sockets.** PULL/SUB/
PAIR never use the peer identity. The driver was cloning a `Bytes`
(Arc bump + drop) unconditionally. Now gated on `needs_identity`
(ROUTER/REP/SERVER/PEER/STREAM only).

| peers | before | after | gain |
|---|---|---|---|
| 8 | 5.54M | **7.03M** | +27% |
| 16 | ~5.5M | **6.57M** | +19% |

### Why not concurrent_queue for in_tx/in_rx?

blume's swap-drain amortizes the shared Mutex cost across batch size:
one lock acquisition gives the consumer ALL queued messages. With
concurrent_queue's per-pop CAS (3 atomics each), 100 queued messages
= 300 atomic ops vs blume's ~4. On single-threaded compio where the
Mutex is never contended, blume wins when messages batch (which they
always do under load).

## Tokio REQ/REP latency: actor bypass on send

REQ/REP serial ping-pong over TCP measured ~81 µs p50 on tokio vs
~35 µs on compio and ~38 µs on zmq.rs. Root cause: every REQ/REP
send traversed 4 task hops (`Socket::send` → `cmd_tx` → actor
wakes → `tokio::spawn(sub.send(...))` → driver wakes → oneshot ack
back) because `TypeState::pre_send` mutates the alternation bit
and the REP envelope, which lived inside the actor.

Fix: share `TypeState` between the socket handle and the actor via
`Arc<std::sync::Mutex<TypeState>>`. `Socket::send` for REQ/REP
locks it inline, calls `pre_send`, and pushes through
`SendSubmitter`. Same path PUSH already takes. Contention is zero
in practice: REQ/REP alternation guarantees send and recv (which
calls `post_recv` under the same lock in the actor) never overlap.

| transport | before | after | zmq.rs | compio |
|---|---|---|---|---|
| TCP 32 B | 81 µs | 72 µs | 38 µs | 34 µs |
| IPC 32 B | 69 µs | 63 µs | 28 µs | 28 µs |

~10 µs saved (the REQ send-side actor roundtrip).

## Tokio REQ recv bypass (recv_direct)

After the send bypass, REQ recv still routed through the actor: driver →
`peer_out` → actor (`post_recv` strips empty delimiter, clears
alternation flag) → `recv_tx`. Two task hops.

Fix: add `Req` to `can_bypass_actor_recv`. The driver pushes
raw messages directly to `recv_tx`. `Socket::recv` applies
`post_recv_req_direct` inline. Strips the delimiter and clears
the flag without checking `req_awaiting_reply` as a precondition.

The unchecked variant is necessary because `on_peer_disconnected`
(actor-side) can clear the flag before `Socket::recv` consumes
the last queued reply. Before recv_direct this race was impossible:
both `post_recv` and `on_peer_disconnected` ran sequentially in
the actor. With recv_direct they're on separate tasks.

REP recv still routes through the actor because it needs the
identity table lookup from `IdentityRecv::wrap`.

| transport | send bypass | + recv bypass | zmq.rs | compio |
|---|---|---|---|---|
| TCP 32 B | 72 µs | 67 µs | 38 µs | 34 µs |
| IPC 32 B | 63 µs | 61 µs | 28 µs | 28 µs |

~5 µs saved. Remaining gap: REP recv still through actor,
send path still hops through DropQueue → driver on both sides.

## Tokio read-path zero copy

The connection driver's read arm did
`Bytes::copy_from_slice(&read_buf[..n])` on every `reader.read`
return. One full memcpy per syscall. Fix: replace the `Vec<u8>`
read buffer with `BytesMut` and call `reader.read_buf(&mut buf)`,
then `buf.split().freeze()` to hand the codec a zero-copy `Bytes`.

PUSH/PULL TCP throughput (two-process, bench_peer):

| size | before | after |
|---|---|---|
| 64 B | 5.0M | 11.4M (+128%) |
| 256 B | 4.2M | 10.6M (+152%) |
| 1 KiB | 2.9M | 6.4M (+121%) |
| 4 KiB | 1.2M | 2.4M (+100%) |

The gain is larger than a single memcpy would explain: `BytesMut`
reuses its allocation across reads (the `split()` advances the
internal cursor without reallocating), so the read path went from
one alloc + one copy per syscall to zero allocs and zero copies in
steady state.

## Atomic REQ alternation flag

REQ's `pre_send` / `post_recv` only mutates a single bool
(`req_awaiting_reply`). REQ strict alternation (send-recv-send-recv)
guarantees no concurrent access between the two. Replaced the
shared `Mutex<TypeState>` lock with an `AtomicBool` (Relaxed
ordering; the DropQueue/async_channel between send and recv
provides happens-before). REP keeps the Mutex because it stores
`Option<Vec<Bytes>>` for the envelope.

Saves ~200 ns per send+recv pair (uncontended Mutex overhead:
CAS + memory barrier + function call).

## Tokio PeerWireSlot: per-peer send bypass

`DirectIo` locked an `Arc<Mutex<Writer>>` from the socket handle to
encode and write inline. This mixed encoding and I/O under one lock:
the hold time was the full `write_vectored` syscall, blocking
concurrent senders and the driver's read loop.

`PeerWireSlot` separates encoding from I/O. The handle encodes
ZMTP frames into a per-peer `EncodedQueue` under a `std::sync::Mutex`
(nanosecond hold time, encode only). The driver flushes to the wire
via a `transmit_notify` select arm. The handle never touches the
writer.

Every wire peer gets a slot, not just single-peer sockets. FanOut
(PUB/XPUB/RADIO) encodes the message once via `pre_encode()` and
pushes shared chunks into each matching peer's slot. Identity
(ROUTER/REP/SERVER) looks up the peer by routing identity and
encodes into the target's slot. This eliminated all pump tasks for
fan-out and identity strategies.

Signal coalescing: a `pending: AtomicBool` gates
`transmit_notify.notify_one()`. N rapid encodes produce one wake.
The driver drain arm loops until the slot is empty (or
`max_batch_bytes`), flushing messages that arrive during
`write_vectored` without re-entering `select!`.

Disabled when a frame transform (CURVE, BLAKE3ZMQ) is active.
The codec's encrypt-in-place flow needs the codec's internal state.

### Dead end: DirectIo with bidirectional handoff

Goal: eliminate both send and recv task hops. Hand reader, writer,
and codec to `DirectIo`, let the driver exit, do all I/O inline
on the user task. REQ/REP IPC latency dropped from 63 µs to
47 µs. But the code needed three workarounds because the driver
was gone and nobody was watching the connection.

1. **`probe_connection()`**: zero-timeout read after every write
   to detect peer EOF. Without a driver reading continuously, a
   dead peer was invisible until the next `recv_msg`. REQ strict
   alternation made this fatal: if the peer died after a send,
   the next send would block forever on `req_awaiting_reply`.

2. **`flush_codec_via_spawn()`**: `recv_msg` held the state lock
   (reader + codec) while blocking on `reader.read_buf`. When the
   codec produced a PONG response, it could not write because the
   writer lived behind a separate mutex that the caller might also
   be contending. Fix: spawn a task per PONG write.

3. **Spawn-on-backpressure**: `send_msg` tried a zero-timeout
   write. On partial write or timeout it transferred the
   `OwnedMutexGuard` to a spawned task so the caller would not
   block. Another spawn per backpressured send.

Each spawn is a heap allocation + scheduler interaction + waker
registration, visible on the hot path under load. Error
propagation was non-local: a write error in a spawned task set
an atomic flag that the next call checked, but the error itself
was lost.

A separate attempt tried a pausable background reader task for
EOF detection (replacing `probe_connection`). Zero-timeout reads
left stale waker registrations in tokio's reactor, causing real
reads to miss wake-ups. REQ/REP IPC hung at 2048 B+.

Send-only DirectIo avoided all three: the driver never exits, so
it detects EOF natively, writes heartbeat PINGs directly, and
applies backpressure through `write_all`. But the `Arc<Mutex<Writer>>`
was still held for the full `write_vectored`. PeerWireSlot
replaced this with encode-only Mutex (nanoseconds) and
driver-exclusive writes.

### ChunkedInputBuf front-cache

`ChunkedInputBuf` is the codec's inbound byte buffer. It held received
data as a `VecDeque<Bytes>`. Every `peek_array` call (two per frame:
flags byte then header) went through `VecDeque::front()` which does
ring buffer indexing (`to_physical_idx` + `wrap_add`). At 14M msg/s
that was 28M ring-index operations per second, showing as 12% self-time
in `peek_frame_header`.

Pulled the front chunk out of the `VecDeque` into a dedicated `front:
Bytes` field on `ChunkedInputBuf`. Reads go through `self.front`
(direct field access) instead of `self.chunks.front()`. When the front
is consumed, `advance_front()` pops the next chunk from the remaining
`VecDeque`.

Result: `peek_frame_header` dropped from 12.3% to 10.1%.
Small but real. The remaining cost is bounds-checked slice indexing
on `self.front[self.front_offset..]`, which is inherent.

### Specialized try_recv for PULL/PAIR

Profiling showed `try_recv` at 29% self-time and `drain_recv_cache`
at 7%. For a PULL socket, every call evaluated five `matches!` checks
(all constant), wrapped/unwrapped two `Result`s that could never be
`Err`, and called two functions whose PULL branches were trivial.
The `Result::branch` from the `?` operator alone was 12.6%.

Added a `simple_recv: bool` flag on `SocketInner`, set at construction
for PULL and PAIR. When true, `try_recv` takes an inline fast path:
direct `cache.pop_front()`, then lock + `swap_messages` + pop. No
function calls, no `Result` wrapping, no `matches!` dispatch. The
generic path for SUB/REQ/REP/etc. is unchanged.

Result: `try_recv` self-time dropped from 29% to 15%.
`drain_recv_cache` disappeared from the top. Combined with the push-side
fix below, 8 B TCP throughput went from ~14M to ~17M msg/s.

### Message::from_slice

The bench was constructing messages via `Bytes::from(vec![b'x'; 8])`
then cloning the `Bytes` each iteration. For 8 bytes, `Bytes` still
heap-allocates (no inline representation). Clone and drop each touch
a refcount. This cost 12.8% of push-side CPU.

Added `Message::from_slice(&[u8])` which copies directly into the
inline `MessageInner::Inline` variant for payloads up to 55 bytes.
No heap allocation, no refcounting. Falls back to
`Bytes::copy_from_slice` for larger payloads.

A real user sending small messages from a `&[u8]` buffer gets the
same benefit. This is the realistic fast path, not a benchmark trick.

### Cell-based send path (replacing atomics and Mutex)

After the recv-side optimizations, the push side became the bottleneck.
Profiling showed 13.8% on `encoded_queue: Mutex<EncodedQueue>` lock
and unlock (two atomic CAS operations per message), plus 5.9% on
`direct_msg_count: AtomicUsize` and `driver_in_select: AtomicBool`.
All accesses are on a single compio runtime thread. The atomics are
correct but unnecessary: each costs 5-20 ns vs <1 ns for a plain
memory write.

Replaced five fields on `DirectIoState` with non-atomic equivalents:

- `encoded_queue: Mutex<EncodedQueue>` -> `EncodedQueueCell` (a
  `Cell<bool>` borrow flag + `UnsafeCell<EncodedQueue>`, with a RAII
  guard that clears the flag on drop). `try_borrow_mut()` is a plain
  bool check instead of an atomic CAS.
- `direct_msg_count: AtomicUsize` -> `Cell<usize>`
- `driver_in_select: AtomicBool` -> `Cell<bool>`
- `handshake_done: AtomicBool` -> `Cell<bool>`
- `socket_closing: AtomicBool` -> `Cell<bool>`

The safety invariant is the same one that already covers `RecvCache`,
`LocalStream`, and the `UnsafeCell` fields on `SocketInner`: compio
is single-threaded, `DirectIoState` never crosses thread boundaries,
and the existing `unsafe impl Sync` on the `Arc` covers `Cell` fields.

Result: 8 B TCP throughput went from 17M to 22M msg/s.

### Dead end: Vec for Connection::messages (replacing VecDeque)

Profiling 8 B TCP PULL at ~14M msg/s showed 10% self-time attributed
to `VecDeque::push_back` / `clear` inside `drive_zmtp`. The theory:
`VecDeque`'s ring buffer indexing (`wrap_index`: add + branch per
`push_back` and `pop_front`) is overhead that a flat `Vec` avoids.

Replaced `messages: VecDeque<Message>` with `Vec<Message>` plus a
`messages_drain: usize` index. `push` is a pointer bump. `poll_message`
reads via `ptr::read` at the drain index (no replacement write).
`swap_messages` exchanges the `Vec` with the recv cache and resets the
drain index. Same O(1) swap semantics.

Also replaced the compio `RecvCache` (`VecDeque<Message>`) with the
same `Vec` + drain index pattern.

Result: neutral. 13.8-14.5M msg/s, same range as before. The perf
attribution was misleading: `mod.rs:1917` pointed at `VecDeque::clear`
(which is never called on the hot path), not `push_back`. The actual
`push_back` cost is already low because `VecDeque` at steady state
never reallocates, and `wrap_index` is a single well-predicted branch.
The `Vec` traded ring buffer indexing for `ptr::read` + `set_len`
bookkeeping with no net gain. Reverted.

## omq-libzmq: direct yring recv bypass

The omq-libzmq compat layer originally relayed received messages
through three thread crossings:

```
driver → async_channel → recv_pump_task → yring → eventfd → C thread
```

The recv pump task called `Socket::recv().await` in a loop, pushed
each message into a yring, and signaled an eventfd on
empty-to-non-empty transitions. The C thread parked on the eventfd
via `libc::poll()` and drained the yring.

This architecture caused massive throughput variance (0.1-5M msg/s
for 8 B TCP) while real libzmq held steady at 6.5M. The variance
came from non-deterministic tokio worker scheduling: when the recv
pump task was slow to wake from `epoll_wait`, the pipeline stalled.

The fix bypasses the `async_channel` and recv pump entirely for the
first connected peer. The `ConnectionDriver` pushes decoded messages
directly into the yring and signals the eventfd:

```
driver → yring → eventfd → C thread
```

One thread crossing instead of three. The driver replicates the
recv pump's push-flush-signal logic inline in `RecvSink::Yring::send`.
Backpressure uses the same `tokio::sync::Notify` the recv pump used:
when the yring is full, the driver awaits `space.notified()`, which
the C thread signals after each pop.

### Why the dead end didn't apply

The general omq-tokio "per-peer wire yring for recv" approach failed
because `tokio::sync::Notify` drops notifications when the driver
pushes multiple messages without yielding. omq-libzmq doesn't use
`tokio::sync::Notify` for recv signaling at all. The C thread does
`libc::poll()` on an eventfd, which is level-triggered and never
loses signals. The driver writes to the eventfd via a `Box<dyn Fn()>`
callback, sidestepping the Notify problem entirely.

### Two yrings: SPSC constraint

The yring is SPSC (one producer, one consumer). Multiple
`ConnectionDriver`s can't share a producer. The solution is two
yrings:

- **fast yring**: producer owned by the first peer's driver.
- **pump yring**: producer owned by the recv pump task.

The C thread drains the fast consumer first, then the pump consumer.
Both signal the same eventfd. For the single-peer case (benchmarks,
most production deployments), the pump yring stays empty and the recv
pump idles on the `async_channel`.

### Peer churn

The fast yring is a one-shot optimization tied to the first peer's
lifetime. The `RecvSink::Yring` is stored in a shared
`Arc<Mutex<Option<RecvSink>>>` slot. The actor takes it for the
first `ConnectionDriver` when `peers.is_empty()`. Subsequent peers
find the slot empty and get the `async_channel` path.

When the first peer disconnects, the driver drops the yring
producer. `Producer::Drop` calls `flush()` (Release store), so
the consumer sees all remaining messages. After draining them,
`prefetch_and_pop()` returns `None` permanently. All subsequent
traffic flows through the recv pump. No races: the slot is
mutex-protected, the actor processes peer events sequentially,
and the eventfd fd outlives the signal closure.

Reconnects after the first peer disconnects use the recv pump.
The fast yring is not restored. This is the pre-optimization
behavior and only matters for long-lived sockets that cycle
through many peer lifetimes.

### Send-path fixes

Two additional fixes reduced send-path overhead:

**`send_accum` Mutex to `UnsafeCell`.** The SNDMORE accumulator was
behind a `Mutex` locked on every `zmq_send`, even for single-part
messages (the common case). Replaced with `UnsafeCell` under the zmq
single-threaded socket contract. Same justification as the existing
`bypass_send` / `bypass_recv` fields.

**`send_ring` `RwLock` guard.** `Socket::try_send` took an `RwLock`
read guard on the inproc SPSC ring on every call, even for TCP
connections where the ring is always `None`. Added an `AtomicBool`
flag (`send_ring_active`) checked before the `RwLock`. TCP sockets
never touch the lock.

### REQ exclusion

REQ sockets are excluded from the yring bypass because
`Socket::recv()` applies post-recv processing (strips the empty
delimiter frame, clears `req_awaiting_reply`). The recv pump
provides this processing by calling `Socket::recv()`. Bypassing it
would deliver raw messages with the delimiter still attached.

### Result

8 B TCP: 1.1M to 4.7M msg/s (4.3x). 32 B TCP: 1.0M to 6.4M msg/s
(6.4x).

## What remains

**Per-peer wire yring for recv (general omq-tokio).** The
omq-libzmq bypass above works because the C thread uses eventfd,
not `tokio::sync::Notify`. The general omq-tokio path still uses
`async_channel` for `recv_direct`. Replacing it with per-peer yring
SPSC rings hits the same `Notify` dead end: notifications are lost
when the driver pushes multiple messages in a tight loop (driver's
`while codec.poll_message()` doesn't yield between messages). The
Notify stores at most one permit; subsequent `notify_one()` calls
are no-ops. The consumer's `try_drain_consumers()` should prefetch
all available items, but empirically hangs after ~28/30 messages in
the random_sizes test. Root cause unclear. Possibly a subtle
interaction between the biased select's `notified()` registration
and the producer's `flush()` visibility.

**Same-thread inproc (~4M).** Uses blume (no ypipe). The ypipe
ring cannot serve same-thread sequential send-all-then-recv-all
patterns without deadlock or ordering violations. Same-thread
throughput is bounded by blume's Mutex + VecDeque path and compio's
per-task-poll overhead (~39% of cycles).

**Tokio REQ/REP latency.** Still ~60-70 µs vs compio's ~34 µs.
PeerWireSlot removed the send-side `Arc<Mutex<Writer>>` lock but
did not change the recv path: REP recv still routes through the
actor for identity-prefix prepending. The remaining gap is the
recv-side actor hop plus tokio's per-task wake cost.

## WebSocket transport (ZWS/2.0)

PUSH/PULL over `ws://127.0.0.1`, 1 peer, 2 s rounds, same machine.
libzmq 4.3.5 built with `ENABLE_DRAFTS=ON`.

```
                    128 B              2 KiB              8 KiB
                msg/s    MB/s     msg/s    MB/s     msg/s    MB/s
libzmq 4.3.5   1,911K    245      289K     592       69K     569
omq-tokio        955K    122      666K   1,364      193K   1,581
omq-compio       112K     14       93K     190       85K     693
```

At small messages libzmq leads 2x over omq-tokio: it uses a custom
WS codec with no tungstenite overhead and batches frames into fewer
syscalls. At 8 KiB omq-tokio is 2.8x faster because the batched
feed-then-flush path amortizes per-frame WS overhead across the
batch, and tokio's multi-threaded runtime overlaps send and recv.

omq-compio is single-threaded (one io_uring thread per socket).
The sender and receiver take turns on the same thread, so there is
no batching window: each message is one feed + flush round-trip
through tungstenite, which is ~8 µs. That per-message latency
dominates at every size.

**What WS costs vs TCP.** On the same box, omq-tokio over
`tcp://` does 5.9M msg/s at 128 B and 4.5 GB/s at 8 KiB. The WS
overhead comes from: (1) per-frame WS header + client-side XOR
masking, (2) no gather I/O (each WS message is an independent
tungstenite write), (3) the HTTP upgrade handshake at connect time.

## Cache-line-aligned inline thresholds

`Payload` was 40 bytes (inline up to 38 B). `Message` was 48 bytes
(inline up to 39 B). Both were sized to minimize struct width, but
neither aligned to a cache line boundary. The 39-to-40 byte cliff
was steep: TCP throughput dropped 29% at the transition (17.8M to
12.6M msg/s at 1 peer).

Bumped both to 64 bytes (one cache line each). `Payload` now
inlines up to 62 B, `Message` up to 55 B. The `Message` bump
drives the improvement because the recv fast path
(`try_advance_ready`) writes directly into `MessageInner::Inline`,
bypassing `Payload` entirely for single-part messages. The
`Payload` bump fills the same cache line that `Single(Bytes)`
already occupied and helps multi-part messages where each frame
goes through `Payload::Inline`.

TCP throughput at 40 B (the old cliff): 12.6M to 17.0M msg/s
(+35%). No regression at any other size. Latency unchanged
(~21 µs p50 REQ/REP TCP).

`Message` at 80 B was tested and rejected. Per-message throughput
did not improve at sizes the 64 B variant already covers, and the
struct crosses a second cache line.


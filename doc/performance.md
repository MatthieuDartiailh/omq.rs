# How to beat libzmq

Design choices and dead ends behind the throughput numbers in
[`../COMPARISONS.md`](../COMPARISONS.md). For anyone building or
maintaining a ZMQ-shaped library who wants to know which
optimizations stack and which don't.

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

## First Rust attempt: pure tokio actor

Per-socket `SocketDriver` actor, per-peer `ConnectionDriver` via
flume. Every message round-trips through the actor.

Result: ~80k 128 B msg/s over TCP. zmq.rs: ~300k. libzmq: ~3M.

Three context switches per send (`cmd_tx.send` + `tokio::spawn` +
oneshot ack) plus a per-peer mpsc hop.

## Choosing an io_uring runtime

Tried `monoio` first. Working port, fast I/O, but the API was
rough (buffer ownership, lifetimes, cancellation). `compio` had
cleaner ergonomics (closer to tokio) and cross-platform support
(io_uring on Linux, IOCP on Windows, kqueue on macOS). Stuck.

omq-tokio is maintained as a second backend because tokio remains
the runtime of choice for most Rust apps.

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

Fix: `EncodedQueue` keeps a `flat_buf: BytesMut`. Messages below
`FLAT_THRESHOLD` (compio: 32 KiB, tokio: 48 KiB) are written
contiguously into `flat_buf`. N small messages -> one iovec for
the whole batch. Above the threshold, the original chunk-list path
wins because memcpy of a large payload would dominate.

Thresholds differ because per-iovec cost differs between runtimes.

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
Global name registry, direct `InprocFrame` exchange via channels.
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
    Inline { len: u8, data: [u8; 38] },  // no heap, no Arc
    Single(Bytes),
    Multi(Vec<Bytes>),
}
```

38 B inline capacity covers every bench size up to 38 B.
Per-frame cost: ~3 atomic ops -> ~0.

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
Now `[Payload; 1]` = 48 B. 62% less copied per push/pop.

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
    Inline { len: u8, data: [MaybeUninit<u8>; 39] },
    Single(Payload),
    Multi(Vec<Payload>),
}
```

48 B, covers up to 39 B inline. `absorb_data_frame` constructs
`Inline` directly. SmallVec::drop disappeared from the profile
(was 6%). `MaybeUninit` skips zeroing the 39 B array -- worth
~13% at 8 B.

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
| after: read_into -> Message -> push_back | 2 | 80 B |
| libzmq: memcpy(msg_t) | 1 | 64 B |

Remaining gap: `VecDeque::push_back` copies 48 B per message.
libzmq's `yqueue_t` writes in-place (one pointer advance).

### Round 9: SmallVec for parts_payload()

`msg.parts_payload()` returned `Vec<Payload>` -- one malloc+free
per single-part send. Fix: `SmallVec<[Payload; 1]>`.

**8 B IPC: 7.26M -> 7.83M msg/s (+8%).**

### Dead end: arena recv (Bytes::slice)

Tried sharing the read buffer's Arc via `Bytes::slice` instead of
inline copy. Arc bump + drop (~10 ns for two atomics) cost the
same as the inline copy + zeroing. A microbenchmark confirmed:
inline wins or ties at every size up to 39 B. For payloads that
fit in a cache line, the atomic in an Arc bump is more expensive
than the copy.

### Net result

8 B TCP: 3.8M -> 8.2M msg/s (0.45x -> parity with libzmq).
32 B TCP: 3.7M -> 6.6M msg/s (0.45x -> 0.74x).

Current (after further tuning, see COMPARISONS.md):
8 B TCP: 8.72M vs libzmq 8.44M (**1.03x**).
32 B TCP: 7.13M vs libzmq 8.45M (**0.84x**).

## Inproc cross-core: blume batching channel

After wire-transport work, inproc-mt at 32 B ran at 2.13M msg/s
-- 25% slower than TCP (2.86M). TCP, which encodes ZMTP frames
and crosses the kernel, was beating a direct in-process channel.

TCP's advantage: batching. Many small messages into one `flat_buf`,
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
sequentially. That is a structural disadvantage the implementation
has to overcome by being shorter everywhere else:

- No actor hop on send for non-REQ/REP.
- No pump task hop for byte-stream peers.
- No async-mutex on encode.
- One iovec per N small messages, not 2N.
- One header alloc per ~7000 frames.
- No vtable/Box on the hot path.
- Encode/write pipelined via lock decomposition (writer mutex
  separate from codec mutex).

The last point is the structural answer: omq does not have a
separate I/O thread, but it pipelines encode against write.

## What remains

**32 B gap (0.84x).** `VecDeque::push_back` copies 48 B per
message vs libzmq's in-place write. Closing it requires either a
chunk-based message queue (`yqueue`-style) or fused
decode-and-deliver (callback/iterator from `handle_input`).

**Single-wire-peer bypass on tokio.** The compio direct-encode
fast path has no equivalent on tokio yet. Analogous shape: per-peer
`EncodedQueue` clone, claimed via `try_lock`.


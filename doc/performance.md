# How omq beat libzmq

A technical article on the design choices and dead ends behind the
performance numbers in [`../BENCHMARKS.md`](../BENCHMARKS.md) and
[`../COMPARISONS.md`](../COMPARISONS.md). Audience: anyone building
or maintaining a ZMQ-shaped library who wants to understand which
optimisations stack and which look promising on paper but lose
throughput in practice.

This is not a tutorial on the codebase -- the structural docs in
[`architecture.md`](architecture.md), [`compio.md`](compio.md), and
[`tokio.md`](tokio.md) cover that. It is the story of how the wire
throughput got to where it is, told in the order the decisions
landed.

## Premise

Beating libzmq at small messages over a real TCP socket turned out to
be the hardest part of the project. Large messages were comparatively
easy: `writev` batching multi-chunk frames in one syscall vs. libzmq's
separate `send()` calls for header + payload gave a 2-3x advantage
above 2 KiB from the start. Small messages (128-512 B) were a
different story.

The reason small messages are hard: at 128 B, encoding is cheap and
kernel round-trips dominate. libzmq's architecture separates the
application thread from a dedicated I/O thread (`zmq_ctx_t` spawns one
reaper + one I/O thread per context by default). The app encodes and
hands a message to the I/O thread via a pipe; the I/O thread does the
actual `send()`. This means the app loop and I/O are truly concurrent:
while the app is encoding message N+1, the I/O thread is writing
message N to the socket. At 128 B that overlap is the primary
advantage. A naive single-threaded ZMQ library cannot keep up.

A pure-Rust ZMQ implementation that aspired to be a real drop-in for
production workloads also had to clear a higher bar than throughput
alone:

- Cancel-safe `recv` -- futures dropped mid-read must not corrupt the
  stream.
- Automatic reconnect with exponential backoff.
- All eleven standard socket types plus the eight DRAFT types
  (CLIENT/SERVER/RADIO/DISH/SCATTER/GATHER/CHANNEL/PEER).
- Inproc transport that bypasses ZMTP entirely.
- UDP transport for RADIO/DISH.
- TCP keepalive plumbing for long-idle connections (stock-feed style).
- Lifecycle event monitoring as a first-class `Stream`.
- Reverse-bind for PUB/SUB topologies.
- LZ4 and zstd compression transports.
- A pyzmq-compatible Python binding so existing users could drop the
  C dependency without rewriting their code.

These were not optional features; they were the table stakes that
distinguish a ZMQ implementation from a toy.

## Origin: a sister project in pure Ruby

The architecture inherits its core shape from a sister project: a
pure-Ruby ZMTP implementation, [OMQ
Ruby](https://github.com/paddor/omq). That project began as an
experiment to test a deliberately simple model -- "what if a ZMQ
library uses just two `Async::Queue` instances per socket, one in and
one out, with per-peer driver fibers in between?" The answer turned
out to be that performance came out far above expectations, courtesy
of `io-stream` and `io-event` which back the Ruby Async runtime with
epoll on Linux (and io_uring when `liburing-dev` is installed). On a
2018 Mac Mini in a Linux VM, OMQ Ruby pushes ~500k 128-byte
messages/second over loopback TCP -- already faster than zmq.rs on the
same wire, despite running on Ruby.

That result raised the natural question: if a pure-Ruby implementation
of this two-queue design clears half a million msg/s at 128 B, what
ceiling does the same design hit in Rust on top of io_uring?

The two-queue model:

- Each socket has exactly one inbound queue and one outbound queue.
  Not one per peer.
- Per-connection driver fibers/tasks push decoded messages into the
  one inbound queue and pull messages from the one outbound queue.
- The outbound queue's bound is the socket's HWM. Backpressure is a
  single cap, not a per-peer matrix.
- Slow peers do not corner the socket. A blocked driver leaves
  messages in the shared outbound queue; faster drivers steal them.
  Head-of-line blocking patterns where one non-draining peer freezes
  the socket simply do not arise.
- Sender code never names a peer. Routing strategy decides; on
  round-robin patterns, "any free driver picks up the next message"
  is the natural shape.

The contrast with libzmq is the per-pipe-per-peer pattern that mirrors
ZMTP wire framing into the socket's internal data structures. libzmq
needs that complexity because of its dedicated-I/O-thread design;
without it the I/O thread has no way to multiplex peers fairly. The
two-queue design lifts the multiplexing one layer up, into the
work-stealing on the outbound queue, which makes the implementation
substantially smaller.

omq.rs -- this repository -- is the Rust port of that idea. The first
target was matching OMQ Ruby's throughput on equivalent hardware. The
second target was beating libzmq at small messages, where libzmq is
strongest. Both targets eventually fell.
[zmq.rs](https://github.com/zeromq/zmq.rs) was the obvious starting
point in pure Rust. The feature gap at the time -- inproc, the DRAFT
socket types, the compression transports, BLAKE3ZMQ -- combined with
throughput that did not yet match what OMQ Ruby was doing on the
same wire, made it easier to start fresh than to retrofit. The two
projects share a lot of design sensibility, and several of the
techniques described later in this article would apply to zmq.rs in
principle as well.

Two further motivations are worth mentioning. A pyzmq-compatible
Python binding on top of a faster Rust core felt useful because a
lot of scientists work in Python end-to-end. And years of poking
around the ZMQ ecosystem kept turning up papers from CERN about
their use of ZMQ and custom compression to move the huge data
volumes their experiments produce. That combination -- ZMQ plus
heavy compression -- kept the compression-transport work in this
repository feeling worthwhile.

## Sans-I/O codec split

The ZMTP **state machine** -- not just the frame parser, but greeting,
mechanism handshake (NULL/CURVE/BLAKE3ZMQ), framing, and the
compression transforms -- lives in `omq-proto` and never touches a
file descriptor. Bytes go in via `Connection::handle_input`, events
come out via `poll_event`, outbound frames accumulate via
`send_message` and are read via `poll_transmit` /
`advance_transmit`. The runtime backends own the I/O loop.

zmq.rs draws the line differently. Its `ZmqCodec` is a
`tokio_util`-style `Decoder` / `Encoder` -- byte-level frame parsing
is sans-I/O. But everything above it (greeting handshake, mechanism
negotiation, per-connection state) is wired through
`Box<dyn AsyncRead + AsyncWrite>` in `FramedIo` and lives inside the
runtime layer. The codec is sans-I/O; the connection isn't.

The shape `omq-proto` uses -- whole-protocol sans-I/O, the one
`rustls::ConnectionCommon` and `quinn-proto` use -- matters for
performance: a sans-I/O state machine can be driven from any runtime
without forcing the runtime into its allocation patterns. The same
crate now drives both a single-thread io_uring backend and a
multi-thread tokio backend, with byte-for-byte identical wire
output.

It also keeps the test surface small. Greeting, mechanism handshake,
frame parsing, subscription matching, and command framing all run in
synchronous tests with no runtime present.

## Multi-chunk frame payloads

`Bytes::clone` is one atomic increment; `bytes.copy_from_slice` is a
memcpy. The codec is built so that every layer can prepend its static
prefix (sentinels, identities, ZMTP frame headers) by pushing one more
`Bytes` onto a `Payload`, never by copying the payload itself.

```rust
// Payload: 4-variant enum, 40 bytes. See "Inline small Payload" below.
type Message = SmallVec<[Payload; 3]>;  // 3 frames inline
```

Inline storage covers the common shapes (single-chunk payloads,
REQ/REP three-frame envelopes) so heap allocation is reserved for
unusual cases. At write time the codec flattens the chunks into a
`Vec<IoSlice>` and the kernel stitches them into one wire write via
`writev` / `sendmsg`.

The 2-3x advantage over libzmq at >= 2 KiB messages comes almost
entirely from this. libzmq makes separate `send()` calls for the frame
header and the payload because its zero-copy design pre-dates writev's
ubiquity; omq does it in one syscall.

## First Rust attempt: pure tokio actor

The initial implementation followed the textbook actor shape, mostly
because that is what zmq.rs does and the symmetry was useful for
bisecting bugs:

- Per-socket `SocketDriver` actor task owns peer table, type state,
  routing strategy.
- `Socket::send` -> `cmd_tx.send(SocketCommand::Send(msg)).await`.
- Actor receives, encodes through codec mutex, hands to per-peer
  `ConnectionDriver` via flume.
- Per-peer driver wakes, calls `clone_transmit_chunks` (N atomic
  increments), `write_vectored`, `advance_transmit` to bump the codec
  cursor.

It worked. It was correct. It was slow: ~80k 128-byte msg/s over
TCP loopback. zmq.rs ran roughly 3x faster on the same hardware (~300k
msg/s) -- the simple actor shape was leaving most of tokio's potential
on the table -- but libzmq did 3M, ten times further out of reach.

The hop count was the obvious culprit. Three context switches per
send (`cmd_tx.send` + `tokio::spawn` + oneshot ack) plus a per-peer
mpsc hop, all to deliver a message that the actor would forward
unchanged. But hop count is a lot to fix all at once.

## Choosing an io_uring runtime

Tokio's epoll-backed performance was disappointing enough to make
io_uring the next obvious axis to push on. Three Rust io_uring
runtimes were candidates: `glommio`, `monoio`, and `compio`.

`monoio` was tried first, on the strength of its GitHub star count and
maturity at the time. A working port came together. The verdict on it
was mixed: the I/O it delivered was fast, but the API was rougher than
expected -- buffer ownership patterns, lifetime conventions, and the
shape of cancellation each cut against the grain of how the code wanted
to be written.

`compio` kept turning up in side reading. Two things stood out: a
cleaner async API (closer to standard tokio's ergonomics with proper
buffer-passing semantics) and cross-platform support (io_uring on
Linux, IOCP on Windows, kqueue on macOS) instead of Linux-only. The
port to compio took less code and read more naturally, and stuck.

omq-tokio is still maintained as the second backend because tokio
remains the runtime of choice for most Rust applications and migrating
an existing tokio service to a non-tokio runtime is a bigger ask than
adding a dependency. Both backends expose an identical public `Socket`
API, verified by a coverage matrix test that runs every socket type x
transport combination on each backend.

## Even with io_uring, the actor shape is the bottleneck

Switching tokio for compio was not, by itself, a big jump. The naive
io_uring port still had every message round-tripping through the
SocketDriver task. Throughput barely improved.

The point is worth dwelling on: io_uring is fast, but its speed shows
up only when the application's hot path is short enough to expose it.
A library that adds two task hops and an async-mutex acquire per
message will see io_uring win by a few percent over epoll, not by an
order of magnitude. The hops are the bottleneck.

What follows is the sequence of hops that came out, smallest changes
first.

## Eliminating spurious task hops on send

The actor existed to serialize state mutation. For PUSH / DEALER /
PUB / PAIR / CLIENT / SCATTER / CHANNEL, however, `pre_send` is the
identity function or a stateless frame-count assert. Routing those
messages through the actor mutates nothing -- it just adds
`cmd_tx.send().await` + per-message `tokio::spawn` + oneshot ack +
flume push (~3 context switches) before the message reaches its peer.

The fix on tokio: `Inner` gained a `SendSubmitter` clone built from
the routing strategy before the driver starts. `Socket::send` matches
on socket type. REQ / REP keep going through `cmd_tx` because their
alternation bit is real per-message state. Everything else inline-
validates frame count and pushes straight into the submitter.

The recv side got the same treatment. For socket types whose recv
path is plain fair-queue delivery, the connection driver gets a clone
of the user-facing `recv_tx: async_channel::Sender<Message>` and
pushes `Event::Message` straight into it, skipping the actor's event
loop entirely. Per-peer ordering is preserved because a single driver
task delivers in TCP order; backpressure still works because the
channel is bounded (`recv_hwm`) and a full channel blocks the driver's
read loop, halting TCP reads.

| Bypassed (recv) | Through actor (recv) | Reason actor still on path |
|---|---|---|
| Pull, Dealer, Sub, XSub, Pair, Client, Channel, Gather | Rep, Router, Server, Peer | Identity-prefix prepending |
|  | Dish | Group membership filter |
|  | XPub | Subscribe-as-message (0x01/0x00) parsing |

128 B PUSH/PULL TCP loopback on tokio went from 84k msg/s to 4.0M
msg/s after this change -- a ~48x lift on hop count alone, before any
multi-core gains. The multi-core parallelism that tokio always had
was suddenly visible because the actor was no longer the serialization
bottleneck. zmq.rs runs the same tokio multi-thread runtime but routes
every message through its socket actor's mpsc inbox; that's why
omq-tokio widens on small messages even on the same wire.

## A second pump task hop, also gone

Independently of the actor bypass, the round-robin routing strategy
kept a shared `DropQueue` receiver and spawned a pump task per peer:
the pump raced `shared_rx`, forwarded one message at a time to the
driver's inbox. Three task hops end-to-end.

The pump went away. Each `ConnectionDriver` now holds the shared
receiver directly and polls it in a dedicated `select!` arm. The arm
greedily drains up to 256 messages or 512 KiB per wakeup, encodes
them all, then flushes with one `write_all` + `write_vectored` cycle.
Result: one task hop for byte-stream sockets. Pump tasks remain for
inproc peers, which use a per-peer inbox.

## Single-peer fast path on compio

The compio backend did not have an actor in the strict tokio sense,
but it did have a per-peer `cmd_tx` channel between the sender and the
driver task. For sockets connected to one wire peer (the typical
PUSH/REQ/REP shape), even one channel hop costs measurable latency.

The compio fast path skips it entirely:

- Each wire connection has a `DirectIoState` shared between the driver
  and any sender.
- `DirectIoState` contains an `EncodedQueue` -- a `VecDeque<Bytes>`
  plus a contiguous `flat_buf: BytesMut`, behind a sync `Mutex`.
- `Socket::send` acquires the queue with `try_lock` (sync, not async),
  encodes the ZMTP frames directly into the queue, and returns.
- The driver drains the queue and writes when it next loops.

A sync `Mutex::try_lock` on a single-thread cooperative runtime almost
never fails, because the driver cannot preempt the sender. When it
does fail (driver is mid-flush), the sender falls back to the channel
path. If even that fails (peer dead, reconnect in flight), it falls
back to the socket-wide shared queue, which is bounded by `send_hwm`
and drained by the new driver after reconnect.

This eliminates three things from the per-message cost:

- Async-mutex acquisition between sender and driver.
- The codec's `clone_transmit_chunks` (N atomic increments) and the
  matching `advance_transmit` cursor bump.
- One `cmd_tx.send_async().await` round-trip.

128 B TCP throughput on compio jumped from ~1.30M to 1.48M msg/s
after this change. Still ~50 % below libzmq, but well within striking
distance.

## Read-side fast path: direct-recv

The send-side wins above were measured in throughput; the read-side
fast path shows up as latency -- REQ/REP round-trip time.

Before, every received message went `kernel -> driver wakeup ->
codec parse -> channel push -> Socket::recv wakeup`. Two task hops
between the kernel and a caller already blocked waiting for the
data.

Direct-recv collapses that to zero hops on the steady-state path
for socket types that fair-queue from one peer (PULL, SUB, REP,
PAIR, single-peer REQ). `Socket::recv` claims the FD via a one-byte
atomic, pulls bytes from a multi-shot recv stream, feeds the codec,
and drains a parsed message -- all on the caller's task. The driver
notices the claim flip and parks on a separate signal; when recv
finishes (or is dropped), the claim releases and the driver
resumes. Auto-PONG flushes through the writer inline so heartbeats
keep flowing under the claim.

Cancel-safety is structural. The recv path uses io_uring's
multi-shot recv against a registered `BUF_RING`: one persistent SQE
per connection, the kernel pulls a buffer from the pool only when
bytes are ready, and dropping the consumer future does not cancel
the SQE. Bytes accumulate as `BufferRef`s in the runtime stream and
are picked up by the next consumer poll. The lock discipline keeps
extract-and-feed atomic: there is no `.await` between pulling a
buffer and calling `handle_input`, so a drop in that window is
impossible.

REQ/REP IPC round-trip at 32-byte messages on compio:

| stage                                  | p50 RTT  |
|----------------------------------------|----------|
| baseline, before any fast paths        | ~150 µs  |
| send-side fast path only               | ~100 µs  |
| send-side + direct-recv                | <60 µs   |
| current, later optimisations stacked   | ~20 µs   |

The <60 µs mark was the goal at the time these two paths landed.
The further drop to ~20 µs came from later refinements -- static
dispatch on the I/O halves, header scratch, codec-skip guards (see
below) -- compounding once the dominant task-wake cost was gone.

## Iovec count matters at small sizes

After the big hop reductions, profiling showed the sender was issuing
many `writev` calls each with 1000+ tiny iovecs at 128 B throughput
peaks. For N back-to-back 128 B messages, the natural encoding pushes
2 chunks per message to the iovec list (header `Bytes` + payload
`Bytes`) -> 2N iovecs per `writev`. The kernel handles up to 1024
iovecs per call, so the sender hits the limit fast and ends up
splitting batches arbitrarily.

The fix: pack small messages into one contiguous `BytesMut` region.

`EncodedQueue` keeps a `flat_buf: BytesMut`. For messages below
`FLAT_THRESHOLD`, the encoder writes header + payload bytes
contiguously into `flat_buf`. No `Bytes` allocation, no Arc bump. N
small messages land in one contiguous region. At flush,
`flat_buf.split().freeze()` produces one `Bytes` covering all N
messages -- one iovec for the whole batch.

For messages at or above `FLAT_THRESHOLD`, the encoder falls back to
the original chunk-list path: header `Bytes` from a reusable scratch
buffer, payload `Bytes` cloned (Arc bump only). The arc-bump approach
wins above the threshold because the memcpy of a large payload into
flat_buf would dominate.

The threshold ends up **different on the two backends** -- compio at
32 KiB, tokio at 48 KiB -- because the per-iovec cost differs. On
tokio's multi-thread runtime each syscall carries more scheduler and
task-wake overhead per iovec, pushing the break-even point further
out; a 32-64 KiB sweep peaked at 48 KiB, where 32 KiB messages jump
from ~3.4 to ~5.0 GB/s while 64 KiB stays cleanly in the codec path.
compio's cooperative single-thread scheduler has lower per-iovec
cost, and the memcpy/arc-bump crossover lands at 32 KiB on that
path. Below the threshold the flat path wins; above it the
arc-bump + `write_vectored` path wins because the memcpy starts to
dominate. Earlier in the project compio also fixed a catastrophic
2 KiB regression (35.9k -> 405k msg/s on tokio) that an old 1 KiB
threshold introduced; the calibrated values today are well past
that.

This single change (combined with skip guards on the codec's mutex
when nothing has changed since the last iteration) lifted compio's
128 B TCP throughput from 1.48M to ~3.00M msg/s -- past parity with
libzmq's 2.95M on the same wire.

## One alloc per 7000 frames, not per frame

At 80k+ msg/s, per-frame `BytesMut::with_capacity(9)` calls for the
1-9 byte ZMTP frame header showed up clearly in `samply` profiles as
a steady stream of malloc/free traffic.

`Connection::header_scratch: BytesMut` is a 64 KiB buffer that the
codec holds across messages. Each `encode_frame_into` writes the
header into the scratch, calls `split().freeze()` (which freezes the
prefix into an owned `Bytes` and leaves the remainder mutable), and
moves on. The buffer is replaced when remaining capacity drops below
9 bytes -- one allocation per ~7000 frames in the worst case.

A second scratch buffer in `EncodedQueue` does the same job for the
direct-encode path on compio. Both are permanently allocated after
the first warmup message; per-message allocator pressure goes to zero
on the steady state.

## Static dispatch on transports

The original transport abstraction was `Box<dyn DynReader>` /
`Box<dyn DynWriter>`. Profiling showed the dynamic dispatch added
allocator pressure and a vtable lookup on every read/write call --
once per message at 80k+ msg/s, those allocations alone moved the
profile flame graph noticeably.

The replacement:

```rust
enum WireReader { Tcp(OwnedReadHalf<TcpStream>), Ipc(OwnedReadHalf<UnixStream>) }
enum WireWriter { Tcp(OwnedWriteHalf<TcpStream>), Ipc(OwnedWriteHalf<UnixStream>) }
```

The compiler emits a static `match` at the call site. No heap alloc,
no vtable. The variant set is closed -- new transports require an
edit to the enum -- which is acceptable because new wire transports
are very rare (one per decade for ZMQ).

## Compression decoupled from the codec

`MessageTransform` was originally a single type holding both encode
and decode state behind the same `PeerIo` mutex. Under `lz4+tcp` /
`zstd+tcp`, the sender's `try_direct_encode` had to race
`peer_io.try_lock()` against the driver's read loop. The driver holds
`peer_io` for the entire `handle_input` call on every received chunk,
so `try_lock` almost always lost -- forcing every compressed send
through the slower `cmd_tx` path.

The split: `MessageEncoder` (send state: compression context, dict,
out_buf) lives in `DirectIoState::encoder` under its own mutex.
`MessageDecoder` (recv state: dict, decompress context) stays in
`PeerIo`. The sender locks the encoder, compresses, then pushes
results into `EncodedQueue` -- the same flush path as uncompressed
messages. The driver's read-path lock no longer blocks the sender.

The split also unlocked an ergonomic improvement on `Payload`: zero-
copy accessors (`as_bytes`, `as_slice`, `is_contiguous`) so callers
inspect single-chunk payloads without coalescing. Useful for the
sentinel-prefix passthrough that lets sub-threshold messages skip the
encoder entirely.

## Inproc bypasses ZMTP

Same-process `inproc://` connections do not need wire framing. There
is nothing the wire format would protect against between two halves
of the same address space.

The inproc transport in omq has no driver, no codec, no handshake. A
global registry maps names to `Sender<InprocConnectRequest>`; `bind`
registers a name, `connect` sends a request, peers exchange an
`InprocPeerSnapshot` (socket type + identity) synchronously, then
messages flow as `InprocFrame` through flume channels.

The hot-path `InprocFrame::SinglePart` variant carries `Option<Bytes>`
(identity) and `Bytes` (body) inline (~72 B). The full `Message`
struct is ~624 B; wrapping it in a box for the multipart case keeps
the channel slot small on the hot path. inproc throughput on compio
sits at ~3M msg/s for any size below 32 KiB and rises past 100 GB/s
nominal at 32 KiB+ because no bytes ever cross the kernel.

## Zero-copy recv for large frames

Up through the work above, the recv side did one userspace memcpy per
buf-ring slot. Each multi-shot CQE delivered a `BufferRef` borrowing a
pooled slot; the driver did `Bytes::copy_from_slice(&buf[..])`, dropped
the slot back to the pool, and fed the owned `Bytes` to the codec.
That copy is needed when slots are held by the codec across CQEs (the
pool deadlocks on any frame bigger than the pool capacity), so we paid
it on every slot. For a 1 MiB payload over 32 KiB slots that is 1 MiB
of memcpy on the recv side, on top of the kernel's NIC -> pool copy.

Two related changes removed both ends of that.

The first was the codec's input buffer. It used to be a single
`BytesMut` extended via `extend_from_slice` on every `handle_input`,
which meant repeated reallocation as a large frame accumulated. The
buffer is now a `ChunkedInputBuf`: a `VecDeque<Bytes>` that takes
owned chunks zero-copy. `peek_array::<N>` reads the first N bytes
across chunk boundaries without consuming. `try_decode_frame` calls
`split_to(payload_len)` which returns a multi-chunk `Payload` by
slicing each contributing chunk -- still no memcpy, the result is a
chunk list of `Bytes::split_to` views into the same allocations.
That removed the O(n log n) reallocation chain on the input side.

The second change targeted the per-slot copy itself. The codec
exposes three new methods -- `peek_next_frame_payload_size`,
`begin_supplied_payload`, `supply_payload` -- that let an I/O backend
take over recv for one frame. After parsing a header whose wire
payload is at least `Options::large_message_threshold` (default 128
KiB) and that has no payload prefix already buffered, the compio
backend cancels the multi-shot recv via a per-stream
`compio::runtime::CancelToken`, drains any in-flight CQEs through the
same stream until `ECANCELED` lands (those drained bytes go straight
into the destination `BytesMut`, bounded by one or two pool slots),
then issues a one-shot Recv on the same fd to pull the remaining
bytes directly into the same contiguous allocation. The codec gets
one `Bytes` covering the whole payload via `supply_payload`, runs the
mechanism decrypt and demux the same way it would for an in-buf
frame, and the multi-shot stream rebuilds for the next iteration.

The drained-prefix step is the reason this needs `CancelToken` rather
than just dropping the stream. Bytes the kernel has already filled
into pool slots between the header CQE and the cancel SQE landing
would otherwise be lost, desyncing ZMTP framing on the next frame.
Draining through the existing stream keeps the bytes accounted for;
only after the drain terminates does the one-shot Recv submit on the
same fd.

The cost of the drained prefix is bounded by the pool slot size, not
the payload size. For a 1 MiB payload that is at most ~32 KiB of
slot-to-`BytesMut` memcpy in the worst case, vs. 1 MiB before. Larger
payloads improve the ratio further: a 100 MiB transfer pays the same
~32 KiB drain prefix and gets the rest at NIC bandwidth into one
contiguous buffer. Small messages stay on the multi-shot path
unchanged; the threshold knob exists so that workloads with no large
messages do not pay the per-cancel SQE-rebuild cost (~tens of µs).

A side effect worth calling out: the assembled payload is one
contiguous `Bytes` rather than a multi-chunk `Payload`. Consumers
that previously triggered a `Payload::as_bytes` concat (hashing,
forwarding into another `sendmsg` as a single iovec) silently get a
faster path. Single-chunk inline storage on `Payload`
(`SmallVec<[Bytes; 1]>`) is sized for exactly this case.

## Amortizing cancel+rearm across consecutive large frames

The zero-copy path above paid its cancel+rearm cost on every large
frame, even when large frames arrived back to back. Each frame
triggered:

1. Fire `CancelToken` on the multi-shot stream.
2. Drain remaining CQEs until `ECANCELED` arrives.
3. Submit one-shot `Recv` to collect the payload.
4. Rebuild the multi-shot stream (`build_recv_stream` + new SQE).

Steps 1-2 and 4 together are two io_uring submissions plus a drain
loop. For a benchmark sending 1000 × 32 MiB messages that is 2000
extra submissions and 1000 drain loops that carry no useful bytes.

The fix is a two-state machine on the recv slot:

```
MultiShot ──[large frame]──> OneShot
    ^                           │
    └───[small frame]───────────┘
```

`RecvStreamState` replaces the plain `Option<CancellableRecvStream>` in
`LocalStream`. On the first large frame, the usual cancel+drain runs and
the slot transitions to `OneShot` instead of immediately rebuilding the
stream. Subsequent large frames skip steps 1-2 and 4 entirely -- there
is no multi-shot stream to cancel and nothing to rebuild. Each frame
costs one one-shot `Recv` and nothing else. When a small frame arrives
in `OneShot` mode, `one_shot_recv_and_feed` re-arms the multi-shot
stream once and transitions back to `MultiShot`.

The threshold is unchanged at 128 KiB. The state machine removes the
concern that the threshold placement matters for sequential large-message
workloads: cancel+rearm fires at most once per run of consecutive large
frames (the transition into `OneShot`), not once per frame.

Cancel-safety is structural in both states:

- **MultiShot → OneShot**: cancel+drain runs to completion before the
  one-shot `Recv` submits. If the future is dropped during the one-shot
  read, the slot is already `OneShot` -- the next poll starts a fresh
  one-shot read with no stream to account for.
- **OneShot → MultiShot**: `build_recv_stream` + store to slot is a
  single synchronous step. If dropped before the store the slot stays
  `OneShot`; the next iteration retries. If dropped after, the new
  stream is live and the driver picks it up on the next wakeup.

## Things tried and dropped

The optimisations above are the ones that survived. Several plausible
ideas did not.

### Direct-write on send (reverted)

After direct-recv landed, the natural next step was to do the
same on the send side: `Socket::send` would acquire the writer lock,
encode + `write_vectored` inline, and return -- skipping the driver
entirely on the send path.

Latency dropped from ~165 µs to ~85 µs RTT. A clean 2x win on paper.

PUSH/PULL throughput at 128 B then collapsed by 4-7x (TCP: ~830k ->
~115k msg/s).

The cause turned out to be that the pre-bypass driver provided
implicit cross-message batching for free. Producers pushed into
`cmd_tx` and returned immediately; the driver drained N queued
messages on its next iteration and issued one `writev` for all of
them. The inline send path collapsed that into per-call inline
encoding + writev: a hot single-producer loop did one syscall per
message instead of one syscall per N.

The lesson: a hop that looks like pure latency overhead may be
providing implicit batching that is critical for throughput. Latency
optimisations that bypass batching points must be measured against
throughput before they are kept. The send-side bypass was reverted;
the recv-side one was kept because it does not interact with cross-
message batching on the send path.

### TCP_CORK (reverted)

Implemented as a `Corker { fd }` wrapper around the TCP write that
toggled `setsockopt(TCP_CORK, on/off)` around each flush, with two
strategies: cork on every flush, and cork only on multi-chunk frames.

Both regressed throughput by 10-15 % on TCP-only benches, well
outside noise.

The reasons:

- Each flush adds two `setsockopt` syscalls. At ~100k msg/s and
  ~500 ns/syscall, that is a 5-10 % constant overhead with no
  upside.
- The savings CORK is supposed to deliver -- coalescing back-to-back
  small writes into one TCP segment -- already came from
  `write_vectored` on a single flush. Multi-chunk frames go to the
  kernel as one `iovec` array -> one `sendmsg(2)` -> one segment when
  the gather fits.
- The cork-then-uncork pattern defeats the latency benefit of
  `TCP_NODELAY`, which is on by default. CORK overrides NODELAY by
  design, and uncorking flushes the buffer. Net effect for REQ/REP
  is "wait -> send -> wait" instead of "send immediately."

The `rzmq` project does ship CORK toggling but inside its io_uring
backend, where the cork/uncork is a queued SQE rather than a syscall.
That is the only model where the cost-benefit flips positive on
Linux. The plumbing is not shipped here; it lives as a documented
null result.

### Allocation eliminations below the noise floor

A handful of theoretically-justified changes were prepared and
measured:

- `Connection::transmit_chunks` returning `SmallVec<[IoSlice; 8]>`
  instead of `Vec<IoSlice>` -- eliminates a small heap alloc per
  flush. Inline storage covers the typical 1-2 chunk case.
- Pre-sizing the codec's `in_buf` (8 KiB) and outbound chunk vec
  (cap 8) -- skips the first-grow reallocation on every fresh
  connection.

Both are provably-correct allocation reductions. Both came in
undetectable below the bench harness's noise floor (a single-cell
delta under +/-20 % is noise at the standard 300 ms round duration).
They are kept on a side branch in case future profiling under a
real coverage tool (samply, perf, criterion at longer rounds) shows
them moving the needle.

The lesson is more about bench discipline than about the changes
themselves. A bench that cannot resolve a 1 % alloc reduction will
also fail to catch a 1 % regression sneaking in elsewhere. Some
rounds-per-cell and round-duration knobs are tuned for fast iteration
during development; long-form regression measurement needs longer
rounds, multiple full-suite passes aggregated by median, and `taskset`
pinning to remove scheduler noise.

## Why libzmq is hard to beat at 128 B, and how it stops being hard

libzmq's I/O thread overlaps app encoding with kernel writes. omq-
compio is single-threaded by design: encoding and `write_vectored`
run sequentially in the same task. That is a structural disadvantage
on the send path that the implementation has to overcome by being
shorter everywhere else.

Stacked, the optimisations above do that:

- No actor hop on send for non-REQ/REP types.
- No pump task hop for byte-stream peers.
- No async-mutex acquisition on the encode side.
- One iovec per N small messages, not 2N.
- One header allocation per ~7000 frames, not per frame.
- No vtable / Box on the per-message hot path.
- Sender encodes message N+1 while the driver writes message N
  (the writer mutex is separate from the codec mutex, so the encode/
  write pipeline overlaps even on a single-thread runtime).

The last point is the structural answer to the I/O-thread question:
omq does not have a separate I/O thread, but it does pipeline encode
against write through careful lock decomposition. On a cooperative
runtime that is enough.

The result is that omq beats libzmq at every transport on every size
on the bench machine, with the largest wins at 2-32 KiB (where the
multi-chunk + writev advantage compounds with the hop reductions) and
parity-to-1.4x at 128 B (where the I/O-thread advantage gets close to
neutralised, but not quite).

## Send-path route caching

After the direct-encode fast path landed, perf profiling revealed that
the send path's routing overhead was the next bottleneck. At 128 B
over TCP, the profile showed `Socket::send` consuming ~15% of CPU
time. The actual encoding (`try_direct_encode`) was only 3%. The
other 12% was synchronization: lock acquisitions to look up the target
peer and verify it was alive.

For a single-peer PUSH socket sending 128 B messages over TCP, each
`send()` call performed:

1. `out_peers.read()` -- RwLock to access the peer list.
2. `peer_alive()` -- RwLock read on `direct_io` handle to check the
   driver was still running.
3. `peer_alive()` again -- a second check for the round-robin chosen
   peer (the first check was `any_alive`, the second was per-candidate).
4. `direct_io.read().clone()` -- a third RwLock read plus an Arc clone
   to extract the `DirectIoState` that `try_direct_encode` needs.

Four lock acquisitions and two `Arc<DirectIoState>` refcount bumps per
message, for a peer set that changes maybe once during the entire
benchmark.

**Fused peer selection.** The first fix collapsed these three
`direct_io` reads into one: read the handle once, derive liveness from
the same guard, and extract the Arc in the same pass. This removed the
`peer_alive` helper entirely. The measured impact was within noise on
TCP (the kernel syscalls dominate) but the code became a single-pass
loop instead of scan-then-recheck.

**Generation-gated route cache.** The second fix added a generation
counter (`peers_gen: AtomicU64`) that increments on any peer mutation
(connect, accept, reconnect, driver exit, close). The send path
checks the generation against a cached `CachedPeerRoute` stored in
`SocketInner`. On cache hit (generation matches), the entire
`out_peers.read()` + `direct_io.read()` sequence is skipped -- the
send path goes straight to `slow_round_robin` with the cached
`PeerOut` and `Arc<DirectIoState>`.

The cache hit path costs one atomic load (`peers_gen`) and one
uncontended mutex lock (to read the cached struct). The miss path
populates the cache after the full peer lookup, so the next send is a
hit.

This mattered most for inproc, where there are no kernel syscalls to
dominate the profile: inproc 1-peer 128 B jumped from 3.07M to 3.42M
msg/s (+11%). TCP and IPC gains were in the noise band (~3-5%), which
is expected -- the eliminated overhead is ~10 ns per message, and the
per-message budget at 2.7M msg/s over TCP is ~370 ns with most of it
in the kernel.

**What didn't work: caching on the recv side.** The recv path has an
analogous `snapshot_direct_io_single_peer()` that reads `out_peers` +
`direct_io` on every `recv()` call. Adding the same cache check there
caused a regression: the `Mutex<CachedPeerRoute>` became contended
between the send thread and recv thread (they run on separate compio
runtimes). The Mutex lock/unlock overhead under cross-thread contention
was worse than the two uncontended RwLock reads it replaced. The recv-
side cache was removed. A future approach might use an `AtomicPtr`
swap or per-thread caching, but the uncontended RwLock reads are cheap
enough (~3-5 ns each) that this isn't a bottleneck worth the
complexity.

## Closing the small-message recv gap (8 B -- 32 B)

At the point the optimizations above were done, omq beat libzmq at
every size from 128 B up but trailed at 8 B and 32 B IPC: ~3.8M vs
~8.4M msg/s (0.45×). Three rounds of work narrowed this to ~7.7M
(0.92×). The bottleneck shifted from the send path to the recv path,
specifically the per-message overhead in the codec and the recv_cache
drain loop.

### Profile before (8 B IPC, PULL side, bench_peer)

| % | Function |
|---|---|
| 20.4 | decode_assembled_frame (codec parsing) |
| 18.3 | try_recv (cache pop + subscription check) |
| 12.9 | __memmove_avx (Bytes::copy_from_slice of BUF_RING data) |
| 8.1 | shared_clone (Bytes Arc increment) |
| 7.8 | shared_drop (Bytes Arc decrement) |
| 5.9 | ChunkedInputBuf::split_to |
| 5.7 | Connection::drive |
| 4.0 | peek_frame_header |
| 2.9 | drain_remaining_user_events_into |

Three areas: codec parsing (38%), Bytes refcounting (16%), and cache
drain overhead (18%).

### Round 1: recv cache + try_recv drain

The bench loop calls `recv()` (which feeds the codec and returns
one message) followed by `while try_recv().is_ok() {}` (drains the
remaining batch from cache). Prior to this, only `recv()` was called
per message. The drain loop alone doubled 8 B throughput from 3.8M
to 6.9M msg/s by reducing the number of async `recv()` round-trips
and I/O submissions.

### Round 2: front_offset, inline Payload, PULL fast path

**`front_offset` in `ChunkedInputBuf`.** The codec's `advance(2)` (skip
the 2-byte frame header) used to call `Bytes::slice()` -- which clones
the Arc backing the front chunk and drops the old reference. Two
atomic RMW per frame, hundreds of frames per 4 KB buffer. A new
`front_offset: usize` field tracks how many bytes have been consumed
from the front chunk. `advance()` bumps the offset. `peek_array()`
indexes directly into `front[front_offset..]` instead of iterating
byte-by-byte through the `VecDeque`. `split_to()` reads from the
offset. The front `Bytes` is only dropped when fully consumed --
amortized over hundreds of frames.

**Inline small `Payload`.** `Payload` was `SmallVec<[Bytes; 1]>` (40 B).
Every decoded frame became a `Bytes` via `split_to` -- one Arc clone.
The new representation is a four-variant enum:

```rust
enum PayloadInner {
    Empty,
    Inline { len: u8, data: [u8; 38] },  // no heap, no Arc
    Single(Bytes),                         // 32 B
    Multi(Vec<Bytes>),                     // 24 B, rare
}
```

`sizeof(Payload)` stays at 40 bytes. 38 is the largest inline
capacity that fits (the Inline variant is 39 bytes; the enum pads
to 40 for alignment with Single's 8-byte align). Covers every bench
size up to 38 B including the 32 B cell. `ChunkedInputBuf::split_to`
copies payloads up to 38 bytes into the inline variant -- zero Arc
operations. Combined with `front_offset`, the per-frame cost on the
codec hot path went from ~3 atomic ops to ~0 (one `Bytes` drop per
chunk, amortized over hundreds of frames).

The 7 callsites that iterate `Payload::chunks()` (all on the encode
side) were updated: flat-buffer extends use `as_slice()` first, gather
I/O paths use `is_contiguous()` + `as_bytes()`, inproc uses
`as_chunk()`. The inline variant returns `&[]` from `chunks()` -- safe
because inline payloads originate from the decode path and reach
the encode path only via REP envelope recycling, where `as_bytes()`
handles the materialization.

**PULL fast path in `try_recv`.** Three levels of specialization
based on socket type: REQ/REP/DISH lock per pop (needs type_state),
SUB holds the lock with subscription filtering, PULL/PAIR skips
both `post_recv_apply` and `matches_subscription` entirely.

### Round 3: cross-crate inlining

After Rounds 1-2, the numbers barely moved. Profiling explained
why: every hot-path function showed up as a separate symbol in
`perf report`. `split_to` alone was 11.9% self time.

The codec lives in `omq-proto`. The socket layer lives in
`omq-compio`. Without LTO, the compiler cannot inline across crate
boundaries. Every `peek_array`, `advance`, `split_to`,
`peek_frame_header`, `try_decode_frame`, `decode_assembled_frame`,
`absorb_data_frame`, `poll_event` call was a real function call with
full prologue/epilogue.

Two fixes: `lto = "thin"` in the workspace `[profile.release]` and
`[profile.bench]`, plus `#[inline]` annotations on all hot-path
functions in `omq-proto` (ChunkedInputBuf methods, frame parsing,
Connection methods, Payload constructors and accessors).

After LTO, `perf report` collapsed the entire codec into one
symbol (`Connection::handle_input` at 43.5%). The structural wins
from Rounds 1-2 became visible.

**Later update: LTO removed.** After the Payload-skip fast path
landed (Round 8), the recv hot path no longer crosses crate
boundaries -- `try_advance_ready` does header peek, buffer read,
and Message construction all inside `omq-proto`. With the fast path
handling all inline-sized single-frame messages, cross-crate
inlining no longer matters for the dominant recv case.

Measurement with the fast path in place:

| config | 8 B msg/s | compile time |
|--------|-----------|--------------|
| `lto = false` | 7.85M | 6 s |
| `lto = "thin"` | 7.57M | 8 s |
| `lto = "fat"` | 7.70M | 24 s |

Thin LTO was slightly *slower* than no LTO at 8 B -- LLVM's
cross-module optimizer made different (worse) inlining decisions
on `peek_frame_header`, pulling it out of `handle_input` as a
separate call at 15% self time. Fat LTO recovered some of that
but not enough to justify the 4x compile-time cost.

Background on the two LTO modes: **thin LTO** runs a fast
cross-module pass that imports and inlines selected function
bodies across crate boundaries, then optimizes each module in
parallel. It sees every crate's IR but applies only targeted
cross-module transforms. **Fat LTO** merges all crate IR into a
single LLVM module and runs the full optimization pipeline on it
-- the global view lets LLVM make better inlining and
devirtualization decisions, but the single-threaded merge +
optimize pass is slow. Neither mode guarantees that a function
marked `#[inline]` will actually be inlined -- LLVM applies its
own cost model and may decline.

LTO is removed from the workspace profile. The `#[inline]`
annotations stay -- they are free at compile time and still help
the within-crate optimizer.

### Round 4: smaller Message, UnsafeCell recv_cache

**`MESSAGE_INLINE_PARTS` 3 → 1.** `Message` was
`SmallVec<[Payload; 3]>` = 128 bytes. For single-part PUSH/PULL,
two of the three inline slots were dead weight copied in and out of
the recv_cache on every message. Dropping to `[Payload; 1]` shrinks
Message to 48 bytes -- 62% less copied per drain_remaining push and
try_recv pop. Multi-part envelopes (REQ, ROUTER, compression
transports) spill to the heap; acceptable for non-pipeline patterns.

**`UnsafeCell` recv_cache.** On compio's single-threaded cooperative
runtime, recv_cache is never contended. Replacing
`Mutex<VecDeque<Message>>` with an `UnsafeCell`-backed `RecvCache`
wrapper removes one atomic CAS + store per try_recv call (~8 ns).
Requires an unsafe `Sync` impl on `SocketInner`, justified by
compio's single-thread guarantee.

### Profile after rounds 3-4 (8 B IPC, PULL side)

| % | Function |
|---|---|
| 66.3 | handle_input (all codec work, inlined) |
| 15.8 | drain_remaining_user_events_into |
| 5.7 | bench_peer main (try_recv loop) |
| 3.7 | SmallVec::drop |
| 2.0 | __memmove_avx |
| 1.3 | __memset_avx (Payload::inline zeroing) |

memcpy collapsed from 24% to 2% (smaller 48 B Messages). try_recv
disappeared as a hotspot (UnsafeCell has no atomic overhead).
`drain_remaining` became the #2 bottleneck at 15.8%.

### Round 5: codec-direct try_recv (partial)

`drain_remaining` pushes N messages from the codec's `Event` queue
into `recv_cache`, one by one. Each push moves an 80-byte `Event`
(the enum is larger than `Message` due to Command and
HandshakeSucceeded variants), matches it, extracts the 48-byte
Message, and pushes that into the VecDeque.

Storing `Arc<DirectIoState>` directly on `SocketInner` (via
`UnsafeCell`) lets try_recv lock `peer_io` and call
`drain_one_user_event` directly -- one Mutex per call (same cost as
the old recv_cache Mutex), but without the 80→48 byte copy overhead
in drain_remaining.

For PULL/PAIR, `try_direct_recv` no longer calls
`drain_remaining_user_events_into`. Events stay in the codec's queue.
try_recv locks `peer_io` directly and pops one event per call.

This cut drain_remaining from 15.8% to 0% but replaced it with
`drain_one_user_event` at 11.1% -- the peer_io Mutex + VecDeque pop
of the 80-byte Event enum.

### Profile after round 5 (8 B IPC, PULL side)

| % | Function |
|---|---|
| 58.7 | handle_input (all codec work, inlined) |
| 17.3 | bench_peer main (try_recv loop + Instant::now) |
| 11.1 | drain_one_user_event (peer_io lock + poll_event) |
| 6.0 | SmallVec::drop |
| 1.6 | __memmove_avx |
| 1.1 | __memset_avx |

### Tried and discarded: codec-direct via cached_route

Before the `direct_recv_io` approach, two attempts tried to reach
`peer_io` through `cached_route` in try_recv:

First attempt: lock `cached_route`, clone `Arc<DirectIoState>`,
release, then try_lock `peer_io`. The Arc clone+drop added 2 atomic
RMW per call; 2 mutex operations vs the cache path's 0 (UnsafeCell).
Throughput halved.

Second attempt: hold `cached_route` for the entire poll, avoiding
the Arc clone. Still 2 mutex locks per call. Throughput halved again.
The lesson: even uncontended, each Mutex lock/unlock is ~8 ns of
atomic CAS + store. Two locks per try_recv call is strictly worse
than the UnsafeCell cache path.

### Round 6: separate message queue, batch swap, driver fix

The remaining 11.1% in `drain_one_user_event` came from popping
80-byte `Event` enums from the codec's event queue and matching on
the variant. Three changes landed together:

**Separate message queue in `Connection`.** `absorb_data_frame`
pushes directly into `messages: VecDeque<Message>` instead of
wrapping in `Event::Message` and pushing into `events`. Data plane
and control plane are separate queues. Messages avoid the 80-byte
Event enum entirely. `poll_message()` and `swap_messages()` expose
the new queue to callers.

**Driver event ordering fix.** Splitting the queue exposed an
ordering bug. The tokio driver drained `poll_message()` before
`poll_event()`. When a single `handle_input` produced both
`HandshakeSucceeded` and the first data frames (common for pipelined
sends), messages arrived at the actor before the handshake event.
The actor's identity map was empty, so REP envelope stripping found
the identity frame as the delimiter. Fix: drain events first, then
messages -- in both the tokio driver (`engine/driver.rs`) and compio
driver (`transport/driver.rs`). The compio driver had a separate
bug: it still matched `Event::Message` from `poll_event()`, which
the queue split had emptied. TCP/IPC messages were silently dropped
until `poll_message()` was wired in.

**Cache-first try_recv.** PULL/PAIR's `try_recv()` and `recv()`
check `recv_cache.pop_front()` before touching `peer_io`. After
`drain_and_swap` fills the cache from the codec's message queue,
~800 messages per batch pop from cache with zero locking
(UnsafeCell). Only when the cache empties does the next `recv()`
re-enter the I/O path.

Profile after round 6 (8 B TCP, PULL side):

| % | Function |
|---|---|
| 62.5 | handle_input (codec parsing, all inlined) |
| 16.8 | bench_peer main (try_recv cache pop + Instant::now) |
| 9.0 | VecDeque::push_back (48 B Message into codec queue) |
| 8.3 | __memmove_avx (VecDeque ring buffer copies) |
| 2.1 | __memset_avx (Payload::inline + Message::from_inline zeroing) |

The async overhead (flume channel checks, mutex, event-listener)
dropped to noise. The codec parse + queue push + memcpy now account
for >80% of all time.

### Round 7: Message enum, Payload internalized

**`Message` as a custom enum.** Replaced `SmallVec<[Payload; 1]>`
with a four-variant enum:

```rust
enum MessageInner {
    Empty,
    Inline { len: u8, data: [MaybeUninit<u8>; 39] },
    Single(Payload),
    Multi(Vec<Payload>),
}
```

`sizeof(Message)` stays at 48 bytes. `Inline` covers every bench
size through 39 B. `absorb_data_frame` constructs `Inline` directly
for single-frame messages up to 39 B, `Single(Payload)` for larger
ones. SmallVec dropped entirely -- no more SmallVec::drop in the
profile (was 6%).

**Payload removed from public API.** `Payload` stays `pub` in
`omq-proto` (the codec and fuzz tests need it) but is no longer
re-exported from omq-tokio or omq-compio. Users see only `Message`.
New public API: `Deref<[u8]>`, `From<Message> for Bytes`,
`msg.iter()`, `msg.pop_front()`, `msg.part_bytes(idx)`,
`Message::with_prefix()`. ZMTP frame encoding moved to
`frame::encode_message_flat/gather/prefixed_*` so `EncodedQueue`
calls those instead of iterating `Payload` parts.

**`MaybeUninit` in the Payload-skip fast path.** The fast path in
`try_advance_ready` (Round 8 below) constructs `MessageInner::Inline`
directly. Its `[u8; 39]` array would otherwise be zeroed on every
message -- 39 bytes of memset for an 8 B payload. Two targeted
`unsafe` blocks use `MaybeUninit` to skip the zeroing: one to
create the uninit array, one to `transmute` it to `[u8; 39]` after
`read_into_uninit` has initialized `data[..payload_len]`. The rest
of message.rs stays safe (Payload::inline and Message::from_inline
zero their arrays normally). Removing the zeroing is worth ~13% at
8 B -- the difference between beating and trailing libzmq.

### Round 8: Payload-skip fast path in the codec

The codec's recv path went: `try_decode_frame` -> `Payload::inline`
(copy N bytes into 38 B array) -> `absorb_data_frame` ->
`Message::from_inline` (copy N bytes into 39 B array). Two copies
of the payload data per message. At 8 B that is 16 B of redundant
copies. At 32 B it is 64 B -- the same total as libzmq's single
64 B `memcpy` of `msg_t`, except omq's copy is split across two
intermediate structures.

The fix: a fast path in `try_advance_ready` that combines header
peek, buffer read, and Message construction in one step. When the
frame is non-command, non-more, inline-sized, no crypto transform
active, and no pending multi-part accumulation, the codec calls
`ChunkedInputBuf::read_into` to copy the payload bytes directly
into `MessageInner::Inline`'s `MaybeUninit` array. No `Frame`, no
`Payload`, no intermediate copy. One memcpy of N bytes.

Per-message bytes written at 32 B, before and after:

| path | copies | total bytes written |
|------|--------|---------------------|
| before: split_to -> Payload::inline -> Message::from_inline -> push_back | 3 | 32 + 32 + 48 = 112 |
| after: read_into -> MessageInner::Inline -> push_back | 2 | 32 + 48 = 80 |
| libzmq: memcpy(msg_t) | 1 | 64 |

The remaining gap vs libzmq is the `VecDeque::push_back` copy: 48
bytes per message, present in every path. libzmq's `yqueue_t`
writes `msg_t` in-place into a pre-allocated chunk -- one pointer
advance, no ring-buffer copy. Closing that gap requires replacing
`VecDeque<Message>` with an arena/chunk allocator, a deeper
structural change.

### Dead end: arena recv (Bytes::slice instead of Payload::inline)

An alternative approach was tested: `ChunkedInputBuf::split_to`
returns `Payload::from_bytes(chunk.slice(..n))` instead of
`Payload::inline` -- the decoded payload shares the 8 KB read
buffer's Arc rather than copying into a stack array.

Profile with arena recv (8 B TCP):

| % | Function |
|---|---|
| 61.1 | handle_input |
| 16.8 | bench_peer main |
| 10.0 | Bytes::shared_drop (Arc decrement on Message drop) |
| 8.6 | Bytes::promotable_even_clone (Arc increment on Bytes::slice) |

Total throughput: unchanged. The Arc bump + drop (~10 ns for two
atomics) costs the same as the inline copy + zeroing it replaced.
A microbenchmark sweeping 1-64 B confirmed: inline wins or ties at
every size up to 39 B. The crossover does not exist in this range
because a single `fetch_add` on x86 (~5 ns) exceeds the cost of
copying 39 bytes from L1 cache (~3-4 ns).

This is the opposite of the conventional wisdom that "zero-copy is
always faster." For payloads that fit in a cache line, the atomic
operation in an Arc bump is more expensive than the copy.

### Net result (current)

8 B TCP: 3.8M -> 8.5M msg/s (0.45x -> **1.07x** of libzmq).
32 B TCP: 3.7M -> 7.3M msg/s (0.45x -> 0.87x of libzmq).

## What remains

### 32 B gap (0.87x)

The profile at 32 B is dominated by the same codec parse (55%) +
`VecDeque::push_back` (12%) + memmove (8%) as at 8 B, but the
per-message payload copy is 32 B vs 8 B. Total bytes written per
message: 80 B (32 B data + 48 B Message into VecDeque) vs libzmq's
64 B (one `msg_t` memcpy into yqueue). The 25% byte overhead
explains most of the remaining gap.

Closing it requires one of:

- **Chunk-based message queue.** Replace `VecDeque<Message>` with
  a `yqueue`-style chunk allocator: 256 x 48 B Messages per chunk,
  spare-chunk recycling. Messages are written in-place; the
  consumer advances a read pointer. No per-message ring-buffer
  copy. This is the structural equivalent of libzmq's approach
  and would bring the total bytes written per message to 32 B
  (one payload copy into the chunk slot), below libzmq's 64 B.

- **Fused decode-and-deliver.** Instead of decode -> queue -> swap ->
  pop, the codec yields messages via a callback or returns an
  iterator. The consumer processes each message inline during
  `handle_input`. No queue at all for the direct-recv path. This
  is a deeper API change to the sans-I/O codec.

### Other directions

- **Multi-runtime for the remaining compio benches.** PUSH/PULL is
  now multi-runtime (PULL on its own thread, PUSHes on another); the
  numbers in `COMPARISONS.md` reflect that 2-core shape. PUB/SUB,
  ROUTER/DEALER, and PAIR are still single-runtime and will gain
  similar 20-40% once converted. REQ/REP and latency are roundtrip
  patterns where multi-runtime adds inter-thread overhead with no
  throughput win, so they stay single-runtime.
- **Single-wire-peer bypass on tokio.** The compio fast path
  (sender encodes directly into a per-peer queue, skipping the
  per-peer command channel) has no equivalent on tokio yet. The
  shape would be analogous: a per-peer EncodedQueue clone in
  `RoundRobin` routing, claimed via `try_lock`.
- **Profile-guided optimization.** Not yet attempted. Likely worth
  a few percent at the small-message end where the hot path is
  short enough to be PGO-shaped.

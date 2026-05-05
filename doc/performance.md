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
type Payload = SmallVec<[Bytes; 2]>;    // 2 chunks inline
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

## What remains

A few directions that look promising but have not been measured yet:

- **Multi-runtime compio for fan-in.** compio benches in
  `BENCHMARKS.md` run on one core; tokio runs on the whole box. A
  multi-runtime compio deployment with `RuntimeBuilder::thread_
  affinity` per worker should lift wire throughput 20-40 % on
  multi-peer fan-in. The bench harness does not exercise that shape.
- **Single-wire-peer bypass on tokio.** The compio fast path
  (sender encodes directly into a per-peer queue, skipping the
  per-peer command channel) has no equivalent on tokio yet. The
  shape would be analogous: a per-peer EncodedQueue clone in
  `RoundRobin` routing, claimed via `try_lock`.
- **Better bench resolution.** Bumping `ROUND_DURATION` from 300 ms
  to 1 s (3.3x wall time, ~2x stdev reduction) plus `ROUNDS=3` and
  `taskset` pinning would expose the ~1-3 % wins that currently sit
  below noise.
- **Profile-guided optimisation.** Not yet attempted. Likely worth
  a few percent at the small-message end where the hot path is
  short enough to be PGO-shaped.

The ceiling of this design is not yet known. Each of the items above
should produce a measurable single-digit improvement, and any one of
them might surface a new bottleneck that opens the next round of
work.

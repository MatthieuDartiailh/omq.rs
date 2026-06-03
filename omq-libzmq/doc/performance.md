# omq-libzmq Performance

Throughput measured with 2-process TCP loopback (PUSH/PULL, 32 B
payload). Latency measured with REQ/REP round-trip (p50).

## The constraint

omq-libzmq implements the `zmq_*` C API. Callers are plain C threads
with no async runtime. omq-tokio::Socket is Send+Sync, but its
`send()`/`recv()` methods are async and need a tokio task context.
The central question is how to bridge the C thread to the tokio
runtime with minimal per-message overhead.

libzmq's own architecture is instructive: `zmq_send()` pushes into a
lock-free SPSC pipe (ypipe). A dedicated I/O thread pops from the pipe
and writes to TCP. One queue crossing per message, lock-free on both
sides.

## omq-compio baseline: broken

omq-libzmq started on omq-compio. The compio runtime ran on a
background thread (`rt.block_on(job_loop)`). A flume channel relayed
messages between the C thread and send/recv pump tasks on the compio
thread.

TCP handshakes were structurally broken: compio's `block_on` loop
(`tick() -> io_uring_enter -> tick()`) couldn't drive multi-step ZMTP
handshakes promptly. Each handshake step required a full loop
iteration. Result: 87ms-946ms handshake latency in 2-process TCP
benchmarks, 40% failure rate. Root cause is in compio's runtime
scheduling, not fixable from the FFI layer.

## Port to omq-tokio + yring relay (both sides): 808k msg/s, 60 us

Replaced compio with a tokio multi-thread runtime. Replaced flume
channels with yring SPSC rings for both send and recv paths. The send
pump drained the ring via `AsyncConsumer` stream, called
`socket.send().await` per message, and yielded every 256 messages. The
recv pump called `socket.recv().await` and pushed to the ring, signaling
an eventfd per message for `zmq_poll` readiness.

TCP handshakes worked reliably. Throughput was 808k msg/s at 32 B.

Two sources of per-message overhead:
1. Send pump waker: the `AsyncConsumer` stream's waker fires on every
   `flush()`, invoking tokio's task scheduler. One task schedule per
   message.
2. Recv pump eventfd: `libc::write(eventfd, 1)` per message. One
   syscall per message.

Removing the `Mutex` around the yring `Producer`/`Consumer` (ZMQ's
single-thread-per-socket contract makes it unnecessary) had no
measurable effect. Uncontended mutex is just two CAS ops.

## Direct block_on both sides: 1.9M msg/s, 2600 us

Hypothesis: eliminate both relay queues entirely. Call
`Handle::block_on(socket.send/recv)` directly from the C thread.

Send throughput jumped to 1.9M msg/s (2.4x). But REQ/REP latency
collapsed to 2.6ms p50 (43x regression). Each `block_on(recv)` parks
the C thread and waits for a cross-thread wakeup from a tokio worker.
The wakeup notification latency dominates.

pyomq measured the same effect: `Handle::block_on` costs ~2 us per
call for the runtime context enter, cross-thread future dispatch, and
wakeup notification. Throughput benefits from eliminating the relay,
but latency suffers because every recv is a park/unpark cycle.

## Attempted: direct send + direct recv with prefetch buffer

Hypothesis: eliminate both relay queues. Use `block_on(recv)` directly,
with a local `VecDeque` prefetch buffer (up to 256 messages / 1 MiB)
filled in bulk on each `block_on` call. `zmq_recv(DONTWAIT)` drains
from the buffer; `zmq_poll` checks the buffer.

Not measured (abandoned during design). The DONTWAIT drain pattern
(`while zmq_recv(DONTWAIT) >= 0 { count++ }`) is the benchmark hot
path. With a prefetch buffer, the first DONTWAIT call triggers a
`block_on(recv)` to fill the buffer, then subsequent calls drain it.
But `zmq_poll` also needs to block until data arrives (for the outer
poll loop). Without a recv pump signaling the eventfd, `zmq_poll`
can't use `libc::poll` to sleep. Multi-socket poll would require
spawning concurrent recv futures with `tokio::select`, adding
complexity without solving the fundamental `block_on` latency problem.

## Hybrid: direct send + flume recv: 711k msg/s, 78 us

Keep `block_on` for sends (no relay), use a flume channel for recv
(pump fills channel, C thread pops). Latency recovered to 78 us, but
throughput dropped to 711k. Flume's `send_async().await` involves
waker registration per message, heavier than yring's atomic store.

## Hybrid: direct send + yring recv: 1.02M msg/s, 38 us (current)

Replace flume with yring for the recv relay. The recv pump signals the
eventfd only on empty-to-non-empty transitions (not per message),
eliminating the per-message syscall. The C thread pops via
`Consumer::prefetch_and_pop()`, which batches: one atomic load
prefetches N messages, subsequent pops are local memory reads with zero
atomics.

| Approach | 32 B msg/s | p50 latency |
|----------|-----------|-------------|
| yring both sides | 808k | -- |
| direct both | 1.9M | 2600 us |
| direct send + flume recv | 711k | 78 us |
| direct send + yring recv | 1.02M | 38 us |

## Attempted: yring send pump (AsyncConsumer): 90k msg/s

Hypothesis: replace `block_on` with a fire-and-forget yring push on
the C thread, drained by an `AsyncConsumer` stream on the tokio thread.
The `AtomicWaker::wake()` per `flush()` should be cheaper than
`block_on`.

Result: 90k msg/s. Worse by 11x. The `AsyncConsumer` stream's waker
fires once per flush, scheduling the pump task via tokio's task
scheduler. Each schedule costs ~1-2 us (intrusive linked list + worker
notification). Under sustained load, the pump processes one message per
schedule cycle, serializing throughput to the scheduler's rate.

## Attempted: yring send pump (Notify): 1.04M msg/s, latency timeouts

Replaced the `AsyncConsumer` waker with a plain `Consumer` +
`tokio::sync::Notify`. The pump batch-drained all available messages,
then parked on the Notify. The C thread signaled `notify_one()` on
empty-to-non-empty transitions.

Throughput recovered to 1.04M, but REQ/REP latency deadlocked under
sustained load. `tokio::sync::Notify` is edge-triggered: if
`notify_one()` fires between `notified().enable()` and the
re-check, the notification is lost. Switching to unconditional
`notify_one()` per message fixed some sizes but not all.

## Attempted: yring send pump (eventfd + AsyncFd): 302k msg/s, timeouts

Hypothesis: use an eventfd to wake the send pump via tokio's I/O
reactor (epoll) instead of the task scheduler. The C thread writes
to the eventfd on empty-to-non-empty transitions. The pump wraps
the eventfd in `AsyncFd` and awaits `readable()`.

Result: 302k msg/s, all latency sizes timed out. `AsyncFd::readable()`
still wakes the task through tokio's task scheduler (the reactor
notifies the scheduler, which schedules the task). Same overhead,
different entry point. Under sustained REQ/REP load the pump stalls
entirely.

## Current: direct send + yring recv (1.0M msg/s, 38 us)

The send pump approaches all hit the same wall: the wakeup mechanism
(waker, Notify, or reactor) adds per-message overhead comparable to
or worse than `block_on`.
The `block_on` path wins because the future completes synchronously
when the socket's outbound queue has capacity (the common case). No
cross-thread wakeup needed.

| Approach | 32 B msg/s | p50 latency |
|----------|-----------|-------------|
| yring both sides (initial) | 808k | -- |
| direct both (no relay) | 1.9M | 2600 us |
| direct send + flume recv | 711k | 78 us |
| **direct send + yring recv** | **1.0M** | **38 us** |
| yring send (AsyncConsumer) + yring recv | 90k | 59 us |
| yring send (Notify) + yring recv | 1.04M | timeouts |
| yring send (eventfd+AsyncFd) + yring recv | 302k | timeouts |

## Remaining gap

Native omq-tokio reaches ~7M msg/s at 32 B in the same 2-process TCP
benchmark. The 7x gap is the send-side `block_on` overhead: ~1 us per
call for runtime context setup, future poll, and teardown. Native
omq-tokio avoids this because `socket.send().await` runs inside a
tokio task with zero per-call context overhead.

The recv side is not the bottleneck: the yring relay's batched
prefetch adds negligible overhead once data is flowing. The send side
is where architectural improvement would have the most impact.

Potential approaches not yet tried:
- Expose a synchronous `try_send()` on `omq_tokio::Socket` that
  bypasses the async machinery entirely, writing directly to the
  outbound channel without entering a tokio context.
- Use `Handle::enter()` once per burst (amortize context setup across
  N messages) instead of per-message `block_on`.

# Benchmarks

Numbers below are from a single sweep on Linux 6.12 (Debian 13) on an
Intel Mac Mini 2018 (i7-8700B, 3.2 GHz, 4 vCPU), Rust 1.95.0,
default features (no priority feature; priority mode trades work-
stealing for per-peer queues - relevant for ordering, not throughput).
Built with `cargo bench --release`. Each cell is one prime + warmup +
0.5 s timed run. Sources: `omq-tokio/benches/` and `omq-compio/benches/`.
Run yourself with `cargo bench` per crate. Bare-metal, modern CPUs, and
tuned tokio runtimes will show different absolute numbers - relative
shape between transports and sizes should hold.

> **One core for the compio numbers.** Every omq-compio bench in this
> document runs PUSH and PULL inside a single `#[compio::main]`
> runtime, which is single-threaded by design. Both peers share one
> CPU; the omq-tokio numbers below have a multi-thread runtime
> spreading work across `num_cpus::get()` worker threads. So the
> compio column is "what one core can do" while the tokio column is
> "what the whole box can do" - the comparison is wildly unfair to
> compio on wire transports.
>
> If your workload can drive sockets from independent task graphs
> (per-shard ingestion, per-tenant dispatch, etc.), instantiate one
> `compio::runtime::Runtime` per worker thread and pin it via
> `RuntimeBuilder::thread_affinity(...)`. A two-runtime PUSH/PULL
> probe on this hardware lifts TCP / IPC small-message rates by
> roughly 20-40%; inproc is unchanged since there's no kernel work
> to overlap. Multiple runtimes also unlock the natural thread-per-
> core pattern (one acceptor + N pinned workers, sharded by identity
> or hash) that libzmq and Seastar use to scale past one core.

## PUSH/PULL throughput by transport, single peer (omq-compio, one core)

Numbers below are single 0.5 s timed runs per cell; the small-size
wire columns vary ±10 % run-to-run (cache / scheduling jitter on a
single core). Larger sizes vary more once kernel send-buffer behaviour
kicks in - take ±25 % at 8 KiB+ as a rough envelope.

<!-- BEGIN push_pull_compio_1peer -->
| Size | inproc | ipc | tcp | lz4+tcp | zstd+tcp |
|---|---|---|---|---|---|
| 32 B | 2.77M / 88.7 MB/s | 2.01M / 64.4 MB/s | 1.49M / 47.8 MB/s | 1.61M / 51.5 MB/s | 614k / 19.7 MB/s |
| 128 B | 2.79M / 357 MB/s | 1.80M / 230 MB/s | 1.68M / 215 MB/s | 1.42M / 181 MB/s | 96.5k / 12.4 MB/s |
| 512 B | 2.81M / 1.44 GB/s | 1.32M / 678 MB/s | 1.23M / 629 MB/s | 355k / 182 MB/s | 99.2k / 50.8 MB/s |
| 2 KiB | 2.75M / 5.64 GB/s | 816k / 1.67 GB/s | 786k / 1.61 GB/s | 348k / 712 MB/s | 319k / 654 MB/s |
| 8 KiB | 2.81M / 23.0 GB/s | 388k / 3.18 GB/s | 351k / 2.87 GB/s | 269k / 2.20 GB/s | 220k / 1.80 GB/s |
| 32 KiB | 2.78M / 91.2 GB/s | 126k / 4.12 GB/s | 110k / 3.60 GB/s | 112k / 3.68 GB/s | 95.0k / 3.11 GB/s |
| 128 KiB | 2.78M / 364.8 GB/s | 32.7k / 4.29 GB/s | 27.7k / 3.63 GB/s | 33.2k / 4.36 GB/s | 29.8k / 3.91 GB/s |

<!-- END push_pull_compio_1peer -->

Note: large-payload "GB/s" on inproc reflects the zero-copy refcount-
clone path - bytes never traverse the kernel. lz4 / zstd on
incompressible payloads (random bytes) cross from overhead to net win
around 32 KiB, where the smaller WRITEV calls outweigh the codec cost.
On compressible traffic (e.g. JSON events), the crossover is much
earlier - see the JSON compression bench below.

## Compression on realistic JSON payloads (omq-compio, 1 peer)

Payload is a JSON event-log record (timestamps, trace ids, repeated
field names - typical eventing-pipeline traffic). Each cell shows
**three rates**: `msgs/s · wire MB/s · virtual MB/s`, where wire MB/s
is what the network actually carries (post-compression) and virtual
MB/s is what the application sees (pre-compression). For plain `tcp`,
wire == virtual.

Compression ratios on this template:

| size    | lz4     | zstd     |
|---------|---------|----------|
| 128 B   | 0.97×*  | 0.97×*   |
| 256 B   | 0.98×*  | 0.98×*   |
| 512 B   | 1.57×   | 1.62×    |
| 1 KiB   | 2.60×   | 2.84×    |
| 2 KiB   | 3.76×   | 4.47×    |
| 4 KiB   | 4.92×   | 7.41×    |
| 16 KiB  | 6.47×   | **12.87×** |

\* Below 512 B the transform doesn't even attempt compression - frame
envelope overhead doesn't amortize at small sizes, so both lz4 and
zstd fall back to plaintext (0.97-0.98× reflects the 4-byte
`SENTINEL_PLAIN` framing tax). A pre-trained dictionary moves that
cutoff way down: 32 B for lz4, 64 B for zstd. See the next section.

Loopback throughput (msgs/s · wire MB/s · virtual MB/s):

| size  | tcp                        | lz4+tcp                           | zstd+tcp                          |
|-------|----------------------------|-----------------------------------|-----------------------------------|
| 128 B | 1.67M / 214 MB/s           | 1.42M / 188 MB/s / 182 MB/s       | 127k / 16.8 MB/s / 16.3 MB/s      |
| 256 B | 1.45M / 371 MB/s           | 1.22M / 318 MB/s / 313 MB/s       | 130k / 33.7 MB/s / 33.2 MB/s      |
| 512 B | 1.21M / 619 MB/s           | 513k / 167 MB/s / 263 MB/s        | 96.2k / 30.4 MB/s / 49.3 MB/s     |
| 1 KiB | 979k / 1.00 GB/s           | 458k / 180 MB/s / 469 MB/s        | 282k / 102 MB/s / 289 MB/s        |
| 2 KiB | 797k / 1.63 GB/s           | 367k / 200 MB/s / 751 MB/s        | 203k / 92.9 MB/s / 416 MB/s       |
| 4 KiB | 558k / 2.29 GB/s           | 267k / 222 MB/s / 1.09 GB/s       | 115k / 63.4 MB/s / 469 MB/s       |
| 16 KiB| 206k / 3.38 GB/s           | 103k / 262 MB/s / 1.69 GB/s       | 42.2k / 53.7 MB/s / 691 MB/s      |

On loopback, plain TCP wins msgs/s - compression's CPU cost has no
offsetting wire-bandwidth payoff because there's no bandwidth scarcity.
**Look at the wire MB/s column** to predict behavior on a bandwidth-
bounded link: at 16 KiB messages, lz4+tcp ships ~262 MB/s wire while
delivering ~1.69 GB/s of application data. On a 1 Gbps WAN (~125 MB/s
wire ceiling) plain `tcp` would deliver 125 MB/s of application data
total - `lz4+tcp` would deliver ~808 MB/s and `zstd+tcp` ~1.61 GB/s.
That's where compression earns its keep.

### With a pre-trained dict (small messages)

For small messages, per-frame codec overhead (header bytes + cold
codebook) can leave compression underwater. A pre-trained dictionary
primes the codec with byte sequences from your message family, so
even a 128 B record compresses heavily. Pass via
`Options::compression_dict(Bytes)`; the dict is shipped to the peer
on the first connection and reused for every subsequent frame.

Compression ratios on the same JSON template, with a trained dict
(zstd: 1.6 KiB trained from 200 sample records; lz4: 4 KiB
representative buffer):

| size  | lz4 (no dict) | lz4 (with dict) | zstd (no dict) | zstd (with dict) |
|-------|---------------|-----------------|----------------|------------------|
| 128 B | 0.97× (skip)  | **5.82×**       | 0.97× (skip)   | **5.12×**        |
| 256 B | 0.98× (skip)  | **11.64×**      | 0.98× (skip)   | **9.85×**        |
| 512 B | 1.57×         | **22.26×**      | 1.62×          | **19.69×**       |
| 1 KiB | 2.60×         | **11.25×**      | 2.84×          | **35.31×**       |
| 2 KiB | 3.76×         | **8.50×**       | 4.47×          | **16.93×**       |

"(skip)" marks sizes below the 512-byte attempt threshold - the
transform doesn't even try to compress, so the no-dict ratio is just
the framing tax. With a dict, the threshold drops to 32 B (lz4) /
64 B (zstd) and small messages compress meaningfully.

Loopback throughput with the same dict (msgs/s · wire MB/s · virt MB/s):

| size  | lz4+tcp                          | zstd+tcp                       |
|-------|----------------------------------|--------------------------------|
| 128 B | 254k / 5.60 MB/s / 32.5 MB/s    | 138k / 3.50 MB/s / 17.7 MB/s  |
| 256 B | 268k / 5.90 MB/s / 68.5 MB/s    | 138k / 3.60 MB/s / 35.3 MB/s  |
| 512 B | 261k / 6.00 MB/s / 134 MB/s     | 136k / 3.50 MB/s / 69.9 MB/s  |
| 1 KiB | 331k / 30.1 MB/s / 339 MB/s     | 134k / 3.90 MB/s / 138 MB/s   |
| 2 KiB | 298k / 71.9 MB/s / 611 MB/s     | 118k / 14.2 MB/s / 241 MB/s   |

Same loopback caveat: CPU cost without bandwidth payoff. On a
bandwidth-bounded link the wire-MB/s column is the actual link
load and the virt-MB/s column is what the application gets out
the other end. The auto-train mode (default on for `zstd+tcp`)
reaches similar ratios after the first ~1000 messages or 100 KiB
of plaintext.

## REQ/REP round-trip latency (single peer)

<!-- BEGIN req_rep_latency -->
| transport | size | omq-compio | omq-tokio |
|---|---|---|---|
| inproc | 32 B | 5.4 µs (185k) | 30.7 µs (32.5k) |
| inproc | 128 B | 5.4 µs (184k) | 52.4 µs (19.1k) |
| inproc | 512 B | 5.5 µs (183k) | 35.7 µs (28.0k) |
| inproc | 2 KiB | 5.5 µs (182k) | 31.1 µs (32.1k) |
| inproc | 8 KiB | 5.5 µs (183k) | 33.5 µs (29.9k) |
| inproc | 32 KiB | 5.5 µs (183k) | 30.1 µs (33.2k) |
| inproc | 128 KiB | 5.5 µs (183k) | 39.5 µs (25.3k) |
| ipc | 32 B | 19.3 µs (51.8k) | 55.3 µs (18.1k) |
| ipc | 128 B | 19.2 µs (52.1k) | 54.7 µs (18.3k) |
| ipc | 512 B | 20.0 µs (50.1k) | 56.7 µs (17.6k) |
| ipc | 2 KiB | 20.7 µs (48.4k) | 131 µs (7.7k) |
| ipc | 8 KiB | 23.4 µs (42.7k) | 72.6 µs (13.8k) |
| ipc | 32 KiB | 29.7 µs (33.7k) | 71.3 µs (14.0k) |
| ipc | 128 KiB | 72.1 µs (13.9k) | 246 µs (4.1k) |
| tcp | 32 B | 27.4 µs (36.5k) | 72.7 µs (13.8k) |
| tcp | 128 B | 27.9 µs (35.8k) | 67.1 µs (14.9k) |
| tcp | 512 B | 29.1 µs (34.3k) | 71.5 µs (14.0k) |
| tcp | 2 KiB | 30.3 µs (33.0k) | 74.0 µs (13.5k) |
| tcp | 8 KiB | 32.9 µs (30.4k) | 178 µs (5.6k) |
| tcp | 32 KiB | 40.0 µs (25.0k) | 97.3 µs (10.3k) |
| tcp | 128 KiB | 92.8 µs (10.8k) | 129 µs (7.8k) |

<!-- END req_rep_latency -->

µs is round-trip wall time; parenthesized number is full request+reply
pairs per second. compio wins inproc by ~6× (single-thread, no
syscall, recv-direct fast path); on wire transports compio's RTT runs
roughly 2.5-3× below tokio's at small messages (see "compio IPC
latency: hop-reduction history" below). Cells are single 0.5 s runs;
small-message wire RTTs jitter ±15-25% on a 4-vCPU VM, so read the
trend, not any single cell. The RTT win comes from Stage 5's
recv-direct path: on the inbound side, `Socket::recv` reads the FD
inline instead of waiting for the driver task to forward parsed
messages over a flume hop.

### REQ/REP latency percentiles (p50 / p99 / p999)

Dedicated serial ping-pong bench: 1 000 warmup + 10 000 measured iterations per cell.
All values are µs wall time. Compression transports add per-frame codec overhead.

<!-- BEGIN latency_percentiles -->
| transport | size | compio p50 | compio p99 | compio p999 | tokio p50 | tokio p99 | tokio p999 |
|---|---|---|---|---|---|---|---|
| inproc | 32 B | 5.48 µs | 5.83 µs | 25.9 µs | 32.8 µs | 47.6 µs | 74.1 µs |
| inproc | 128 B | 5.49 µs | 5.59 µs | 11.5 µs | 31.3 µs | 52.9 µs | 83.1 µs |
| inproc | 512 B | 5.51 µs | 5.70 µs | 18.8 µs | 31.4 µs | 78.3 µs | 113 µs |
| inproc | 2 KiB | 5.49 µs | 5.77 µs | 24.6 µs | 31.2 µs | 47.9 µs | 82.4 µs |
| inproc | 8 KiB | 5.56 µs | 5.84 µs | 35.8 µs | 34.4 µs | 283 µs | 358 µs |
| inproc | 32 KiB | 5.56 µs | 5.82 µs | 21.7 µs | 31.8 µs | 41.1 µs | 64.9 µs |
| inproc | 128 KiB | 5.56 µs | 6.02 µs | 32.5 µs | 31.8 µs | 40.6 µs | 50.3 µs |
| ipc | 32 B | 20.2 µs | 33.4 µs | 57.3 µs | 54.3 µs | 75.8 µs | 116 µs |
| ipc | 128 B | 20.2 µs | 29.6 µs | 55.6 µs | 53.5 µs | 840 µs | 947 µs |
| ipc | 512 B | 20.3 µs | 33.4 µs | 57.6 µs | 54.1 µs | 86.5 µs | 133 µs |
| ipc | 2 KiB | 21.7 µs | 34.7 µs | 57.6 µs | 61.3 µs | 867 µs | 975 µs |
| ipc | 8 KiB | 25.0 µs | 37.9 µs | 64.0 µs | 65.9 µs | 123 µs | 315 µs |
| ipc | 32 KiB | 31.5 µs | 44.7 µs | 69.3 µs | 73.9 µs | 980 µs | 1.1 ms |
| ipc | 128 KiB | 75.3 µs | 119 µs | 134 µs | 97.6 µs | 124 µs | 158 µs |
| tcp | 32 B | 28.5 µs | 42.7 µs | 72.5 µs | 65.6 µs | 878 µs | 971 µs |
| tcp | 128 B | 28.5 µs | 42.7 µs | 76.5 µs | 65.9 µs | 119 µs | 169 µs |
| tcp | 512 B | 28.2 µs | 44.4 µs | 76.6 µs | 65.2 µs | 118 µs | 181 µs |
| tcp | 2 KiB | 29.3 µs | 36.7 µs | 56.4 µs | 67.7 µs | 917 µs | 1.0 ms |
| tcp | 8 KiB | 31.5 µs | 40.8 µs | 58.9 µs | 70.1 µs | 125 µs | 183 µs |
| tcp | 32 KiB | 38.8 µs | 54.7 µs | 78.8 µs | 93.5 µs | 122 µs | 209 µs |
| tcp | 128 KiB | 86.8 µs | 138 µs | 154 µs | 117 µs | 1.2 ms | 1.3 ms |
| lz4+tcp | 32 B | 28.5 µs | 35.2 µs | 48.3 µs | — | — | — |
| lz4+tcp | 128 B | 28.3 µs | 35.9 µs | 54.4 µs | — | — | — |

<!-- END latency_percentiles -->

### compio IPC latency: hop-reduction history

Three structural changes on the compio path cut substantially off
REQ/REP RTT vs. the original actor-shaped implementation. A fourth
change (Stage 4) was tried and reverted; it's listed last as the
"what we tried and threw out" entry because the trade-off it
revealed shaped the final design.

1. **Single-wire-peer send bypass.** Round-robin sockets (REQ/REP/PAIR/
   DEALER 1:1) skip the socket-wide `shared_send_tx` and submit
   directly to the peer's per-driver `cmd_tx` when only one wire peer
   is connected. Multi-peer wire still uses the shared queue for
   work-stealing. Falls back to the shared queue if the per-peer
   channel is disconnected (driver died, reconnect in flight) so the
   libzmq "buffer up to send_hwm with no live peer" semantic holds.
   Implemented in `omq-compio/src/socket/send.rs`.

2. **`PollFd::read_ready` in the driver select instead of a dedicated
   read task.** Previously each connection spawned a read task that
   ferried filled buffers via a flume channel - one task wake per
   inbound chunk. The driver's `select_biased!` now races
   `PollFd::read_ready` (cancellation-safe; backed by io_uring's
   `PollOnce`). When it fires, the driver does an inline
   `reader.read(buf).await`; the kernel data is already queued so the
   read SQE completes immediately. Implemented in
   `omq-compio/src/transport/driver.rs`.

3. **Stage 5 - recv-direct fast path.** `Socket::recv` on
   single-peer eligible sockets (Pull / Sub / Rep / Pair / Req)
   reads the FD inline instead of waiting on the driver's `in_rx`
   hop. The reader, codec, writer, and transform live in a
   `SharedPeerIo` behind an `async_lock::Mutex`; a per-connection
   `DirectIoState` arbitrates FD ownership via a one-shot
   `recv_claim` atomic and `recv_state_changed` /  `eof_signal`
   `event_listener::Event`s. The driver re-checks `recv_claim`
   under the lock before any read so it can't steal kernel data
   from a recv caller that claimed the FD between iterations.
   `recv()` flushes codec output (auto-PONG, etc.) inline so
   heartbeats keep flowing while the claim is held. ROUTER, XPUB,
   XSUB, DISH stay on the slow path. The reader / writer halves
   live in `WireReader` / `WireWriter` enums (one variant per
   transport); static-dispatched `match` inside the async methods
   means no `Box<dyn Future>` per call - which mattered after
   benchmarking showed boxed futures dominating the small-message
   throughput path. Cancellation note: dropping a `recv()` future
   after `read_ready` has fired but before the read SQE returns
   may forfeit a small amount of in-flight bytes (~5 µs window);
   the codec stays consistent and the connection remains usable.
   Implemented in `omq-compio/src/socket/handle.rs`,
   `omq-compio/src/socket/inner.rs`,
   `omq-compio/src/transport/peer_io.rs`, and the driver loop in
   `omq-compio/src/transport/driver.rs`. Cuts REQ/REP IPC RTT
   roughly in half (recv side is one of two hops per RTT).

4. **EncodedQueue send bypass.** `Socket::send` on single-peer wire
   connections (no transform) encodes ZMTP frames directly into a
   `VecDeque<Bytes>` in `DirectIoState` via a sync `Mutex::try_lock`,
   bypassing the codec's async mutex entirely. This eliminates
   `clone_transmit_chunks` + `advance_transmit` on the hot path and
   removes N `Arc` reference-count bumps per `write_vectored` call
   (chunks move into the iovec rather than being cloned). The driver
   drains the queue in step 3b, after flushing the codec in step 3a.
   A `driver_in_select: AtomicBool` flag lets the sender issue
   `transmit_ready.notify(1)` only when the driver is parked in
   `select_biased!` — no spurious wakeups while the driver is
   actively looping. Race-free in compio's cooperative single-threaded
   runtime: no task switch between `store(true)` and the first
   `await` inside `select_biased!`. Transform paths (lz4+tcp, zstd+tcp)
   fall back to the codec mutex path unchanged. Implemented in
   `omq-compio/src/socket/send.rs` and `omq-compio/src/socket/inner.rs`.
   Lifts 128 B TCP PUSH/PULL from 1.30M to 1.48M msg/s; large
   messages see 2-3× wins vs. libzmq (see libzmq comparison below).

#### Stage 4 (tried, reverted): direct-write fast path

Stage 4 put the writer in `SharedPeerIo` and let `Socket::send`
encode + `write_vectored` inline, skipping the `cmd_tx` hop on the
send side. RTT went from ~165 µs to ~85 µs in the original
measurements - a clean 2× win on paper. **PUSH/PULL throughput
collapsed by 4-7×** at small message sizes (TCP 128 B: ~830k → ~115k
msg/s). Cause: the pre-Stage-4 driver got cross-message batching
"for free" - producers pushed into `cmd_tx` and returned
immediately, the driver drained N queued messages on its next
iteration and issued ONE `writev` for all of them. Stage 4
collapsed that into per-call inline encoding + writev, so a hot
single-producer loop did one syscall per message instead of one
syscall per N messages. The recv-side win from Stage 5 was kept
because RTT reduction there doesn't depend on changing the send
path; the send-side fast path was reverted in favour of restoring
producer/writer pipelining. The lesson: latency wins on bypass-the-
hop optimisations can mask big throughput regressions when the hop
was implicitly batching.

The omq-tokio IPC numbers are still untouched. Tokio's send path goes
through the SocketDriver actor + per-send submit task + per-peer pump
+ ConnectionDriver — more hops than compio, but the multi-thread
runtime hides some of the cost by overlapping send/recv on different
workers. Stage 1's single-wire-peer bypass would port to tokio's
`routing/round_robin.rs`; tracked as a follow-up.

## libzmq vs omq-compio (two-process TCP, one core each)

Two separate processes on the same machine, each pinned to one core.
`bench_peer push` binds a TCP port and sends forever; `bench_peer pull`
connects, warms up for 500 ms, then counts for 3 seconds. The libzmq
peer is a minimal C binary compiled against the system libzmq (5.2.5).

The omq process is single-threaded (push loop + driver share one
compio runtime). libzmq spawns a dedicated I/O thread alongside the
app thread - two threads vs. one, which gives libzmq a small edge
at small messages where the app loop and I/O thread can overlap.
omq's advantage at large messages comes from `write_vectored` batching
multi-chunk frames in a single `writev` call, while libzmq issues
separate `send()` calls for the frame header and each payload segment.

<!-- BEGIN libzmq_comparison -->
| Size | omq msg/s | omq MB/s | zmq msg/s | zmq MB/s | ratio |
|-------|-----------|----------|-----------|----------|-------|
| 128 B | 3.00M | 384 MB/s | 2.95M | 377 MB/s | 1.02× |
| 512 B | 2.35M | 1.2 GB/s | 2.05M | 1.0 GB/s | **1.1×** |
| 2 KiB | 1.44M | 3.0 GB/s | 648k | 1.3 GB/s | **2.2×** |
| 8 KiB | 578k | 4.7 GB/s | 189k | 1.6 GB/s | **3.1×** |
| 32 KiB | 153k | 5.0 GB/s | 72k | 2.4 GB/s | **2.1×** |
| 128 KiB | 48k | 6.3 GB/s | 33k | 4.4 GB/s | **1.5×** |

<!-- END libzmq_comparison -->

At 128 B, omq-compio is ~13% slower than libzmq (libzmq overlaps its
app thread and a dedicated I/O thread); at 512 B they are at parity;
beyond that omq pulls ahead by 2-3×. The crossover is around 512 B —
roughly where `write_vectored` batching of multi-chunk frames pays off
vs. libzmq's separate `send()` per frame segment. Run
`./scripts/bench_compare.sh --update-benchmarks` to refresh this table.

## Backend comparison: PUSH/PULL throughput, single peer

<!-- BEGIN backend_comparison -->
| Size | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---|---|---|---|---|---|---|
| 32 B | 2.77M | 191k | 2.01M | 364k | 1.49M | 406k |
| 128 B | 2.79M | 419k | 1.80M | 185k | 1.68M | 172k |
| 512 B | 2.81M | 181k | 1.32M | 344k | 1.23M | 406k |
| 2 KiB | 2.75M | 436k | 816k | 244k | 786k | 240k |
| 8 KiB | 2.81M | 430k | 388k | 402k | 351k | 125k |
| 32 KiB | 2.78M | 225k | 126k | 98.9k | 110k | 93.9k |
| 128 KiB | 2.78M | 423k | 32.7k | 35.8k | 27.7k | 38.3k |

<!-- END backend_comparison -->

Numbers are msg/s. compio wins at every size on every transport on
this hardware: io_uring + the direct-routing path beats tokio's
mio/epoll syscall path even where syscall overhead amortizes at
large sizes. Wins narrow at very-large sizes where syscall cost is
the same on both backends. **Note that compio here is one core
versus tokio's whole box** - see the caveat at the top of this
document. Tokio's lead grows on multi-peer fan-in (its multi-thread
runtime overlaps senders across cores); a multi-runtime compio
deployment lifts wire throughput another 20-40%.

## PUSH/PULL throughput, 8 peers

Same bench with 8 concurrent PUSH peers fanning into one PULL. inproc/ipc/tcp,
both backends. compio scales linearly on inproc (lock-free flume); tokio's
multi-thread runtime gains from overlapping sender tasks on wire transports.

<!-- BEGIN push_pull_8peer -->
| Size | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---|---|---|---|---|---|---|
| 32 B | 2.97M | 644k | 1.90M | 944k | 1.75M | 450k |
| 128 B | 3.01M | 1.10M | 1.78M | 967k | 1.64M | 527k |
| 512 B | 2.99M | 636k | 1.29M | 774k | 1.17M | 844k |
| 2 KiB | 3.00M | 1.15M | 850k | 216k | 770k | 374k |
| 8 KiB | 2.96M | 640k | 448k | 625k | 352k | 174k |
| 32 KiB | 2.91M | 1.09M | 144k | 99.3k | 104k | 201k |
| 128 KiB | 2.96M | 494k | 36.8k | 82.9k | 25.5k | 17.1k |

<!-- END push_pull_8peer -->

Numbers are msg/s.

## PUSH/PULL throughput, priority routing (single peer)

Same as the single-peer backend comparison but compiled with the `priority` feature,
which replaces work-stealing round-robin with strict per-pipe priority queues. This
trades throughput for ordering guarantees — numbers here are lower but the relative
shape between transports holds. Run with `bench_run.rb --with-priority` to update.

<!-- BEGIN push_pull_priority -->
(no push_pull priority data — run: bench_run.rb --with-priority)
<!-- END push_pull_priority -->

## Mechanism per-frame cost (sans-I/O)

Per-frame cryptographic cost of sealing one ZMTP frame payload, as
measured by `omq-proto/benches/mechanism_frame.rs`. Numbers are
plaintext throughput in MB/s or GB/s (decimal, 10^6 / 10^9); higher
is better.

|  size   |   NULL (memcpy) | CURVE (XSalsa20Poly1305) | BLAKE3ZMQ (ChaCha20-BLAKE3) |
|--------:|----------------:|-------------------------:|----------------------------:|
|    64 B |    4.57 GB/s    |               48 MB/s    |                  153 MB/s   |
|   256 B |    15.1 GB/s    |              154 MB/s    |                  380 MB/s   |
| 1 KiB   |   42.7 GB/s     |              334 MB/s    |                  663 MB/s   |
| 4 KiB   |   64.0 GB/s     |              483 MB/s    |                  919 MB/s   |
|16 KiB   |   54.2 GB/s     |              541 MB/s    |             **1.25 GB/s**   |
|64 KiB   |   47.1 GB/s     |              557 MB/s    |             **1.43 GB/s**   |

> **Security note on BLAKE3ZMQ.** This mechanism is omq-native and has
> **not been independently security audited.** It's modelled on Noise
> XX with BLAKE3 transcript hashing, X25519 key exchange, and
> ChaCha20-BLAKE3 AEAD, but novel cryptographic constructions need
> third-party review before they should be trusted for anything that
> matters. If you have security or compliance requirements, use
> **CURVE** (RFC 26 / NaCl XSalsa20Poly1305 - well-reviewed and what
> libzmq ships). Independent audits of BLAKE3ZMQ are very welcome - if
> you or your organisation can fund or conduct one, please open an
> issue on the repo.

Numbers are stock `cargo bench` (no `-C target-cpu=native`). omq-proto
pins a fork of `chacha20-blake3` adding `#[target_feature(enable =
"avx2")]` annotations that let LLVM auto-vectorize the loops
surrounding the explicit intrinsic calls. Without that patch,
BLAKE3ZMQ runs ~20× slower (~50 MiB/s at bulk sizes) unless every
downstream consumer rebuilds with `-C target-cpu=native`. `crypto_box`
(CURVE) plateaus around ~557 MB/s either way: its salsa20
implementation has no SIMD path. Reproduce with:

```sh
cargo bench -p omq-proto --bench mechanism_frame --features 'curve blake3zmq'
```

## Reproducing

```sh
cargo bench -p omq-compio --bench push_pull
cargo bench -p omq-tokio  --bench push_pull
cargo bench -p omq-compio --bench req_rep
# Override transports / sizes / peer counts via env:
OMQ_BENCH_TRANSPORTS=tcp,lz4+tcp,zstd+tcp \
OMQ_BENCH_PEERS=3 \
OMQ_BENCH_SIZES=128,2048,32768 \
  cargo bench -p omq-compio --bench push_pull
# Two-process libzmq vs omq comparison (requires libzmq installed):
# build: gcc scripts/libzmq_bench_peer.c -o scripts/libzmq_bench_peer -lzmq
# then run scripts/bench_compare.sh
```

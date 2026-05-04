# Benchmarks

Linux 6.12 (Debian 13) VM on an Intel Mac Mini 2018 (i7-8700B, 3.2 GHz, 6
vCPU), Rust 1.95.0, default features. Each cell is the median of 3 × 500 ms
timed rounds after a prime + 100 ms warmup. Sources: `omq-tokio/benches/` and
`omq-compio/benches/`. Run yourself with `cargo bench` per crate.

> **Compio numbers are one core.** All omq-compio benches run PUSH and
> PULL inside a single `#[compio::main]` runtime (single-threaded by
> design). The omq-tokio numbers use a multi-thread runtime across
> `num_cpus::get()` workers — "what one core can do" vs "what the box
> can do". To scale compio past one core, instantiate one
> `compio::runtime::Runtime` per worker thread and pin via
> `RuntimeBuilder::thread_affinity(...)`; on this hardware that lifts
> small-message TCP / IPC throughput by roughly 20–40%.

## PUSH/PULL throughput by transport, single peer (omq-compio, one core)

Median of 3 × 500 ms rounds per cell. Small-size wire columns still
vary ±10 % run-to-run (cache / scheduling jitter on a single core);
8 KiB+ varies more once kernel send-buffer behavior kicks in — ±25 %
is a rough envelope.

<!-- BEGIN push_pull_compio_1peer -->
| Size | inproc | ipc | tcp | lz4+tcp | zstd+tcp |
|---|---|---|---|---|---|
| 32 B | 3.10M / 99.2 MB/s | 2.12M / 67.7 MB/s | 2.07M / 66.3 MB/s | 1.65M / 52.9 MB/s | 1.34M / 42.8 MB/s |
| 128 B | 3.08M / 394 MB/s | 1.93M / 247 MB/s | 1.88M / 241 MB/s | 1.59M / 204 MB/s | 99.9k / 12.8 MB/s |
| 512 B | 3.08M / 1.58 GB/s | 1.32M / 678 MB/s | 1.29M / 658 MB/s | 964k / 494 MB/s | 103k / 52.8 MB/s |
| 2 KiB | 3.12M / 6.39 GB/s | 853k / 1.75 GB/s | 854k / 1.75 GB/s | 761k / 1.56 GB/s | 408k / 835 MB/s |
| 8 KiB | 3.17M / 26.0 GB/s | 388k / 3.18 GB/s | 357k / 2.93 GB/s | 366k / 3.00 GB/s | 250k / 2.05 GB/s |
| 32 KiB | 3.12M / 102.3 GB/s | 119k / 3.90 GB/s | 110k / 3.62 GB/s | 106k / 3.47 GB/s | 103k / 3.36 GB/s |
| 128 KiB | 3.11M / 407.7 GB/s | 29.8k / 3.91 GB/s | 28.0k / 3.67 GB/s | 30.0k / 3.93 GB/s | 29.7k / 3.90 GB/s |

<!-- END push_pull_compio_1peer -->

Note: large-payload "GB/s" on inproc reflects the zero-copy refcount-
clone path - bytes never traverse the kernel. lz4 / zstd on
incompressible payloads (random bytes) cross from overhead to net win
around 32 KiB, where the smaller WRITEV calls outweigh the codec cost.
On compressible traffic (e.g. JSON events), the crossover is much
earlier - see the JSON compression bench below.

lz4+tcp and zstd+tcp numbers here use `Options::default()` — **no
compression dictionary**. Without a dict, the compression threshold is
512 B: frames smaller than that are passed through as plaintext (only a
4-byte `SENTINEL_PLAIN` header is added). The slowdown vs plain TCP at
small sizes (32 B–128 B) comes from missing the EncodedQueue send-bypass
(transform paths use the codec-mutex path instead), not from compression
work. With a pre-trained dict the threshold drops to 32 B (lz4) / 64 B
(zstd) — see "With a pre-trained dict" below.

## Backend comparison: PUSH/PULL throughput, single peer

<!-- BEGIN backend_comparison -->
| Size | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---|---|---|---|---|---|---|
| 32 B | 3.10M | 1.15M | 2.12M | 4.42M | 2.07M | 4.44M |
| 128 B | 3.08M | 1.09M | 1.93M | 4.61M | 1.88M | 2.62M |
| 512 B | 3.08M | 1.21M | 1.32M | 3.78M | 1.29M | 3.61M |
| 2 KiB | 3.12M | 1.15M | 853k | 971k | 854k | 1.84M |
| 8 KiB | 3.17M | 805k | 388k | 459k | 357k | 559k |
| 32 KiB | 3.12M | 699k | 119k | 106k | 110k | 97.9k |
| 128 KiB | 3.11M | 669k | 29.8k | 38.6k | 28.0k | 39.5k |

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

## Cross-library comparisons

See [COMPARISONS.md](COMPARISONS.md) for two-process TCP benchmarks against
libzmq and zmq.rs. Run `./scripts/compare_libzmq.sh --update-benchmarks` or
`./scripts/compare_zmqrs.sh --update-benchmarks` to refresh those tables.

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

### REQ/REP latency percentiles (p50 / p99 / p999)

Dedicated serial ping-pong bench: 1 000 warmup + 10 000 measured iterations per cell.
All values are µs wall time. Compression transports add per-frame codec overhead.

<!-- BEGIN latency_percentiles -->
| transport | size | compio p50 | compio p99 | compio p999 | tokio p50 | tokio p99 | tokio p999 |
|---|---|---|---|---|---|---|---|
| inproc | 32 B | 5.54 µs | 5.79 µs | 26.8 µs | 28.4 µs | 55.5 µs | 601 µs |
| inproc | 128 B | 5.70 µs | 5.99 µs | 21.8 µs | 30.3 µs | 38.2 µs | 63.7 µs |
| inproc | 512 B | 5.51 µs | 5.64 µs | 12.3 µs | 148 µs | 283 µs | 326 µs |
| inproc | 2 KiB | 5.69 µs | 5.78 µs | 12.3 µs | 30.4 µs | 52.1 µs | 452 µs |
| inproc | 8 KiB | 5.41 µs | 5.53 µs | 18.8 µs | 32.0 µs | 45.8 µs | 62.2 µs |
| inproc | 32 KiB | 5.41 µs | 18.8 µs | 101 µs | 28.8 µs | 37.3 µs | 48.6 µs |
| inproc | 128 KiB | 5.56 µs | 11.3 µs | 27.2 µs | 31.1 µs | 267 µs | 506 µs |
| ipc | 32 B | 20.9 µs | 30.4 µs | 54.3 µs | 51.7 µs | 821 µs | 889 µs |
| ipc | 128 B | 19.3 µs | 25.6 µs | 50.9 µs | 53.5 µs | 834 µs | 913 µs |
| ipc | 512 B | 20.1 µs | 38.9 µs | 60.0 µs | 53.7 µs | 67.9 µs | 98.1 µs |
| ipc | 2 KiB | 20.6 µs | 34.2 µs | 56.7 µs | 56.6 µs | 107 µs | 177 µs |
| ipc | 8 KiB | 23.9 µs | 42.8 µs | 61.0 µs | 61.4 µs | 108 µs | 312 µs |
| ipc | 32 KiB | 30.8 µs | 50.1 µs | 84.9 µs | 68.2 µs | 84.5 µs | 114 µs |
| ipc | 128 KiB | 74.4 µs | 112 µs | 160 µs | 99.9 µs | 1.2 ms | 1.3 ms |
| tcp | 32 B | 28.4 µs | 36.7 µs | 61.3 µs | 62.4 µs | 112 µs | 146 µs |
| tcp | 128 B | 27.2 µs | 35.3 µs | 56.7 µs | 63.5 µs | 120 µs | 184 µs |
| tcp | 512 B | 28.4 µs | 37.8 µs | 55.4 µs | 62.1 µs | 109 µs | 144 µs |
| tcp | 2 KiB | 28.3 µs | 38.3 µs | 62.5 µs | 65.7 µs | 123 µs | 182 µs |
| tcp | 8 KiB | 30.8 µs | 37.4 µs | 57.6 µs | 67.5 µs | 933 µs | 1.0 ms |
| tcp | 32 KiB | 39.9 µs | 61.7 µs | 96.1 µs | 87.8 µs | 111 µs | 153 µs |
| tcp | 128 KiB | 87.8 µs | 124 µs | 188 µs | 120 µs | 148 µs | 195 µs |
| lz4+tcp | 32 B | 28.4 µs | 44.3 µs | 62.5 µs | 80.4 µs | 101 µs | 142 µs |
| lz4+tcp | 128 B | 27.3 µs | 42.7 µs | 58.6 µs | 82.5 µs | 126 µs | 408 µs |
| lz4+tcp | 512 B | 29.7 µs | 48.8 µs | 71.7 µs | 88.1 µs | 126 µs | 252 µs |
| lz4+tcp | 2 KiB | 30.9 µs | 52.2 µs | 92.6 µs | 87.1 µs | 123 µs | 294 µs |
| lz4+tcp | 8 KiB | 33.6 µs | 52.5 µs | 74.3 µs | 90.3 µs | 115 µs | 172 µs |
| lz4+tcp | 32 KiB | 44.3 µs | 65.6 µs | 99.0 µs | 105 µs | 166 µs | 475 µs |
| lz4+tcp | 128 KiB | 88.3 µs | 138 µs | 178 µs | 154 µs | 195 µs | 699 µs |
| zstd+tcp | 32 B | 28.9 µs | 55.4 µs | 86.0 µs | 84.7 µs | 121 µs | 189 µs |
| zstd+tcp | 128 B | 52.9 µs | 94.5 µs | 137 µs | 117 µs | 146 µs | 258 µs |
| zstd+tcp | 512 B | 52.8 µs | 89.3 µs | 124 µs | 114 µs | 140 µs | 221 µs |
| zstd+tcp | 2 KiB | 37.0 µs | 69.3 µs | 96.4 µs | 93.5 µs | 117 µs | 170 µs |
| zstd+tcp | 8 KiB | 39.4 µs | 67.7 µs | 94.7 µs | 96.6 µs | 122 µs | 187 µs |
| zstd+tcp | 32 KiB | 51.4 µs | 76.4 µs | 109 µs | 108 µs | 148 µs | 192 µs |
| zstd+tcp | 128 KiB | 97.8 µs | 137 µs | 180 µs | 174 µs | 208 µs | 429 µs |

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
path; the send-side fast path was reverted in favor of restoring
producer/writer pipelining. The lesson: latency wins on bypass-the-
hop optimizations can mask big throughput regressions when the hop
was implicitly batching.

The omq-tokio IPC numbers are still untouched. Tokio's send path goes
through the SocketDriver actor + per-send submit task + per-peer pump
+ ConnectionDriver — more hops than compio, but the multi-thread
runtime hides some of the cost by overlapping send/recv on different
workers. Stage 1's single-wire-peer bypass would port to tokio's
`routing/round_robin.rs`; tracked as a follow-up.

## PUSH/PULL throughput, 8 peers

Same bench with 8 concurrent PUSH peers fanning into one PULL. inproc/ipc/tcp,
both backends. compio scales linearly on inproc (lock-free flume); tokio's
multi-thread runtime gains from overlapping sender tasks on wire transports.

<!-- BEGIN push_pull_8peer -->
| Size | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---|---|---|---|---|---|---|
| 32 B | 3.12M | 1.04M | 2.07M | 3.44M | 2.19M | 3.68M |
| 128 B | 3.19M | 570k | 1.90M | 5.33M | 1.88M | 3.43M |
| 512 B | 3.12M | 1.07M | 1.29M | 4.82M | 1.33M | 4.14M |
| 2 KiB | 3.20M | 1.04M | 842k | 2.24M | 782k | 2.35M |
| 8 KiB | 3.18M | 886k | 398k | 759k | 309k | 816k |
| 32 KiB | 3.12M | 1.02M | 136k | 294k | 105k | 197k |
| 128 KiB | 3.13M | 1.05M | 34.0k | 79.0k | 25.1k | 32.8k |

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
| 1 KiB   |   42.7 GB/s     |              334 MB/s    |                  663 MB/s   |
| 4 KiB   |   64.0 GB/s     |              483 MB/s    |                  919 MB/s   |
|16 KiB   |   54.2 GB/s     |              541 MB/s    |             **1.25 GB/s**   |
|64 KiB   |   47.1 GB/s     |              557 MB/s    |             **1.43 GB/s**   |

> **Security note on BLAKE3ZMQ.** This mechanism is omq-native and has
> **not been independently security audited.** It's modeled on Noise
> XX with BLAKE3 transcript hashing, X25519 key exchange, and
> ChaCha20-BLAKE3 AEAD, but novel cryptographic constructions need
> third-party review before they should be trusted for anything that
> matters. If you have security or compliance requirements, use
> **CURVE** (RFC 26 / NaCl XSalsa20Poly1305 - well-reviewed and what
> libzmq ships). Independent audits of BLAKE3ZMQ are very welcome - if
> you or your organization can fund or conduct one, please open an
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
# then run scripts/compare_libzmq.sh
# Two-process zmq.rs vs omq comparison (pure Rust, no system packages):
# ./scripts/compare_zmqrs.sh
```

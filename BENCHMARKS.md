# Benchmarks

Linux 6.12 (Debian 13) VM on an Intel Mac Mini 2018 (i7-8700B, 3.2 GHz
base, turbo disabled, governor=performance, 6 vCPU), Rust 1.95.0,
default features. Each cell is the **min wall time** across 3 × 500 ms
timed rounds after a prime + 100 ms warmup — peak throughput, closest
to the hardware ceiling and least perturbed by scheduler/IRQ jitter.
Sources: `omq-tokio/benches/` and `omq-compio/benches/`.

> **Compio bench topology.** `inproc`: single runtime, single thread
> (sender + receiver cooperatively scheduled — IO-bound workloads).
> `inproc-mt`: multi-runtime inproc — PULL on its own thread/runtime,
> PUSHes on another (CPU-bound workloads). Wire transports
> (TCP/IPC/lz4+tcp/zstd+tcp): same multi-runtime shape as inproc-mt.
> omq-tokio uses a multi-thread runtime across all available cores
> throughout.

## PUSH/PULL throughput by transport, single peer (omq-compio, two cores)

<!-- BEGIN push_pull_compio_1peer -->
| Size | inproc | inproc-mt | ipc | tcp | lz4+tcp | zstd+tcp |
|---|---|---|---|---|---|---|
| 32 B | 3.50M / 112 MB/s | 3.03M / 97.1 MB/s | 6.13M / 196 MB/s | 6.58M / 210 MB/s | 4.55M / 146 MB/s | 2.67M / 85.4 MB/s |
| 128 B | 3.48M / 446 MB/s | 2.62M / 336 MB/s | 4.59M / 588 MB/s | 5.01M / 641 MB/s | 3.84M / 491 MB/s | 107k / 13.7 MB/s |
| 512 B | 3.64M / 1.86 GB/s | 2.57M / 1.32 GB/s | 3.41M / 1.75 GB/s | 3.46M / 1.77 GB/s | 1.61M / 826 MB/s | 111k / 56.8 MB/s |
| 2 KiB | 3.43M / 7.02 GB/s | 2.55M / 5.22 GB/s | 1.94M / 3.98 GB/s | 1.58M / 3.24 GB/s | 1.55M / 3.17 GB/s | 568k / 1.16 GB/s |
| 8 KiB | 3.48M / 28.5 GB/s | 2.75M / 22.6 GB/s | 675k / 5.53 GB/s | 589k / 4.83 GB/s | 581k / 4.76 GB/s | 381k / 3.12 GB/s |
| 32 KiB | 3.79M / 124.2 GB/s | 2.59M / 84.8 GB/s | 176k / 5.77 GB/s | 177k / 5.81 GB/s | 144k / 4.73 GB/s | 91.0k / 2.98 GB/s |
| 128 KiB | 3.78M / 494.9 GB/s | 2.78M / 364.2 GB/s | 48.9k / 6.41 GB/s | 64.1k / 8.40 GB/s | 11.7k / 1.53 GB/s | 22.5k / 2.95 GB/s |

<!-- END push_pull_compio_1peer -->

Inproc "GB/s" at large payloads reflects zero-copy Arc-clone — no kernel
traversal.

lz4+tcp and zstd+tcp use `Options::default()` — **no compression dictionary**.
Without a dict, the threshold is 512 B (smaller frames pass as plaintext with a
4-byte `SENTINEL_PLAIN` header).

## Backend comparison: PUSH/PULL throughput, single peer

<!-- BEGIN backend_comparison -->
| Size | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---|---|---|---|---|---|---|
| 32 B | 3.50M | 1.23M | 6.13M | 4.87M | 6.58M | 4.65M |
| 128 B | 3.48M | 1.62M | 4.59M | 2.32M | 5.01M | 4.21M |
| 512 B | 3.64M | 1.31M | 3.41M | 3.21M | 3.46M | 3.82M |
| 2 KiB | 3.43M | 1.57M | 1.94M | 1.12M | 1.58M | 1.78M |
| 8 KiB | 3.48M | 1.79M | 675k | 428k | 589k | 579k |
| 32 KiB | 3.79M | 445k | 176k | 123k | 177k | 155k |
| 128 KiB | 3.78M | 909k | 48.9k | 44.6k | 64.1k | 32.3k |

<!-- END backend_comparison -->

Numbers are msg/s. **Compio = one core; tokio = whole box** (see caveat above).

## Cross-library comparisons

See [COMPARISONS.md](COMPARISONS.md) for two-process TCP benchmarks against
libzmq and zmq.rs. Run `./scripts/compare_libzmq.sh --update-benchmarks` or
`./scripts/compare_zmqrs.sh --update-benchmarks` to refresh those tables.

## Compression on realistic JSON payloads (omq-compio, 1 peer)

JSON event-log payload (timestamps, trace IDs, repeated field names). Cells
show `msgs/s · wire MB/s · virtual MB/s`; for plain `tcp`, wire == virtual.

Compression ratios:

| size    | lz4     | zstd     |
|---------|---------|----------|
| 128 B   | 0.97×*  | 0.97×*   |
| 512 B   | 1.57×   | 1.62×    |
| 1 KiB   | 2.60×   | 2.84×    |
| 2 KiB   | 3.76×   | 4.47×    |
| 4 KiB   | 4.92×   | 7.41×    |
| 16 KiB  | 6.47×   | **12.87×** |

\* Below 512 B both codecs fall back to plaintext (0.97–0.98× = 4-byte
`SENTINEL_PLAIN` tax). A pre-trained dict moves the cutoff further down (see below).

Loopback throughput (msgs/s · wire MB/s · virtual MB/s):

| size  | tcp                        | lz4+tcp                           | zstd+tcp                          |
|-------|----------------------------|-----------------------------------|-----------------------------------|
| 128 B | 1.67M / 214 MB/s           | 1.42M / 188 MB/s / 182 MB/s       | 127k / 16.8 MB/s / 16.3 MB/s      |
| 512 B | 1.21M / 619 MB/s           | 513k / 167 MB/s / 263 MB/s        | 96.2k / 30.4 MB/s / 49.3 MB/s     |
| 1 KiB | 979k / 1.00 GB/s           | 458k / 180 MB/s / 469 MB/s        | 282k / 102 MB/s / 289 MB/s        |
| 2 KiB | 797k / 1.63 GB/s           | 367k / 200 MB/s / 751 MB/s        | 203k / 92.9 MB/s / 416 MB/s       |
| 4 KiB | 558k / 2.29 GB/s           | 267k / 222 MB/s / 1.09 GB/s       | 115k / 63.4 MB/s / 469 MB/s       |
| 16 KiB| 206k / 3.38 GB/s           | 103k / 262 MB/s / 1.69 GB/s       | 42.2k / 53.7 MB/s / 691 MB/s      |

Loopback: plain TCP wins msg/s (no bandwidth scarcity). **Wire MB/s column**
predicts WAN behavior: at 16 KiB, lz4+tcp ships ~262 MB/s wire / ~1.69 GB/s
virtual. On a 1 Gbps WAN (~125 MB/s ceiling): plain tcp ~125 MB/s, lz4+tcp ~808
MB/s, zstd+tcp ~1.61 GB/s virtual throughput.

### With a pre-trained dict (small messages)

Dict primes the codec with message-family byte sequences so even 128 B records
compress well. Pass via `Options::compression_dict(Bytes)`; shipped to peer on
first connection, reused every frame.

Ratios on same JSON template (zstd: 1.6 KiB dict from 200 samples; lz4: 4 KiB buffer):

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

Loopback: same caveat as above. Wire MB/s = link load; virt MB/s = application
throughput. `zstd+tcp` auto-trains by default, reaching similar ratios after
~1000 messages or 100 KiB.

## REQ/REP round-trip latency (single peer)

### REQ/REP latency percentiles (p50 / p99 / p999)

Dedicated serial ping-pong bench: 1 000 warmup + 10 000 measured iterations per cell.
All values are µs wall time. Compression transports add per-frame codec overhead.

<!-- BEGIN latency_percentiles -->
| transport | size | compio p50 | compio p99 | compio p999 | tokio p50 | tokio p99 | tokio p999 |
|---|---|---|---|---|---|---|---|
| inproc | 32 B | 5.57 µs | 5.95 µs | 28.3 µs | 27.4 µs | 63.8 µs | 83.3 µs |
| inproc | 128 B | 5.45 µs | 18.3 µs | 29.6 µs | 28.0 µs | 41.4 µs | 81.9 µs |
| inproc | 512 B | 5.52 µs | 5.69 µs | 13.1 µs | 29.1 µs | 40.2 µs | 57.3 µs |
| inproc | 2 KiB | 5.59 µs | 5.77 µs | 28.4 µs | 30.0 µs | 40.0 µs | 72.6 µs |
| inproc | 8 KiB | 5.53 µs | 5.63 µs | 24.1 µs | 30.7 µs | 41.1 µs | 67.3 µs |
| inproc | 32 KiB | 5.55 µs | 5.95 µs | 36.2 µs | 26.4 µs | 41.1 µs | 78.7 µs |
| inproc | 128 KiB | 5.60 µs | 6.02 µs | 25.3 µs | 23.8 µs | 34.4 µs | 41.0 µs |
| ipc | 32 B | 17.4 µs | 23.8 µs | 45.1 µs | 48.6 µs | 66.6 µs | 89.0 µs |
| ipc | 128 B | 17.1 µs | 26.2 µs | 44.2 µs | 51.1 µs | 72.9 µs | 103 µs |
| ipc | 512 B | 18.0 µs | 36.7 µs | 46.4 µs | 53.5 µs | 823 µs | 919 µs |
| ipc | 2 KiB | 18.2 µs | 24.0 µs | 43.6 µs | 57.0 µs | 105 µs | 236 µs |
| ipc | 8 KiB | 22.2 µs | 31.5 µs | 51.4 µs | 61.8 µs | 133 µs | 465 µs |
| ipc | 32 KiB | 30.7 µs | 42.7 µs | 63.3 µs | 77.0 µs | 1.0 ms | 1.1 ms |
| ipc | 128 KiB | 67.0 µs | 86.9 µs | 116 µs | 97.1 µs | 137 µs | 185 µs |
| tcp | 32 B | 24.4 µs | 37.9 µs | 50.3 µs | 59.3 µs | 114 µs | 179 µs |
| tcp | 128 B | 23.6 µs | 37.1 µs | 61.3 µs | 61.2 µs | 79.6 µs | 165 µs |
| tcp | 512 B | 25.1 µs | 39.2 µs | 64.3 µs | 60.3 µs | 109 µs | 185 µs |
| tcp | 2 KiB | 26.0 µs | 47.9 µs | 78.5 µs | 64.2 µs | 95.2 µs | 120 µs |
| tcp | 8 KiB | 28.2 µs | 46.6 µs | 94.6 µs | 68.5 µs | 86.7 µs | 117 µs |
| tcp | 32 KiB | 38.1 µs | 46.7 µs | 74.2 µs | 79.3 µs | 1.0 ms | 1.1 ms |
| tcp | 128 KiB | 80.4 µs | 110 µs | 162 µs | 118 µs | 166 µs | 196 µs |
| lz4+tcp | 32 B | 25.9 µs | 39.3 µs | 59.9 µs | 77.8 µs | 100 µs | 129 µs |
| lz4+tcp | 128 B | 25.1 µs | 39.1 µs | 68.6 µs | 82.2 µs | 1.0 ms | 1.1 ms |
| lz4+tcp | 512 B | 28.3 µs | 42.1 µs | 67.3 µs | 82.3 µs | 1.0 ms | 1.1 ms |
| lz4+tcp | 2 KiB | 27.8 µs | 42.2 µs | 69.7 µs | 85.8 µs | 119 µs | 178 µs |
| lz4+tcp | 8 KiB | 30.9 µs | 43.7 µs | 61.0 µs | 88.7 µs | 122 µs | 154 µs |
| lz4+tcp | 32 KiB | 43.7 µs | 57.9 µs | 96.5 µs | 100 µs | 1.1 ms | 1.2 ms |
| lz4+tcp | 128 KiB | 87.5 µs | 118 µs | 157 µs | 151 µs | 1.4 ms | 1.5 ms |
| zstd+tcp | 32 B | 26.0 µs | 39.4 µs | 60.3 µs | 82.9 µs | 118 µs | 143 µs |
| zstd+tcp | 128 B | 50.0 µs | 64.3 µs | 109 µs | 108 µs | 146 µs | 288 µs |
| zstd+tcp | 512 B | 50.3 µs | 69.7 µs | 97.1 µs | 112 µs | 971 µs | 1.2 ms |
| zstd+tcp | 2 KiB | 34.2 µs | 63.2 µs | 90.4 µs | 91.6 µs | 117 µs | 138 µs |
| zstd+tcp | 8 KiB | 37.2 µs | 59.6 µs | 90.5 µs | 96.8 µs | 154 µs | 1.1 ms |
| zstd+tcp | 32 KiB | 49.7 µs | 69.2 µs | 93.7 µs | 112 µs | 148 µs | 222 µs |
| zstd+tcp | 128 KiB | 96.5 µs | 117 µs | 152 µs | 164 µs | 1.5 ms | 1.7 ms |

<!-- END latency_percentiles -->

## PUSH/PULL throughput, 8 peers

8 PUSH peers → 1 PULL, all transports, both backends. Numbers are msg/s.

<!-- BEGIN push_pull_8peer -->
| Size | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---|---|---|---|---|---|---|
| 32 B | 3.62M | 2.73M | 3.76M | 4.60M | 3.62M | 6.22M |
| 128 B | 3.48M | 2.83M | 4.21M | 3.89M | 4.28M | 4.38M |
| 512 B | 3.76M | 2.66M | 3.08M | 2.97M | 2.33M | 4.19M |
| 2 KiB | 3.46M | 3.05M | 1.38M | 2.15M | 1.29M | 2.09M |
| 8 KiB | 3.44M | 2.53M | 441k | 611k | 395k | 651k |
| 32 KiB | 3.78M | 2.81M | 155k | 178k | 113k | 175k |
| 128 KiB | 3.78M | 2.27M | 40.4k | 72.5k | 29.0k | 53.7k |

<!-- END push_pull_8peer -->

## PUSH/PULL throughput, priority routing (single peer)

Same as backend comparison but with `priority` feature (strict per-pipe
queues). Lower throughput; transport-relative shape holds. Run with
`bench_run.rb --with-priority` to update.

<!-- BEGIN push_pull_priority -->
(no push_pull priority data — run: bench_run.rb --with-priority)
<!-- END push_pull_priority -->

## Mechanism per-frame cost (sans-I/O)

Per-frame seal cost from `omq-proto/benches/mechanism_frame.rs`. Plaintext
throughput (MB/s or GB/s, decimal); higher is better.

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

Stock `cargo bench` (no `-C target-cpu=native`). omq-proto pins a
`chacha20-blake3` fork with `#[target_feature(enable = "avx2")]` annotations;
without them BLAKE3ZMQ drops to ~50 MiB/s at bulk sizes. CURVE plateaus at ~557
MB/s regardless (salsa20 has no SIMD path). Reproduce:

```sh
cargo bench -p omq-proto --bench mechanism_frame --features 'curve blake3zmq'
```

## Reproducing

```sh
cargo bench -p omq-compio --bench push_pull
cargo bench -p omq-tokio  --bench push_pull
cargo bench -p omq-compio --bench req_rep

# Convenience:
./scripts/bench_run.rb [--all-features] [--all-sizes]    # adds results to JSONL
./scripts/bench_report.rb [--update-benchmarks]          # compares results

# Override transports / sizes / peer counts via env:
OMQ_BENCH_TRANSPORTS=tcp,lz4+tcp,zstd+tcp OMQ_BENCH_PEERS=3 OMQ_BENCH_SIZES=128,2048,32768 cargo bench -p omq-compio --bench push_pull

# Two-process libzmq vs omq comparison (requires libzmq installed):
# build: gcc scripts/libzmq_bench_peer.c -o scripts/libzmq_bench_peer -lzmq
# then run scripts/compare_libzmq.sh [--update-benchmarks]

# Two-process zmq.rs vs omq comparison (pure Rust, no system packages):
# ./scripts/compare_zmqrs.sh [--update-benchmarks]
```

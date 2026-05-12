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
| 32 B | 3.50M / 112 MB/s | 3.00M / 95.9 MB/s | 6.85M / 219 MB/s | 6.71M / 215 MB/s | 4.69M / 150 MB/s | 2.89M / 92.5 MB/s |
| 128 B | 3.57M / 456 MB/s | 2.81M / 359 MB/s | 4.87M / 623 MB/s | 5.18M / 663 MB/s | 3.87M / 496 MB/s | 106k / 13.6 MB/s |
| 512 B | 3.58M / 1.83 GB/s | 2.86M / 1.46 GB/s | 3.43M / 1.76 GB/s | 3.44M / 1.76 GB/s | 1.82M / 933 MB/s | 111k / 57.0 MB/s |
| 2 KiB | 3.59M / 7.35 GB/s | 2.89M / 5.91 GB/s | 1.99M / 4.08 GB/s | 1.67M / 3.41 GB/s | 1.43M / 2.94 GB/s | 579k / 1.19 GB/s |
| 8 KiB | 3.56M / 29.2 GB/s | 2.81M / 23.0 GB/s | 624k / 5.11 GB/s | 581k / 4.76 GB/s | 543k / 4.44 GB/s | 313k / 2.57 GB/s |
| 32 KiB | 3.58M / 117.5 GB/s | 2.84M / 93.1 GB/s | 156k / 5.10 GB/s | 148k / 4.84 GB/s | 147k / 4.82 GB/s | 95.1k / 3.11 GB/s |
| 128 KiB | 3.58M / 469.8 GB/s | 3.00M / 393.7 GB/s | 39.4k / 5.17 GB/s | 39.4k / 5.17 GB/s | 32.7k / 4.29 GB/s | 18.7k / 2.45 GB/s |

<!-- END push_pull_compio_1peer -->

Inproc "GB/s" at large payloads reflects zero-copy Arc-clone — no kernel
traversal.

## Backend comparison: PUSH/PULL throughput, single peer

<!-- BEGIN backend_comparison -->
| Size | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---|---|---|---|---|---|---|
| 32 B | 3.50M | 2.06M | 6.85M | 2.58M | 6.71M | 3.86M |
| 128 B | 3.57M | 1.14M | 4.87M | 3.85M | 5.18M | 3.92M |
| 512 B | 3.58M | 2.19M | 3.43M | 4.41M | 3.44M | 3.55M |
| 2 KiB | 3.59M | 1.38M | 1.99M | 1.08M | 1.67M | 1.53M |
| 8 KiB | 3.56M | 2.09M | 624k | 217k | 581k | 417k |
| 32 KiB | 3.58M | 1.13M | 156k | 114k | 148k | 66.7k |
| 128 KiB | 3.58M | 987k | 39.4k | 35.6k | 39.4k | 28.9k |

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

Loopback throughput with the same dict (msgs/s · wire MB/s · virt MB/s):

| size  | lz4+tcp                          | zstd+tcp                       |
|-------|----------------------------------|--------------------------------|
| 128 B | 254k / 5.60 MB/s / 32.5 MB/s    | 138k / 3.50 MB/s / 17.7 MB/s  |
| 512 B | 261k / 6.00 MB/s / 134 MB/s     | 136k / 3.50 MB/s / 69.9 MB/s  |
| 1 KiB | 331k / 30.1 MB/s / 339 MB/s     | 134k / 3.90 MB/s / 138 MB/s   |
| 2 KiB | 298k / 71.9 MB/s / 611 MB/s     | 118k / 14.2 MB/s / 241 MB/s   |

## REQ/REP round-trip latency (single peer)

### REQ/REP latency percentiles (p50 / p99 / p999)

Dedicated serial ping-pong bench: 1 000 warmup + 10 000 measured iterations per cell.
All values are µs wall time. Compression transports add per-frame codec overhead.

<!-- BEGIN latency_percentiles -->
| transport | size | compio p50 | compio p99 | compio p999 | tokio p50 | tokio p99 | tokio p999 |
|---|---|---|---|---|---|---|---|
| inproc | 32 B | 5.50 µs | 6.06 µs | 27.5 µs | 26.9 µs | 63.7 µs | 98.7 µs |
| inproc | 128 B | 5.57 µs | 5.78 µs | 21.7 µs | 26.6 µs | 38.7 µs | 66.2 µs |
| inproc | 512 B | 5.70 µs | 5.91 µs | 21.7 µs | 26.3 µs | 50.7 µs | 85.0 µs |
| inproc | 2 KiB | 5.73 µs | 5.81 µs | 12.2 µs | 28.2 µs | 70.6 µs | 92.9 µs |
| inproc | 8 KiB | 5.76 µs | 5.92 µs | 11.8 µs | 29.5 µs | 66.2 µs | 84.8 µs |
| inproc | 32 KiB | 5.74 µs | 5.91 µs | 18.6 µs | 27.9 µs | 147 µs | 4.1 ms |
| inproc | 128 KiB | 5.76 µs | 5.95 µs | 18.8 µs | 26.4 µs | 40.9 µs | 77.0 µs |
| ipc | 32 B | 17.4 µs | 24.8 µs | 44.3 µs | 49.3 µs | 69.0 µs | 102 µs |
| ipc | 128 B | 17.3 µs | 21.0 µs | 41.8 µs | 49.5 µs | 68.4 µs | 103 µs |
| ipc | 512 B | 17.6 µs | 21.8 µs | 38.9 µs | 52.4 µs | 88.0 µs | 121 µs |
| ipc | 2 KiB | 18.9 µs | 32.1 µs | 48.1 µs | 58.1 µs | 105 µs | 123 µs |
| ipc | 8 KiB | 22.4 µs | 27.7 µs | 52.7 µs | 67.4 µs | 174 µs | 355 µs |
| ipc | 32 KiB | 32.0 µs | 40.4 µs | 61.7 µs | 80.0 µs | 135 µs | 397 µs |
| ipc | 128 KiB | 192 µs | 238 µs | 281 µs | 108 µs | 1.2 ms | 1.3 ms |
| tcp | 32 B | 25.0 µs | 34.1 µs | 63.1 µs | 64.1 µs | 118 µs | 203 µs |
| tcp | 128 B | 24.5 µs | 45.2 µs | 77.2 µs | 64.7 µs | 119 µs | 355 µs |
| tcp | 512 B | 25.1 µs | 47.5 µs | 73.8 µs | 65.4 µs | 86.0 µs | 116 µs |
| tcp | 2 KiB | 26.9 µs | 48.3 µs | 79.8 µs | 66.2 µs | 85.2 µs | 123 µs |
| tcp | 8 KiB | 29.6 µs | 49.8 µs | 83.1 µs | 69.2 µs | 966 µs | 1.0 ms |
| tcp | 32 KiB | 42.2 µs | 75.1 µs | 108 µs | 81.6 µs | 178 µs | 210 µs |
| tcp | 128 KiB | 204 µs | 266 µs | 332 µs | 131 µs | 185 µs | 505 µs |
| lz4+tcp | 32 B | 25.3 µs | 33.2 µs | 63.6 µs | 81.9 µs | 116 µs | 148 µs |
| lz4+tcp | 128 B | 25.1 µs | 31.0 µs | 50.3 µs | 82.1 µs | 1.1 ms | 1.1 ms |
| lz4+tcp | 512 B | 27.6 µs | 40.6 µs | 67.6 µs | 84.2 µs | 103 µs | 126 µs |
| lz4+tcp | 2 KiB | 28.4 µs | 34.6 µs | 55.7 µs | 83.7 µs | 1.1 ms | 1.1 ms |
| lz4+tcp | 8 KiB | 31.1 µs | 38.8 µs | 61.1 µs | 84.6 µs | 105 µs | 127 µs |
| lz4+tcp | 32 KiB | 42.2 µs | 57.7 µs | 85.9 µs | 98.2 µs | 1.1 ms | 1.2 ms |
| lz4+tcp | 128 KiB | 83.8 µs | 99.6 µs | 137 µs | 149 µs | 1.4 ms | 1.5 ms |
| zstd+tcp | 32 B | 27.1 µs | 35.5 µs | 60.6 µs | 81.2 µs | 1.0 ms | 1.1 ms |
| zstd+tcp | 128 B | 51.4 µs | 65.8 µs | 106 µs | 108 µs | 141 µs | 166 µs |
| zstd+tcp | 512 B | 51.2 µs | 69.5 µs | 110 µs | 107 µs | 145 µs | 229 µs |
| zstd+tcp | 2 KiB | 34.6 µs | 54.1 µs | 86.4 µs | 90.4 µs | 1.1 ms | 1.2 ms |
| zstd+tcp | 8 KiB | 38.0 µs | 51.5 µs | 90.2 µs | 94.0 µs | 123 µs | 149 µs |
| zstd+tcp | 32 KiB | 50.9 µs | 65.4 µs | 109 µs | 108 µs | 135 µs | 183 µs |
| zstd+tcp | 128 KiB | 99.2 µs | 148 µs | 172 µs | 163 µs | 1.5 ms | 1.7 ms |

<!-- END latency_percentiles -->

## PUSH/PULL throughput, 8 peers

8 PUSH peers → 1 PULL, all transports, both backends. Numbers are msg/s.

<!-- BEGIN push_pull_8peer -->
| Size | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---|---|---|---|---|---|---|
| 32 B | 3.47M | 603k | 3.68M | 2.52M | 3.55M | 1.91M |
| 128 B | 3.57M | 3.04M | 4.10M | 1.54M | 4.02M | 748k |
| 512 B | 3.57M | 2.98M | 3.07M | 3.05M | 2.36M | 3.22M |
| 2 KiB | 3.57M | 2.02M | 1.36M | 2.11M | 1.24M | 2.23M |
| 8 KiB | 3.58M | 2.38M | 434k | 588k | 380k | 729k |
| 32 KiB | 3.59M | 2.76M | 159k | 119k | 115k | 154k |
| 128 KiB | 3.55M | 2.04M | 38.6k | 14.0k | 28.3k | 48.2k |

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

> **BLAKE3ZMQ is not independently audited.** Use **CURVE** (RFC 26) for production.

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

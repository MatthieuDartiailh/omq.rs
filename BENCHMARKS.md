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
| 32 B | 3.72M / 119 MB/s | 2.99M / 95.8 MB/s | 6.89M / 221 MB/s | 6.99M / 224 MB/s | 4.58M / 146 MB/s | 2.92M / 93.5 MB/s |
| 128 B | 3.72M / 476 MB/s | 2.93M / 375 MB/s | 4.74M / 607 MB/s | 5.29M / 678 MB/s | 4.02M / 514 MB/s | 119k / 15.3 MB/s |
| 512 B | 3.63M / 1.86 GB/s | 2.68M / 1.37 GB/s | 3.32M / 1.70 GB/s | 3.44M / 1.76 GB/s | 1.72M / 879 MB/s | 121k / 61.9 MB/s |
| 2 KiB | 3.63M / 7.44 GB/s | 2.86M / 5.86 GB/s | 2.03M / 4.16 GB/s | 1.77M / 3.62 GB/s | 1.46M / 2.98 GB/s | 610k / 1.25 GB/s |
| 8 KiB | 3.64M / 29.8 GB/s | 2.81M / 23.1 GB/s | 767k / 6.29 GB/s | 628k / 5.14 GB/s | 600k / 4.92 GB/s | 431k / 3.53 GB/s |
| 32 KiB | 3.65M / 119.7 GB/s | 2.78M / 91.2 GB/s | 177k / 5.80 GB/s | 41.1k / 1.35 GB/s | 168k / 5.50 GB/s | 89.3k / 2.93 GB/s |
| 128 KiB | 3.65M / 478.1 GB/s | 2.92M / 382.2 GB/s | 63.2k / 8.28 GB/s | 12.2k / 1.59 GB/s | 42.2k / 5.53 GB/s | 22.8k / 2.99 GB/s |

<!-- END push_pull_compio_1peer -->

Inproc "GB/s" at large payloads reflects zero-copy Arc-clone — no kernel
traversal.

## Backend comparison: PUSH/PULL throughput, single peer

<!-- BEGIN backend_comparison -->
| Size | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---|---|---|---|---|---|---|
| 32 B | 3.72M | 1.37M | 6.89M | 4.17M | 6.99M | 4.00M |
| 128 B | 3.72M | 1.26M | 4.74M | 4.47M | 5.29M | 3.92M |
| 512 B | 3.63M | 1.60M | 3.32M | 4.19M | 3.44M | 3.44M |
| 2 KiB | 3.63M | 1.38M | 2.03M | 1.39M | 1.77M | 1.42M |
| 8 KiB | 3.64M | 1.66M | 767k | 513k | 628k | 573k |
| 32 KiB | 3.65M | 1.70M | 177k | 149k | 41.1k | 159k |
| 128 KiB | 3.65M | 988k | 63.2k | 37.6k | 12.2k | 35.7k |

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
| inproc | 32 B | 5.69 µs | 6.02 µs | 29.9 µs | 26.8 µs | 50.0 µs | 74.9 µs |
| inproc | 128 B | 5.66 µs | 5.76 µs | 11.8 µs | 26.7 µs | 71.7 µs | 82.9 µs |
| inproc | 512 B | 5.53 µs | 5.63 µs | 11.9 µs | 32.2 µs | 306 µs | 359 µs |
| inproc | 2 KiB | 5.50 µs | 5.66 µs | 14.2 µs | 28.8 µs | 36.6 µs | 74.8 µs |
| inproc | 8 KiB | 5.44 µs | 5.86 µs | 36.1 µs | 26.5 µs | 73.2 µs | 86.1 µs |
| inproc | 32 KiB | 5.45 µs | 5.75 µs | 24.6 µs | 26.5 µs | 74.1 µs | 84.8 µs |
| inproc | 128 KiB | 5.48 µs | 5.79 µs | 24.4 µs | 26.3 µs | 72.6 µs | 121 µs |
| ipc | 32 B | 17.1 µs | 23.7 µs | 49.3 µs | 52.5 µs | 98.5 µs | 127 µs |
| ipc | 128 B | 17.2 µs | 21.6 µs | 33.4 µs | 50.2 µs | 67.9 µs | 101 µs |
| ipc | 512 B | 17.5 µs | 22.0 µs | 31.2 µs | 51.1 µs | 81.2 µs | 105 µs |
| ipc | 2 KiB | 18.9 µs | 23.4 µs | 32.0 µs | 52.5 µs | 80.3 µs | 116 µs |
| ipc | 8 KiB | 22.6 µs | 30.7 µs | 55.7 µs | 61.2 µs | 123 µs | 183 µs |
| ipc | 32 KiB | 29.0 µs | 41.6 µs | 67.8 µs | 75.9 µs | 127 µs | 237 µs |
| ipc | 128 KiB | 167 µs | 416 µs | 463 µs | 93.1 µs | 1.1 ms | 1.2 ms |
| tcp | 32 B | 23.6 µs | 31.8 µs | 51.6 µs | 60.6 µs | 114 µs | 184 µs |
| tcp | 128 B | 23.7 µs | 29.8 µs | 46.8 µs | 62.4 µs | 121 µs | 281 µs |
| tcp | 512 B | 23.7 µs | 32.2 µs | 52.3 µs | 61.5 µs | 109 µs | 183 µs |
| tcp | 2 KiB | 25.2 µs | 32.0 µs | 55.7 µs | 64.6 µs | 113 µs | 187 µs |
| tcp | 8 KiB | 27.8 µs | 38.0 µs | 60.9 µs | 66.8 µs | 114 µs | 314 µs |
| tcp | 32 KiB | 35.2 µs | 49.2 µs | 81.0 µs | 79.5 µs | 1.0 ms | 1.1 ms |
| tcp | 128 KiB | 77.1 µs | 91.1 µs | 124 µs | 115 µs | 141 µs | 177 µs |
| lz4+tcp | 32 B | 25.1 µs | 33.4 µs | 60.0 µs | 82.9 µs | 114 µs | 145 µs |
| lz4+tcp | 128 B | 24.9 µs | 32.6 µs | 55.0 µs | 82.4 µs | 1.0 ms | 1.1 ms |
| lz4+tcp | 512 B | 27.5 µs | 34.3 µs | 53.6 µs | 84.9 µs | 109 µs | 136 µs |
| lz4+tcp | 2 KiB | 28.4 µs | 35.1 µs | 54.6 µs | 85.4 µs | 1.1 ms | 1.1 ms |
| lz4+tcp | 8 KiB | 31.2 µs | 39.5 µs | 59.1 µs | 87.4 µs | 109 µs | 136 µs |
| lz4+tcp | 32 KiB | 42.1 µs | 80.2 µs | 96.0 µs | 101 µs | 141 µs | 196 µs |
| lz4+tcp | 128 KiB | 85.0 µs | 124 µs | 145 µs | 151 µs | 1.4 ms | 1.5 ms |
| zstd+tcp | 32 B | 25.6 µs | 37.3 µs | 63.9 µs | 81.7 µs | 114 µs | 149 µs |
| zstd+tcp | 128 B | 49.6 µs | 85.5 µs | 109 µs | 111 µs | 149 µs | 212 µs |
| zstd+tcp | 512 B | 49.1 µs | 86.6 µs | 107 µs | 109 µs | 136 µs | 170 µs |
| zstd+tcp | 2 KiB | 34.3 µs | 48.0 µs | 86.0 µs | 91.6 µs | 1.1 ms | 1.2 ms |
| zstd+tcp | 8 KiB | 37.5 µs | 46.4 µs | 72.6 µs | 95.7 µs | 117 µs | 134 µs |
| zstd+tcp | 32 KiB | 49.4 µs | 61.7 µs | 88.8 µs | 107 µs | 1.2 ms | 1.3 ms |
| zstd+tcp | 128 KiB | 96.7 µs | 150 µs | 179 µs | 163 µs | 1.3 ms | 1.5 ms |

<!-- END latency_percentiles -->

## PUSH/PULL throughput, 8 peers

8 PUSH peers → 1 PULL, all transports, both backends. Numbers are msg/s.

<!-- BEGIN push_pull_8peer -->
| Size | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---|---|---|---|---|---|---|
| 32 B | 3.49M | 2.67M | 5.09M | 4.08M | 5.00M | 4.66M |
| 128 B | 3.61M | 2.94M | 4.16M | 3.33M | 4.46M | 3.78M |
| 512 B | 3.62M | 2.81M | 3.35M | 3.54M | 3.08M | 3.34M |
| 2 KiB | 3.57M | 2.87M | 1.52M | 1.89M | 1.32M | 1.82M |
| 8 KiB | 3.62M | 3.02M | 483k | 585k | 413k | 735k |
| 32 KiB | 3.61M | 2.47M | 169k | 143k | 117k | 207k |
| 128 KiB | 3.75M | 2.18M | 42.5k | 52.4k | 26.2k | 50.8k |

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

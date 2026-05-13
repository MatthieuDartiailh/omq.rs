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
| 32 B | 3.50M / 112 MB/s | 2.96M / 94.6 MB/s | 6.87M / 220 MB/s | 7.07M / 226 MB/s | 4.53M / 145 MB/s | 2.94M / 94.1 MB/s |
| 128 B | 3.61M / 462 MB/s | 2.92M / 374 MB/s | 5.14M / 658 MB/s | 4.96M / 635 MB/s | 1.86M / 238 MB/s | 110k / 14.0 MB/s |
| 512 B | 3.67M / 1.88 GB/s | 2.90M / 1.48 GB/s | 3.36M / 1.72 GB/s | 3.43M / 1.75 GB/s | 1.93M / 987 MB/s | 118k / 60.4 MB/s |
| 2 KiB | 3.65M / 7.47 GB/s | 2.81M / 5.76 GB/s | 2.00M / 4.10 GB/s | 1.71M / 3.50 GB/s | 1.59M / 3.26 GB/s | 608k / 1.25 GB/s |
| 8 KiB | 3.65M / 29.9 GB/s | 2.85M / 23.3 GB/s | 710k / 5.81 GB/s | 643k / 5.26 GB/s | 571k / 4.68 GB/s | 391k / 3.21 GB/s |
| 32 KiB | 3.61M / 118.1 GB/s | 2.69M / 88.1 GB/s | 180k / 5.89 GB/s | 172k / 5.65 GB/s | 153k / 5.00 GB/s | 68.5k / 2.25 GB/s |
| 128 KiB | 3.66M / 480.1 GB/s | 3.05M / 399.5 GB/s | 60.2k / 7.89 GB/s | 60.5k / 7.93 GB/s | 36.6k / 4.80 GB/s | 22.6k / 2.97 GB/s |

<!-- END push_pull_compio_1peer -->

Inproc "GB/s" at large payloads reflects zero-copy Arc-clone — no kernel
traversal.

## Backend comparison: PUSH/PULL throughput, single peer

<!-- BEGIN backend_comparison -->
| Size | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---|---|---|---|---|---|---|
| 32 B | 3.50M | 1.39M | 6.87M | 3.81M | 7.07M | 4.16M |
| 128 B | 3.61M | 514k | 5.14M | 1.10M | 4.96M | 4.08M |
| 512 B | 3.67M | 1.54M | 3.36M | 3.02M | 3.43M | 4.26M |
| 2 KiB | 3.65M | 1.69M | 2.00M | 1.37M | 1.71M | 1.46M |
| 8 KiB | 3.65M | 1.39M | 710k | 508k | 643k | 531k |
| 32 KiB | 3.61M | 1.49M | 180k | 148k | 172k | 136k |
| 128 KiB | 3.66M | 929k | 60.2k | 35.6k | 60.5k | 33.9k |

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
| inproc | 32 B | 5.64 µs | 9.58 µs | 29.7 µs | 27.0 µs | 39.8 µs | 54.3 µs |
| inproc | 128 B | 5.62 µs | 9.78 µs | 21.6 µs | 33.2 µs | 324 µs | 388 µs |
| inproc | 512 B | 5.65 µs | 8.45 µs | 19.0 µs | 26.3 µs | 61.6 µs | 79.3 µs |
| inproc | 2 KiB | 5.69 µs | 5.85 µs | 19.2 µs | 26.4 µs | 72.7 µs | 83.9 µs |
| inproc | 8 KiB | 5.75 µs | 10.3 µs | 21.8 µs | 26.1 µs | 43.5 µs | 77.8 µs |
| inproc | 32 KiB | 5.74 µs | 6.01 µs | 21.2 µs | 25.4 µs | 37.2 µs | 57.3 µs |
| inproc | 128 KiB | 5.75 µs | 6.23 µs | 20.4 µs | 32.1 µs | 312 µs | 492 µs |
| ipc | 32 B | 16.9 µs | 24.9 µs | 44.9 µs | 48.5 µs | 64.5 µs | 95.5 µs |
| ipc | 128 B | 17.0 µs | 21.5 µs | 44.2 µs | 49.9 µs | 70.1 µs | 101 µs |
| ipc | 512 B | 17.1 µs | 21.6 µs | 40.1 µs | 51.1 µs | 838 µs | 911 µs |
| ipc | 2 KiB | 18.3 µs | 22.8 µs | 41.6 µs | 52.8 µs | 90.4 µs | 107 µs |
| ipc | 8 KiB | 22.4 µs | 30.8 µs | 49.6 µs | 60.3 µs | 78.4 µs | 105 µs |
| ipc | 32 KiB | 28.7 µs | 42.8 µs | 67.1 µs | 75.1 µs | 129 µs | 349 µs |
| ipc | 128 KiB | 111 µs | 194 µs | 240 µs | 94.4 µs | 1.1 ms | 1.2 ms |
| tcp | 32 B | 24.8 µs | 33.1 µs | 50.6 µs | 59.9 µs | 117 µs | 177 µs |
| tcp | 128 B | 25.1 µs | 35.4 µs | 58.8 µs | 60.7 µs | 114 µs | 178 µs |
| tcp | 512 B | 25.2 µs | 44.4 µs | 69.0 µs | 61.4 µs | 112 µs | 176 µs |
| tcp | 2 KiB | 26.5 µs | 34.9 µs | 56.3 µs | 64.8 µs | 111 µs | 184 µs |
| tcp | 8 KiB | 29.3 µs | 39.2 µs | 60.0 µs | 67.8 µs | 121 µs | 222 µs |
| tcp | 32 KiB | 37.6 µs | 51.2 µs | 76.0 µs | 78.2 µs | 150 µs | 294 µs |
| tcp | 128 KiB | 82.5 µs | 133 µs | 151 µs | 114 µs | 147 µs | 197 µs |
| lz4+tcp | 32 B | 25.1 µs | 33.2 µs | 55.6 µs | 79.6 µs | 989 µs | 1.1 ms |
| lz4+tcp | 128 B | 25.0 µs | 32.5 µs | 49.5 µs | 80.1 µs | 115 µs | 256 µs |
| lz4+tcp | 512 B | 27.1 µs | 36.3 µs | 55.7 µs | 84.5 µs | 111 µs | 135 µs |
| lz4+tcp | 2 KiB | 28.5 µs | 41.5 µs | 58.8 µs | 80.0 µs | 123 µs | 138 µs |
| lz4+tcp | 8 KiB | 31.2 µs | 40.1 µs | 59.7 µs | 84.8 µs | 107 µs | 135 µs |
| lz4+tcp | 32 KiB | 42.2 µs | 57.3 µs | 81.2 µs | 100 µs | 1.0 ms | 1.2 ms |
| lz4+tcp | 128 KiB | 84.9 µs | 528 µs | 579 µs | 150 µs | 1.4 ms | 1.6 ms |
| zstd+tcp | 32 B | 25.6 µs | 32.9 µs | 51.6 µs | 79.9 µs | 122 µs | 150 µs |
| zstd+tcp | 128 B | 48.5 µs | 61.3 µs | 91.1 µs | 109 µs | 127 µs | 142 µs |
| zstd+tcp | 512 B | 48.5 µs | 315 µs | 367 µs | 108 µs | 1.2 ms | 1.3 ms |
| zstd+tcp | 2 KiB | 33.5 µs | 43.1 µs | 66.6 µs | 91.5 µs | 131 µs | 158 µs |
| zstd+tcp | 8 KiB | 36.2 µs | 56.4 µs | 86.2 µs | 92.1 µs | 1.1 ms | 1.2 ms |
| zstd+tcp | 32 KiB | 48.3 µs | 59.4 µs | 87.1 µs | 107 µs | 133 µs | 180 µs |
| zstd+tcp | 128 KiB | 95.3 µs | 116 µs | 155 µs | 163 µs | 1.5 ms | 1.6 ms |

<!-- END latency_percentiles -->

## PUSH/PULL throughput, 8 peers

8 PUSH peers → 1 PULL, all transports, both backends. Numbers are msg/s.

<!-- BEGIN push_pull_8peer -->
| Size | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---|---|---|---|---|---|---|
| 32 B | 3.58M | 2.52M | 5.13M | 4.65M | 5.20M | 4.68M |
| 128 B | 3.64M | 2.66M | 4.78M | 3.84M | 4.60M | 3.73M |
| 512 B | 3.69M | 2.93M | 3.11M | 3.54M | 2.43M | 2.65M |
| 2 KiB | 3.68M | 2.69M | 1.40M | 1.60M | 1.19M | 1.74M |
| 8 KiB | 3.64M | 2.80M | 437k | 621k | 381k | 792k |
| 32 KiB | 3.69M | 2.45M | 157k | 137k | 109k | 133k |
| 128 KiB | 3.58M | 1.93M | 38.3k | 14.5k | 25.7k | 47.8k |

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

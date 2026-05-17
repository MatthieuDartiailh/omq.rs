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
| 32 B | 3.78M / 121 MB/s | 16.44M / 526 MB/s | 7.05M / 226 MB/s | 7.13M / 228 MB/s | 4.91M / 157 MB/s | 2.28M / 73.0 MB/s |
| 128 B | 3.75M / 480 MB/s | 10.29M / 1.32 GB/s | 4.78M / 612 MB/s | 5.14M / 658 MB/s | 4.04M / 517 MB/s | 118k / 15.0 MB/s |
| 512 B | 3.73M / 1.91 GB/s | 14.46M / 7.40 GB/s | 3.31M / 1.69 GB/s | 3.55M / 1.82 GB/s | 1.99M / 1.02 GB/s | 131k / 67.0 MB/s |
| 2 KiB | 3.76M / 7.70 GB/s | 12.14M / 24.9 GB/s | 2.02M / 4.13 GB/s | 1.71M / 3.49 GB/s | 1.65M / 3.37 GB/s | 614k / 1.26 GB/s |
| 8 KiB | 3.76M / 30.8 GB/s | 12.31M / 100.8 GB/s | 716k / 5.87 GB/s | 618k / 5.06 GB/s | 613k / 5.03 GB/s | 439k / 3.60 GB/s |
| 32 KiB | 3.75M / 123.0 GB/s | 14.85M / 486.7 GB/s | 184k / 6.02 GB/s | 166k / 5.44 GB/s | 166k / 5.45 GB/s | 94.2k / 3.09 GB/s |
| 128 KiB | 3.70M / 484.6 GB/s | 14.37M / 1883.4 GB/s | 57.0k / 7.47 GB/s | 59.5k / 7.80 GB/s | 42.7k / 5.59 GB/s | 25.3k / 3.32 GB/s |

<!-- END push_pull_compio_1peer -->

Inproc "GB/s" at large payloads reflects zero-copy Arc-clone — no kernel
traversal.

## Backend comparison: PUSH/PULL throughput, single peer

<!-- BEGIN backend_comparison -->
| Size | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---|---|---|---|---|---|---|
| 32 B | 3.78M | 4.17M | 7.05M | 3.91M | 7.13M | 3.77M |
| 128 B | 3.75M | 4.26M | 4.78M | 4.45M | 5.14M | 4.71M |
| 512 B | 3.73M | 4.05M | 3.31M | 2.35M | 3.55M | 3.60M |
| 2 KiB | 3.76M | 4.20M | 2.02M | 1.23M | 1.71M | 1.53M |
| 8 KiB | 3.76M | 3.51M | 716k | 445k | 618k | 615k |
| 32 KiB | 3.75M | 4.26M | 184k | 119k | 166k | 167k |
| 128 KiB | 3.70M | 3.66M | 57.0k | 34.5k | 59.5k | 46.4k |

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
| inproc | 32 B | 2.56 µs | 21.2 µs | 24.3 µs | 24.1 µs | 74.5 µs | 107 µs |
| inproc | 128 B | 2.58 µs | 5.52 µs | 26.8 µs | 22.8 µs | 73.1 µs | 87.3 µs |
| inproc | 512 B | 2.56 µs | 3.78 µs | 8.78 µs | 23.6 µs | 75.6 µs | 162 µs |
| inproc | 2 KiB | 2.58 µs | 2.65 µs | 9.58 µs | 25.4 µs | 80.5 µs | 98.1 µs |
| inproc | 8 KiB | 2.59 µs | 2.65 µs | 6.86 µs | 24.9 µs | 79.6 µs | 119 µs |
| inproc | 32 KiB | 2.57 µs | 2.63 µs | 7.53 µs | 25.0 µs | 80.2 µs | 93.1 µs |
| inproc | 128 KiB | 2.56 µs | 2.62 µs | 8.35 µs | 28.9 µs | 290 µs | 349 µs |
| ipc | 32 B | 14.6 µs | 83.3 µs | 125 µs | 50.9 µs | 72.5 µs | 106 µs |
| ipc | 128 B | 14.6 µs | 22.7 µs | 46.4 µs | 52.9 µs | 80.1 µs | 117 µs |
| ipc | 512 B | 15.1 µs | 21.3 µs | 36.6 µs | 54.1 µs | 75.5 µs | 107 µs |
| ipc | 2 KiB | 16.4 µs | 22.4 µs | 39.9 µs | 55.0 µs | 95.7 µs | 188 µs |
| ipc | 8 KiB | 19.7 µs | 27.0 µs | 49.0 µs | 61.9 µs | 84.2 µs | 147 µs |
| ipc | 32 KiB | 25.9 µs | 34.8 µs | 55.6 µs | 67.9 µs | 113 µs | 178 µs |
| ipc | 128 KiB | 185 µs | 245 µs | 290 µs | 89.9 µs | 108 µs | 154 µs |
| tcp | 32 B | 21.5 µs | 30.7 µs | 47.3 µs | 55.2 µs | 119 µs | 131 µs |
| tcp | 128 B | 21.5 µs | 40.4 µs | 54.7 µs | 61.5 µs | 111 µs | 158 µs |
| tcp | 512 B | 21.6 µs | 35.5 µs | 58.7 µs | 60.3 µs | 120 µs | 208 µs |
| tcp | 2 KiB | 22.7 µs | 43.0 µs | 63.4 µs | 62.4 µs | 115 µs | 180 µs |
| tcp | 8 KiB | 26.0 µs | 42.3 µs | 71.8 µs | 65.5 µs | 119 µs | 250 µs |
| tcp | 32 KiB | 33.9 µs | 237 µs | 272 µs | 78.1 µs | 109 µs | 165 µs |
| tcp | 128 KiB | 81.1 µs | 131 µs | 161 µs | 108 µs | 133 µs | 169 µs |
| lz4+tcp | 32 B | 22.3 µs | 35.4 µs | 50.9 µs | 881 µs | 1.1 ms | 1.3 ms |
| lz4+tcp | 128 B | 22.0 µs | 30.3 µs | 48.2 µs | 82.0 µs | 190 µs | 202 µs |
| lz4+tcp | 512 B | 24.0 µs | 37.2 µs | 54.0 µs | 83.5 µs | 171 µs | 203 µs |
| lz4+tcp | 2 KiB | 24.8 µs | 34.6 µs | 53.8 µs | 83.6 µs | 184 µs | 198 µs |
| lz4+tcp | 8 KiB | 27.3 µs | 46.2 µs | 68.5 µs | 88.3 µs | 195 µs | 207 µs |
| lz4+tcp | 32 KiB | 38.5 µs | 57.0 µs | 75.6 µs | 98.6 µs | 153 µs | 364 µs |
| lz4+tcp | 128 KiB | 80.6 µs | 108 µs | 141 µs | 146 µs | 216 µs | 336 µs |
| zstd+tcp | 32 B | 22.8 µs | 39.7 µs | 62.9 µs | 82.8 µs | 186 µs | 200 µs |
| zstd+tcp | 128 B | 253 µs | 436 µs | 939 µs | 106 µs | 155 µs | 240 µs |
| zstd+tcp | 512 B | 44.7 µs | 68.9 µs | 98.2 µs | 108 µs | 1.2 ms | 1.3 ms |
| zstd+tcp | 2 KiB | 29.2 µs | 50.9 µs | 72.3 µs | 89.7 µs | 450 µs | 844 µs |
| zstd+tcp | 8 KiB | 32.7 µs | 52.7 µs | 79.3 µs | 93.5 µs | 1.1 ms | 1.2 ms |
| zstd+tcp | 32 KiB | 44.2 µs | 61.8 µs | 91.5 µs | 107 µs | 210 µs | 406 µs |
| zstd+tcp | 128 KiB | 92.1 µs | 116 µs | 159 µs | 159 µs | 1.5 ms | 1.6 ms |

<!-- END latency_percentiles -->

## PUSH/PULL throughput, 8 peers

8 PUSH peers → 1 PULL, all transports, both backends. Numbers are msg/s.

<!-- BEGIN push_pull_8peer -->
| Size | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---|---|---|---|---|---|---|
| 32 B | 3.87M | 3.53M | 5.66M | 3.73M | 5.40M | 5.26M |
| 128 B | 3.83M | 3.45M | 4.22M | 5.23M | 3.96M | 3.61M |
| 512 B | 3.81M | 3.54M | 3.03M | 3.83M | 2.29M | 3.97M |
| 2 KiB | 3.81M | 3.51M | 1.45M | 1.99M | 1.34M | 2.01M |
| 8 KiB | 3.83M | 3.49M | 472k | 552k | 403k | 787k |
| 32 KiB | 3.72M | 3.56M | 161k | 134k | 115k | 187k |
| 128 KiB | 3.86M | 3.51M | 39.8k | 57.0k | 27.5k | 41.1k |

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

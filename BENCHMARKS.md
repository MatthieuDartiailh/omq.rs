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

Median of 3 × 500 ms rounds per cell.

<!-- BEGIN push_pull_compio_1peer -->
| Size | inproc | ipc | tcp | lz4+tcp | zstd+tcp |
|---|---|---|---|---|---|
| 32 B | 3.08M / 98.7 MB/s | 2.25M / 72.0 MB/s | 969k / 31.0 MB/s | 1.85M / 59.1 MB/s | 1.52M / 48.7 MB/s |
| 128 B | 3.05M / 391 MB/s | 1.89M / 242 MB/s | 1.84M / 236 MB/s | 1.64M / 210 MB/s | 107k / 13.7 MB/s |
| 512 B | 3.05M / 1.56 GB/s | 1.40M / 718 MB/s | 1.34M / 686 MB/s | 1.07M / 550 MB/s | 109k / 56.0 MB/s |
| 2 KiB | 3.07M / 6.29 GB/s | 823k / 1.69 GB/s | 833k / 1.71 GB/s | 778k / 1.59 GB/s | 105k / 216 MB/s |
| 8 KiB | 3.08M / 25.3 GB/s | 372k / 3.05 GB/s | 187k / 1.53 GB/s | 369k / 3.02 GB/s | 267k / 2.18 GB/s |
| 32 KiB | 3.08M / 100.8 GB/s | 120k / 3.93 GB/s | 117k / 3.83 GB/s | 114k / 3.74 GB/s | 103k / 3.39 GB/s |
| 128 KiB | 3.07M / 402.6 GB/s | 30.8k / 4.03 GB/s | 29.8k / 3.91 GB/s | 30.0k / 3.93 GB/s | 29.3k / 3.84 GB/s |

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
| 32 B | 3.08M | 953k | 2.25M | 3.64M | 969k | 4.78M |
| 128 B | 3.05M | 948k | 1.89M | 4.12M | 1.84M | 4.62M |
| 512 B | 3.05M | 505k | 1.40M | 2.67M | 1.34M | 3.60M |
| 2 KiB | 3.07M | 1.15M | 823k | 1.38M | 833k | 1.79M |
| 8 KiB | 3.08M | 1.09M | 372k | 466k | 187k | 570k |
| 32 KiB | 3.08M | 832k | 120k | 125k | 117k | 149k |
| 128 KiB | 3.07M | 804k | 30.8k | 45.5k | 29.8k | 40.9k |

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
| inproc | 32 B | 5.33 µs | 11.2 µs | 30.2 µs | 29.6 µs | 45.6 µs | 60.5 µs |
| inproc | 128 B | 5.53 µs | 5.63 µs | 19.1 µs | 26.8 µs | 44.3 µs | 70.0 µs |
| inproc | 512 B | 5.41 µs | 5.71 µs | 29.8 µs | 26.5 µs | 37.2 µs | 47.6 µs |
| inproc | 2 KiB | 5.41 µs | 5.50 µs | 11.3 µs | 27.0 µs | 47.3 µs | 79.5 µs |
| inproc | 8 KiB | 5.32 µs | 5.76 µs | 26.0 µs | 26.8 µs | 268 µs | 320 µs |
| inproc | 32 KiB | 5.30 µs | 5.69 µs | 26.7 µs | 146 µs | 300 µs | 354 µs |
| inproc | 128 KiB | 5.28 µs | 5.92 µs | 36.5 µs | 27.4 µs | 36.0 µs | 50.4 µs |
| ipc | 32 B | 19.5 µs | 33.3 µs | 60.7 µs | 51.0 µs | 70.1 µs | 103 µs |
| ipc | 128 B | 19.1 µs | 35.5 µs | 57.0 µs | 50.4 µs | 66.2 µs | 97.0 µs |
| ipc | 512 B | 19.8 µs | 33.8 µs | 59.9 µs | 49.9 µs | 803 µs | 870 µs |
| ipc | 2 KiB | 20.6 µs | 35.4 µs | 59.0 µs | 50.4 µs | 66.5 µs | 99.7 µs |
| ipc | 8 KiB | 24.2 µs | 40.6 µs | 65.4 µs | 62.1 µs | 879 µs | 1.0 ms |
| ipc | 32 KiB | 30.0 µs | 56.0 µs | 75.7 µs | 76.8 µs | 903 µs | 1.0 ms |
| ipc | 128 KiB | 73.6 µs | 119 µs | 145 µs | 100 µs | 124 µs | 175 µs |
| tcp | 32 B | 27.2 µs | 46.4 µs | 69.7 µs | 61.6 µs | 112 µs | 166 µs |
| tcp | 128 B | 27.1 µs | 41.4 µs | 65.1 µs | 62.8 µs | 892 µs | 975 µs |
| tcp | 512 B | 27.4 µs | 46.4 µs | 65.0 µs | 63.5 µs | 118 µs | 187 µs |
| tcp | 2 KiB | 28.3 µs | 50.1 µs | 69.6 µs | 65.2 µs | 954 µs | 1.0 ms |
| tcp | 8 KiB | 31.0 µs | 50.2 µs | 71.7 µs | 66.4 µs | 106 µs | 184 µs |
| tcp | 32 KiB | 38.3 µs | 63.2 µs | 81.0 µs | 79.5 µs | 965 µs | 1.1 ms |
| tcp | 128 KiB | 86.4 µs | 139 µs | 156 µs | 127 µs | 1.3 ms | 1.4 ms |
| lz4+tcp | 32 B | 27.7 µs | 41.6 µs | 64.9 µs | 79.3 µs | 104 µs | 132 µs |
| lz4+tcp | 128 B | 27.4 µs | 40.8 µs | 63.8 µs | 77.7 µs | 1.0 ms | 1.1 ms |
| lz4+tcp | 512 B | 30.3 µs | 45.0 µs | 70.3 µs | 82.5 µs | 103 µs | 124 µs |
| lz4+tcp | 2 KiB | 31.2 µs | 45.1 µs | 69.2 µs | 85.2 µs | 1.0 ms | 1.1 ms |
| lz4+tcp | 8 KiB | 34.3 µs | 60.6 µs | 80.7 µs | 86.2 µs | 115 µs | 140 µs |
| lz4+tcp | 32 KiB | 44.6 µs | 81.5 µs | 97.9 µs | 100 µs | 1.2 ms | 1.3 ms |
| lz4+tcp | 128 KiB | 87.4 µs | 138 µs | 156 µs | 151 µs | 1.4 ms | 1.6 ms |
| zstd+tcp | 32 B | 28.3 µs | 41.5 µs | 61.1 µs | 819 µs | 1.1 ms | 1.2 ms |
| zstd+tcp | 128 B | 52.7 µs | 106 µs | 1.1 ms | 142 µs | 1.3 ms | 1.4 ms |
| zstd+tcp | 512 B | 52.7 µs | 964 µs | 1.2 ms | 108 µs | 149 µs | 186 µs |
| zstd+tcp | 2 KiB | 36.9 µs | 63.6 µs | 90.7 µs | 88.0 µs | 108 µs | 128 µs |
| zstd+tcp | 8 KiB | 39.9 µs | 65.3 µs | 94.7 µs | 91.1 µs | 139 µs | 170 µs |
| zstd+tcp | 32 KiB | 52.4 µs | 90.1 µs | 118 µs | 107 µs | 1.2 ms | 1.3 ms |
| zstd+tcp | 128 KiB | 101 µs | 155 µs | 187 µs | 158 µs | 195 µs | 1.5 ms |

<!-- END latency_percentiles -->

## PUSH/PULL throughput, 8 peers

8 PUSH peers → 1 PULL, all transports, both backends. Numbers are msg/s.

<!-- BEGIN push_pull_8peer -->
| Size | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---|---|---|---|---|---|---|
| 32 B | 3.22M | 905k | 2.10M | 5.21M | 1.70M | 3.35M |
| 128 B | 3.21M | 1.04M | 1.48M | 4.31M | 1.85M | 3.17M |
| 512 B | 3.23M | 981k | 808k | 4.67M | 1.32M | 4.16M |
| 2 KiB | 3.23M | 1.02M | 830k | 1.66M | 784k | 2.02M |
| 8 KiB | 3.18M | 1.00M | 384k | 633k | 310k | 797k |
| 32 KiB | 3.17M | 868k | 128k | 215k | 105k | 220k |
| 128 KiB | 3.20M | 1.05M | 34.4k | 78.9k | 24.9k | 55.8k |

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

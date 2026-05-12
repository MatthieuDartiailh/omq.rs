# Comparisons

Two-process TCP benchmarks. 3 s timed window after 500 ms warmup.
Hardware: Linux 6.12 (Debian 13) VM, Intel i7-8700B 3.2 GHz 6-core, Rust 1.95.0.

## libzmq vs omq (two-process TCP, one core each)

Push binds, pull connects. Each process pinned to one core.
libzmq peer: minimal C binary, system libzmq 5.2.5.
omq-compio peer: single-threaded (io_uring). omq-tokio peer: multi-thread runtime.
Refresh: `./scripts/compare_libzmq.sh --update-benchmarks`

<!-- BEGIN libzmq_comparison -->
| Size | libzmq msg/s | libzmq MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|
| 8 B | 8.50M | 68 MB/s | 8.28M | 66 MB/s | 0.97× | 5.09M | 41 MB/s | 0.60× |
| 32 B | 8.56M | 274 MB/s | 6.99M | 224 MB/s | 0.82× | 4.47M | 143 MB/s | 0.52× |
| 128 B | 2.93M | 376 MB/s | 5.17M | 662 MB/s | **1.8×** | 5.72M | 732 MB/s | **1.9×** |
| 512 B | 2.01M | 1.0 GB/s | 3.38M | 1.7 GB/s | **1.7×** | 3.99M | 2.0 GB/s | **2.0×** |
| 2 KiB | 677k | 1.4 GB/s | 1.64M | 3.4 GB/s | **2.4×** | 1.34M | 2.7 GB/s | **2.0×** |
| 8 KiB | 178k | 1.5 GB/s | 574k | 4.7 GB/s | **3.2×** | 491k | 4.0 GB/s | **2.8×** |
| 32 KiB | 76k | 2.5 GB/s | 156k | 5.1 GB/s | **2.0×** | 149k | 4.9 GB/s | **2.0×** |
| 128 KiB | 33k | 4.3 GB/s | 52k | 6.8 GB/s | **1.6×** | 46k | 6.0 GB/s | **1.4×** |
| 512 KiB | 10k | 5.2 GB/s | 16k | 8.5 GB/s | **1.6×** | 13k | 6.8 GB/s | **1.3×** |
| 2 MiB | 3k | 5.4 GB/s | 2k | 4.8 GB/s | 0.88× | 3k | 5.5 GB/s | 1.00× |
| 8 MiB | 590 | 4.9 GB/s | 397 | 3.3 GB/s | 0.67× | 473 | 4.0 GB/s | 0.80× |
| 32 MiB | 145 | 4.9 GB/s | 80 | 2.7 GB/s | 0.55× | 100 | 3.4 GB/s | 0.69× |

<!-- END libzmq_comparison -->

## zmq.rs vs omq (two-process TCP)

Push binds, pull connects, TCP loopback.
zmq.rs peer: `scripts/zmqrs_bench_peer/` (zeromq crate, tokio multi-thread runtime).
omq-compio peer: single io_uring runtime on one core. omq-tokio peer: multi-thread runtime.
zmq.rs <-> omq-tokio is apples-to-apples; omq-compio is intentionally CPU-constrained (one core).
Refresh: `./scripts/compare_zmqrs.sh --update-benchmarks`

<!-- BEGIN zmqrs_comparison -->
| Size | zmq.rs msg/s | zmq.rs MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|
| 8 B | 428k | 3 MB/s | 7.84M | 63 MB/s | **18.3×** | 6.63M | 53 MB/s | **15.5×** |
| 32 B | 393k | 13 MB/s | 6.62M | 212 MB/s | **16.8×** | 6.52M | 209 MB/s | **16.6×** |
| 128 B | 351k | 45 MB/s | 4.81M | 616 MB/s | **13.7×** | 4.75M | 608 MB/s | **13.5×** |
| 512 B | 316k | 162 MB/s | 3.51M | 1.8 GB/s | **11.1×** | 2.74M | 1.4 GB/s | **8.7×** |
| 2 KiB | 289k | 591 MB/s | 1.60M | 3.3 GB/s | **5.6×** | 1.28M | 2.6 GB/s | **4.4×** |
| 8 KiB | 228k | 1.9 GB/s | 586k | 4.8 GB/s | **2.6×** | 548k | 4.5 GB/s | **2.4×** |
| 32 KiB | 133k | 4.4 GB/s | 169k | 5.6 GB/s | **1.3×** | 152k | 5.0 GB/s | **1.1×** |
| 128 KiB | 33k | 4.4 GB/s | 61k | 8.0 GB/s | **1.8×** | 43k | 5.6 GB/s | **1.3×** |
| 512 KiB | 8k | 4.2 GB/s | 15k | 7.7 GB/s | **1.8×** | 14k | 7.2 GB/s | **1.7×** |
| 2 MiB | 2k | 3.2 GB/s | 3k | 6.4 GB/s | **2.0×** | 3k | 6.8 GB/s | **2.1×** |
| 8 MiB | 311 | 2.6 GB/s | 591 | 5.0 GB/s | **1.9×** | 715 | 6.0 GB/s | **2.3×** |
| 32 MiB | 110 | 3.7 GB/s | 134 | 4.5 GB/s | **1.2×** | 166 | 5.6 GB/s | **1.5×** |

<!-- END zmqrs_comparison -->

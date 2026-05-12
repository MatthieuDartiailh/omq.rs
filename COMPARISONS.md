# Comparisons

Two-process benchmarks. 3 s timed window after 500 ms warmup.
Hardware: Linux 6.12 (Debian 13) VM, Intel i7-8700B 3.2 GHz 6-core, Rust 1.95.0.

## libzmq vs omq — TCP

Push binds, pull connects. Each process pinned to one core.
libzmq peer: minimal C binary, system libzmq 5.2.5.
omq-compio peer: single-threaded (io_uring). omq-tokio peer: multi-thread runtime.
Refresh: `./scripts/compare_libzmq.sh --update-benchmarks`

<!-- BEGIN libzmq_comparison -->
| Size | libzmq msg/s | libzmq MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|
| 8 B | 8.80M | 70 MB/s | 8.29M | 66 MB/s | 0.94× | 4.71M | 38 MB/s | 0.54× |
| 32 B | 8.96M | 287 MB/s | 7.02M | 225 MB/s | 0.78× | 4.81M | 154 MB/s | 0.54× |
| 128 B | 2.86M | 366 MB/s | 5.06M | 648 MB/s | **1.8×** | 4.95M | 634 MB/s | **1.7×** |
| 512 B | 1.94M | 995 MB/s | 3.48M | 1.8 GB/s | **1.8×** | 3.85M | 2.0 GB/s | **2.0×** |
| 2 KiB | 660k | 1.4 GB/s | 1.70M | 3.5 GB/s | **2.6×** | 1.69M | 3.5 GB/s | **2.6×** |
| 8 KiB | 188k | 1.5 GB/s | 627k | 5.1 GB/s | **3.3×** | 468k | 3.8 GB/s | **2.5×** |
| 32 KiB | 69k | 2.3 GB/s | 166k | 5.4 GB/s | **2.4×** | 150k | 4.9 GB/s | **2.2×** |
| 128 KiB | 32k | 4.2 GB/s | 58k | 7.7 GB/s | **1.8×** | 43k | 5.7 GB/s | **1.3×** |
| 512 KiB | 10k | 5.1 GB/s | 9k | 4.8 GB/s | 0.94× | 14k | 7.3 GB/s | **1.4×** |
| 2 MiB | 3k | 5.2 GB/s | 2k | 4.7 GB/s | 0.90× | 2k | 4.3 GB/s | 0.82× |
| 8 MiB | 598 | 5.0 GB/s | 389 | 3.3 GB/s | 0.65× | 435 | 3.6 GB/s | 0.73× |
| 32 MiB | 61 | 2.0 GB/s | 44 | 1.5 GB/s | 0.72× | 39 | 1.3 GB/s | 0.64× |

<!-- END libzmq_comparison -->

## libzmq vs omq — IPC

Push binds, pull connects, Unix-domain socket loopback.
libzmq peer: minimal C binary, system libzmq 5.2.5.
omq-compio peer: single-threaded (io_uring). omq-tokio peer: multi-thread runtime.
Refresh: `./scripts/compare_libzmq.sh --ipc --update-benchmarks`

<!-- BEGIN libzmq_comparison_ipc -->
| Size | libzmq msg/s | libzmq MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|
| 8 B | 8.45M | 68 MB/s | 7.87M | 63 MB/s | 0.93× | 4.22M | 34 MB/s | 0.50× |
| 32 B | 7.86M | 252 MB/s | 6.79M | 217 MB/s | 0.86× | 4.13M | 132 MB/s | 0.53× |
| 128 B | 2.87M | 368 MB/s | 4.85M | 620 MB/s | **1.7×** | 4.95M | 634 MB/s | **1.7×** |
| 512 B | 2.36M | 1.2 GB/s | 3.23M | 1.7 GB/s | **1.4×** | 4.36M | 2.2 GB/s | **1.8×** |
| 2 KiB | 799k | 1.6 GB/s | 2.04M | 4.2 GB/s | **2.6×** | 1.44M | 2.9 GB/s | **1.8×** |
| 8 KiB | 245k | 2.0 GB/s | 702k | 5.8 GB/s | **2.9×** | 440k | 3.6 GB/s | **1.8×** |
| 32 KiB | 105k | 3.4 GB/s | 182k | 6.0 GB/s | **1.7×** | 163k | 5.3 GB/s | **1.5×** |
| 128 KiB | 34k | 4.5 GB/s | 43k | 5.6 GB/s | **1.3×** | 31k | 4.1 GB/s | 0.92× |
| 512 KiB | 11k | 5.9 GB/s | 10k | 5.2 GB/s | 0.88× | 10k | 5.2 GB/s | 0.87× |
| 2 MiB | 3k | 5.5 GB/s | 2k | 4.8 GB/s | 0.87× | 3k | 6.1 GB/s | **1.1×** |
| 8 MiB | 579 | 4.9 GB/s | 383 | 3.2 GB/s | 0.66× | 499 | 4.2 GB/s | 0.86× |
| 32 MiB | 61 | 2.1 GB/s | 44 | 1.5 GB/s | 0.72× | 48 | 1.6 GB/s | 0.79× |

<!-- END libzmq_comparison_ipc -->

## zmq.rs vs omq — TCP

Push binds, pull connects, TCP loopback.
zmq.rs peer: `scripts/zmqrs_bench_peer/` (zeromq crate, tokio multi-thread runtime).
omq-compio peer: single io_uring runtime on one core. omq-tokio peer: multi-thread runtime.
zmq.rs <-> omq-tokio is apples-to-apples; omq-compio is intentionally CPU-constrained (one core).
Refresh: `./scripts/compare_zmqrs.sh --update-benchmarks`

<!-- BEGIN zmqrs_comparison -->
| Size | zmq.rs msg/s | zmq.rs MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|
| 8 B | 435k | 4 MB/s | 8.13M | 65 MB/s | **18.7×** | 4.73M | 38 MB/s | **10.9×** |
| 32 B | 382k | 12 MB/s | 6.92M | 221 MB/s | **18.1×** | 4.74M | 152 MB/s | **12.4×** |
| 128 B | 347k | 44 MB/s | 4.97M | 637 MB/s | **14.3×** | 4.90M | 627 MB/s | **14.1×** |
| 512 B | 323k | 165 MB/s | 3.46M | 1.8 GB/s | **10.7×** | 4.13M | 2.1 GB/s | **12.8×** |
| 2 KiB | 284k | 582 MB/s | 1.58M | 3.2 GB/s | **5.6×** | 1.39M | 2.8 GB/s | **4.9×** |
| 8 KiB | 228k | 1.9 GB/s | 600k | 4.9 GB/s | **2.6×** | 495k | 4.1 GB/s | **2.2×** |
| 32 KiB | 130k | 4.3 GB/s | 157k | 5.1 GB/s | **1.2×** | 157k | 5.1 GB/s | **1.2×** |
| 128 KiB | 32k | 4.2 GB/s | 44k | 5.7 GB/s | **1.4×** | 44k | 5.8 GB/s | **1.4×** |
| 512 KiB | 8k | 4.1 GB/s | 13k | 6.9 GB/s | **1.7×** | 13k | 6.9 GB/s | **1.7×** |
| 2 MiB | 1k | 3.1 GB/s | 2k | 4.8 GB/s | **1.5×** | 2k | 4.7 GB/s | **1.5×** |
| 8 MiB | 318 | 2.7 GB/s | 383 | 3.2 GB/s | **1.2×** | 379 | 3.2 GB/s | **1.2×** |
| 32 MiB | 98 | 3.3 GB/s | 44 | 1.5 GB/s | 0.45× | 40 | 1.3 GB/s | 0.41× |

<!-- END zmqrs_comparison -->

## zmq.rs vs omq — IPC

Push binds, pull connects, Unix-domain socket loopback.
zmq.rs peer: `scripts/zmqrs_bench_peer/` (zeromq crate, tokio multi-thread runtime).
omq-compio peer: single io_uring runtime on one core. omq-tokio peer: multi-thread runtime.
Refresh: `./scripts/compare_zmqrs.sh --ipc --update-benchmarks`

<!-- BEGIN zmqrs_comparison_ipc -->
| Size | zmq.rs msg/s | zmq.rs MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|
| 8 B | 733k | 6 MB/s | 7.84M | 63 MB/s | **10.7×** | 4.20M | 34 MB/s | **5.7×** |
| 32 B | 719k | 23 MB/s | 6.69M | 214 MB/s | **9.3×** | 3.85M | 123 MB/s | **5.4×** |
| 128 B | 688k | 88 MB/s | 4.88M | 624 MB/s | **7.1×** | 4.73M | 605 MB/s | **6.9×** |
| 512 B | 677k | 347 MB/s | 3.20M | 1.6 GB/s | **4.7×** | 4.27M | 2.2 GB/s | **6.3×** |
| 2 KiB | 582k | 1.2 GB/s | 1.91M | 3.9 GB/s | **3.3×** | 1.35M | 2.8 GB/s | **2.3×** |
| 8 KiB | 369k | 3.0 GB/s | 745k | 6.1 GB/s | **2.0×** | 459k | 3.8 GB/s | **1.2×** |
| 32 KiB | 131k | 4.3 GB/s | 183k | 6.0 GB/s | **1.4×** | 124k | 4.0 GB/s | 0.94× |
| 128 KiB | 30k | 3.9 GB/s | 43k | 5.7 GB/s | **1.5×** | 50k | 6.6 GB/s | **1.7×** |
| 512 KiB | 8k | 4.2 GB/s | 10k | 5.5 GB/s | **1.3×** | 12k | 6.4 GB/s | **1.6×** |
| 2 MiB | 2k | 3.5 GB/s | 2k | 4.7 GB/s | **1.3×** | 3k | 7.2 GB/s | **2.0×** |
| 8 MiB | 316 | 2.7 GB/s | 396 | 3.3 GB/s | **1.3×** | 495 | 4.2 GB/s | **1.6×** |
| 32 MiB | 88 | 3.0 GB/s | 45 | 1.5 GB/s | 0.51× | 51 | 1.7 GB/s | 0.58× |

<!-- END zmqrs_comparison_ipc -->

# Comparisons

Two-process benchmarks (inproc: single-process). 3 s timed window after 500 ms warmup.
Hardware: Linux 6.12 (Debian 13) VM, Intel i7-8700B 3.2 GHz 6-core, Rust 1.95.0.

## libzmq vs omq — inproc

Push and pull run in the same process; no kernel socket overhead.
libzmq peer: minimal C binary, system libzmq 5.2.5.
omq-compio peer: single-threaded (io_uring). omq-tokio peer: multi-thread runtime.
Refresh: `./scripts/compare_libzmq.sh --inproc --update-benchmarks`

<!-- BEGIN libzmq_comparison_inproc -->
| Size | libzmq msg/s | libzmq MB/s | compio-mt msg/s | compio-mt MB/s | mt × | compio-st msg/s | compio-st MB/s | st × | tokio msg/s | tokio MB/s | tokio × |
|-------|-------------|------------|----------------|---------------|------|----------------|---------------|------|------------|-----------|---------|
| 8 B | 10.78M | 86 MB/s | 15.88M | 127 MB/s | **1.5×** | 4.35M | 35 MB/s | 0.40× | 4.14M | 33 MB/s | 0.38× |
| 32 B | 10.50M | 336 MB/s | 14.51M | 464 MB/s | **1.4×** | 4.26M | 136 MB/s | 0.41× | 3.43M | 110 MB/s | 0.33× |
| 128 B | 3.10M | 397 MB/s | 12.26M | 1.6 GB/s | **4.0×** | 4.20M | 538 MB/s | **1.4×** | 4.14M | 530 MB/s | **1.3×** |
| 512 B | 2.90M | 1.5 GB/s | 11.88M | 6.1 GB/s | **4.1×** | 4.26M | 2.2 GB/s | **1.5×** | 4.18M | 2.1 GB/s | **1.4×** |
| 2 KiB | 1.90M | 3.9 GB/s | 12.02M | 24.6 GB/s | **6.3×** | 4.41M | 9.0 GB/s | **2.3×** | 4.11M | 8.4 GB/s | **2.2×** |
| 8 KiB | 1.78M | 14.5 GB/s | 12.17M | 99.7 GB/s | **6.9×** | 4.40M | 36.1 GB/s | **2.5×** | 4.15M | 34.0 GB/s | **2.3×** |
| 32 KiB | 397k | 13.0 GB/s | 11.24M | 368.2 GB/s | **28.3×** | 4.45M | 145.7 GB/s | **11.2×** | 4.23M | 138.7 GB/s | **10.7×** |
| 128 KiB | 236k | 30.9 GB/s | 12.10M | 1586.3 GB/s | **51.3×** | 4.19M | 549.8 GB/s | **17.8×** | 3.93M | 515.7 GB/s | **16.7×** |
| 512 KiB | 55k | 28.6 GB/s | 11.79M | 6183.8 GB/s | **216.4×** | 4.21M | 2206.7 GB/s | **77.2×** | 3.95M | 2070.6 GB/s | **72.5×** |
| 2 MiB | 13k | 28.2 GB/s | 11.88M | 24908.3 GB/s | **883.3×** | 4.39M | 9203.6 GB/s | **326.4×** | 3.66M | 7677.1 GB/s | **272.2×** |
| 8 MiB | 1.3k | 11.0 GB/s | 12.23M | 102606.4 GB/s | **9322.9×** | 4.28M | 35937.7 GB/s | **3265.3×** | 3.86M | 32375.7 GB/s | **2941.7×** |
| 32 MiB | 67 | 2.3 GB/s | 12.29M | 412350.6 GB/s | **183418.0×** | 4.47M | 149830.3 GB/s | **66646.1×** | 4.16M | 139573.7 GB/s | **62083.9×** |

<!-- END libzmq_comparison_inproc -->

## libzmq vs omq — IPC

Push binds, pull connects. Abstract-namespace Unix-domain socket.
libzmq peer: minimal C binary, system libzmq 5.2.5.
omq-compio peer: single-threaded (io_uring). omq-tokio peer: multi-thread runtime.
Refresh: `./scripts/compare_libzmq.sh --ipc --update-benchmarks`

<!-- BEGIN libzmq_comparison_ipc -->
| Size | libzmq msg/s | libzmq MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|
| 8 B | 8.79M | 70 MB/s | 8.46M | 68 MB/s | 0.96× | 4.31M | 34 MB/s | 0.49× |
| 32 B | 7.89M | 252 MB/s | 7.27M | 233 MB/s | 0.92× | 4.06M | 130 MB/s | 0.51× |
| 128 B | 3.07M | 393 MB/s | 5.03M | 644 MB/s | **1.6×** | 5.47M | 700 MB/s | **1.8×** |
| 512 B | 2.35M | 1.2 GB/s | 3.45M | 1.8 GB/s | **1.5×** | 3.73M | 1.9 GB/s | **1.6×** |
| 2 KiB | 783k | 1.6 GB/s | 1.97M | 4.0 GB/s | **2.5×** | 1.30M | 2.7 GB/s | **1.7×** |
| 8 KiB | 249k | 2.0 GB/s | 778k | 6.4 GB/s | **3.1×** | 466k | 3.8 GB/s | **1.9×** |
| 32 KiB | 104k | 3.4 GB/s | 187k | 6.1 GB/s | **1.8×** | 117k | 3.8 GB/s | **1.1×** |
| 128 KiB | 36k | 4.7 GB/s | 62k | 8.1 GB/s | **1.7×** | 56k | 7.3 GB/s | **1.5×** |
| 512 KiB | 11k | 6.0 GB/s | 21k | 11.2 GB/s | **1.9×** | 10k | 5.3 GB/s | 0.89× |
| 2 MiB | 2.9k | 6.1 GB/s | 5.5k | 11.5 GB/s | **1.9×** | 4.3k | 8.9 GB/s | **1.5×** |
| 8 MiB | 659 | 5.5 GB/s | 1.1k | 9.6 GB/s | **1.7×** | 736 | 6.2 GB/s | **1.1×** |
| 32 MiB | 106 | 3.6 GB/s | 169 | 5.7 GB/s | **1.6×** | 179 | 6.0 GB/s | **1.7×** |

<!-- END libzmq_comparison_ipc -->

## libzmq vs omq — TCP

Push binds, pull connects. Each process pinned to one core.
libzmq peer: minimal C binary, system libzmq 5.2.5.
omq-compio peer: single-threaded (io_uring). omq-tokio peer: multi-thread runtime.
Refresh: `./scripts/compare_libzmq.sh --tcp --update-benchmarks`

<!-- BEGIN libzmq_comparison_tcp -->
| Size | libzmq msg/s | libzmq MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|
| 8 B | 8.44M | 68 MB/s | 8.72M | 70 MB/s | 1.03× | 4.90M | 39 MB/s | 0.58× |
| 32 B | 8.45M | 270 MB/s | 7.13M | 228 MB/s | 0.84× | 4.69M | 150 MB/s | 0.55× |
| 128 B | 2.92M | 374 MB/s | 5.35M | 684 MB/s | **1.8×** | 5.40M | 691 MB/s | **1.8×** |
| 512 B | 1.99M | 1.0 GB/s | 3.55M | 1.8 GB/s | **1.8×** | 3.85M | 2.0 GB/s | **1.9×** |
| 2 KiB | 653k | 1.3 GB/s | 1.74M | 3.6 GB/s | **2.7×** | 1.41M | 2.9 GB/s | **2.2×** |
| 8 KiB | 188k | 1.5 GB/s | 607k | 5.0 GB/s | **3.2×** | 461k | 3.8 GB/s | **2.5×** |
| 32 KiB | 75k | 2.5 GB/s | 175k | 5.7 GB/s | **2.3×** | 150k | 4.9 GB/s | **2.0×** |
| 128 KiB | 31k | 4.0 GB/s | 65k | 8.5 GB/s | **2.1×** | 43k | 5.7 GB/s | **1.4×** |
| 512 KiB | 10k | 5.3 GB/s | 17k | 8.8 GB/s | **1.7×** | 14k | 7.5 GB/s | **1.4×** |
| 2 MiB | 2.7k | 5.6 GB/s | 3.8k | 7.9 GB/s | **1.4×** | 3.0k | 6.3 GB/s | **1.1×** |
| 8 MiB | 609 | 5.1 GB/s | 758 | 6.4 GB/s | **1.2×** | 659 | 5.5 GB/s | 1.08× |
| 32 MiB | 120 | 4.0 GB/s | 121 | 4.1 GB/s | 1.01× | 152 | 5.1 GB/s | **1.3×** |

<!-- END libzmq_comparison_tcp -->

> **zmq.rs inproc:** zeromq 0.6 does not implement the inproc transport, so no zmq.rs vs omq inproc comparison is available. See the libzmq vs omq — inproc table above for omq's inproc numbers against a reference implementation.

## zmq.rs vs omq — IPC

Push binds, pull connects. zmq.rs peer uses a socket file; omq peers use abstract-namespace sockets.
zmq.rs peer: `scripts/zmqrs_bench_peer/` (zeromq crate, tokio multi-thread runtime).
omq-compio peer: single io_uring runtime on one core. omq-tokio peer: multi-thread runtime.
Refresh: `./scripts/compare_zmqrs.sh --ipc --update-benchmarks`

<!-- BEGIN zmqrs_comparison_ipc -->
| Size | zmq.rs msg/s | zmq.rs MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|
| 8 B | 741k | 6 MB/s | 8.25M | 66 MB/s | **11.1×** | 4.34M | 35 MB/s | **5.9×** |
| 32 B | 742k | 24 MB/s | 7.01M | 224 MB/s | **9.4×** | 4.15M | 133 MB/s | **5.6×** |
| 128 B | 741k | 95 MB/s | 4.96M | 634 MB/s | **6.7×** | 5.02M | 642 MB/s | **6.8×** |
| 512 B | 677k | 347 MB/s | 3.51M | 1.8 GB/s | **5.2×** | 3.90M | 2.0 GB/s | **5.8×** |
| 2 KiB | 619k | 1.3 GB/s | 2.05M | 4.2 GB/s | **3.3×** | 1.21M | 2.5 GB/s | **2.0×** |
| 8 KiB | 380k | 3.1 GB/s | 731k | 6.0 GB/s | **1.9×** | 607k | 5.0 GB/s | **1.6×** |
| 32 KiB | 133k | 4.3 GB/s | 184k | 6.0 GB/s | **1.4×** | 131k | 4.3 GB/s | 0.99× |
| 128 KiB | 32k | 4.2 GB/s | 66k | 8.7 GB/s | **2.1×** | 54k | 7.1 GB/s | **1.7×** |
| 512 KiB | 8k | 4.2 GB/s | 21k | 11.3 GB/s | **2.7×** | 8k | 4.4 GB/s | 1.05× |
| 2 MiB | 2k | 3.6 GB/s | 6k | 11.7 GB/s | **3.3×** | 4k | 8.0 GB/s | **2.2×** |
| 8 MiB | 330 | 2.8 GB/s | 1k | 8.9 GB/s | **3.2×** | 633 | 5.3 GB/s | **1.9×** |
| 32 MiB | 94 | 3.2 GB/s | 164 | 5.5 GB/s | **1.7×** | 176 | 5.9 GB/s | **1.9×** |

<!-- END zmqrs_comparison_ipc -->

## zmq.rs vs omq — TCP

Push binds, pull connects.
zmq.rs peer: `scripts/zmqrs_bench_peer/` (zeromq crate, tokio multi-thread runtime).
omq-compio peer: single io_uring runtime on one core. omq-tokio peer: multi-thread runtime.
zmq.rs <-> omq-tokio is apples-to-apples; omq-compio is intentionally CPU-constrained (one core).
Refresh: `./scripts/compare_zmqrs.sh --tcp --update-benchmarks`

<!-- BEGIN zmqrs_comparison_tcp -->
| Size | zmq.rs msg/s | zmq.rs MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|
| 8 B | 483k | 4 MB/s | 8.59M | 69 MB/s | **17.8×** | 4.52M | 36 MB/s | **9.4×** |
| 32 B | 379k | 12 MB/s | 6.99M | 224 MB/s | **18.4×** | 4.47M | 143 MB/s | **11.8×** |
| 128 B | 342k | 44 MB/s | 5.21M | 667 MB/s | **15.2×** | 5.05M | 646 MB/s | **14.7×** |
| 512 B | 324k | 166 MB/s | 3.60M | 1.8 GB/s | **11.1×** | 4.16M | 2.1 GB/s | **12.8×** |
| 2 KiB | 295k | 604 MB/s | 1.65M | 3.4 GB/s | **5.6×** | 1.57M | 3.2 GB/s | **5.3×** |
| 8 KiB | 238k | 1.9 GB/s | 567k | 4.6 GB/s | **2.4×** | 470k | 3.9 GB/s | **2.0×** |
| 32 KiB | 128k | 4.2 GB/s | 175k | 5.7 GB/s | **1.4×** | 156k | 5.1 GB/s | **1.2×** |
| 128 KiB | 32k | 4.2 GB/s | 60k | 7.9 GB/s | **1.9×** | 43k | 5.6 GB/s | **1.3×** |
| 512 KiB | 8k | 4.0 GB/s | 16k | 8.6 GB/s | **2.2×** | 14k | 7.2 GB/s | **1.8×** |
| 2 MiB | 1k | 3.1 GB/s | 4k | 7.7 GB/s | **2.5×** | 3k | 6.5 GB/s | **2.1×** |
| 8 MiB | 292 | 2.4 GB/s | 661 | 5.5 GB/s | **2.3×** | 601 | 5.0 GB/s | **2.1×** |
| 32 MiB | 105 | 3.5 GB/s | 117 | 3.9 GB/s | **1.1×** | 144 | 4.8 GB/s | **1.4×** |

<!-- END zmqrs_comparison_tcp -->

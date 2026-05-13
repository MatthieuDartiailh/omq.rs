# Comparisons

Two-process benchmarks (inproc: single-process). 3 s timed window after 500 ms warmup.
Hardware: Linux 6.12 (Debian 13) VM, Intel i7-8700B 3.2 GHz 6-core, Rust 1.95.0.

## libzmq vs omq — inproc

Push and pull run in the same process; no kernel socket overhead.
libzmq peer: minimal C binary, system libzmq 5.2.5.
omq-compio peer: single-threaded (io_uring). omq-tokio peer: multi-thread runtime.
Refresh: `./scripts/compare_libzmq.sh --inproc --update-benchmarks`

<!-- BEGIN libzmq_comparison_inproc -->
| Size | libzmq msg/s | libzmq MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|
| 8 B | 10.49M | 84 MB/s | 4.11M | 33 MB/s | 0.39× | 1.41M | 11 MB/s | 0.13× |
| 32 B | 9.95M | 318 MB/s | 4.08M | 131 MB/s | 0.41× | 1.43M | 46 MB/s | 0.14× |
| 128 B | 3.00M | 384 MB/s | 4.36M | 559 MB/s | **1.5×** | 1.40M | 180 MB/s | 0.47× |
| 512 B | 2.73M | 1.4 GB/s | 4.33M | 2.2 GB/s | **1.6×** | 1.37M | 702 MB/s | 0.50× |
| 2 KiB | 2.08M | 4.3 GB/s | 4.24M | 8.7 GB/s | **2.0×** | 1.38M | 2.8 GB/s | 0.66× |
| 8 KiB | 1.85M | 15.2 GB/s | 4.20M | 34.4 GB/s | **2.3×** | 1.43M | 11.8 GB/s | 0.77× |
| 32 KiB | 885k | 29.0 GB/s | 4.36M | 142.8 GB/s | **4.9×** | 1.37M | 44.8 GB/s | **1.5×** |
| 128 KiB | 247k | 32.3 GB/s | 4.25M | 557.3 GB/s | **17.2×** | 997k | 130.7 GB/s | **4.0×** |
| 512 KiB | 57k | 29.9 GB/s | 4.10M | 2151.4 GB/s | **71.9×** | 467k | 245.0 GB/s | **8.2×** |
| 2 MiB | 15k | 31.3 GB/s | 4.25M | 8918.5 GB/s | **285.3×** | 463k | 970.4 GB/s | **31.0×** |
| 8 MiB | 1k | 11.0 GB/s | 4.26M | 35713.3 GB/s | **3240.0×** | 475k | 3986.8 GB/s | **361.7×** |
| 32 MiB | 69 | 2.3 GB/s | 4.36M | 146420.2 GB/s | **63241.5×** | 479k | 16087.7 GB/s | **6948.6×** |

<!-- END libzmq_comparison_inproc -->

## libzmq vs omq — IPC

Push binds, pull connects. Abstract-namespace Unix-domain socket.
libzmq peer: minimal C binary, system libzmq 5.2.5.
omq-compio peer: single-threaded (io_uring). omq-tokio peer: multi-thread runtime.
Refresh: `./scripts/compare_libzmq.sh --ipc --update-benchmarks`

<!-- BEGIN libzmq_comparison_ipc -->
| Size | libzmq msg/s | libzmq MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|
| 8 B | 8.58M | 69 MB/s | 7.69M | 62 MB/s | 0.90× | 4.10M | 33 MB/s | 0.48× |
| 32 B | 8.49M | 272 MB/s | 7.05M | 226 MB/s | 0.83× | 4.49M | 144 MB/s | 0.53× |
| 128 B | 2.97M | 380 MB/s | 4.81M | 616 MB/s | **1.6×** | 5.29M | 677 MB/s | **1.8×** |
| 512 B | 2.38M | 1.2 GB/s | 3.44M | 1.8 GB/s | **1.4×** | 3.99M | 2.0 GB/s | **1.7×** |
| 2 KiB | 792k | 1.6 GB/s | 2.03M | 4.2 GB/s | **2.6×** | 1.32M | 2.7 GB/s | **1.7×** |
| 8 KiB | 237k | 1.9 GB/s | 746k | 6.1 GB/s | **3.2×** | 449k | 3.7 GB/s | **1.9×** |
| 32 KiB | 104k | 3.4 GB/s | 176k | 5.8 GB/s | **1.7×** | 133k | 4.3 GB/s | **1.3×** |
| 128 KiB | 35k | 4.6 GB/s | 64k | 8.4 GB/s | **1.8×** | 53k | 7.0 GB/s | **1.5×** |
| 512 KiB | 12k | 6.1 GB/s | 21k | 10.9 GB/s | **1.8×** | 12k | 6.1 GB/s | 0.99× |
| 2 MiB | 3k | 6.0 GB/s | 6k | 11.8 GB/s | **2.0×** | 3k | 6.7 GB/s | **1.1×** |
| 8 MiB | 644 | 5.4 GB/s | 1k | 8.6 GB/s | **1.6×** | 617 | 5.2 GB/s | 0.96× |
| 32 MiB | 87 | 2.9 GB/s | 163 | 5.5 GB/s | **1.9×** | 159 | 5.3 GB/s | **1.8×** |

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

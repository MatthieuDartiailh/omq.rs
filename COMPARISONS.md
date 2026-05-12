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
| 8 B | 8.63M | 69 MB/s | 8.50M | 68 MB/s | 0.99× | 5.23M | 42 MB/s | 0.61× |
| 32 B | 7.61M | 244 MB/s | 7.00M | 224 MB/s | 0.92× | 4.81M | 154 MB/s | 0.63× |
| 128 B | 2.85M | 365 MB/s | 5.08M | 650 MB/s | **1.8×** | 5.16M | 660 MB/s | **1.8×** |
| 512 B | 2.03M | 1.0 GB/s | 3.40M | 1.7 GB/s | **1.7×** | 4.01M | 2.1 GB/s | **2.0×** |
| 2 KiB | 674k | 1.4 GB/s | 1.76M | 3.6 GB/s | **2.6×** | 1.11M | 2.3 GB/s | **1.6×** |
| 8 KiB | 192k | 1.6 GB/s | 611k | 5.0 GB/s | **3.2×** | 499k | 4.1 GB/s | **2.6×** |
| 32 KiB | 73k | 2.4 GB/s | 165k | 5.4 GB/s | **2.3×** | 155k | 5.1 GB/s | **2.1×** |
| 128 KiB | 31k | 4.1 GB/s | 65k | 8.6 GB/s | **2.1×** | 43k | 5.7 GB/s | **1.4×** |
| 512 KiB | 10k | 5.2 GB/s | 16k | 8.3 GB/s | **1.6×** | 14k | 7.5 GB/s | **1.4×** |
| 2 MiB | 3k | 5.5 GB/s | 4k | 8.1 GB/s | **1.5×** | 3k | 6.7 GB/s | **1.2×** |
| 8 MiB | 587 | 4.9 GB/s | 723 | 6.1 GB/s | **1.2×** | 618 | 5.2 GB/s | 1.05× |
| 32 MiB | 138 | 4.6 GB/s | 119 | 4.0 GB/s | 0.86× | 150 | 5.0 GB/s | 1.09× |

<!-- END libzmq_comparison -->

## libzmq vs omq — IPC

Push binds, pull connects, Unix-domain socket loopback.
libzmq peer: minimal C binary, system libzmq 5.2.5.
omq-compio peer: single-threaded (io_uring). omq-tokio peer: multi-thread runtime.
Refresh: `./scripts/compare_libzmq.sh --ipc --update-benchmarks`

<!-- BEGIN libzmq_comparison_ipc -->
| Size | libzmq msg/s | libzmq MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|
| 8 B | 8.31M | 66 MB/s | 8.14M | 65 MB/s | 0.98× | 4.06M | 32 MB/s | 0.49× |
| 32 B | 8.19M | 262 MB/s | 6.83M | 219 MB/s | 0.83× | 4.09M | 131 MB/s | 0.50× |
| 128 B | 3.06M | 391 MB/s | 4.81M | 616 MB/s | **1.6×** | 5.25M | 672 MB/s | **1.7×** |
| 512 B | 2.42M | 1.2 GB/s | 3.43M | 1.8 GB/s | **1.4×** | 4.04M | 2.1 GB/s | **1.7×** |
| 2 KiB | 795k | 1.6 GB/s | 1.95M | 4.0 GB/s | **2.5×** | 1.40M | 2.9 GB/s | **1.8×** |
| 8 KiB | 252k | 2.1 GB/s | 746k | 6.1 GB/s | **3.0×** | 459k | 3.8 GB/s | **1.8×** |
| 32 KiB | 104k | 3.4 GB/s | 177k | 5.8 GB/s | **1.7×** | 138k | 4.5 GB/s | **1.3×** |
| 128 KiB | 35k | 4.6 GB/s | 65k | 8.6 GB/s | **1.9×** | 53k | 7.0 GB/s | **1.5×** |
| 512 KiB | 12k | 6.3 GB/s | 21k | 11.0 GB/s | **1.8×** | 11k | 5.8 GB/s | 0.92× |
| 2 MiB | 3k | 6.3 GB/s | 6k | 12.0 GB/s | **1.9×** | 4k | 8.1 GB/s | **1.3×** |
| 8 MiB | 673 | 5.6 GB/s | 1k | 8.9 GB/s | **1.6×** | 652 | 5.5 GB/s | 0.97× |
| 32 MiB | 86 | 2.9 GB/s | 163 | 5.5 GB/s | **1.9×** | 177 | 5.9 GB/s | **2.1×** |

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
| 8 B | 512k | 4 MB/s | 8.41M | 67 MB/s | **16.4×** | 4.84M | 39 MB/s | **9.5×** |
| 32 B | 385k | 12 MB/s | 7.24M | 232 MB/s | **18.8×** | 4.60M | 147 MB/s | **11.9×** |
| 128 B | 338k | 43 MB/s | 5.13M | 657 MB/s | **15.2×** | 5.23M | 670 MB/s | **15.5×** |
| 512 B | 308k | 158 MB/s | 3.44M | 1.8 GB/s | **11.2×** | 4.48M | 2.3 GB/s | **14.5×** |
| 2 KiB | 282k | 578 MB/s | 1.72M | 3.5 GB/s | **6.1×** | 1.45M | 3.0 GB/s | **5.1×** |
| 8 KiB | 231k | 1.9 GB/s | 609k | 5.0 GB/s | **2.6×** | 503k | 4.1 GB/s | **2.2×** |
| 32 KiB | 132k | 4.3 GB/s | 179k | 5.9 GB/s | **1.4×** | 147k | 4.8 GB/s | **1.1×** |
| 128 KiB | 33k | 4.3 GB/s | 65k | 8.5 GB/s | **2.0×** | 43k | 5.7 GB/s | **1.3×** |
| 512 KiB | 8k | 4.1 GB/s | 16k | 8.2 GB/s | **2.0×** | 14k | 7.5 GB/s | **1.8×** |
| 2 MiB | 2k | 3.2 GB/s | 4k | 7.7 GB/s | **2.4×** | 3k | 6.5 GB/s | **2.0×** |
| 8 MiB | 296 | 2.5 GB/s | 702 | 5.9 GB/s | **2.4×** | 619 | 5.2 GB/s | **2.1×** |
| 32 MiB | 107 | 3.6 GB/s | 120 | 4.0 GB/s | **1.1×** | 150 | 5.0 GB/s | **1.4×** |

<!-- END zmqrs_comparison -->

## zmq.rs vs omq — IPC

Push binds, pull connects, Unix-domain socket loopback.
zmq.rs peer: `scripts/zmqrs_bench_peer/` (zeromq crate, tokio multi-thread runtime).
omq-compio peer: single io_uring runtime on one core. omq-tokio peer: multi-thread runtime.
Refresh: `./scripts/compare_zmqrs.sh --ipc --update-benchmarks`

<!-- BEGIN zmqrs_comparison_ipc -->
| Size | zmq.rs msg/s | zmq.rs MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|
| 8 B | 734k | 6 MB/s | 8.14M | 65 MB/s | **11.1×** | 4.16M | 33 MB/s | **5.7×** |
| 32 B | 726k | 23 MB/s | 6.97M | 223 MB/s | **9.6×** | 4.17M | 134 MB/s | **5.7×** |
| 128 B | 725k | 93 MB/s | 4.68M | 599 MB/s | **6.5×** | 5.10M | 653 MB/s | **7.0×** |
| 512 B | 690k | 353 MB/s | 3.43M | 1.8 GB/s | **5.0×** | 4.23M | 2.2 GB/s | **6.1×** |
| 2 KiB | 591k | 1.2 GB/s | 2.05M | 4.2 GB/s | **3.5×** | 1.34M | 2.7 GB/s | **2.3×** |
| 8 KiB | 367k | 3.0 GB/s | 748k | 6.1 GB/s | **2.0×** | 458k | 3.8 GB/s | **1.3×** |
| 32 KiB | 127k | 4.2 GB/s | 183k | 6.0 GB/s | **1.4×** | 126k | 4.1 GB/s | 1.00× |
| 128 KiB | 31k | 4.1 GB/s | 66k | 8.6 GB/s | **2.1×** | 54k | 7.0 GB/s | **1.7×** |
| 512 KiB | 8k | 4.0 GB/s | 22k | 11.7 GB/s | **2.9×** | 5k | 2.5 GB/s | 0.64× |
| 2 MiB | 2k | 3.6 GB/s | 6k | 11.8 GB/s | **3.3×** | 1k | 3.0 GB/s | 0.83× |
| 8 MiB | 341 | 2.9 GB/s | 1k | 9.6 GB/s | **3.3×** | 684 | 5.7 GB/s | **2.0×** |
| 32 MiB | 93 | 3.1 GB/s | 164 | 5.5 GB/s | **1.8×** | 181 | 6.1 GB/s | **1.9×** |

<!-- END zmqrs_comparison_ipc -->

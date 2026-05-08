# Comparisons

Two-process TCP benchmarks against libzmq and zmq.rs. Each cell: 3 s timed
window after 500 ms warmup. Hardware: Linux 6.12 (Debian 13) VM on an Intel
Mac Mini 2018 (i7-8700B, 3.2 GHz, 6 vCPU), Rust 1.95.0.

## libzmq vs omq-compio vs omq-tokio (two-process TCP, one core each)

Two separate processes on the same machine, each pinned to one core.
`bench_peer push` binds a TCP port and sends forever; `bench_peer pull`
connects, warms up for 500 ms, then counts for 3 seconds. The libzmq
peer is a minimal C binary compiled against the system libzmq (5.2.5).

The omq process is single-threaded (push loop + driver share one
compio runtime). libzmq spawns a dedicated I/O thread alongside the
app thread - two threads vs. one, which gives libzmq a small edge
at small messages where the app loop and I/O thread can overlap.
omq's advantage at large messages comes from `write_vectored` batching
multi-chunk frames in a single `writev` call, while libzmq issues
separate `send()` calls for the frame header and each payload segment.

<!-- BEGIN libzmq_comparison -->
| Size | libzmq msg/s | libzmq MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|
| 8 B | 8.46M | 68 MB/s | 7.47M | 60 MB/s | 0.88× | 7.04M | 56 MB/s | 0.83× |
| 32 B | 8.69M | 278 MB/s | 6.34M | 203 MB/s | 0.73× | 6.24M | 200 MB/s | 0.72× |
| 128 B | 2.95M | 377 MB/s | 4.81M | 616 MB/s | **1.6×** | 5.01M | 641 MB/s | **1.7×** |
| 512 B | 1.93M | 988 MB/s | 3.39M | 1.7 GB/s | **1.8×** | 3.74M | 1.9 GB/s | **1.9×** |
| 2 KiB | 679k | 1.4 GB/s | 1.70M | 3.5 GB/s | **2.5×** | 1.37M | 2.8 GB/s | **2.0×** |
| 8 KiB | 190k | 1.6 GB/s | 604k | 4.9 GB/s | **3.2×** | 496k | 4.1 GB/s | **2.6×** |
| 32 KiB | 75k | 2.4 GB/s | 172k | 5.6 GB/s | **2.3×** | 149k | 4.9 GB/s | **2.0×** |
| 128 KiB | 31k | 4.0 GB/s | 63k | 8.3 GB/s | **2.1×** | 42k | 5.5 GB/s | **1.4×** |
| 512 KiB | 10k | 5.4 GB/s | 11k | 5.8 GB/s | 1.07× | 14k | 7.3 GB/s | **1.4×** |
| 2 MiB | 3k | 5.6 GB/s | 3k | 6.3 GB/s | **1.1×** | 3k | 6.8 GB/s | **1.2×** |
| 8 MiB | 598 | 5.0 GB/s | 656 | 5.5 GB/s | 1.10× | 762 | 6.4 GB/s | **1.3×** |
| 32 MiB | 140 | 4.7 GB/s | 136 | 4.6 GB/s | 0.97× | 163 | 5.5 GB/s | **1.2×** |

<!-- END libzmq_comparison -->

At 128 B, omq-compio is ~13% slower than libzmq (libzmq overlaps its
app thread and a dedicated I/O thread); at 512 B they are at parity;
beyond that omq pulls ahead by 2-3×. The crossover is around 512 B —
roughly where `write_vectored` batching of multi-chunk frames pays off
vs. libzmq's separate `send()` per frame segment. Run
`./scripts/compare_libzmq.sh --update-benchmarks` to refresh this table.

## zmq.rs vs omq-compio vs omq-tokio (two-process TCP)

Two separate processes on the same machine, TCP loopback. `bench_peer
push` binds and sends forever; `bench_peer pull` connects, warms up for
500 ms, then counts for 3 seconds. The zmq.rs peer is built from
`scripts/zmqrs_bench_peer/` (zeromq crate, tokio runtime); the
omq-tokio peer is `omq-tokio/src/bin/bench_peer_tokio.rs`; the
omq-compio peer is the same binary used in the libzmq comparison above.

**Threading model is asymmetric, by design:**

- **omq-compio is single-threaded** — one io_uring runtime on one core.
  Multi-core deployments instantiate one runtime per worker thread
  (typically pinned via `RuntimeBuilder::thread_affinity`); this bench
  is one runtime, so the compio column is "what one core can do."
- **omq-tokio uses the multi-thread tokio runtime** — work-stealing
  across all cores. zmq.rs (also tokio-based) does the same. So the
  zmq.rs ↔ omq-tokio comparison is apples-to-apples (same runtime,
  same thread model), while the omq-compio column is intentionally
  CPU-constrained.

Unlike libzmq (which spawns a dedicated I/O thread alongside the app
thread), zmq.rs drives I/O on the same tokio executor as the send/recv
loop — structurally closer to omq-tokio than to libzmq.

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

Run `./scripts/compare_zmqrs.sh --update-benchmarks` to populate
this table. Requires Rust toolchain; no system packages needed (zeromq
is pure Rust).

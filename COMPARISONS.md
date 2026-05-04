# Comparisons

Two-process TCP benchmarks against libzmq and zmq.rs. Each cell: 3 s timed
window after 500 ms warmup. Hardware: Linux 6.12 (Debian 13) VM on an Intel
Mac Mini 2018 (i7-8700B, 3.2 GHz, 6 vCPU), Rust 1.95.0.

## libzmq vs omq-compio (two-process TCP, one core each)

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
| Size | omq msg/s | omq MB/s | zmq msg/s | zmq MB/s | ratio |
|-------|-----------|----------|-----------|----------|-------|
| 128 B | 3.24M | 415 MB/s | 3.00M | 384 MB/s | 1.08× |
| 512 B | 2.40M | 1.2 GB/s | 1.96M | 1.0 GB/s | **1.2×** |
| 2 KiB | 1.53M | 3.1 GB/s | 686k | 1.4 GB/s | **2.2×** |
| 8 KiB | 515k | 4.2 GB/s | 187k | 1.5 GB/s | **2.8×** |
| 32 KiB | 177k | 5.8 GB/s | 77k | 2.5 GB/s | **2.3×** |
| 128 KiB | 48k | 6.3 GB/s | 34k | 4.5 GB/s | **1.4×** |

<!-- END libzmq_comparison -->

At 128 B, omq-compio is ~13% slower than libzmq (libzmq overlaps its
app thread and a dedicated I/O thread); at 512 B they are at parity;
beyond that omq pulls ahead by 2-3×. The crossover is around 512 B —
roughly where `write_vectored` batching of multi-chunk frames pays off
vs. libzmq's separate `send()` per frame segment. Run
`./scripts/compare_libzmq.sh --update-benchmarks` to refresh this table.

## zmq.rs vs omq-tokio vs omq-compio (two-process TCP)

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
| Size | zmq.rs msg/s | zmq.rs MB/s | omq-tokio msg/s | omq-tokio MB/s | tokio × | omq-compio msg/s | omq-compio MB/s | compio × |
|-------|-------------|------------|----------------|---------------|---------|-----------------|----------------|---------|
| 128 B | 307k | 39 MB/s | 5.15M | 660 MB/s | **16.8×** | 3.24M | 414 MB/s | **10.5×** |
| 512 B | 280k | 143 MB/s | 3.40M | 1.7 GB/s | **12.1×** | 2.36M | 1.2 GB/s | **8.4×** |
| 2 KiB | 271k | 555 MB/s | 1.91M | 3.9 GB/s | **7.0×** | 1.49M | 3.1 GB/s | **5.5×** |
| 8 KiB | 201k | 1.6 GB/s | 477k | 3.9 GB/s | **2.4×** | 586k | 4.8 GB/s | **2.9×** |
| 32 KiB | 130k | 4.2 GB/s | 148k | 4.9 GB/s | **1.1×** | 176k | 5.8 GB/s | **1.4×** |
| 128 KiB | 33k | 4.3 GB/s | 39k | 5.1 GB/s | **1.2×** | 46k | 6.0 GB/s | **1.4×** |

<!-- END zmqrs_comparison -->

Run `./scripts/compare_zmqrs.sh --update-benchmarks` to populate
this table. Requires Rust toolchain; no system packages needed (zeromq
is pure Rust).

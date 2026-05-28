# Comparisons

Two-process benchmarks (inproc: single-process). 3 s timed window after 500 ms warmup.
Hardware: Linux 6.12 (Debian 13) VM, Intel i7-8700B 3.2 GHz 6-core, Rust 1.95.0.
Compared against libzmq v4.3.5 and zmq.rs (zeromq crate v0.6.0).

<p align="center">
  <img src="doc/charts/comparison.svg" alt="PUSH/PULL throughput and REQ/REP latency: TCP loopback" width="850">
</p>

<p align="center">
  <img src="doc/charts/comparison_inproc.svg" alt="PUSH/PULL throughput and REQ/REP latency: inproc" width="850">
</p>

## libzmq vs omq — inproc

Same process, no kernel socket overhead. libzmq v4.3.5 (C binary) vs omq-compio (io_uring, single thread) and omq-tokio (multi-thread).

omq inproc is true zero-copy: payloads are `Arc`-cloned, not memcpy'd. libzmq copies every message through its internal queues, so its throughput drops with size. omq stays flat.

Refresh: `python3 scripts/run_comparisons.py --transport inproc --latency --update-markdown`

**omq-compio:**

<!-- BEGIN libzmq_comparison_inproc_compio -->
| Size | libzmq msg/s | libzmq MB/s | compio-mt msg/s | compio-mt MB/s | mt × | compio-st msg/s | compio-st MB/s | st × |
|-------|-------------|------------|----------------|---------------|------|----------------|---------------|------|
| 32 B | 10.39M | 332 MB/s | 14.66M | 469 MB/s | **1.4×** | 4.11M | 131 MB/s | 0.40× |
| 1 KiB | 2.28M | 2.3 GB/s | 11.46M | 11.7 GB/s | **5.0×** | 4.26M | 4.4 GB/s | **1.9×** |
| 4 KiB | 1.74M | 7.1 GB/s | 10.62M | 43.5 GB/s | **6.1×** | 4.19M | 17.2 GB/s | **2.4×** |

<!-- END libzmq_comparison_inproc_compio -->

**omq-tokio:**

<!-- BEGIN libzmq_comparison_inproc_tokio -->
| Size | libzmq msg/s | libzmq MB/s | tokio msg/s | tokio MB/s | tokio × |
|-------|-------------|------------|------------|-----------|----------|
| 32 B | 10.39M | 332 MB/s | 3.58M | 115 MB/s | 0.34× |
| 1 KiB | 2.28M | 2.3 GB/s | 4.17M | 4.3 GB/s | **1.8×** |
| 4 KiB | 1.74M | 7.1 GB/s | 4.08M | 16.7 GB/s | **2.3×** |

<!-- END libzmq_comparison_inproc_tokio -->

## libzmq vs omq — IPC

Abstract-namespace Unix socket. Push binds, pull connects. libzmq v4.3.5 (C binary) vs omq-compio (io_uring, single thread) and omq-tokio (multi-thread).

Refresh: `python3 scripts/run_comparisons.py --transport ipc --update-markdown`

**omq-compio:**

<!-- BEGIN libzmq_comparison_ipc_compio -->
| Size | libzmq msg/s | libzmq MB/s | omq-compio msg/s | omq-compio MB/s | compio × |
|-------|-------------|------------|-----------------|----------------|----------|
| 32 B | 8.10M | 259 MB/s | 6.14M | 197 MB/s | 0.76× |
| 1 KiB | 1.39M | 1.4 GB/s | 2.98M | 3.1 GB/s | **2.1×** |
| 4 KiB | 432k | 1.8 GB/s | 1.31M | 5.4 GB/s | **3.0×** |

<!-- END libzmq_comparison_ipc_compio -->

**omq-tokio:**

<!-- BEGIN libzmq_comparison_ipc_tokio -->
| Size | libzmq msg/s | libzmq MB/s | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|----------------|---------------|----------|
| 32 B | 8.10M | 259 MB/s | 6.57M | 210 MB/s | 0.81× |
| 1 KiB | 1.39M | 1.4 GB/s | 3.46M | 3.5 GB/s | **2.5×** |
| 4 KiB | 432k | 1.8 GB/s | 1.10M | 4.5 GB/s | **2.5×** |

<!-- END libzmq_comparison_ipc_tokio -->

## libzmq vs omq — TCP

TCP loopback, each process pinned to one core. Push binds, pull connects. libzmq v4.3.5 (C binary) vs omq-compio (io_uring, single thread) and omq-tokio (multi-thread).

Refresh: `python3 scripts/run_comparisons.py --transport tcp --update-markdown`

**omq-compio:**

<!-- BEGIN libzmq_comparison_tcp_compio -->
| Size | libzmq msg/s | libzmq MB/s | omq-compio msg/s | omq-compio MB/s | compio × |
|-------|-------------|------------|-----------------|----------------|----------|
| 32 B | 8.38M | 268 MB/s | 5.80M | 186 MB/s | 0.69× |
| 1 KiB | 1.13M | 1.2 GB/s | 2.60M | 2.7 GB/s | **2.3×** |
| 4 KiB | 352k | 1.4 GB/s | 1.10M | 4.5 GB/s | **3.1×** |

<!-- END libzmq_comparison_tcp_compio -->

**omq-tokio:**

<!-- BEGIN libzmq_comparison_tcp_tokio -->
| Size | libzmq msg/s | libzmq MB/s | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|----------------|---------------|----------|
| 32 B | 8.38M | 268 MB/s | 6.97M | 223 MB/s | 0.83× |
| 1 KiB | 1.13M | 1.2 GB/s | 3.77M | 3.9 GB/s | **3.3×** |
| 4 KiB | 352k | 1.4 GB/s | 1.20M | 4.9 GB/s | **3.4×** |

<!-- END libzmq_comparison_tcp_tokio -->

## libzmq vs omq — WebSocket

ZWS/2.0 (RFC 45) over TCP loopback. Push binds, pull connects. Requires libzmq built with WebSocket support (4.3.5+) and omq built with the `ws` feature.

Refresh: `python3 scripts/run_comparisons.py --transport ws --update-markdown`

**omq-compio:**

<!-- BEGIN libzmq_comparison_ws_compio -->
| Size | libzmq msg/s | libzmq MB/s | omq-compio msg/s | omq-compio MB/s | compio × |
|-------|-------------|------------|-----------------|----------------|----------|
| 32 B | 7.68M | 246 MB/s | 2.36M | 76 MB/s | 0.31× |
| 512 B | 1.95M | 1.0 GB/s | 2.02M | 1.0 GB/s | 1.03× |
| 8 KiB | 196k | 1.6 GB/s | 536k | 4.4 GB/s | **2.7×** |

<!-- END libzmq_comparison_ws_compio -->

**omq-tokio:**

<!-- BEGIN libzmq_comparison_ws_tokio -->
| Size | libzmq msg/s | libzmq MB/s | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|----------------|---------------|----------|
| 32 B | 7.68M | 246 MB/s | 3.91M | 125 MB/s | 0.51× |
| 512 B | 1.95M | 1.0 GB/s | 2.85M | 1.5 GB/s | **1.5×** |
| 8 KiB | 196k | 1.6 GB/s | 588k | 4.8 GB/s | **3.0×** |

<!-- END libzmq_comparison_ws_tokio -->

> **zmq.rs inproc:** zeromq 0.6 does not implement the inproc transport, so no zmq.rs vs omq inproc comparison is available. See the libzmq vs omq — inproc table above for omq's inproc numbers against a reference implementation.

## zmq.rs vs omq — IPC

Push binds, pull connects. zmq.rs uses a socket file; omq uses abstract-namespace sockets. zmq.rs peer: `scripts/zmqrs_bench_peer/` (zeromq crate, tokio multi-thread). omq-compio: single io_uring thread. omq-tokio: multi-thread.

Refresh: `python3 scripts/run_comparisons.py --transport ipc --update-markdown`

**omq-compio:**

<!-- BEGIN zmqrs_comparison_ipc_compio -->
| Size | zmq.rs msg/s | zmq.rs MB/s | omq-compio msg/s | omq-compio MB/s | compio × |
|-------|-------------|------------|-----------------|----------------|---------|
| 32 B | 724k | 23 MB/s | 7.34M | 235 MB/s | **10.1×** |
| 512 B | 701k | 359 MB/s | 3.36M | 1.7 GB/s | **4.8×** |
| 8 KiB | 374k | 3.1 GB/s | 725k | 5.9 GB/s | **1.9×** |

<!-- END zmqrs_comparison_ipc_compio -->

**omq-tokio:**

<!-- BEGIN zmqrs_comparison_ipc_tokio -->
| Size | zmq.rs msg/s | zmq.rs MB/s | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|----------------|---------------|---------|
| 32 B | 724k | 23 MB/s | 4.23M | 135 MB/s | **5.8×** |
| 512 B | 701k | 359 MB/s | 3.93M | 2.0 GB/s | **5.6×** |
| 8 KiB | 374k | 3.1 GB/s | 431k | 3.5 GB/s | **1.2×** |

<!-- END zmqrs_comparison_ipc_tokio -->

## zmq.rs vs omq — TCP

TCP loopback, push binds, pull connects. zmq.rs <-> omq-tokio is apples-to-apples (both tokio multi-thread). omq-compio is intentionally CPU-constrained (single io_uring thread).

Refresh: `python3 scripts/run_comparisons.py --transport tcp --update-markdown`

**omq-compio:**

<!-- BEGIN zmqrs_comparison_tcp_compio -->
| Size | zmq.rs msg/s | zmq.rs MB/s | omq-compio msg/s | omq-compio MB/s | compio × |
|-------|-------------|------------|-----------------|----------------|---------|
| 32 B | 385k | 12 MB/s | 7.01M | 224 MB/s | **18.2×** |
| 1 KiB | 308k | 316 MB/s | 2.63M | 2.7 GB/s | **8.5×** |
| 4 KiB | 269k | 1.1 GB/s | 1.13M | 4.6 GB/s | **4.2×** |

<!-- END zmqrs_comparison_tcp_compio -->

**omq-tokio:**

<!-- BEGIN zmqrs_comparison_tcp_tokio -->
| Size | zmq.rs msg/s | zmq.rs MB/s | omq-tokio msg/s | omq-tokio MB/s | tokio × |
|-------|-------------|------------|----------------|---------------|---------|
| 32 B | 385k | 12 MB/s | 5.07M | 162 MB/s | **13.2×** |
| 1 KiB | 308k | 316 MB/s | 3.25M | 3.3 GB/s | **10.5×** |
| 4 KiB | 269k | 1.1 GB/s | 1.22M | 5.0 GB/s | **4.5×** |

<!-- END zmqrs_comparison_tcp_tokio -->

## REQ/REP latency — libzmq vs omq

Serial ping-pong: one REQ/REP round-trip at a time, p50 and p99 in microseconds.
Lower is better; speedup = libzmq / omq.

### inproc

Refresh: `python3 scripts/run_comparisons.py --transport inproc --update-markdown`

<!-- BEGIN libzmq_latency_inproc -->
(run `python3 scripts/run_comparisons.py --transport inproc --update-markdown` to populate)
<!-- END libzmq_latency_inproc -->

### IPC

Refresh: `python3 scripts/run_comparisons.py --transport ipc --update-markdown`

<!-- BEGIN libzmq_latency_ipc -->
(run `python3 scripts/run_comparisons.py --transport ipc --update-markdown` to populate)
<!-- END libzmq_latency_ipc -->

### TCP

Refresh: `python3 scripts/run_comparisons.py --transport tcp --update-markdown`

<!-- BEGIN libzmq_latency_tcp -->
| Size | libzmq p50 | libzmq p99 | omq-compio p50 | omq-compio p99 | compio × | omq-tokio p50 | omq-tokio p99 | tokio × |
|-------|-----------|-----------|---------------|---------------|---------|--------------|--------------|--------|
| 32 B | 66.0 µs | 116 µs | 35.0 µs | 63.0 µs | **1.9×** | 54.0 µs | 85.1 µs | **1.2×** |
| 1 KiB | 75.6 µs | 125 µs | 34.8 µs | 68.8 µs | **2.2×** | 60.8 µs | 140 µs | **1.2×** |
| 4 KiB | 73.7 µs | 109 µs | 37.9 µs | 50.8 µs | **1.9×** | 56.8 µs | 78.6 µs | **1.3×** |

<!-- END libzmq_latency_tcp -->

### WebSocket

Refresh: `python3 scripts/run_comparisons.py --transport ws --update-markdown`

<!-- BEGIN libzmq_latency_ws -->
(run `python3 scripts/run_comparisons.py --transport ws --update-markdown` to populate)
<!-- END libzmq_latency_ws -->

## REQ/REP latency — zmq.rs vs omq

### IPC

Refresh: `python3 scripts/run_comparisons.py --transport ipc --update-markdown`

<!-- BEGIN zmqrs_latency_ipc -->
(run `python3 scripts/run_comparisons.py --transport ipc --update-markdown` to populate)
<!-- END zmqrs_latency_ipc -->

### TCP

Refresh: `python3 scripts/run_comparisons.py --transport tcp --update-markdown`

<!-- BEGIN zmqrs_latency_tcp -->
| Size | zmq.rs p50 | zmq.rs p99 | omq-compio p50 | omq-compio p99 | compio × | omq-tokio p50 | omq-tokio p99 | tokio × |
|-------|-----------|-----------|---------------|---------------|---------|--------------|--------------|--------|
| 32 B | 41.8 µs | 63.6 µs | 35.5 µs | 56.8 µs | **1.2×** | 80.7 µs | 97.9 µs | 0.52× |
| 1 KiB | 37.7 µs | 67.3 µs | 36.3 µs | 63.9 µs | 1.04× | 76.2 µs | 100 µs | 0.49× |
| 4 KiB | 39.8 µs | 61.1 µs | 37.9 µs | 60.7 µs | 1.05× | 80.8 µs | 101 µs | 0.49× |

<!-- END zmqrs_latency_tcp -->

## ZMQ_STREAM: omq-compio vs libzmq v4.3.5

Ping-pong throughput: one raw TCP client connected to a STREAM socket.
Each iteration sends one message and waits for the response before
sending the next (latency-bound, not pipelined). Single-threaded, TCP
loopback, release builds. 200K iterations at 8/128 B, 100K at 1K/8K B,
preceded by a 2K-iteration warmup.

The omq side uses omq-compio with io_uring and the default buffer pool.
The libzmq side uses its internal I/O thread. Both have `TCP_NODELAY`
on the raw client socket.

Measured 2026-05-21 on Linux 6.12, Rust 1.93 nightly, `gcc -O2` for the
libzmq harness. Two consecutive runs showed <5% variance.

### recv (raw TCP client writes, STREAM socket reads)

| Size | libzmq (msg/s) | omq (msg/s) | Ratio |
|------|---------------|------------|-------|
| 8 B | 42,000 | 134,000 | 3.2x |
| 128 B | 42,000 | 136,000 | 3.2x |
| 1,024 B | 43,000 | 135,000 | 3.1x |
| 8,192 B | 40,000 | 119,000 | 3.0x |

### send (STREAM socket writes, raw TCP client reads)

| Size | libzmq (msg/s) | omq (msg/s) | Ratio |
|------|---------------|------------|-------|
| 8 B | 42,000 | 151,000 | 3.6x |
| 128 B | 41,000 | 148,000 | 3.6x |
| 1,024 B | 39,000 | 149,000 | 3.8x |
| 8,192 B | 39,000 | 132,000 | 3.4x |

omq send at 8 KiB: 1.08 GB/s vs libzmq's 316 MB/s. Ping-pong
latency ~7 µs (omq) vs ~24 µs (libzmq)

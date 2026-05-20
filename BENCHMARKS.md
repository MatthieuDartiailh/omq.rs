# Benchmarks

Linux 6.12 (Debian 13) VM on an Intel Mac Mini 2018 (i7-8700B, 3.2 GHz
base, turbo disabled, governor=performance, 6 vCPU), Rust 1.95.0,
default features.

Each cell is the **min wall time** across multiple runs with warmup.
Sources: `omq-compio/benches/` and `omq-tokio/benches/`.

> **Compio bench topology.** `inproc`: single runtime, single thread
> (sender + receiver cooperatively scheduled). `inproc-mt`:
> multi-runtime inproc: PULL on its own thread/runtime, PUSHes on
> another. Wire transports (TCP/IPC): same multi-runtime shape as
> inproc-mt. omq-tokio uses a multi-thread runtime across all
> available cores throughout.

## PUSH/PULL throughput, single peer

Cells show `msgs/s / MB/s`.

**omq-compio:**

<!-- BEGIN push_pull_1peer_compio -->
| Size | inproc | inproc-mt | ipc | tcp |
|---|---|---|---|---|
| 32 B | 3.83M / 122 MB/s | 16.51M / 528 MB/s | 7.42M / 238 MB/s | 7.39M / 236 MB/s |
| 128 B | 3.76M / 481 MB/s | 17.13M / 2.19 GB/s | 4.79M / 613 MB/s | 5.18M / 663 MB/s |
| 512 B | 3.80M / 1.95 GB/s | 12.67M / 6.49 GB/s | 3.47M / 1.78 GB/s | 3.58M / 1.83 GB/s |
| 2 KiB | 3.79M / 7.76 GB/s | 14.32M / 29.3 GB/s | 1.98M / 4.05 GB/s | 1.80M / 3.69 GB/s |
| 8 KiB | 3.78M / 30.9 GB/s | 13.41M / 109.8 GB/s | 711k / 5.83 GB/s | 608k / 4.98 GB/s |
| 32 KiB | 3.79M / 124.1 GB/s | 14.75M / 483.4 GB/s | 179k / 5.87 GB/s | 174k / 5.69 GB/s |
| 128 KiB | 3.79M / 496.2 GB/s | 11.19M / 1466.2 GB/s | 57.3k / 7.51 GB/s | 58.3k / 7.64 GB/s |

<!-- END push_pull_1peer_compio -->

**omq-tokio:**

<!-- BEGIN push_pull_1peer_tokio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 4.56M / 146 MB/s | 3.78M / 121 MB/s | 4.38M / 140 MB/s |
| 128 B | 4.29M / 549 MB/s | 4.82M / 616 MB/s | 4.74M / 607 MB/s |
| 512 B | 3.84M / 1.96 GB/s | 2.44M / 1.25 GB/s | 3.76M / 1.93 GB/s |
| 2 KiB | 4.04M / 8.28 GB/s | 1.24M / 2.54 GB/s | 1.52M / 3.11 GB/s |
| 8 KiB | 4.05M / 33.2 GB/s | 447k / 3.66 GB/s | 491k / 4.02 GB/s |
| 32 KiB | 4.28M / 140.3 GB/s | 125k / 4.09 GB/s | 170k / 5.56 GB/s |
| 128 KiB | 4.15M / 543.9 GB/s | 32.4k / 4.25 GB/s | 35.0k / 4.58 GB/s |

<!-- END push_pull_1peer_tokio -->

Inproc "GB/s" at large payloads reflects zero-copy Arc-clone: no kernel
traversal.

## PUSH/PULL throughput, 8 peers

8 PUSH peers -> 1 PULL. Cells show `msgs/s / MB/s`.

**omq-compio:**

<!-- BEGIN push_pull_8peer_compio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 3.82M / 122 MB/s | 5.51M / 176 MB/s | 5.56M / 178 MB/s |
| 128 B | 3.56M / 455 MB/s | 3.89M / 498 MB/s | 3.91M / 501 MB/s |
| 512 B | 3.59M / 1.84 GB/s | 2.94M / 1.51 GB/s | 2.29M / 1.17 GB/s |
| 2 KiB | 3.74M / 7.66 GB/s | 1.44M / 2.95 GB/s | 1.32M / 2.70 GB/s |
| 8 KiB | 3.72M / 30.5 GB/s | 473k / 3.88 GB/s | 406k / 3.33 GB/s |
| 32 KiB | 3.78M / 123.8 GB/s | 153k / 5.03 GB/s | 114k / 3.73 GB/s |
| 128 KiB | 3.77M / 494.4 GB/s | 39.3k / 5.15 GB/s | 30.8k / 4.03 GB/s |

<!-- END push_pull_8peer_compio -->

**omq-tokio:**

<!-- BEGIN push_pull_8peer_tokio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 3.43M / 110 MB/s | 3.83M / 123 MB/s | 4.13M / 132 MB/s |
| 128 B | 3.46M / 443 MB/s | 3.73M / 478 MB/s | 3.02M / 386 MB/s |
| 512 B | 3.46M / 1.77 GB/s | 3.41M / 1.75 GB/s | 3.63M / 1.86 GB/s |
| 2 KiB | 3.48M / 7.12 GB/s | 2.05M / 4.20 GB/s | 1.72M / 3.52 GB/s |
| 8 KiB | 3.48M / 28.5 GB/s | 578k / 4.73 GB/s | 558k / 4.57 GB/s |
| 32 KiB | 3.57M / 117.1 GB/s | 162k / 5.32 GB/s | 191k / 6.27 GB/s |
| 128 KiB | 3.43M / 449.5 GB/s | 58.9k / 7.72 GB/s | 54.3k / 7.12 GB/s |

<!-- END push_pull_8peer_tokio -->

## REQ/REP latency (single peer)

Serial ping-pong: 1 000 warmup + 10 000 measured iterations per cell.
All values are wall time.

<!-- BEGIN latency_percentiles -->
| transport | size | compio p50 | compio p99 | tokio p50 | tokio p99 |
|---|---|---|---|---|---|
| inproc | 32 B | 2.61 µs | 2.68 µs | 24.8 µs | 78.7 µs |
| inproc | 64 B | 5.19 µs | 18.4 µs | 28.4 µs | 36.4 µs |
| inproc | 128 B | 2.64 µs | 2.71 µs | 23.9 µs | 74.9 µs |
| inproc | 256 B | 5.28 µs | 6.31 µs | 27.8 µs | 46.5 µs |
| inproc | 512 B | 2.61 µs | 2.68 µs | 25.2 µs | 78.8 µs |
| inproc | 1 KiB | 5.32 µs | 5.50 µs | 27.6 µs | 44.4 µs |
| inproc | 2 KiB | 2.61 µs | 2.68 µs | 27.1 µs | 80.6 µs |
| inproc | 4 KiB | 5.36 µs | 5.62 µs | 29.9 µs | 40.5 µs |
| inproc | 8 KiB | 2.63 µs | 2.73 µs | 27.1 µs | 80.9 µs |
| inproc | 32 KiB | 2.68 µs | 2.76 µs | 26.8 µs | 81.6 µs |
| inproc | 128 KiB | 2.67 µs | 2.80 µs | 24.8 µs | 81.9 µs |
| ipc | 32 B | 15.1 µs | 22.9 µs | 51.5 µs | 111 µs |
| ipc | 64 B | 21.8 µs | 31.0 µs | 62.5 µs | 861 µs |
| ipc | 128 B | 14.6 µs | 23.3 µs | 52.7 µs | 105 µs |
| ipc | 256 B | 22.6 µs | 31.7 µs | 63.7 µs | 77.0 µs |
| ipc | 512 B | 15.1 µs | 21.9 µs | 49.2 µs | 82.3 µs |
| ipc | 1 KiB | 22.9 µs | 32.3 µs | 64.4 µs | 861 µs |
| ipc | 2 KiB | 16.4 µs | 22.8 µs | 57.9 µs | 100 µs |
| ipc | 4 KiB | 24.9 µs | 44.4 µs | 64.0 µs | 80.0 µs |
| ipc | 8 KiB | 20.0 µs | 27.0 µs | 60.3 µs | 836 µs |
| ipc | 32 KiB | 26.1 µs | 35.2 µs | 69.8 µs | 112 µs |
| ipc | 128 KiB | 87.8 µs | 239 µs | 82.7 µs | 107 µs |
| tcp | 32 B | 22.4 µs | 30.8 µs | 62.9 µs | 107 µs |
| tcp | 64 B | 29.8 µs | 45.0 µs | 76.4 µs | 994 µs |
| tcp | 128 B | 22.2 µs | 32.5 µs | 61.1 µs | 110 µs |
| tcp | 256 B | 29.7 µs | 44.1 µs | 77.0 µs | 95.5 µs |
| tcp | 512 B | 22.3 µs | 30.0 µs | 63.9 µs | 114 µs |
| tcp | 1 KiB | 29.9 µs | 44.9 µs | 77.9 µs | 97.9 µs |
| tcp | 2 KiB | 23.5 µs | 31.1 µs | 68.8 µs | 115 µs |
| tcp | 4 KiB | 31.8 µs | 47.0 µs | 77.7 µs | 950 µs |
| tcp | 8 KiB | 26.7 µs | 40.5 µs | 66.1 µs | 115 µs |
| tcp | 32 KiB | 34.8 µs | 43.8 µs | 78.8 µs | 96.4 µs |
| tcp | 128 KiB | 203 µs | 251 µs | 115 µs | 135 µs |

<!-- END latency_percentiles -->

## REQ/REP throughput (single peer)

Cells show `msgs/s / MB/s`.

**omq-compio:**

<!-- BEGIN req_rep_compio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 410k / 13.1 MB/s | 69.4k / 2.22 MB/s | 46.3k / 1.48 MB/s |
| 128 B | 407k / 52.0 MB/s | 66.1k / 8.46 MB/s | 45.9k / 5.88 MB/s |
| 512 B | 386k / 198 MB/s | 64.4k / 33.0 MB/s | 43.4k / 22.2 MB/s |
| 2 KiB | 386k / 791 MB/s | 60.1k / 123 MB/s | 41.6k / 85.2 MB/s |
| 8 KiB | 387k / 3.17 GB/s | 49.1k / 402 MB/s | 37.5k / 307 MB/s |
| 32 KiB | 409k / 13.4 GB/s | 39.0k / 1.28 GB/s | 28.8k / 943 MB/s |
| 128 KiB | 408k / 53.4 GB/s | 6.1k / 793 MB/s | 8.5k / 1.11 GB/s |

<!-- END req_rep_compio -->

**omq-tokio:**

<!-- BEGIN req_rep_tokio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 36.7k / 1.17 MB/s | 15.6k / 0.50 MB/s | 16.3k / 0.52 MB/s |
| 128 B | 36.5k / 4.68 MB/s | 15.9k / 2.04 MB/s | 15.8k / 2.03 MB/s |
| 512 B | 36.8k / 18.9 MB/s | 18.1k / 9.25 MB/s | 15.9k / 8.13 MB/s |
| 2 KiB | 37.4k / 76.6 MB/s | 17.5k / 35.9 MB/s | 15.0k / 30.6 MB/s |
| 8 KiB | 37.0k / 304 MB/s | 16.7k / 137 MB/s | 14.3k / 117 MB/s |
| 32 KiB | 36.5k / 1.20 GB/s | 13.0k / 427 MB/s | 12.7k / 416 MB/s |
| 128 KiB | 37.4k / 4.90 GB/s | 11.1k / 1.46 GB/s | 8.8k / 1.15 GB/s |

<!-- END req_rep_tokio -->

## PUB/SUB throughput (3 peers)

1 PUB -> 3 SUB. Cells show `msgs/s / MB/s`.

**omq-compio:**

<!-- BEGIN pub_sub_compio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 1.24M / 39.8 MB/s | 1.43M / 45.8 MB/s | 1.42M / 45.5 MB/s |
| 128 B | 1.18M / 151 MB/s | 1.22M / 156 MB/s | 1.22M / 156 MB/s |
| 512 B | 1.16M / 595 MB/s | 1.01M / 515 MB/s | 995k / 510 MB/s |
| 2 KiB | 1.16M / 2.38 GB/s | 519k / 1.06 GB/s | 491k / 1.01 GB/s |
| 8 KiB | 1.18M / 9.66 GB/s | 179k / 1.47 GB/s | 163k / 1.33 GB/s |
| 32 KiB | 1.18M / 38.6 GB/s | 94.6k / 3.10 GB/s | 79.5k / 2.60 GB/s |
| 128 KiB | 1.16M / 151.8 GB/s | 24.7k / 3.24 GB/s | 21.7k / 2.85 GB/s |

<!-- END pub_sub_compio -->

**omq-tokio:**

<!-- BEGIN pub_sub_tokio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 1.33M / 42.7 MB/s | 1.73M / 55.3 MB/s | 1.67M / 53.4 MB/s |
| 128 B | 1.16M / 148 MB/s | 1.12M / 143 MB/s | 1.24M / 159 MB/s |
| 512 B | 1.32M / 674 MB/s | 1.30M / 667 MB/s | 1.09M / 556 MB/s |
| 2 KiB | 1.14M / 2.33 GB/s | 764k / 1.56 GB/s | 767k / 1.57 GB/s |
| 8 KiB | 1.27M / 10.4 GB/s | 401k / 3.29 GB/s | 335k / 2.74 GB/s |
| 32 KiB | 1.06M / 34.6 GB/s | 106k / 3.48 GB/s | 114k / 3.72 GB/s |
| 128 KiB | 638k / 83.6 GB/s | 34.8k / 4.57 GB/s | 7.9k / 1.03 GB/s |

<!-- END pub_sub_tokio -->

## ROUTER/DEALER throughput (3 peers)

3 DEALER -> 1 ROUTER. Cells show `msgs/s / MB/s`.

**omq-compio:**

<!-- BEGIN router_dealer_compio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 3.65M / 117 MB/s | 3.23M / 103 MB/s | 3.07M / 98.1 MB/s |
| 128 B | 3.77M / 483 MB/s | 2.74M / 351 MB/s | 2.58M / 330 MB/s |
| 512 B | 3.74M / 1.91 GB/s | 2.14M / 1.10 GB/s | 1.83M / 936 MB/s |
| 2 KiB | 3.59M / 7.35 GB/s | 1.27M / 2.59 GB/s | 1.14M / 2.33 GB/s |
| 8 KiB | 3.59M / 29.4 GB/s | 488k / 4.00 GB/s | 484k / 3.97 GB/s |
| 32 KiB | 3.80M / 124.4 GB/s | 164k / 5.38 GB/s | 116k / 3.79 GB/s |
| 128 KiB | 3.79M / 496.3 GB/s | 43.9k / 5.75 GB/s | 27.9k / 3.66 GB/s |

<!-- END router_dealer_compio -->

**omq-tokio:**

<!-- BEGIN router_dealer_tokio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 1.24M / 39.7 MB/s | 1.08M / 34.7 MB/s | 1.05M / 33.6 MB/s |
| 128 B | 1.29M / 165 MB/s | 1.05M / 134 MB/s | 894k / 114 MB/s |
| 512 B | 1.29M / 660 MB/s | 1.27M / 648 MB/s | 1.15M / 590 MB/s |
| 2 KiB | 1.30M / 2.65 GB/s | 1.20M / 2.45 GB/s | 1.00M / 2.06 GB/s |
| 8 KiB | 1.33M / 10.9 GB/s | 546k / 4.48 GB/s | 432k / 3.54 GB/s |
| 32 KiB | 1.11M / 36.3 GB/s | 160k / 5.24 GB/s | 128k / 4.19 GB/s |
| 128 KiB | 1.01M / 132.1 GB/s | 72.9k / 9.56 GB/s | 39.4k / 5.17 GB/s |

<!-- END router_dealer_tokio -->

## PAIR throughput (single peer)

Cells show `msgs/s / MB/s`.

**omq-compio:**

<!-- BEGIN pair_compio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 3.81M / 122 MB/s | 6.79M / 217 MB/s | 6.37M / 204 MB/s |
| 128 B | 3.80M / 487 MB/s | 4.94M / 632 MB/s | 4.85M / 621 MB/s |
| 512 B | 4.00M / 2.05 GB/s | 3.56M / 1.82 GB/s | 3.36M / 1.72 GB/s |
| 2 KiB | 3.97M / 8.14 GB/s | 1.98M / 4.06 GB/s | 1.73M / 3.55 GB/s |
| 8 KiB | 3.85M / 31.5 GB/s | 599k / 4.90 GB/s | 617k / 5.05 GB/s |
| 32 KiB | 3.96M / 129.9 GB/s | 170k / 5.56 GB/s | 171k / 5.61 GB/s |
| 128 KiB | 3.94M / 516.0 GB/s | 59.0k / 7.74 GB/s | 66.0k / 8.65 GB/s |

<!-- END pair_compio -->

**omq-tokio:**

<!-- BEGIN pair_tokio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 1.52M / 48.6 MB/s | 3.98M / 127 MB/s | 4.11M / 131 MB/s |
| 128 B | 1.52M / 194 MB/s | 4.43M / 568 MB/s | 4.77M / 610 MB/s |
| 512 B | 1.48M / 759 MB/s | 2.34M / 1.20 GB/s | 3.44M / 1.76 GB/s |
| 2 KiB | 1.51M / 3.08 GB/s | 1.44M / 2.95 GB/s | 1.49M / 3.04 GB/s |
| 8 KiB | 1.43M / 11.7 GB/s | 427k / 3.50 GB/s | 623k / 5.10 GB/s |
| 32 KiB | 1.60M / 52.5 GB/s | 115k / 3.77 GB/s | 168k / 5.51 GB/s |
| 128 KiB | 912k / 119.6 GB/s | 34.3k / 4.49 GB/s | 38.9k / 5.10 GB/s |

<!-- END pair_tokio -->

## Cross-library comparisons

See [COMPARISONS.md](COMPARISONS.md) for two-process TCP benchmarks against
libzmq and zmq.rs. Run `./scripts/compare_libzmq.sh --update-benchmarks` or
`./scripts/compare_zmqrs.sh --update-benchmarks` to refresh those tables.

## Compression transport benchmarks

See [BENCHMARKS_COMPRESSION.md](BENCHMARKS_COMPRESSION.md) for bandwidth-limited throughput charts
and compression ratio tables. Those benchmarks use structured JSON payloads
over `tc`-rate-limited loopback and are run separately from the tables above.

## PUSH/PULL throughput, priority routing (single peer)

Same topology as the single-peer table but with `priority` feature (strict
per-pipe queues). Run with `bench_run.rb --with-priority` to update.

**omq-compio:**

<!-- BEGIN push_pull_priority_compio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 4.47M | 4.13M | 4.18M |
| 128 B | 4.14M | 3.70M | 3.65M |
| 512 B | 4.19M | 2.99M | 2.95M |
| 2 KiB | 4.08M | 1.74M | 1.58M |
| 8 KiB | 4.17M | 669k | 575k |
| 32 KiB | 4.17M | 176k | 162k |
| 128 KiB | 4.19M | 59.6k | 61.2k |

<!-- END push_pull_priority_compio -->

**omq-tokio:**

<!-- BEGIN push_pull_priority_tokio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 3.49M | 4.01M | 3.83M |
| 128 B | 4.30M | 3.26M | 3.17M |
| 512 B | 3.46M | 2.81M | 2.50M |
| 2 KiB | 4.23M | 1.17M | 1.51M |
| 8 KiB | 3.93M | 522k | 461k |
| 32 KiB | 4.16M | 115k | 167k |
| 128 KiB | 3.80M | 35.1k | 43.7k |

<!-- END push_pull_priority_tokio -->

## Mechanism overhead (PUSH/PULL over TCP)

End-to-end throughput with NULL (no crypto), CURVE (XSalsa20-Poly1305), and
BLAKE3ZMQ (ChaCha20-BLAKE3) over loopback TCP. Higher is better. omq-proto
pins a `chacha20-blake3` fork with `#[target_feature(enable = "avx2")]`;
without it BLAKE3ZMQ drops to ~50 MiB/s at bulk sizes. CURVE plateaus at
~557 MB/s (salsa20 has no SIMD path).

> **BLAKE3ZMQ is not independently audited.** Use **CURVE** (RFC 26) for
> production.

<!-- BEGIN mechanism_frame -->
| Size | NULL | CURVE | BLAKE3ZMQ |
|---|---:|---:|---:|
| 32 B | 245 MB/s | 19.5 MB/s | 37.3 MB/s |
| 128 B | 664 MB/s | 61.1 MB/s | 117 MB/s |
| 512 B | 1.81 GB/s | 179 MB/s | 356 MB/s |
| 2 KiB | 3.44 GB/s | 279 MB/s | 556 MB/s |
| 8 KiB | 4.00 GB/s | 443 MB/s | 856 MB/s |
| 32 KiB | 5.17 GB/s | 473 MB/s | 901 MB/s |
| 128 KiB | 6.45 GB/s | 487 MB/s | 1.16 GB/s |

<!-- END mechanism_frame -->

<p align="center">
  <img src="doc/mechanism_chart.svg" alt="Mechanism overhead" width="850">
</p>

## Reproducing

```sh
cargo bench -p omq-compio --bench push_pull
cargo bench -p omq-tokio  --bench push_pull
cargo bench -p omq-compio --bench req_rep

# Convenience:
./scripts/bench_run.rb [--all-features] [--all-sizes]    # adds results to JSONL
./scripts/bench_run.rb --with-priority [--all-sizes]     # priority feature only
./scripts/bench_report.rb [--update-benchmarks]          # regenerates tables

# Override transports / sizes / peer counts via env:
OMQ_BENCH_TRANSPORTS=tcp OMQ_BENCH_PEERS=3 OMQ_BENCH_SIZES=128,2048,32768 cargo bench -p omq-compio --bench push_pull

# Two-process libzmq vs omq comparison (requires libzmq installed):
# build: gcc scripts/libzmq_bench_peer.c -o scripts/libzmq_bench_peer -lzmq
# then run scripts/compare_libzmq.sh [--update-benchmarks]

# Two-process zmq.rs vs omq comparison (pure Rust, no system packages):
# ./scripts/compare_zmqrs.sh [--update-benchmarks]

# Charts (SVG, generated from COMPARISONS.md or JSONL data):
python3 scripts/gen_comparison_chart.py          # doc/comparison_chart.svg (from COMPARISONS.md)
python3 scripts/gen_mechanism_chart.py            # doc/mechanism_chart.svg (from BENCHMARKS.md)

# Compression charts require a bench run first (writes JSONL):
#   1. Rate-limit loopback:
#      sudo tc qdisc replace dev lo root tbf rate 1gbit burst 128kb latency 1ms
#   2. Run bench:
#      cargo bench -p omq-compio --features lz4,zstd --bench compression
#   3. Generate chart:
python3 scripts/gen_compression_chart.py --link 1g    # doc/compression_chart_1g.svg
python3 scripts/gen_compression_chart.py --link 100m  # doc/compression_chart_100m.svg
#   Use --run-prefix ts-NNNNN to select a specific bench run from the JSONL.
#   Use --tput-max N (MB/s) to override the right-axis scale.
#   4. Remove rate limit: sudo tc qdisc del dev lo root
```

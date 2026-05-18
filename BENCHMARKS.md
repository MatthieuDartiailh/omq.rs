# Benchmarks

Linux 6.12 (Debian 13) VM on an Intel Mac Mini 2018 (i7-8700B, 3.2 GHz
base, turbo disabled, governor=performance, 6 vCPU), Rust 1.95.0,
default features. Each cell is the **min wall time** across 3 x 500 ms
timed rounds after a prime + 100 ms warmup: peak throughput, closest
to the hardware ceiling and least perturbed by scheduler/IRQ jitter.
Sources: `omq-tokio/benches/` and `omq-compio/benches/`.

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
| 32 B | 4.00M / 128 MB/s | 17.20M / 551 MB/s | 7.11M / 227 MB/s | 7.42M / 238 MB/s |
| 128 B | 3.95M / 506 MB/s | 14.90M / 1.91 GB/s | 4.77M / 611 MB/s | 5.12M / 656 MB/s |
| 512 B | 3.93M / 2.01 GB/s | 13.21M / 6.76 GB/s | 3.27M / 1.68 GB/s | 3.49M / 1.78 GB/s |
| 2 KiB | 3.94M / 8.08 GB/s | 11.48M / 23.5 GB/s | 1.94M / 3.98 GB/s | 1.76M / 3.60 GB/s |
| 8 KiB | 3.94M / 32.2 GB/s | 12.87M / 105.5 GB/s | 708k / 5.80 GB/s | 620k / 5.08 GB/s |
| 32 KiB | 3.95M / 129.4 GB/s | 10.89M / 357.0 GB/s | 179k / 5.85 GB/s | 170k / 5.56 GB/s |
| 128 KiB | 3.95M / 517.5 GB/s | 13.04M / 1709.2 GB/s | 56.6k / 7.42 GB/s | 59.5k / 7.80 GB/s |

<!-- END push_pull_1peer_compio -->

**omq-tokio:**

<!-- BEGIN push_pull_1peer_tokio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 4.03M / 129 MB/s | 4.09M / 131 MB/s | 3.42M / 109 MB/s |
| 128 B | 3.91M / 501 MB/s | 3.73M / 477 MB/s | 4.38M / 561 MB/s |
| 512 B | 4.13M / 2.11 GB/s | 2.40M / 1.23 GB/s | 2.55M / 1.31 GB/s |
| 2 KiB | 3.48M / 7.13 GB/s | 1.23M / 2.52 GB/s | 1.56M / 3.18 GB/s |
| 8 KiB | 3.90M / 32.0 GB/s | 413k / 3.39 GB/s | 426k / 3.49 GB/s |
| 32 KiB | 3.72M / 121.9 GB/s | 118k / 3.86 GB/s | 102k / 3.35 GB/s |
| 128 KiB | 3.84M / 503.1 GB/s | 32.1k / 4.21 GB/s | 38.2k / 5.01 GB/s |

<!-- END push_pull_1peer_tokio -->

Inproc "GB/s" at large payloads reflects zero-copy Arc-clone: no kernel
traversal.

## PUSH/PULL throughput, 8 peers

8 PUSH peers -> 1 PULL. Cells show `msgs/s / MB/s`.

**omq-compio:**

<!-- BEGIN push_pull_8peer_compio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 3.98M / 127 MB/s | 3.10M / 99.3 MB/s | 3.23M / 103 MB/s |
| 128 B | 3.88M / 497 MB/s | 2.95M / 377 MB/s | 3.07M / 393 MB/s |
| 512 B | 3.91M / 2.00 GB/s | 2.48M / 1.27 GB/s | 2.33M / 1.19 GB/s |
| 2 KiB | 3.93M / 8.04 GB/s | 1.28M / 2.63 GB/s | 1.18M / 2.43 GB/s |
| 8 KiB | 3.79M / 31.1 GB/s | 419k / 3.43 GB/s | 358k / 2.93 GB/s |
| 32 KiB | 3.91M / 128.1 GB/s | 155k / 5.09 GB/s | 105k / 3.45 GB/s |
| 128 KiB | 3.95M / 517.6 GB/s | 41.2k / 5.39 GB/s | 31.4k / 4.11 GB/s |

<!-- END push_pull_8peer_compio -->

**omq-tokio:**

<!-- BEGIN push_pull_8peer_tokio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 3.58M / 115 MB/s | 3.94M / 126 MB/s | 3.21M / 103 MB/s |
| 128 B | 3.48M / 445 MB/s | 2.95M / 378 MB/s | 3.87M / 496 MB/s |
| 512 B | 3.45M / 1.77 GB/s | 4.25M / 2.18 GB/s | 3.51M / 1.80 GB/s |
| 2 KiB | 3.50M / 7.16 GB/s | 2.12M / 4.34 GB/s | 2.26M / 4.62 GB/s |
| 8 KiB | 3.52M / 28.9 GB/s | 495k / 4.06 GB/s | 595k / 4.88 GB/s |
| 32 KiB | 3.43M / 112.4 GB/s | 152k / 4.98 GB/s | 156k / 5.11 GB/s |
| 128 KiB | 3.52M / 461.8 GB/s | 53.9k / 7.07 GB/s | 44.4k / 5.82 GB/s |

<!-- END push_pull_8peer_tokio -->

## REQ/REP latency (single peer)

Serial ping-pong: 1 000 warmup + 10 000 measured iterations per cell.
All values are wall time.

<!-- BEGIN latency_percentiles -->
| transport | size | compio p50 | compio p99 | tokio p50 | tokio p99 |
|---|---|---|---|---|---|
| inproc | 32 B | 2.54 µs | 5.12 µs | 25.8 µs | 61.5 µs |
| inproc | 64 B | 5.19 µs | 18.4 µs | 28.4 µs | 36.4 µs |
| inproc | 128 B | 2.60 µs | 2.78 µs | 25.7 µs | 78.7 µs |
| inproc | 256 B | 5.28 µs | 6.31 µs | 27.8 µs | 46.5 µs |
| inproc | 512 B | 2.59 µs | 2.65 µs | 25.9 µs | 80.5 µs |
| inproc | 1 KiB | 5.32 µs | 5.50 µs | 27.6 µs | 44.4 µs |
| inproc | 2 KiB | 2.60 µs | 2.66 µs | 25.6 µs | 81.4 µs |
| inproc | 4 KiB | 5.36 µs | 5.62 µs | 29.9 µs | 40.5 µs |
| inproc | 8 KiB | 2.59 µs | 2.68 µs | 25.8 µs | 80.3 µs |
| inproc | 32 KiB | 2.54 µs | 2.62 µs | 25.1 µs | 81.9 µs |
| inproc | 128 KiB | 2.55 µs | 2.63 µs | 25.1 µs | 78.2 µs |
| ipc | 32 B | 14.1 µs | 21.8 µs | 49.6 µs | 77.0 µs |
| ipc | 64 B | 21.8 µs | 31.0 µs | 62.5 µs | 861 µs |
| ipc | 128 B | 14.2 µs | 27.3 µs | 49.2 µs | 66.2 µs |
| ipc | 256 B | 22.6 µs | 31.7 µs | 63.7 µs | 77.0 µs |
| ipc | 512 B | 14.4 µs | 29.3 µs | 52.3 µs | 97.4 µs |
| ipc | 1 KiB | 22.9 µs | 32.3 µs | 64.4 µs | 861 µs |
| ipc | 2 KiB | 15.8 µs | 29.3 µs | 55.5 µs | 109 µs |
| ipc | 4 KiB | 24.9 µs | 44.4 µs | 64.0 µs | 80.0 µs |
| ipc | 8 KiB | 18.9 µs | 35.0 µs | 61.9 µs | 92.6 µs |
| ipc | 32 KiB | 25.2 µs | 44.3 µs | 74.9 µs | 127 µs |
| ipc | 128 KiB | 187 µs | 254 µs | 88.4 µs | 109 µs |
| tcp | 32 B | 21.2 µs | 35.9 µs | 58.6 µs | 82.8 µs |
| tcp | 64 B | 29.8 µs | 45.0 µs | 76.4 µs | 994 µs |
| tcp | 128 B | 20.6 µs | 39.6 µs | 60.5 µs | 92.0 µs |
| tcp | 256 B | 29.7 µs | 44.1 µs | 77.0 µs | 95.5 µs |
| tcp | 512 B | 20.7 µs | 40.1 µs | 62.0 µs | 117 µs |
| tcp | 1 KiB | 29.9 µs | 44.9 µs | 77.9 µs | 97.9 µs |
| tcp | 2 KiB | 22.2 µs | 41.0 µs | 65.5 µs | 115 µs |
| tcp | 4 KiB | 31.8 µs | 47.0 µs | 77.7 µs | 950 µs |
| tcp | 8 KiB | 24.8 µs | 43.4 µs | 66.1 µs | 119 µs |
| tcp | 32 KiB | 32.8 µs | 52.5 µs | 78.1 µs | 120 µs |
| tcp | 128 KiB | 200 µs | 274 µs | 117 µs | 153 µs |

<!-- END latency_percentiles -->

## REQ/REP throughput (single peer)

Cells show `msgs/s / MB/s`.

**omq-compio:**

<!-- BEGIN req_rep_compio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 388k / 12.4 MB/s | 68.7k / 2.20 MB/s | 48.1k / 1.54 MB/s |
| 128 B | 385k / 49.2 MB/s | 67.8k / 8.68 MB/s | 47.5k / 6.08 MB/s |
| 512 B | 388k / 199 MB/s | 67.8k / 34.7 MB/s | 46.8k / 24.0 MB/s |
| 2 KiB | 371k / 759 MB/s | 57.4k / 118 MB/s | 43.8k / 89.8 MB/s |
| 8 KiB | 386k / 3.16 GB/s | 51.6k / 423 MB/s | 39.6k / 324 MB/s |
| 32 KiB | 381k / 12.5 GB/s | 38.8k / 1.27 GB/s | 30.6k / 1.00 GB/s |
| 128 KiB | 379k / 49.7 GB/s | 5.2k / 680 MB/s | 5.6k / 732 MB/s |

<!-- END req_rep_compio -->

**omq-tokio:**

<!-- BEGIN req_rep_tokio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 35.0k / 1.12 MB/s | 16.9k / 0.54 MB/s | 15.5k / 0.50 MB/s |
| 128 B | 35.7k / 4.57 MB/s | 17.3k / 2.21 MB/s | 16.1k / 2.06 MB/s |
| 512 B | 37.1k / 19.0 MB/s | 17.2k / 8.83 MB/s | 15.9k / 8.17 MB/s |
| 2 KiB | 36.4k / 74.5 MB/s | 17.3k / 35.4 MB/s | 14.0k / 28.7 MB/s |
| 8 KiB | 36.8k / 301 MB/s | 17.7k / 145 MB/s | 14.4k / 118 MB/s |
| 32 KiB | 36.5k / 1.20 GB/s | 12.9k / 422 MB/s | 12.7k / 417 MB/s |
| 128 KiB | 35.2k / 4.62 GB/s | 10.9k / 1.43 GB/s | 8.5k / 1.11 GB/s |

<!-- END req_rep_tokio -->

## PUB/SUB throughput (3 peers)

1 PUB -> 3 SUB. Cells show `msgs/s / MB/s`.

**omq-compio:**

<!-- BEGIN pub_sub_compio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 1.21M / 38.9 MB/s | 1.42M / 45.4 MB/s | 1.42M / 45.5 MB/s |
| 128 B | 1.18M / 152 MB/s | 1.20M / 154 MB/s | 1.21M / 155 MB/s |
| 512 B | 1.19M / 609 MB/s | 990k / 507 MB/s | 996k / 510 MB/s |
| 2 KiB | 1.19M / 2.43 GB/s | 454k / 930 MB/s | 458k / 939 MB/s |
| 8 KiB | 1.17M / 9.62 GB/s | 162k / 1.33 GB/s | 152k / 1.25 GB/s |
| 32 KiB | 1.19M / 39.1 GB/s | 93.9k / 3.08 GB/s | 80.4k / 2.64 GB/s |
| 128 KiB | 1.18M / 154.3 GB/s | 24.7k / 3.24 GB/s | 7.0k / 914 MB/s |

<!-- END pub_sub_compio -->

**omq-tokio:**

<!-- BEGIN pub_sub_tokio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 1.32M / 42.3 MB/s | 1.56M / 50.0 MB/s | 1.64M / 52.5 MB/s |
| 128 B | 1.26M / 161 MB/s | 1.41M / 181 MB/s | 1.32M / 170 MB/s |
| 512 B | 1.29M / 662 MB/s | 1.25M / 639 MB/s | 1.24M / 632 MB/s |
| 2 KiB | 1.29M / 2.64 GB/s | 797k / 1.63 GB/s | 781k / 1.60 GB/s |
| 8 KiB | 1.26M / 10.3 GB/s | 367k / 3.00 GB/s | 222k / 1.82 GB/s |
| 32 KiB | 1.16M / 38.0 GB/s | 97.3k / 3.19 GB/s | 33.3k / 1.09 GB/s |
| 128 KiB | 805k / 105.6 GB/s | 32.8k / 4.29 GB/s | 7.4k / 968 MB/s |

<!-- END pub_sub_tokio -->

## ROUTER/DEALER throughput (3 peers)

3 DEALER -> 1 ROUTER. Cells show `msgs/s / MB/s`.

**omq-compio:**

<!-- BEGIN router_dealer_compio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 3.44M / 110 MB/s | 2.38M / 76.3 MB/s | 2.35M / 75.1 MB/s |
| 128 B | 3.63M / 465 MB/s | 2.30M / 294 MB/s | 2.37M / 304 MB/s |
| 512 B | 3.61M / 1.85 GB/s | 2.04M / 1.04 GB/s | 2.00M / 1.02 GB/s |
| 2 KiB | 3.58M / 7.33 GB/s | 1.12M / 2.29 GB/s | 1.12M / 2.28 GB/s |
| 8 KiB | 3.60M / 29.5 GB/s | 443k / 3.63 GB/s | 440k / 3.61 GB/s |
| 32 KiB | 3.54M / 116.1 GB/s | 158k / 5.19 GB/s | 109k / 3.59 GB/s |
| 128 KiB | 3.57M / 467.6 GB/s | 41.8k / 5.47 GB/s | 26.2k / 3.44 GB/s |

<!-- END router_dealer_compio -->

**omq-tokio:**

<!-- BEGIN router_dealer_tokio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 1.29M / 41.4 MB/s | 1.29M / 41.2 MB/s | 783k / 25.1 MB/s |
| 128 B | 1.32M / 169 MB/s | 1.17M / 149 MB/s | 1.25M / 160 MB/s |
| 512 B | 1.23M / 629 MB/s | 1.44M / 736 MB/s | 1.09M / 558 MB/s |
| 2 KiB | 853k / 1.75 GB/s | 1.20M / 2.45 GB/s | 1.13M / 2.32 GB/s |
| 8 KiB | 1.20M / 9.86 GB/s | 486k / 3.98 GB/s | 499k / 4.09 GB/s |
| 32 KiB | 677k / 22.2 GB/s | 195k / 6.39 GB/s | 152k / 4.97 GB/s |
| 128 KiB | 916k / 120.1 GB/s | 49.1k / 6.44 GB/s | 31.1k / 4.08 GB/s |

<!-- END router_dealer_tokio -->

## PAIR throughput (single peer)

Cells show `msgs/s / MB/s`.

**omq-compio:**

<!-- BEGIN pair_compio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 3.88M / 124 MB/s | 6.03M / 193 MB/s | 6.39M / 205 MB/s |
| 128 B | 3.87M / 495 MB/s | 4.89M / 625 MB/s | 4.57M / 585 MB/s |
| 512 B | 3.90M / 2.00 GB/s | 3.45M / 1.77 GB/s | 3.46M / 1.77 GB/s |
| 2 KiB | 3.73M / 7.64 GB/s | 1.98M / 4.05 GB/s | 1.69M / 3.45 GB/s |
| 8 KiB | 3.77M / 30.9 GB/s | 628k / 5.14 GB/s | 611k / 5.01 GB/s |
| 32 KiB | 3.76M / 123.1 GB/s | 177k / 5.79 GB/s | 170k / 5.56 GB/s |
| 128 KiB | 3.77M / 494.0 GB/s | 57.0k / 7.47 GB/s | 63.1k / 8.27 GB/s |

<!-- END pair_compio -->

**omq-tokio:**

<!-- BEGIN pair_tokio -->
| Size | inproc | ipc | tcp |
|---|---|---|---|
| 32 B | 2.00M / 64.1 MB/s | 3.48M / 111 MB/s | 4.04M / 129 MB/s |
| 128 B | 1.32M / 169 MB/s | 4.29M / 550 MB/s | 3.59M / 460 MB/s |
| 512 B | 1.66M / 849 MB/s | 2.54M / 1.30 GB/s | 3.15M / 1.61 GB/s |
| 2 KiB | 1.47M / 3.01 GB/s | 1.21M / 2.48 GB/s | 1.65M / 3.39 GB/s |
| 8 KiB | 1.57M / 12.8 GB/s | 415k / 3.40 GB/s | 409k / 3.35 GB/s |
| 32 KiB | 1.26M / 41.4 GB/s | 113k / 3.70 GB/s | 164k / 5.36 GB/s |
| 128 KiB | 834k / 109.3 GB/s | 29.4k / 3.86 GB/s | 43.3k / 5.68 GB/s |

<!-- END pair_tokio -->

## Cross-library comparisons

See [COMPARISONS.md](COMPARISONS.md) for two-process TCP benchmarks against
libzmq and zmq.rs. Run `./scripts/compare_libzmq.sh --update-benchmarks` or
`./scripts/compare_zmqrs.sh --update-benchmarks` to refresh those tables.

## Compression transport overhead

Codec overhead of lz4+tcp and zstd+tcp vs bare tcp on synthetic payloads,
single peer. Payloads are uniform bytes; compression ratio is unrealistic but
the numbers isolate per-frame codec cost.

### PUSH/PULL

**omq-compio:**

<!-- BEGIN compression_push_pull_compio -->
| Size | tcp | lz4+tcp | zstd+tcp |
|---|---|---|---|
| 32 B | 7.42M / 238 MB/s | 3.05M / 97.6 MB/s | 2.80M / 89.4 MB/s |
| 128 B | 5.12M / 656 MB/s | 3.96M / 507 MB/s | 101k / 12.9 MB/s |
| 512 B | 3.49M / 1.78 GB/s | 1.58M / 807 MB/s | 108k / 55.2 MB/s |
| 2 KiB | 1.76M / 3.60 GB/s | 1.24M / 2.54 GB/s | 491k / 1.00 GB/s |
| 8 KiB | 620k / 5.08 GB/s | 486k / 3.98 GB/s | 230k / 1.89 GB/s |
| 32 KiB | 170k / 5.56 GB/s | 122k / 3.98 GB/s | 70.5k / 2.31 GB/s |
| 128 KiB | 59.5k / 7.80 GB/s | 27.4k / 3.59 GB/s | 26.0k / 3.41 GB/s |

<!-- END compression_push_pull_compio -->

**omq-tokio:**

<!-- BEGIN compression_push_pull_tokio -->
| Size | tcp | lz4+tcp | zstd+tcp |
|---|---|---|---|
| 32 B | 3.42M / 109 MB/s | 134k / 4.29 MB/s | 141k / 4.50 MB/s |
| 128 B | 4.38M / 561 MB/s | 139k / 17.8 MB/s | 71.8k / 9.20 MB/s |
| 512 B | 2.55M / 1.31 GB/s | 140k / 71.7 MB/s | 66.9k / 34.3 MB/s |
| 2 KiB | 1.56M / 3.18 GB/s | 138k / 282 MB/s | 121k / 249 MB/s |
| 8 KiB | 426k / 3.49 GB/s | 133k / 1.09 GB/s | 116k / 948 MB/s |
| 32 KiB | 102k / 3.35 GB/s | 74.7k / 2.45 GB/s | 91.8k / 3.01 GB/s |
| 128 KiB | 38.2k / 5.01 GB/s | 33.5k / 4.39 GB/s | 28.7k / 3.77 GB/s |

<!-- END compression_push_pull_tokio -->

### REQ/REP

**omq-compio:**

<!-- BEGIN compression_req_rep_compio -->
| Size | tcp | lz4+tcp | zstd+tcp |
|---|---|---|---|
| 32 B | 48.1k / 1.54 MB/s | 45.6k / 1.46 MB/s | 43.2k / 1.38 MB/s |
| 128 B | 47.5k / 6.08 MB/s | 45.8k / 5.87 MB/s | 20.5k / 2.63 MB/s |
| 512 B | 46.8k / 24.0 MB/s | 41.9k / 21.5 MB/s | 20.4k / 10.5 MB/s |
| 2 KiB | 43.8k / 89.8 MB/s | 41.4k / 84.9 MB/s | 31.2k / 63.8 MB/s |
| 8 KiB | 39.6k / 324 MB/s | 36.3k / 297 MB/s | 28.0k / 229 MB/s |
| 32 KiB | 30.6k / 1.00 GB/s | 25.8k / 846 MB/s | 20.6k / 674 MB/s |
| 128 KiB | 5.6k / 732 MB/s | 12.3k / 1.61 GB/s | 10.4k / 1.37 GB/s |

<!-- END compression_req_rep_compio -->

**omq-tokio:**

<!-- BEGIN compression_req_rep_tokio -->
| Size | tcp | lz4+tcp | zstd+tcp |
|---|---|---|---|
| 32 B | 15.5k / 0.50 MB/s | 11.3k / 0.36 MB/s | 11.0k / 0.35 MB/s |
| 128 B | 16.1k / 2.06 MB/s | 11.9k / 1.52 MB/s | 9.1k / 1.17 MB/s |
| 512 B | 15.9k / 8.17 MB/s | 12.0k / 6.13 MB/s | 8.9k / 4.57 MB/s |
| 2 KiB | 14.0k / 28.7 MB/s | 11.1k / 22.8 MB/s | 10.5k / 21.4 MB/s |
| 8 KiB | 14.4k / 118 MB/s | 10.5k / 85.7 MB/s | 9.1k / 74.5 MB/s |
| 32 KiB | 12.7k / 417 MB/s | 9.6k / 313 MB/s | 9.1k / 297 MB/s |
| 128 KiB | 8.5k / 1.11 GB/s | 4.7k / 619 MB/s | 6.0k / 792 MB/s |

<!-- END compression_req_rep_tokio -->

## Compression on realistic JSON payloads (omq-compio, 1 peer)

JSON event-log payload (timestamps, trace IDs, repeated field names).
The ratio is the multiplier compression buys on a bandwidth-bounded link:
on a 1 Gbps link saturated at 125 MB/s, zstd at 2 KiB (4.47x) delivers
the equivalent of 559 MB/s of application data.

Compression ratios:

| size    | lz4     | zstd     |
|---------|---------|----------|
| 128 B   | 0.97x*  | 0.97x*   |
| 512 B   | 1.57x   | 1.62x    |
| 1 KiB   | 2.60x   | 2.84x    |
| 2 KiB   | 3.76x   | 4.47x    |
| 4 KiB   | 4.92x   | 7.41x    |
| 16 KiB  | 6.47x   | **12.87x** |

\* Below 512 B both codecs fall back to plaintext (0.97-0.98x = 4-byte
`SENTINEL_PLAIN` tax). A pre-trained dict moves the cutoff further down (see below).

### With a pre-trained dict (small messages)

Dict primes the codec with message-family byte sequences so even 128 B records
compress well. Pass via `Options::compression_dict(Bytes)`; shipped to peer on
first connection, reused every frame.

Ratios on same JSON template (zstd: 1.6 KiB dict from 200 samples; lz4: 4 KiB buffer):

| size  | lz4 (no dict) | lz4 (with dict) | zstd (no dict) | zstd (with dict) |
|-------|---------------|-----------------|----------------|------------------|
| 128 B | 0.97x (skip)  | **5.82x**       | 0.97x (skip)   | **5.12x**        |
| 512 B | 1.57x         | **22.26x**      | 1.62x          | **19.69x**       |
| 1 KiB | 2.60x         | **11.25x**      | 2.84x          | **35.31x**       |
| 2 KiB | 3.76x         | **8.50x**       | 4.47x          | **16.93x**       |

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
| 32 B | 233 MB/s | 17.6 MB/s | 36.1 MB/s |
| 128 B | 620 MB/s | 57.1 MB/s | 107 MB/s |
| 512 B | 1.71 GB/s | 159 MB/s | 328 MB/s |
| 2 KiB | 3.30 GB/s | 305 MB/s | 528 MB/s |
| 8 KiB | 4.77 GB/s | 391 MB/s | 801 MB/s |
| 32 KiB | 5.08 GB/s | 439 MB/s | 944 MB/s |
| 128 KiB | 8.56 GB/s | 448 MB/s | 1.15 GB/s |

<!-- END mechanism_frame -->

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
```

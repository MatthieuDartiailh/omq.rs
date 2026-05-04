# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/paddor/omq.rs/releases/tag/omq-compio-v0.2.0) - 2026-05-04

### Added

- add try_send / try_recv (non-blocking send/recv)

### Fixed

- *(compio)* revert work_pending guard — restored throughput regression
- *(priority)* correct strict-precedence routing; unblock REQ/REP under priority
- *(compio)* defer dialers.clear() until after linger drain
- *(compio,tokio)* resolve REQ/REP deadlocks and tier-1 test regressions
- Socket::drop cancels tasks; REQ/REP reset on disconnect; tier-1 tests
- reconnect after mid-session drop + close() listener/dialer teardown
- *(clippy)* collapse nested if-let chains (collapsible_if, new in 1.93)
- *(curve)* wire-compatible with libzmq + pyzmq interop suite
- XPUB honors legacy ZMTP 3.0 0x01-prefix message subscribes

### Other

- pre-release polish, American English, comprehensive test-all.sh
- tighten default size sweep to 128 B / 2 KiB / 8 KiB
- *(payload)* Phase C — as_bytes(), single-chunk transforms, PAYLOAD_INLINE_CHUNKS=1
- fix all clippy warnings across workspace (--all-features)
- *(compression)* use common::sizes() for throughput cells; respects --all-sizes
- median-of-3 runs; default 3 sizes; --all-sizes flag
- *(transform)* split MessageTransform into MessageEncoder + MessageDecoder
- *(tokio)* FLAT_THRESHOLD 32 KiB — fix 2 KiB TCP regression; fix(compio): second codec_maybe_dirty race; bench: add 32B size, 8-peer column
- *(compio)* flat-buf encoding, drain-vec reuse, codec-skip guards, passthrough fast path
- *(compio)* EncodedQueue send bypass — skip codec mutex on hot path
- cargo fmt --all (first-time sweep)
- parity fixes, new tests, and pyomq monitor/connections API
- *(benchmarks)* rerun at 0.5s/cell, MB/GB units, BLAKE3ZMQ audit caveat
- *(clippy)* silence all pedantic warnings across feature combos
- CURVE + BLAKE3ZMQ across the strategy buckets
- per-peer group filter for RADIO over wire + Ruby interop
- DRY sync/async dispatch + remove dead code
- full Stage/Phase narrative sweep across all src/
- drop Stage/Phase narrative from hot files
- socket-type x transport coverage + cross-runtime interop
- parity for CLIENT/SERVER/SCATTER/CHANNEL/PEER + DISH groups
- recv-direct fast path + enum-dispatch wire halves
- Rust ZMQ: omq-proto codec, omq-tokio/omq-compio backends, pyomq binding

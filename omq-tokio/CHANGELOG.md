# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2](https://github.com/paddor/omq.rs/compare/omq-tokio-v0.2.1...omq-tokio-v0.2.2) - 2026-05-04

### Fixed

- *(tokio)* sync reconnect test on second handshake under priority — the
  `peer_drop_mid_send_is_handled_cleanly` test was racing the disconnect
  with its post-reconnect send, surfacing under `--features priority`
  because the per-pipe inbox can't survive its peer's exit. Test-only
  change; no library behavior change. The standard ZMQ "messages queued
  for a vanished peer are lost" semantic is now documented in the
  priority-mode block of `routing/round_robin.rs`.

## [0.2.1](https://github.com/paddor/omq.rs/releases/tag/omq-tokio-v0.2.1) - 2026-05-04

### Added

- add try_send / try_recv (non-blocking send/recv)

### Fixed

- *(priority)* correct strict-precedence routing; unblock REQ/REP under priority
- *(compio,tokio)* resolve REQ/REP deadlocks and tier-1 test regressions
- Socket::drop cancels tasks; REQ/REP reset on disconnect; tier-1 tests
- reconnect after mid-session drop + close() listener/dialer teardown
- *(clippy)* collapse nested if-let chains (collapsible_if, new in 1.93)
- *(curve)* wire-compatible with libzmq + pyzmq interop suite
- XPUB honors legacy ZMTP 3.0 0x01-prefix message subscribes

### Other

- optimize flat-buf threshold and update benchmarks
- pre-release polish, American English, comprehensive test-all.sh
- tighten default size sweep to 128 B / 2 KiB / 8 KiB
- *(payload)* Phase C — as_bytes(), single-chunk transforms, PAYLOAD_INLINE_CHUNKS=1
- *(tokio)* bypass actor on hot send/recv paths; add zmq.rs bench
- fix all clippy warnings across workspace (--all-features)
- median-of-3 runs; default 3 sizes; --all-sizes flag
- *(transform)* split MessageTransform into MessageEncoder + MessageDecoder
- *(tokio)* FLAT_THRESHOLD 32 KiB — fix 2 KiB TCP regression; fix(compio): second codec_maybe_dirty race; bench: add 32B size, 8-peer column
- *(tokio)* 64 KiB read buffer, direct shared-queue arm, eliminate pump tasks
- *(compio)* flat-buf encoding, drain-vec reuse, codec-skip guards, passthrough fast path
- cargo fmt --all (first-time sweep)
- parity fixes, new tests, and pyomq monitor/connections API
- *(benchmarks)* rerun at 0.5s/cell, MB/GB units, BLAKE3ZMQ audit caveat
- *(clippy)* silence all pedantic warnings across feature combos
- CURVE + BLAKE3ZMQ across the strategy buckets
- full Stage/Phase narrative sweep across all src/
- drop Stage/Phase narrative from hot files
- integrate Ruby OMQ wire-interop test
- socket-type x transport coverage + cross-runtime interop
- Rust ZMQ: omq-proto codec, omq-tokio/omq-compio backends, pyomq binding

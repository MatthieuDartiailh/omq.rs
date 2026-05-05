# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.7](https://github.com/paddor/omq.rs/compare/omq-compio-v0.2.6...omq-compio-v0.2.7) - 2026-05-05

### Fixed

- Wire ordering between fast-path and cmd-channel sends. Once
  `encoded_queue` exceeded the 512 KiB direct-write cap, subsequent
  sends fell back to the per-peer cmd channel and were encoded into the
  codec; the driver loop drains the codec (step 3a) before
  `encoded_queue` (step 3b), so cmd-channel messages reached the wire
  before earlier fast-path messages still sitting in the queue. User
  messages now route through `encoded_queue` from both paths so a
  single ordered queue carries them.
- CURVE / BLAKE3ZMQ encryption no longer bypassed by the cmd-channel
  arm. The above ordering fix initially routed every non-transform
  send through `encoded_queue`, which writes raw plaintext frames;
  crypto sockets must keep using `codec.send_message` so the active
  mechanism wraps each frame as `nonce || ciphertext || mac`.

## [0.2.6](https://github.com/paddor/omq.rs/compare/omq-compio-v0.2.5...omq-compio-v0.2.6) - 2026-05-05

### Changed

- *(deps)* require `omq-proto = 0.2.3` for the wire-compatible zstd dict
  shipment (see omq-proto CHANGELOG 0.2.3). compio's structurally
  immune to the tokio-side linger race fixed in `omq-tokio = 0.2.3` —
  no behaviour change in this crate beyond the dependency bump and
  test-suite migration to `train_zdict` for the static-dict path.

## [0.2.5](https://github.com/paddor/omq.rs/compare/omq-compio-v0.2.3...omq-compio-v0.2.5) - 2026-05-04

### Fixed

- *(compio)* fix deadlock in sequential REQ/REP under the `priority` feature.
  The driver's PollOnce SQE could fire while `try_direct_recv` concurrently
  held `peer_io` and drained the kernel buffer; by the time the driver
  acquired the lock `recv_claim` had returned to 0 and `outbound_pending` was
  false (priority sends go to the cmd inbox, not `encoded_queue`), so the
  driver entered a blocking `reader.read()` on an empty buffer while
  `SendMessage(pong)` sat unprocessed in the inbox. Fixed by adding a
  `drain_generation` counter to `DirectIoState` (priority feature only):
  `try_direct_recv` increments it after each successful read; the driver
  snapshots it before `select_biased!` and bails if it changed, re-entering
  select so the cmd arm can fire.

## [0.2.3](https://github.com/paddor/omq.rs/compare/omq-compio-v0.2.2...omq-compio-v0.2.3) - 2026-05-04

### Fixed

- *(compio)* hold writer lock across the full snapshot → write → advance
  flush sequence on both the driver's step-3a path and the recv-direct path
  (`Socket::recv` → `handle.rs`). Previously both paths acquired the writer
  lock only at write time; a concurrent heartbeat PING could be cloned by
  both, written twice, and then over-advanced, panicking in
  `advance_transmit`.

## [0.2.2](https://github.com/paddor/omq.rs/compare/omq-compio-v0.2.1...omq-compio-v0.2.2) - 2026-05-04

### Fixed

- *(compio)* skip the `try_direct_encode` fast path on crypto connections.
  The fast path writes raw frames into the wire-side buffer, bypassing the
  codec's `send_message` — for CURVE / BLAKE3ZMQ that meant shipping
  plaintext to encrypted peers, which then rejected or silently discarded
  the frames. Adds a `uses_crypto` flag on `DirectIoState` and short-
  circuits the fast path when set.

## [0.2.1](https://github.com/paddor/omq.rs/releases/tag/omq-compio-v0.2.1) - 2026-05-04

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

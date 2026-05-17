# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2026-05-17

### Changed

- *(deps)* Replace `blume::spsc` with standalone `yring` crate.
- *(deps)* Bump `blume` to 0.2.0.

## [0.4.0] - 2026-05-14

### Added

- ROUTER/SERVER identity handover: when a new connection claims an identity
  already held by an existing peer, the old connection is evicted.

### Changed

- *(deps)* Bump `omq-proto` to 0.8.0.

## [0.3.0] - 2026-05-14

### Added

- PLAIN security mechanism support via `plain` feature flag.
- PLAIN interop tests against libzmq via pyzmq.

### Fixed

- *(test)* Fix interop_ruby TCP port collisions (AddrInUse).

### Changed

- *(deps)* Bump `omq-proto` to 0.7.0.

## [0.2.8] - 2026-05-13

### Changed

- *(deps)* Bump `omq-proto` to 0.6.0.
- Post-handshake READY/ERROR commands now drop the connection (protocol
  violation per ZMTP RFC 23). Test updated accordingly.

## [0.2.7](https://github.com/paddor/omq.rs/compare/omq-tokio-v0.2.6...omq-tokio-v0.2.7) - 2026-05-12

### Changed

- *(deps)* Bump `omq-proto` to 0.4.0.

## [0.2.6](https://github.com/paddor/omq.rs/compare/omq-tokio-v0.2.5...omq-tokio-v0.2.6) - 2026-05-09

### Changed

- *(deps)* replace `compio` git dev-dep with `version = "0.19.0-rc.1"` to
  match omq-compio's registry dep; avoids duplicate compio-runtime instances
  that caused TLS mismatch panics in interop_compio tests. No library change.

## [0.2.5](https://github.com/paddor/omq.rs/compare/omq-tokio-v0.2.4...omq-tokio-v0.2.5) - 2026-05-09

### Fixed

- *(test)* `inproc_equal_priorities_round_robin`: wait for all 3 handshakes
  before sending. Without the barrier, messages sent before a peer registered
  in the routing table skewed the distribution, causing a spurious
  "tier round-robin starved" failure. No library behavior change.

## [0.2.4](https://github.com/paddor/omq.rs/compare/omq-tokio-v0.2.3...omq-tokio-v0.2.4) - 2026-05-09

### Added

- `Options::large_message_threshold(n)` /
  `Options::disable_large_message_path()` are accepted on tokio for
  API parity with omq-compio. They have no effect: tokio's recv path
  does not use buf-rings, so the multi-shot vs one-shot switch the
  knob controls only matters on omq-compio. Code that compiles
  against the compio backend stays compiling against tokio.

### Changed

- The codec inbound buffer (from `omq-proto`) is now a chunked queue. For
  tokio, the read path still copies from the stack buffer into `Bytes` once
  per read (same as before), but the codec no longer reallocates as
  messages grow: each received slice is appended as a fixed chunk rather
  than into a growing `BytesMut`. Large messages see one copy per read
  instead of O(n log n) copies from repeated doubling.

## [0.2.3](https://github.com/paddor/omq.rs/compare/omq-tokio-v0.2.2...omq-tokio-v0.2.3) - 2026-05-05

### Fixed

- *(tokio)* keep race-arrived peers spawned during `begin_close()` linger
  drain. When a TCP handshake completed after `closing = true` was set,
  the actor's `Connected`/`Accepted` arms used to drop the peer entirely,
  leaving messages already in the outbound queue unsent (incl. zstd dict
  shipments). Now the peer is spawned whenever the send-strategy queue
  is non-empty; teardown still cancels it once drained or linger expires.

### Changed

- *(deps)* require `omq-proto = 0.2.3` for the wire-compatible zstd dict
  shipment (see omq-proto CHANGELOG 0.2.3).

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

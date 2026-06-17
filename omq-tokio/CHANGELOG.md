# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.14.3] - 2026-06-17

### Fixed

- Connection churn: tolerate small message reordering between wire slot bypass and driver inbox paths during reconnection.

### Performance

- PUB/SUB fan-out: shared `FanOutArena` + `fan_out_pump` task. Encode once into a shared arena, pump distributes pre-encoded bytes to all subscribers. Eliminates per-peer encode on the send path.
- Cached multi-peer dispatch: `Submitter` caches target list and encoder across generations, avoiding lock acquisition on every send when the peer set is stable.
- Dynamic yield interval: scale with peer count instead of fixed 256-message interval.
- Disable 10ms safety timeout polling in connection driver, eliminating ~6400 spurious wakeups/sec at 64 peers.

### Changed

- *(deps)* Bump `omq-proto` to 0.17.2, `yring` to 0.3.1.

## [0.14.2] - 2026-06-12

### Fixed

- lz4+tcp fan-out: PUB/XPUB/RADIO with lz4 compression now encodes once and distributes the compressed bytes to all subscribers via `push_pre_encoded`. Previously the fan-out path framed messages without applying the lz4 transform, causing subscribers to reject frames with "unknown lz4 sentinel" and reset connections (~150x throughput loss at 2+ subscribers).

### Changed

- *(deps)* Bump `omq-proto` to 0.17.1.

## [0.14.1] - 2026-06-12

### Removed

- `zstd` feature: `zstd+tcp://` transport removed in favor of `lz4+tcp://`.

### Changed

- *(deps)* Bump `omq-proto` to 0.17.0. Tighten `rustls-pki-types` to 1.14.

## [0.14.0] - 2026-06-10

### Added

- `PeerWireSlot`: per-peer encode buffer (`std::sync::Mutex`, nanosecond hold time) replaces `DirectIo`'s `Mutex<Writer>`. Handle encodes, driver flushes via `data_ready` select arm.
- `PeerSend` enum (`Wire`/`Inbox`): unified dispatch for fan-out, identity, and exclusive routing. Eliminates pump tasks for all strategies.
- `Exclusive` routing strategy for PAIR/CHANNEL (single slot, no queue).
- Encode-once PUB/SUB fan-out via `push_pre_encoded`.
- Subscription elision: skip per-peer filtering when all peers have `subscribe_all`.
- `RecvSink::Yring`: `ConnectionDriver` pushes decoded messages directly into a yring, bypassing the `async_channel` relay for single-peer TCP/IPC.
- `CompressionPool`: `spawn_blocking` offload for large messages with warm `MessageEncoder` reuse.
- `last_bound_endpoint()`.
- `Options::xpub_nodrop` support: `FanOut` pre-checks wire slots and returns `Full(msg)` when capacity is reached.
- 10 ms safety-net timers on notification-based await points.
- Fair-share batch limiting for shared-queue send.

### Fixed

- `PeerSend::Wire` falls back to driver inbox for crypto/compression-ineligible messages (was silently dropping).
- Dead `PeerWireSlot` no longer surfaces `Err(Closed)` to `send()`: falls through to `SendSubmitter` queue.
- SPSC and recv-yring fast paths recover after peer churn (were one-shot).
- Lost-wakeup race in `SpscAwareRecv::recv()`.
- Silent message loss in `encode_msg` (was `unwrap_or_default()`).
- WSS cert panic: return `Err` when no system certs found.
- REQ: set `req_awaiting_reply` after successful send, not before. Loop to skip malformed messages in `try_recv`.
- `Exclusive::send()` awaits peer instead of returning error before connect.
- Flush encode slot on cancel (was losing pending data).
- `SO_REUSEADDR` on TCP listener sockets.

### Performance

- PeerWireSlot: handle encodes under nanosecond Mutex, driver flushes. Replaces DirectIo async Mutex.
- Fan-out: encode once, push pre-encoded bytes to all subscribers.
- Batch yring consumer pops, defer Release store.

### Changed

- Remove `priority` feature.
- Remove `DirectIo`, `SharedWriter`, `DirectIoSlot`.
- *(deps)* Bump `omq-proto` to 0.16.0, `chacha20-blake3` to 0.10.0.

## [0.13.0] - 2026-05-30

### Added

- Direct I/O bypass for single-peer connections.

### Fixed

- DirectIo misrouting when multiple peers connect.

### Changed

- Refactor direct I/O to keep driver alive after handoff.
- *(deps)* Bump `omq-proto` to 0.15.0. Tighten `concurrent-queue` to 2.5.0, `rustc-hash` to 2.1.0, `thiserror` to 2.0.18, `tokio` to 1.52.0.

## [0.12.0] - 2026-05-25

### Added

- Zero-copy read path with `BytesMut`.
- recv-direct bypass for REQ sockets.
- Bypass actor for REQ/REP send (latency optimization).
- Atomic REQ alternation flag, bypass Mutex on hot path.

### Fixed

- Drain codec events before propagating `handle_input` error.

### Changed

- Use `FxHashMap`/`FxHashSet` for internal maps.
- *(deps)* Bump `omq-proto` to 0.14.0, `yring` to 0.2.2. Upgrade `rand` 0.8 → 0.10.

## [0.11.1] - 2026-05-23

### Changed

- *(deps)* Bump `yring` to 0.2.1.

## [0.11.0] - 2026-05-23

### Added

- WebSocket transport (`ws://` / `wss://`).
- `ZMQ_STREAM` socket type for raw TCP communication.
- WS `EncodedQueue` fast paths (server-side and client-side).

### Fixed

- WS driver: large-message bypass, leftover bytes, and encode path correctness.
- REQ socket state machine reset race on reconnect.

### Changed

- Drop `tungstenite` / `tokio-tungstenite` dependencies.
- *(deps)* Bump `omq-proto` to 0.13.0.

## [0.10.0] - 2026-05-21

### Changed

- Replace `flume` send queue with `concurrent-queue` + `tokio::sync::Notify`.
- Batch semaphore permit release in `DropQueue`: one `add_permits(N)` per batch instead of N individual calls. 128B 1-PUSH/8-PULL TCP: 559 -> 940 MB/s (+68%).
- Remove unused `blume` dependency.
- *(deps)* Bump `omq-proto` to 0.12.0.

### Fixed

- Dead-code errors when building with `feature = "priority"`.

## [0.9.0] - 2026-05-21

### Changed

- *(deps)* Bump `omq-proto` to 0.11.0.

## [0.8.1] - 2026-05-20

### Changed

- Route compression encoder output through `EncodedQueue` for batched vectored writes. lz4+tcp 32B: 142k -> 2.3M msg/s; lz4+tcp 512B: 140k -> 1.5M msg/s.
- Sub-threshold messages on compression transports take a sentinel-prefix fast path that bypasses the encoder entirely.

## [0.8.0] - 2026-05-20

### Changed

- *(deps)* Bump `omq-proto` to 0.10.0.

## [0.7.0] - 2026-05-20

### Fixed

- Flaky `inproc_strict_precedence` priority test: replaced monitor-event polling with `connections()` polling to ensure the routing table is populated before sending.

### Changed

- Bench warmup: time-bound prime phase (500 ms cap) and start calibration at small iteration counts. Large-message cells no longer spend 30+ seconds in warmup.

## [0.6.1] - 2026-05-19

### Fixed

- Priority-mode message loss during reconnect storms: close the peer inbox and cancel the driver token on EOF so concurrent senders see the peer as dead immediately. Before: 67.9% delivery at 300 s; after: 99.6% delivery at 120 s.

## [0.6.0] - 2026-05-19

### Added

- `impl SocketApi for Socket` for compile-time API parity with omq-compio.

### Changed

- `bind()` returns `Result<Endpoint>` instead of `Result<()>`.
- `actor.rs` split into `actor/endpoints.rs` and `actor/peer.rs` for readability.

## [0.5.5] - 2026-05-18

### Added

- `EncodedQueue`: port of omq-compio's unified flat+gather encoding queue. Replaces the separate `flat_buf` and codec write paths with a single drain-based flush that reuses a `Vec<Bytes>` across iterations.
- Configurable batch byte cap via `OMQ_BATCH_BYTES` env var (default 1 MiB, up from hard-coded 512 KiB).

### Changed

- `FLAT_THRESHOLD` raised from 48 KiB to 64 KiB.
- `flush_once` now uses `transmit_chunks_capped(128)` to bound iovec count.
- *(deps)* Bump `omq-proto` to 0.8.4.

### Fixed

- Exclude PAIR from inproc SPSC eligibility: both sides receive, so concurrent recv would compete for messages from the same ring.

## [0.5.4] - 2026-05-18

### Added

- `soak` Cargo feature gating 12 long-running leak-detection scenarios.

## [0.5.3] - 2026-05-17

### Changed

- *(deps)* Bump `flume` to 0.12, `socket2` to 0.6.

## [0.5.2] - 2026-05-17

### Changed

- *(deps)* Bump `omq-proto` to 0.8.1, `blume` to 0.2.1.

## [0.5.1] - 2026-05-17

### Changed

- *(deps)* Bump `yring` to 0.2.0.

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

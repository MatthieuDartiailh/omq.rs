# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.12.5] - 2026-06-27

### Fixed

- PUB single-subscriber direct-encode yield starvation.
- Heartbeat timeout check guarded with `hb_ping_sent`.
- Fan-out backpressure on compio.

### Changed

- *(deps)* Bump `omq-proto` to 0.18.1, `bytes` 1.11 to 1.12, `socket2` 0.6.3 to 0.6.4.

## [0.12.4] - 2026-06-26

### Added

- `lz4+ws://`, `lz4+wss://` compressed WebSocket transport.

### Fixed

- WS send bypass: encode as WS binary frames instead of raw ZMTP frames.
- Direct-encode send path: yield periodically to prevent driver starvation under sustained load. Fixes `soak_multi_socket` throughput collapse.
- Eliminate double-load of `large_recv_pending` atomic in recv path.

### Performance

- Fan-out (PUB/XPUB multi-peer): arena-only dispatch for small messages (memcpy, no `Bytes::clone` atomics), adaptive byte-based yield interval, thread-local `EncodedQueue` + `Vec<Bytes>` reuse.
- Single-peer PUB/XPUB: direct-encode into peer's `EncodedQueue`, bypassing `PeerOut::send` channel + driver wakeup.

### Changed

- Introduce `LocalCell<T>` wrapping `UnsafeCell` with debug-mode thread-ID check. Replaces `RecvCache` and raw `UnsafeCell` fields, eliminating ~25 inline unsafe blocks.
- *(deps)* Bump `omq-proto` to 0.18.0, `blume` to 0.4.2, `yring` to 0.3.2.

## [0.12.3] - 2026-06-22

### Fixed

- `EncodedQueueCell::borrow_mut` aliasing check upgraded from `debug_assert` to `assert`.
- `try_send_radio`: capture `libc::send` return value instead of discarding it.

### Changed

- *(deps)* Bump `omq-proto` to 0.17.3.

## [0.12.2] - 2026-06-17

### Fixed

- `flush_codec_to_wire` / `flush_codec_output` race condition.
- Heartbeat priority inversion: heartbeat timer was polled before `transmit_ready` in the driver select loop, causing heartbeat frames to starve pending data and trigger spurious connection timeouts under sustained traffic.

### Changed

- *(deps)* Bump `omq-proto` to 0.17.2, `blume` to 0.4.1, `yring` to 0.3.1.

## [0.12.0] - 2026-05-30

### Fixed

- `close()` with `priority` feature: drain `pre_connect_buf` before teardown and re-snapshot wire peers inside the close loop so newly connected peers are detected.
- `close()` resource leak under sustained traffic.
- Fall back to one-shot on multishot recv stream `None` during accumulation.
- Enforce send HWM on direct-encode path.

### Performance

- Replace send-path atomics with `Cell` (17M to 22M msg/s).
- Optimize 8 B TCP recv (14M to 17M msg/s).
- Persist futures across driver/recv select iterations.

### Changed

- *(deps)* Bump `omq-proto` to 0.15.0, `blume` to 0.3.0. Tighten `rustc-hash` to 2.1.0, `concurrent-queue` to 2.5.0.

## [0.11.0] - 2026-05-25

### Fixed

- Framing desync: route pending messages through encoded queue.
- `flush_codec_to_wire` race with recv-direct's `flush_codec_output`.
- recv-direct cancel-safety: `ENOBUFS`/`ECANCELED` under sustained load no longer kills connections.
- Round-robin send blocking forever when no peer is connected.
- `MultiShot` recv disabled on non-Linux (use `OneShot` instead).
- Drain `pre_connect_buf` on peer install for priority feature.

### Performance

- Optimize fan-in recv: inline `InboundFrame`, skip identity clone.
- Optimize multi-peer wire send path.
- Bypass per-message routing overhead on single-peer wire send.
- Use `FxHashMap`/`FxHashSet` for internal maps.

### Changed

- Register peer identity synchronously during handshake dispatch.
- *(deps)* Bump `omq-proto` to 0.14.0, `yring` to 0.2.2, `blume` to 0.2.4. Upgrade `rand` 0.8 → 0.10.

## [0.10.1] - 2026-05-23

### Changed

- *(deps)* Bump `yring` to 0.2.1.

## [0.10.0] - 2026-05-23

### Added

- WebSocket transport (`ws://`) routed through the internal TCP driver; no `compio-ws` or `tungstenite` dependency.
- WSS/TLS transport (`wss://`).
- DNS resolution for TCP and WebSocket transports.
- `ZMQ_STREAM` socket type for raw TCP communication.

### Fixed

- WS driver: large-message bypass, leftover bytes, and encode path correctness.
- Build without `ws` feature.
- REQ socket state machine reset race on reconnect.
- Clippy pedantic warnings.

### Performance

- WS small-message throughput 3× via recv fast path and send direct-encode.

### Changed

- *(deps)* Bump `omq-proto` to 0.13.0.

## [0.9.1] - 2026-05-21

### Fixed

- `Poller.poll()` busy-wait: call `signal_close()` on destroy, use `Selector`-based `wait_any`.

### Changed

- *(deps)* Bump `omq-proto` to 0.12.0.

## [0.9.0] - 2026-05-21

### Changed

- *(deps)* Bump `omq-proto` to 0.11.0.

## [0.8.0] - 2026-05-20

### Changed

- *(deps)* Bump `omq-proto` to 0.10.0.
- Compression benchmark: sender and receiver now run on separate threads so compression and decompression overlap. Add `OMQ_BENCH_ZSTD_LEVEL` env var.

## [0.7.0] - 2026-05-20

### Added

- `Socket::multishot_rearms()` counter for diagnosing recv path transitions.

### Changed

- Recv: avoid multi-shot re-arm between consecutive large messages. After the first `MultiShot` → `OneShot` transition, subsequent large frames stay in one-shot mode instead of cycling through the BUF_RING pool each time.
- Bench warmup: time-bound prime phase (500 ms cap) and start calibration at small iteration counts. Large-message cells no longer spend 30+ seconds in warmup.

## [0.6.1] - 2026-05-19

### Fixed

- Memory leak under peer churn: replaced `Vec<PeerSlot>` with `Slab<PeerSlot>` so dead peer entries are reclaimed rather than accumulated. Before: 100 MiB to 208 MiB in 300 s; after: stable at 8.6 MiB.

## [0.6.0] - 2026-05-19

### Added

- `MonitorPublisher` convenience methods (`listening`, `accepted`, `connected`, `disconnected`, `handshake_succeeded`, `handshake_failed`, `closed`) replacing verbose `MonitorEvent` construction.
- `DirectIoState::lock_io()` centralizing peer_io mutex acquisition.
- `DirectIoState::signal_eof()` replacing 11 inline `eof_signal.notify` calls.
- `RecvAction` enum replacing opaque `ControlFlow` return types in recv.rs.
- `DriverLoopState` struct consolidating 9 mutable driver-loop locals into methods.
- `impl SocketApi for Socket` for compile-time API parity with omq-tokio.

### Changed

- `bind()` returns `Result<Endpoint>` instead of `Result<()>`.
- Large files split into focused submodules: `handle.rs` into `bind.rs`, `connect.rs`, `recv.rs`; `inner.rs` into `direct_io.rs`, `encoded_queue.rs`, `peer.rs`; `driver.rs` into `dispatch.rs`, `recv_stream.rs`.
- `install.rs`: extracted `spawn_snap_listener` and `handle_driver_exit` from `spawn_wire_driver`.

## [0.5.5] - 2026-05-18

### Fixed

- `identity_to_slot` memory leak: stale identity entries accumulated on reconnect (~88 B/cycle, 275 MiB over 2 h). Now removed when a peer gets a new generated identity on the same slot.
- `close()` hang: replaced unbounded `send_async(Close)` with `try_send` + 100 ms timeout so close completes even when the driver's command channel is full.
- Soak RSS threshold: added 10 MiB absolute floor so sub-10 MiB growth from allocator noise does not trip the percentage gate.

### Added

- `soak` Cargo feature gating 12 long-running leak-detection scenarios.

## [0.5.4] - 2026-05-17

### Fixed

- Inproc connect-before-bind now works on the single-threaded runtime. The connect is deferred to a background task that waits indefinitely for the bind, matching TCP/IPC reconnect behavior.

## [0.5.3] - 2026-05-17

### Changed

- *(deps)* Bump `flume` to 0.12.

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

### Changed

- *(deps)* Bump `omq-proto` to 0.7.0.

## [0.2.14] - 2026-05-13

### Changed

- *(deps)* Bump `omq-proto` to 0.6.0.

## [0.2.13] - 2026-05-13

### Changed

- Large-message recv rewritten: accumulation + ENOBUFS-triggered one-shot
  transition replaces the unreliable CancelToken-based cancel+drain path.
  Messages above `Options::large_message_threshold` (default 128 KiB) are
  accumulated into a pre-allocated `BytesMut`, bypassing the codec's
  `ChunkedInputBuf` coalesce copy (~2× memcpy → ~1×). When the payload
  exceeds the BUF_RING pool, the kernel terminates the multi-shot SQE
  with `ENOBUFS`; the recv path transitions to one-shot and `read_until`
  pulls the remainder in a single syscall. Consecutive large messages
  stay in one-shot mode. A small message re-arms multi-shot.
- Accumulation state (`pending_acc`, `large_recv_pending`) lives in
  `DirectIoState` and survives recv-future cancellation. `AccRestore`
  drop guard saves partial progress during one-shot `read_until`.
- Removed dead cancel+drain code path from `try_one_shot_large_recv`.
- Updated `runtime.rs` pool sizing docs: pool is a small-message
  burst-absorption knob, not a large-message sizing requirement.

### Performance

- IPC bench_peer vs libzmq: 512 KiB 1.8×, 2 MiB 1.9×, 8 MiB 1.6×,
  32 MiB 1.9× (was 0.72× at 32 MiB before this change).

## [0.2.12](https://github.com/paddor/omq.rs/compare/omq-compio-v0.2.11...omq-compio-v0.2.12) - 2026-05-12

### Changed

- *(deps)* Bump `omq-proto` to 0.4.0.
- Adapt to `omq-proto` Message API changes: use `msg.part_bytes(0)` in
  inproc single-part path (was `Bytes::from(msg)`).
- Use `Payload::as_slice()` (now infallible) in large-recv prefix copy.

### Fixed

- Publish `MonitorEvent::HandshakeFailed` when pending messages are dropped
  after a failed ZMTP handshake. Previously the drop was silent.

## [0.2.11](https://github.com/paddor/omq.rs/compare/omq-compio-v0.2.10...omq-compio-v0.2.11) - 2026-05-09

### Changed

- *(deps)* replace `compio` git dep with `version = "0.19.0-rc.1"` for
  crates.io publish. Rev `453ed63` is identical to that release. No code change.

## [0.2.10](https://github.com/paddor/omq.rs/compare/omq-compio-v0.2.9...omq-compio-v0.2.10) - 2026-05-09

### Changed

- *(deps)* pin `blume = { version = "0.1.0" }` in Cargo.toml so `cargo publish`
  resolves it from crates.io. No code or behavior change.

## [0.2.9](https://github.com/paddor/omq.rs/compare/omq-compio-v0.2.8...omq-compio-v0.2.9) - 2026-05-09

### Added

- Recv path now switches to a sized one-shot read for inbound frames
  whose wire payload exceeds `Options::large_message_threshold`
  (default 128 KiB). After the codec parses a large-frame header, the
  per-stream `compio` `CancelToken` cancels the multi-shot recv SQE,
  any in-flight CQEs are drained into the destination buffer, and the
  remaining bytes are read directly into one contiguous `BytesMut`.
  Result: zero userspace memcpy on the long tail of the payload (the
  drained prefix is bounded by the io_uring pool slot size). The
  multi-shot stream is rebuilt before normal recv resumes, so the
  small-message path is unchanged. Pass `0` or
  `Options::disable_large_message_path()` to keep every recv on the
  multi-shot path.

### Changed

- Recv path copies each buf-ring slot into a `Bytes` and drops the
  `BufferRef` immediately so the slot returns to the pool, then calls
  `handle_input(Bytes)` directly. This replaces the previous
  `BytesMut::extend_from_slice` path which triggered O(n log n)
  reallocation copies as large messages accumulated bytes; now each
  received slot is exactly one copy regardless of message size.
- `runtime.rs` module doc and `doc/compio.md` now include a pool sizing
  recipe: a table mapping peak message size to recommended slot size and
  pool RAM, with guidance on slot count trade-offs. The very-large
  message trade-off in that recipe is rewritten to point at
  `large_message_threshold` instead of describing the old
  `extend_from_slice` regrowth behaviour. The `RecvStream` section
  documents the per-stream `CancelToken` and the `.with_cancel`
  registration discipline. `doc/performance.md` adds a "Zero-copy
  recv for large frames" chapter walking through both halves of the
  change (chunked input buf + cancel-and-drain one-shot recv).

## [0.2.8](https://github.com/paddor/omq.rs/compare/omq-compio-v0.2.7...omq-compio-v0.2.8) - 2026-05-05

### Changed

- Recv path migrated to io_uring multi-shot recv against a registered
  `BUF_RING`. One persistent SQE per connection; the kernel pulls a
  buffer from the pool when bytes are ready and delivers a `BufferRef`
  to the runtime stream. Dropping a `recv()` future no longer cancels
  the SQE, so bytes are not lost on cancellation. The recv-claim and
  poll-readiness scaffolding that worked around the old hazard is
  gone. Requires Linux >= 6.0 (multi-shot recv with provided buffers).
- `peer_io` is now a `std::sync::Mutex` rather than `async_lock::Mutex`.
  The codec is driven from a single-thread runtime and the lock is
  never held across `.await`, so the sync mutex never blocks waiting
  on a parked holder. This is what keeps extract-buffer-and-feed
  atomic in the recv path: there is no `.await` between pulling a
  buffer and calling `handle_input`.
- New `omq_compio::runtime::ProactorBuilderExt::with_omq_buffer_pool()`
  helper sizes the runtime's buffer pool (128 x 32 KiB by default).
  `omq_compio::build_default_runtime()` is the convenience entry
  point. Bench harnesses and the binding now use it. External
  consumers who build their own `Runtime` should call it too.

### Fixed

- Recv-future cancellation no longer corrupts the byte stream. Before
  this change, dropping a `recv()` after the kernel had selected a
  user-space buffer but before the consumer observed it could forfeit
  those bytes, desyncing ZMTP framing on the next read. The new
  multi-shot recv path keeps bytes queued in the `BUF_RING` across
  consumer drops; the next `recv()` continues from the same byte
  position.

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

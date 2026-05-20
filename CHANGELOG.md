## Changelog

All notable changes to omq.rs will be documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.10] - 2026-05-20

### omq-tokio 0.8.1

- Route compression encoder output through `EncodedQueue` for batched writes. Up to 15x faster on compression transports.

### omq 0.8.1, omq-zeromq 0.3.3

- Dependency version bumps.

## [0.2.9] - 2026-05-20

### omq-proto 0.10.0

- `Options::compression_level(i32)` to configure zstd compression level.

### omq-compio 0.8.0, omq-tokio 0.8.0, omq 0.8.0, omq-zmq 0.1.4, omq-zeromq 0.3.2, pyomq 0.5.0

- Dependency version bumps.

## [0.2.8] - 2026-05-17

### omq-proto 0.8.1, blume 0.2.1, omq-zeromq 0.2.2

- Doc comments on all public API items for docs.rs coverage.

### omq-compio 0.5.2, omq-tokio 0.5.2, omq 0.5.1, omq-zmq 0.1.2

- Dependency version bumps only.

## [0.2.7] - 2026-05-14

### omq-proto 0.8.0

- `DisconnectReason::Handover` variant for ROUTER/SERVER identity handover.

### omq-compio 0.4.0

- ROUTER/SERVER identity handover: new connection with duplicate identity
  evicts the old peer.

### omq-tokio 0.4.0

- ROUTER/SERVER identity handover: new connection with duplicate identity
  evicts the old peer.

### omq 0.4.0

- Bump `omq-compio` to 0.4.0, `omq-tokio` to 0.4.0.

## [0.2.6] - 2026-05-12

### omq-proto 0.4.0

- **Breaking:** Remove `Deref<Target=[u8]>` and `From<Message> for Bytes`.
  Use `msg.get(i)` or `&msg[i]` for zero-copy `&[u8]` frame access;
  `msg.part_bytes(i)` for owned `Bytes`.
- **Breaking:** Remove `Payload` from public API. `PayloadInner::Multi`
  removed — all payloads are now guaranteed contiguous.
- `Payload::as_slice()` returns `&[u8]` (was `Option<&[u8]>`).
- `ChunkedInputBuf::split_to()` coalesces when spanning chunk boundaries
  instead of producing multi-chunk payloads.
- New: `Message::get(index) -> Option<&[u8]>` — checked zero-copy frame access.
- New: `impl Index<usize> for Message` — `&msg[0]` returns `&[u8]`, panics on OOB.
- Fixed: account for per-part overhead in `max_message_size` check. Zero-length
  MORE frames no longer bypass the limit.
- Fixed: reject oversized frames at header parse time.
- Fixed: `Options::authenticator` is `#[track_caller]`; panics point to the call site.
- Perf *(blake3zmq)*: stack-allocate 9-byte AAD buffer instead of `Vec` per frame.
- Security *(blake3zmq)*: `Session` key and nonce zeroized on drop.

### omq-compio 0.2.12

- Fixed: publish `MonitorEvent::HandshakeFailed` when pending messages are dropped
  after a failed ZMTP handshake. Previously the drop was silent.
- Adapted to `omq-proto` 0.4.0 Message API changes.

### omq-tokio 0.2.7

- Bump `omq-proto` to 0.4.0.

### pyomq 0.2.4

- Bump `omq-compio` to 0.2.12 and `omq-proto` to 0.4.0.

## [0.2.5] - 2026-05-09

### omq-compio 0.2.10

- Pin `blume = { version = "0.1.0" }` for crates.io publish. No code change.

## [0.2.4] - 2026-05-09

### omq-proto 0.3.0

- **Breaking:** `Connection::handle_input` now takes `Bytes` instead of
  `&[u8]`. Callers with a slice use `Bytes::copy_from_slice`; callers with
  an already-owned `Bytes` pass it directly with no copy.
- Codec inbound buffer replaced with `ChunkedInputBuf`: received bytes are
  appended as owned `Bytes` chunks without copying; the frame decoder slices
  into them directly. Eliminates O(n log n) `BytesMut` reallocation for
  large messages.
- New `Options::large_message_threshold(n)` /
  `Options::disable_large_message_path()`: tune the frame size at which
  io_uring backends switch from multi-shot to a sized one-shot read
  (default 128 KiB).
- New `Connection` API for direct-recv backends:
  `peek_next_frame_payload_size`, `begin_supplied_payload`, `supply_payload`.

### omq-compio 0.2.9

- Large-frame one-shot recv: for frames whose wire payload exceeds
  `large_message_threshold`, the multi-shot SQE is cancelled, any in-flight
  CQEs are drained, and the remainder is read directly into one contiguous
  `BytesMut`. Zero userspace memcpy on the long tail of the payload.
- Buf-ring: each slot is now copied into a `Bytes` and the `BufferRef`
  dropped immediately so the slot returns to the pool; `handle_input(Bytes)`
  is called directly. Replaces the old `BytesMut::extend_from_slice` path.

### omq-tokio 0.2.4

- `Options::large_message_threshold` / `disable_large_message_path` accepted
  for API parity with omq-compio; no effect on tokio (no buf-rings).
- Codec inbound buffer is now a chunked queue (same change as omq-proto 0.3.0
  delivers); large messages see one copy per read instead of O(n log n).

### pyomq 0.2.0

- First PyPI release. Python binding for omq-compio (compio/io_uring backend).
  Linux x86_64 and aarch64 wheels; stable ABI covers Python 3.9+.

## [0.2.0] - 2026-05-04

First public release. Three-crate Rust ZeroMQ implementation, wire-compatible
with libzmq. compio is the primary backend; omq-tokio is the cross-runtime
alternative.

### Crates

- **omq-proto**: sans-I/O ZMTP 3.1 codec, message types, mechanism state
  machines, message-transform layer, routing, options builder. No async,
  no I/O, no traits on the hot path.
- **omq-compio**: io_uring socket driver. Single-threaded runtime; spawn
  multiple runtimes for multi-core workloads.
- **omq-tokio**: multi-thread tokio backend. Identical public Socket API.

### Bindings

- **pyomq** (`bindings/pyomq`): PyO3 wrapper over `omq-compio`. Sync API
  plus an `asyncio`-compatible bridge. Built out-of-tree via maturin
  (excluded from `cargo build --workspace`).

### Socket API

- `Socket::new`, `bind`, `connect`, `unbind`, `disconnect`, `send`, `recv`,
  `try_send`, `try_recv`. Identical signatures across both backends.
  `try_send` / `try_recv` are synchronous and non-blocking: they return
  `Err(Error::WouldBlock)` immediately rather than suspending the task.
  `Error::WouldBlock` is the new variant in `omq-proto::Error`.
- `Socket::connect_with(endpoint, ConnectOpts)` (gated `priority` feature)
  for strict per-pipe priority on round-robin patterns.
- `Socket::join` / `Socket::leave` for DISH (RFC 48).
- `Socket::monitor()`: socket-like `Stream` with owned `PeerInfo` context
  on every event.
- `Endpoint` enum with `Display` / `FromStr` round-trip.

### Socket types

Standard (RFC 28 + RFC 47): PUSH, PULL, PUB, SUB, XPUB, XSUB, REQ, REP,
DEALER, ROUTER, PAIR. Group/transport drafts: RADIO, DISH (RFC 48). Draft
RFC stubs: CLIENT/SERVER (RFC 41), SCATTER/GATHER (RFC 49), CHANNEL
(RFC 51), PEER, RAW.

### Transports

- `tcp://` (IPv4 + IPv6).
- `ipc://` (Unix domain sockets, filesystem or abstract namespace).
- `inproc://` (process-local lock-free channel; process-wide registry).
- `udp://` (RADIO/DISH, RFC 48 datagram framing).
- `lz4+tcp://` (gated `lz4`, optional pre-trained dictionary).
- `zstd+tcp://` (gated `zstd`, optional static or auto-trained dict).

### Mechanisms

- **NULL** (default): plaintext, no handshake-time auth.
- **CURVE** (gated `curve`, RFC 26): Curve25519 box per data frame.
- **BLAKE3ZMQ** (gated `blake3zmq`): omq-native AEAD; X25519 + BLAKE3 +
  ChaCha20 + BLAKE3 MAC.

### Cargo features

All opt-in. Default build is the smallest deploy: NULL + TCP/IPC/inproc/UDP,
no C compiler required. Features: `curve`, `blake3zmq`, `lz4`, `zstd`,
`priority`. See `README.md` for the table.

### Options

Typed builder over `Options`. `ReconnectPolicy::default()` is `Fixed(100ms)`
matching libzmq's `ZMQ_RECONNECT_IVL`; `Exponential { min, max }` is opt-in.
`OnMute` controls send-HWM behavior: `Block` (default), `DropNewest`,
`DropOldest`. Defaults differ from libzmq in two places: per-socket HWM
semantics, conflate restricted to FanOut patterns.

### Performance

See [BENCHMARKS.md](BENCHMARKS.md).

### Conventions

- Rust 2024 edition, MSRV 1.93.
- Workspace lints: rust `missing_debug_implementations` denied; clippy
  `pedantic` warned with noisy rules silenced.
- ASCII-only source.
- `Cargo.lock` deliberately untracked (this is a library workspace).

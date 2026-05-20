# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.11.0] - 2026-05-21

### Changed

- CURVE mechanism hardened: server handshake is now stateless until the client cookie is verified, low-order public keys are rejected, and all key comparisons use constant-time equality.
- BLAKE3ZMQ: `SessionKeys` are zeroized on drop.
- *(deps)* `CurveSecretKey::derive_public()` added for pyomq `curve_public()`.

## [0.10.0] - 2026-05-20

### Added

- `Options::compression_level(i32)` to configure zstd compression level (default -3).
- `pub` visibility on `transform::zstd::DEFAULT_LEVEL`.

## [0.9.0] - 2026-05-19

### Added

- `SocketApi` trait formalizing the shared public API between compio and tokio backends.

### Changed

- `Connection` internals split into `connection/inbound.rs` and `connection/outbound.rs` for readability.

### Breaking

- `SocketApi::bind` returns `Result<Endpoint>` instead of `Result<()>`, providing the resolved endpoint (with actual port for wildcard binds) directly.

## [0.8.4] - 2026-05-18

### Added

- `Connection::transmit_chunks_capped`: like `transmit_chunks` but caps the number of iovecs returned, preventing `SmallVec` heap spill on large batches.

## [0.8.3] - 2026-05-18

### Fixed

- `Payload::from_slice`: fall back to heap allocation when an inline `Message` (39 B max) exceeds `Payload`'s inline limit (38 B). Previously panicked during compression transforms.

## [0.8.2] - 2026-05-17

### Changed

- *(deps)* Bump `socket2` to 0.6.

## [0.8.1] - 2026-05-17

### Added

- Doc comments on all public API items for docs.rs coverage.

## [0.8.0] - 2026-05-14

### Added

- `DisconnectReason::Handover` variant for ROUTER/SERVER identity handover.

## [0.7.0] - 2026-05-14

### Added

- PLAIN security mechanism (RFC 24): four-command handshake providing
  username/password authentication. Feature-gated behind `plain` with zero deps.

### Fixed

- Return error instead of panicking on overlong CURVE metadata properties.

## [0.6.0] - 2026-05-13

### Changed

- **Breaking:** `Connection::transmit_chunks()` returns `SmallVec<[IoSlice; 8]>`
  instead of `Vec<IoSlice>`, avoiding a heap allocation on typical flushes.
- **Breaking:** Post-handshake READY/ERROR commands are now rejected as protocol
  violations (ZMTP RFC 23), dropping the connection.
- REQ/REP `pre_send` uses `into_parts_payload()` to move payloads instead of
  double-cloning via `parts_payload()`.

### Fixed

- CURVE `cookie_key`, BLAKE3ZMQ `Keypair.secret`, and `CookieKeyring` keys are
  now zeroized on drop via `Zeroizing<[u8; 32]>`.
- `SessionKeys` `Debug` impl redacted — no longer prints session keys/nonces.

### Refactored

- BLAKE3ZMQ: replaced `Role` enum + `mem::replace` placeholder dance with flat
  `Option` fields, matching CURVE's pattern.

## [0.5.0] - 2026-05-13

### Changed

- **Breaking:** Rename `tcp_recv_buffer_size` / `tcp_send_buffer_size` to
  `recv_buffer_size` / `send_buffer_size` (they apply to IPC too).

## [0.4.0](https://github.com/paddor/omq.rs/compare/omq-proto-v0.3.2...omq-proto-v0.4.0) - 2026-05-12

### Changed

- **Breaking:** Remove `Deref<Target=[u8]>` and `From<Message> for Bytes`.
  Use `msg.get(i)` or `&msg[i]` for zero-copy `&[u8]` frame access;
  `msg.part_bytes(i)` for owned `Bytes`.
- **Breaking:** Remove `Payload` from public API. `PayloadInner::Multi`
  removed — all payloads are now guaranteed contiguous.
- `Payload::as_slice()` returns `&[u8]` (was `Option<&[u8]>`).
- `ChunkedInputBuf::split_to()` coalesces when spanning chunk boundaries
  instead of producing multi-chunk payloads.

### Added

- `Message::get(index) -> Option<&[u8]>` — checked zero-copy frame access.
- `impl Index<usize> for Message` — `&msg[0]` returns `&[u8]`, panics on OOB.

### Fixed

- Account for per-part overhead (`size_of::<Payload>()`) in `max_message_size`
  check. Zero-length MORE frames no longer bypass the limit.
- Reject oversized frames at header parse time instead of waiting for
  the full payload to arrive.
- `Options::authenticator` is now `#[track_caller]`; panics point to the
  call site instead of inside the library.

### Performance

- *(blake3zmq)* Stack-allocate 9-byte AAD buffer instead of `Vec` per frame.

### Security

- *(blake3zmq)* `Session` key and nonce are zeroized on drop via
  `ZeroizeOnDrop`. Key material no longer lingers in freed memory.

## [0.3.2](https://github.com/paddor/omq.rs/compare/omq-proto-v0.3.1...omq-proto-v0.3.2) - 2026-05-10

### Added

- *(lz4)* LZ4M multi-block encoding for parts larger than 1 GiB.
  The LZ4 block API caps a single call at ~2 GiB (`i32` parameters);
  LZ4M chunks the payload into 1 GiB blocks, each independently
  compressed and decompressed. Wire format: `LZ4M` sentinel
  (`4C 5A 34 4D`) + `u64 LE` total decompressed size + per-block
  `(u32 LE compressed_len | LZ4 block)` pairs. Parts at or below
  the block size continue to use the existing `LZ4B` single-block
  encoding. `Lz4Encoder` and `Lz4Decoder` gain `with_block_size()`
  to override the default for testing.

### Fixed

- *(curve)* Enforce monotonic nonce counter on incoming CURVE MESSAGE
  commands per RFC 26. Previously, any counter value was accepted,
  allowing replay of captured encrypted frames.

### Changed

- `Message::parts_payload()` returns `SmallVec<[Payload; 1]>` instead
  of `Vec<Payload>`, eliminating a per-send heap allocation for
  single-part messages (+8% on 8 B IPC throughput).

## [0.3.1](https://github.com/paddor/omq.rs/compare/omq-proto-v0.3.0...omq-proto-v0.3.1) - 2026-05-09

### Fixed

- *(blake3zmq)* raise `chacha20-blake3` floor to 0.9.12 (paddor/chacha20-blake3);
  fixes ARM CI (aarch64 build failure introduced in 0.9.11).

## [0.3.0](https://github.com/paddor/omq.rs/compare/omq-proto-v0.2.3...omq-proto-v0.3.0) - 2026-05-09

### Added

- `Options::large_message_threshold(n)` and
  `Options::disable_large_message_path()`: tune the wire-payload size at
  which compatible recv backends switch from a buffer-pool multi-shot
  read to a single sized one-shot read. Default: `Some(128 * 1024)`.
  Honoured by `omq-compio`; accepted but ignored on `omq-tokio` for API
  parity.
- New `Connection` API for direct-recv I/O backends:
  `peek_next_frame_payload_size`, `begin_supplied_payload`, and
  `supply_payload`. A backend that has decided to recv a large frame's
  payload directly into a sized buffer (instead of via the codec's
  inbound chunk queue) consumes the wire-frame header from the codec
  with `begin_supplied_payload`, recvs the payload bytes itself, and
  hands them back as one `Bytes` via `supply_payload`. The codec runs
  the same mechanism decrypt and demux path as it would on an
  in-buffer-assembled frame. Existing callers that never invoke the
  new methods are unaffected.

### Changed

- Codec inbound buffer replaced with a chunked queue (`ChunkedInputBuf`):
  received bytes are appended as owned `Bytes` chunks without copying,
  and the frame decoder slices into them directly. This eliminates the
  `BytesMut` reallocation chain that previously scaled as O(n log n) for
  large messages, cutting total copies to one per received chunk.
- `Connection::handle_input` now takes `Bytes` instead of `&[u8]`. Callers
  with a slice use `Bytes::copy_from_slice`; callers with an already-owned
  `Bytes` (e.g. from a buf-ring slot) pass it directly with no copy.
- `frame::try_decode_frame` and `greeting::try_decode` are now
  `pub(crate)` (they were never part of the stable public API).

## [0.2.3](https://github.com/paddor/omq.rs/compare/omq-proto-v0.2.2...omq-proto-v0.2.3) - 2026-05-05

### Fixed

- *(zstd)* dict shipment is now wire-compatible with `omq-zstd` Ruby.
  The encoder used to prepend `SENTINEL_DICT` ahead of a dict body that
  already begins with `ZDICT_MAGIC`, doubling the magic on the wire; the
  decoder then stripped 4 bytes, so Rust↔Rust round-tripped only by
  symmetric mistake. Ruby ships the dict raw (per RFC), so Ruby→Rust
  dict-aware decompress failed at the first compressed message after
  auto-train (~msg 256 at 100 KiB / 402 B). Encoder now ships
  `Message::single(dict)`, decoder stores the whole received bytes, and
  `ZstdEncoder::with_send_dict` requires the dict to start with
  `ZDICT_MAGIC` (mirrors Ruby's `install_send_dict`).

### Added

- *(zstd)* public `omq_proto::proto::transform::train_zdict(samples, capacity)`
  for callers that want to ship a static dict but only have a sample
  corpus to train from. Returns ZDICT-format bytes accepted directly by
  `Options::compression_dict` / `ZstdEncoder::with_send_dict`.

## [0.2.2](https://github.com/paddor/omq.rs/compare/omq-proto-v0.2.1...omq-proto-v0.2.2) - 2026-05-04

### Changed

- *(blake3zmq)* switch `chacha20-blake3` dep from a git-pinned fork to
  crates.io v0.9.11 (paddor/chacha20-blake3). The fork inlines the `chacha`
  subcrate and carries `#[target_feature(enable = "avx2/avx512")]`
  annotations; full AVX2 throughput (~1 GiB/s) requires
  `RUSTFLAGS="-C target-cpu=native"` at build time; scalar path runs
  ~55 MiB/s without it.

## [0.2.1](https://github.com/paddor/omq.rs/releases/tag/omq-proto-v0.2.1) - 2026-05-04

### Added

- add try_send / try_recv (non-blocking send/recv)

### Fixed

- Socket::drop cancels tasks; REQ/REP reset on disconnect; tier-1 tests
- *(clippy)* use is_multiple_of in z85 encode/decode guards
- *(curve)* wire-compatible with libzmq + pyzmq interop suite

### Other

- pre-release polish, American English, comprehensive test-all.sh
- tighten default size sweep to 128 B / 2 KiB / 8 KiB
- *(payload)* Phase C — as_bytes(), single-chunk transforms, PAYLOAD_INLINE_CHUNKS=1
- fix all clippy warnings across workspace (--all-features)
- *(transform)* split MessageTransform into MessageEncoder + MessageDecoder
- *(tokio)* FLAT_THRESHOLD 32 KiB — fix 2 KiB TCP regression; fix(compio): second codec_maybe_dirty race; bench: add 32B size, 8-peer column
- *(compio)* flat-buf encoding, drain-vec reuse, codec-skip guards, passthrough fast path
- *(proto)* O(1) pending_transmit_size via cached out_bytes_total
- cargo fmt --all (first-time sweep)
- *(benchmarks)* rerun at 0.5s/cell, MB/GB units, BLAKE3ZMQ audit caveat
- *(clippy)* silence all pedantic warnings across feature combos
- full Stage/Phase narrative sweep across all src/
- Rust ZMQ: omq-proto codec, omq-tokio/omq-compio backends, pyomq binding

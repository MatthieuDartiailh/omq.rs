# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

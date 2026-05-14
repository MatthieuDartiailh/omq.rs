# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-05-14

### Added

- `plain` feature flag forwarded to both backends.

### Changed

- *(deps)* Bump `omq-compio` to 0.3.0, `omq-tokio` to 0.3.0.

## [0.2.7] - 2026-05-13

### Changed

- *(deps)* Bump `omq-compio` to 0.2.14, `omq-tokio` to 0.2.8.

## [0.2.5](https://github.com/paddor/omq.rs/compare/omq-v0.2.4...omq-v0.2.5) - 2026-05-09

### Changed

- *(deps)* track `omq-compio = 0.2.10` (blume dep fix; no behavior change).

## [0.2.4](https://github.com/paddor/omq.rs/compare/omq-v0.2.3...omq-v0.2.4) - 2026-05-09

### Changed

- *(deps)* track `omq-compio = 0.2.9` and `omq-tokio = 0.2.4`. Surface the
  large-frame zero-copy recv path (compio) and chunked codec inbound buffer
  (both) to consumers who depend on `omq` directly. See per-backend
  CHANGELOGs for details.

## [0.2.3](https://github.com/paddor/omq.rs/compare/omq-v0.2.2...omq-v0.2.3) - 2026-05-05

### Added

- `pub_sub_lz4` example (`omq/examples/pub_sub_lz4.rs`): pub/sub over
  `lz4+tcp://` with prefix-match subscribe. Run with
  `cargo run -p omq --example pub_sub_lz4 --no-default-features --features tokio-backend,lz4`.

### Changed

- *(deps)* track `omq-compio = 0.2.8` to surface the io_uring multi-shot
  recv migration and the recv-cancellation byte-stream fix to consumers
  who depend on `omq` directly. See the omq-compio CHANGELOG for
  details.

## [0.2.2](https://github.com/paddor/omq.rs/compare/omq-v0.2.1...omq-v0.2.2) - 2026-05-05

### Changed

- *(deps)* track `omq-tokio = 0.2.3` and `omq-compio = 0.2.6`. The
  facade carries no source change of its own — the bump exists to
  surface the wire-compatible zstd dict shipment fix and the tokio
  linger drain fix to consumers who depend on `omq` directly. See the
  per-backend changelogs for details.

## [0.2.1](https://github.com/paddor/omq.rs/releases/tag/omq-v0.2.1) - 2026-05-04

### Other

- pre-release polish, American English, comprehensive test-all.sh
- *(payload)* Phase C — as_bytes(), single-chunk transforms, PAYLOAD_INLINE_CHUNKS=1
- cargo fmt --all (first-time sweep)
- *(clippy)* silence all pedantic warnings across feature combos
- omq facade rustdoc, GH Actions, pyomq warnings to 0
- facade crate with mutually-exclusive backend features

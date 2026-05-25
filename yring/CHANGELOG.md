# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2] - 2026-05-25

### Fixed

- Sync `Producer::flush()` missed wakeup on stale `cached_head`.
- `async_spsc` missed wakeup on low-throughput flush.

### Changed

- *(deps)* Pin `atomic-waker` to 1.1.0.

## [0.2.1] - 2026-05-23

### Fixed

- `AsyncProducer::flush()`: refresh `cached_head` before the `was_empty` check to avoid using a stale value, which broke alternating push-pop patterns.

## [0.2.0] - 2026-05-17

### Added

- `async` feature: `AsyncProducer` (auto-wakes on flush) and `AsyncConsumer`
  (implements `futures_core::Stream`). No runtime dependency.
- `examples/basic.rs`: cross-thread producer/consumer demonstration.
- `benches/throughput.rs`: u64 and 128-byte throughput benchmark.

## [0.1.0] - 2026-05-17

### Added

- Bounded SPSC ring with ypipe-style batched flush/prefetch.
- `Producer`: zero-atomic `push`, single-Release `flush`.
- `Consumer`: zero-atomic `pop`, single-Acquire `prefetch`.
- `FlushResult` with `was_empty` flag for waker integration.

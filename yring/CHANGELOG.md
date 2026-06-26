# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.2] - 2026-06-26

### Changed

- Add `// SAFETY:` comments on `push`, `pop`, `drop_remaining` unsafe blocks.

## [0.3.1] - 2026-06-17

### Fixed

- `AsyncConsumer`/`AsyncProducer`: use explicit `consumer_dropped`/`producer_dropped` atomic flags instead of `Arc::strong_count` for disconnect detection. Eliminates false-positive EOF when a third `Arc` clone exists.
- `AsyncConsumer::poll_next`: release consumed positions before parking so the producer can reuse slots while the consumer waits.

## [0.3.0] - 2026-05-30

### Added

- `AsyncProducer::push_async`: async push that waits for space when the ring is full.
- `Consumer::release()` / `AsyncConsumer::release()`: explicit publish of consumed position. Enables batch-pop without a Release store per item.
- `Consumer::is_disconnected()`: detect producer-dropped + ring-drained.
- `Producer::flush_and_check()`: flush + report `was_empty` (one extra Acquire load). The plain `flush()` is now a single Release store.

### Changed

- Deduplicate sync/async ring operations into `Ring<T>` shared core.
- `Consumer::pop()` no longer publishes `head` on every call; callers must call `release()` after a batch.
- `Producer::flush()` simplified to a single Release store (no `head` load). Use `flush_and_check()` when `was_empty` is needed.
- `push_and_flush` returns `Result<(), T>` instead of `Result<FlushResult, T>`.
- `Producer::drop` / `Consumer::drop` flush and release automatically.

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

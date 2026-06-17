# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.1] - 2026-06-17

### Fixed

- `Receiver::close()` and `Receiver::drop()`: recover from poisoned mutex instead of panicking.

## [0.4.0] - 2026-06-10

### Changed

- `Receiver`: replace internal `Mutex<VecDeque<T>>` cache with `RefCell<VecDeque<T>>` (single-consumer, no contention).
- `recv_batch`: return count of newly drained items, not total `out.len()`.
- `Sender::drop`: recover from poisoned mutex instead of panicking.

## [0.3.0] - 2026-05-30

### Added

- `Receiver::close()` method to signal sender disconnection.

## [0.2.4] - 2026-05-25

### Changed

- *(deps)* Pin `event-listener` to 5.4.0.

## [0.2.3] - 2026-05-23

### Changed

- Add `readme`, `keywords`, and `categories` to crate metadata.

## [0.2.2] - 2026-05-17

### Changed

- *(dev-deps)* Bump `flume` to 0.12.

## [0.2.1] - 2026-05-17

### Added

- Crate-level and item-level doc comments on all public API items.

## [0.2.0] - 2026-05-17

### Removed

- `pub mod spsc` (moved to standalone `yring` crate).

## [0.1.0] - 2026-05-14

Initial release: batching MPSC channel with swap-drain consumer.

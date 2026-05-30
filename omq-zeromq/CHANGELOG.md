# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.1] - 2026-05-30

### Changed

- *(deps)* Bump `omq-tokio` to 0.13.0, `omq-proto` to 0.15.0. Tighten `tokio` to 1.52.0.

## [0.7.0] - 2026-05-25

### Changed

- *(deps)* Bump `omq-tokio` to 0.12.0, `omq-proto` to 0.14.0.

## [0.6.1] - 2026-05-23

### Changed

- *(deps)* Bump `omq-tokio` to 0.11.1.

## [0.6.0] - 2026-05-23

### Changed

- *(deps)* Bump `omq-tokio` to 0.11.0, `omq-proto` to 0.13.0.

## [0.5.0] - 2026-05-21

### Changed

- *(deps)* Bump `omq-tokio` to 0.9.0, `omq-proto` to 0.11.0.

## [0.4.0] - 2026-05-20

### Added

- `Socket::disconnect()` method for closing individual connections.
- `SocketOptions::tcp_keepalive()` setter and `KeepAlive` re-export.

## [0.3.3] - 2026-05-20

### Changed

- *(deps)* Bump `omq-tokio` to 0.8.1.

## [0.3.2] - 2026-05-20

### Changed

- *(deps)* Bump `omq-tokio` to 0.8.0, `omq-proto` to 0.10.0.

## [0.3.1] - 2026-05-17

### Changed

- *(deps)* Bump `omq-tokio` to 0.5.3, `omq-proto` to 0.8.2.

## [0.3.0] - 2026-05-17

### Added

- `From<Vec<Bytes>>` and `From<VecDeque<Bytes>>` for `ZmqMessage`.
- `TryFrom<ZmqMessage>` for `String` and `Vec<u8>` (single-frame messages).
- `util` module re-exporting `PeerIdentity` for zmq.rs import path compatibility.
- `trybuild` compatibility test suite documenting API gaps with zmq.rs.

### Changed

- **Breaking:** `Socket::close()` now takes `self` and returns `Vec<ZmqError>`, matching the zmq.rs signature.

## [0.2.2] - 2026-05-17

### Added

- Crate-level doc comment for docs.rs.

### Changed

- *(deps)* Bump `omq-tokio` to 0.5.2, `omq-proto` to 0.8.1.

## [0.2.1] - 2026-05-17

### Added

- README with migration guide, code example, and benchmark comparison table.

## [0.2.0] - 2026-05-17

### Changed

- *(deps)* Bump `omq-tokio` to 0.5.0.

## [0.1.0] - 2026-05-14

Initial release: drop-in `zeromq` crate replacement backed by omq-tokio.

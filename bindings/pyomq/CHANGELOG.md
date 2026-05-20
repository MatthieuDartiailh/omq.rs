# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.1] - 2026-05-21

### Fixed

- Fix example in README.
- Fix flaky CI port allocation with OS-assigned wildcard ports.

## [0.6.0] - 2026-05-20

### Added

- Socket attribute-style option access (`socket.linger = 0`, `socket.identity`, etc.).
- `Socket.poll(timeout, flags)` per-socket polling method.
- `Socket.set_hwm()` / `Socket.get_hwm()` / `hwm` property.
- `Socket.set_string()` / `Socket.get_string()` aliases.
- `Socket.send_serialized()` / `Socket.recv_serialized()` for custom serialization.
- `Socket.__repr__()` showing socket type.
- `Socket.underlying` property (returns self, pyzmq compat).
- `Context.closed` property.
- `Context.destroy(linger=None)` with socket tracking.
- `Poller.sockets` property.
- `zmq.select(rlist, wlist, xlist, timeout)` function.
- `zmq.zmq_version()`, `zmq.pyomq_version()`, `zmq.pyomq_version_info()`.
- `zmq.curve_keypair()` and `zmq.curve_public(secret)` (when curve feature compiled).
- `zmq.has()` now checks compiled features (curve, plain, lz4, zstd, blake3zmq).
- `ZMQVersionError` exception.
- 30+ missing pyzmq constants (ROUTING_ID, MECHANISM, PLAIN_*, SNDBUF, RCVBUF, device types, security mechanism IDs).
- `getsockopt` support for RECONNECT_IVL, RECONNECT_IVL_MAX, HEARTBEAT_IVL/TTL/TIMEOUT, HANDSHAKE_IVL, CONFLATE.
- No-op `setsockopt`/`getsockopt` for 15+ pyzmq compat constants.
- SNDBUF/RCVBUF wired through to socket options.
- PLAIN_SERVER/USERNAME/PASSWORD stored in overlay, wired to `MechanismConfig` when plain feature enabled.
- `socket_id()` on AsyncSocket (enables async Poller).
- Async parity: `asyncio.Poller`, all new Socket convenience methods, attribute access, `Context.closed`/`destroy()`.
- `CurveSecretKey::derive_public()` in omq-proto.
- `plain` Cargo feature forwarding to omq-compio.

### Removed

- Dead pyproject.toml extras (curve, blake3zmq, lz4, zstd, all). The published wheel includes all features.

### Fixed

- README install section: document that all features are built into the wheel.

## [0.5.0] - 2026-05-20

### Changed

- *(deps)* Bump `omq-compio` to 0.8.0, `omq-proto` to 0.10.0.

## [0.4.2] - 2026-05-20

### Changed

- *(deps)* Bump `omq-compio` to 0.7.0, `omq-proto` to 0.9.0.

## [0.4.1] - 2026-05-19

### Fixed

- Fix `pyproject.toml` version not matching `Cargo.toml` (maturin uses `pyproject.toml`).

## [0.4.0] - 2026-05-19

### Changed

- *(deps)* Bump `omq-compio` to 0.6.0, `omq-proto` to 0.9.0.

## [0.3.1] - 2026-05-18

### Fixed

- `destroy_socket`: wait for pump tasks to drain and call `sock.close()` before returning. Previously leaked driver tasks and buffers on every Context/Socket teardown cycle.

### Added

- Soak test suite (7 scenarios) covering PUSH/PULL throughput, reconnect storm, PUB/SUB churn, peer churn, REQ/REP cycles, context/socket creation churn, and large messages. Each monitors RSS for memory leaks.

## [0.3.0] - 2026-05-14

### Changed

- *(deps)* Bump `omq-compio` to 0.4.0 and `omq-proto` to 0.8.0.

## [0.2.4](https://github.com/paddor/omq.rs/compare/pyomq-v0.2.3...pyomq-v0.2.4) - 2026-05-12

### Changed

- *(deps)* Bump `omq-compio` to 0.2.12 and `omq-proto` to 0.4.0. Update `Cargo.lock`.

## [0.2.3](https://github.com/paddor/omq.rs/compare/pyomq-v0.2.2...pyomq-v0.2.3) - 2026-05-09

### Changed

- Commit `Cargo.lock` (omitted from 0.2.2). pyomq ships its own lockfile.

## [0.2.2](https://github.com/paddor/omq.rs/compare/pyomq-v0.2.1...pyomq-v0.2.2) - 2026-05-09

### Changed

- *(deps)* bump `pyproject.toml` version to match `Cargo.toml`. The two were
  out of sync; maturin reads `pyproject.toml` as the authoritative version.

## [0.2.1](https://github.com/paddor/omq.rs/compare/pyomq-v0.2.0...pyomq-v0.2.1) - 2026-05-09

### Changed

- *(deps)* replace `compio` git dep with `version = "0.19.0-rc.1"` and add
  `"time"` feature. Aligns with omq-compio's registry dep to avoid duplicate
  compio-runtime instances; `compio::time::sleep` was already used in the
  source but the feature was missing from Cargo.toml.

## [0.2.0](https://github.com/paddor/omq.rs/releases/tag/pyomq-v0.2.0) - 2026-05-04

### Added

- First PyPI release. Python binding for omq-compio (compio/io_uring backend).
  Linux x86_64 and aarch64 wheels; stable ABI covers Python 3.9+.

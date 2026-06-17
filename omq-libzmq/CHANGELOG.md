# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.6] - 2026-06-17

### Added

- Complete `zmq_setsockopt`/`zmq_getsockopt` coverage: all 124 libzmq option constants defined. Unknown options now return `EINVAL` instead of silently succeeding.
- `ZMQ_IPV4ONLY` get/set support (inverse of `ZMQ_IPV6`).
- `ZMQ_BLOCKY`, `ZMQ_STREAM_NOTIFY` getsockopt stubs.

### Changed

- Deduplicate blocking recv logic (`block_recv` helper) and stale bypass cleanup (`clear_stale_bypass`).
- *(deps)* Bump `omq-tokio` to 0.14.3, `yring` to 0.3.1.

## [0.4.5] - 2026-06-12

### Changed

- *(deps)* Bump `omq-tokio` to 0.14.2.

## [0.4.4] - 2026-06-12

### Removed

- `zstd` feature: `zstd+tcp://` transport removed (follows `omq-tokio`).

### Changed

- *(deps)* Bump `omq-tokio` to 0.14.1. Tighten `tokio` to 1.52.

## [0.4.3] - 2026-06-10

### Changed

- Port from `omq-compio` to `omq-tokio` backend.
- Eliminate recv-pump relay: `RecvSink::Yring` for single-peer TCP/IPC. Inproc byte ring eliminates per-message heap allocation. TCP throughput stabilized at 5.5M msg/s (was 0.1-5M).
- Yield every 64 messages or 1 MiB sent to prevent starving the tokio worker.
- `send_accum` `Mutex` replaced with `UnsafeCell`, `send_ring` `RwLock` replaced with `AtomicBool` guard.
- Reduce `worker_threads` from 2 to 1.
- *(deps)* Bump `omq-tokio` to 0.14.0.

### Added

- `ZMQ_XPUB_NODROP` option.

### Fixed

- Inproc bypass recv hang on multipart messages.
- SPSC and recv-yring fast paths recover after peer churn.
- Harden FFI layer against panics: `lock_overlay!` macro, `OmqContext::new` returns `Option`, `run_on` returns `Result`.
- `zmq_poll`: reject negative `nitems`.
- Use `ptr::read/write_unaligned` for FFI int access.
- Stacked Borrows UB in inproc bypass: `Box<[UnsafeCell<u8>]>` for `RingBuf.buf`.

## [0.4.2] - 2026-05-30

### Changed

- *(deps)* Bump `omq-compio` to 0.12.0. Tighten `rustc-hash` to 2.1.0.

## [0.4.1] - 2026-05-25

### Changed

- *(deps)* Bump `omq-compio` to 0.11.0.

## [0.4.0] - 2026-05-25

### Fixed

- `drain_eventfds` on non-Linux: loop until pipe is empty.
- Set `O_NONBLOCK` on pipe fds in non-Linux `NotifyFd`.

### Changed

- Use `FxHashMap` for internal maps.
- *(deps)* Bump `omq-compio` to 0.11.0, `yring` to 0.2.2.

## [0.3.1] - 2026-05-23

### Changed

- *(deps)* Bump `yring` to 0.2.1.

## [0.3.0] - 2026-05-23

### Added

- `ZMQ_STREAM` socket type for raw TCP communication.

### Fixed

- Portable `errno` access in `zmq_poll`: use `libc::__errno_location()` via the `libc` crate rather than a direct extern declaration.

### Changed

- *(deps)* Bump `omq-compio` to 0.10.0.

## [0.2.0] - 2026-05-21

### Changed

- *(breaking)* Package renamed from `omq-zmq` to `omq-libzmq`. Library name
  (`omq_zmq`) and output filenames (`libomq_zmq.so` etc.) unchanged.
- 7 socket options that returned `ENOTSUP` (`ZMQ_BACKLOG`, `ZMQ_IMMEDIATE`,
  `ZMQ_CONNECT_TIMEOUT`, `ZMQ_PROBE_ROUTER`, `ZMQ_REQ_CORRELATE`,
  `ZMQ_REQ_RELAXED`, `ZMQ_XPUB_NODROP`) now store and round-trip their values.
- 13 rarely-used socket options (`ZMQ_AFFINITY`, `ZMQ_RATE`, etc.) are now
  explicitly accepted as no-ops instead of silently ignored by the wildcard arm.
- `zmq_msg_get`: returns routing ID for property 5, returns -1/EINVAL for
  unknown properties (matching libzmq).
- `zmq_msg_gets`: returns empty string for known property names (`Socket-Type`,
  `Identity`, `Routing-Id`, `Peer-Address`) instead of always failing.
- `zmq_ctx_set`/`zmq_ctx_get`: accept `ZMQ_SOCKET_LIMIT` (3) and `ZMQ_IPV6` (42).
- `zmq_socket` enforces `ZMQ_MAX_SOCKETS`; returns `EMFILE` when exceeded.
- `zmq_send` enforces `ZMQ_MAX_MSGSZ`; returns `EMSGSIZE` for oversized frames.
- `ZMQ_CONNECT_TIMEOUT` wired to backend handshake timeout.
- *(deps)* Bump `omq-compio` to 0.9.0.

## [0.1.4] - 2026-05-20

### Changed

- *(deps)* Bump `omq-compio` to 0.8.0.

## [0.1.3] - 2026-05-17

### Changed

- *(deps)* Bump `flume` to 0.12.

## [0.1.2] - 2026-05-17

### Changed

- *(deps)* Bump `omq-compio` to 0.5.2.

## [0.1.1] - 2026-05-17

### Changed

- *(deps)* Bump `yring` to 0.2.0.

## [0.1.0] - 2026-05-17

Initial release: libzmq-compatible C interface backed by omq-compio.

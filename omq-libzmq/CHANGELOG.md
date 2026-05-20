# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

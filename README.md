# ØMQ.rs

[![CI](https://github.com/paddor/omq.rs/actions/workflows/ci.yml/badge.svg)](https://github.com/paddor/omq.rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/omq?color=e9573f)](https://crates.io/crates/omq)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-%3E%3D%201.93-orange?logo=rust&logoColor=white)](https://www.rust-lang.org)

> **3.5M msg/s** inproc | **6.13M msg/s** ipc | **6.58M msg/s** tcp
>
> **5.57 µs** inproc latency | **17.4 µs** ipc | **24.4 µs** tcp

Pure Rust ZeroMQ. Wire-compatible with libzmq, faster at all message sizes.

- 11 standard socket types + 8 draft types
- Transports: inproc / IPC / TCP; UDP (RADIO/DISH only)
- Mechanisms: NULL / CURVE / BLAKE3ZMQ
- Compression: `lz4+tcp://` and `zstd+tcp://`

## Install

```sh
cargo add omq                                 # compio backend (default)
cargo add omq --no-default-features --features tokio-backend
```

```rust
use omq::{Endpoint, Message, Options, Socket, SocketType};

let push = Socket::new(SocketType::Push, Options::default());
push.connect("tcp://127.0.0.1:5555".parse()?).await?;
push.send(Message::single("hi")).await?;
```

Pub/sub with `lz4+tcp://` compression: [`omq/examples/pub_sub_lz4.rs`](omq/examples/pub_sub_lz4.rs)

`omq` is a thin facade — pick one backend at build time:

- `compio-backend` (default): single-thread io_uring/IOCP ([`omq-compio`](omq-compio/))
- `tokio-backend`: multi-thread tokio + mio ([`omq-tokio`](omq-tokio/))

Identical public `Socket` API on both, verified by `coverage_matrix` + `interop_compio` test suites.

## Cargo features

All optional. Default build is the smallest deploy: NULL mechanism +
TCP / IPC / inproc / UDP, no C compiler required. Enable any of:

| feature           | what it adds                                      | extra deps                       |
|-------------------|---------------------------------------------------|----------------------------------|
| `compio-backend`  | (default) compio io_uring/IOCP backend            | -                                |
| `tokio-backend`   | tokio multi-thread backend                        | -                                |
| `curve`           | CURVE encrypted-handshake mechanism (RFC 26)      | `crypto_box`, `crypto_secretbox` |
| `blake3zmq`       | OMQ-native BLAKE3 + ChaCha20 mechanism ([RFC](https://github.com/paddor/omq-blake3zmq/blob/main/RFC.md)) | `blake3`, `chacha20-blake3`, `x25519-dalek` |
| `lz4`             | `lz4+tcp://` compression transport ([RFC](https://github.com/paddor/omq-lz4/blob/main/RFC.md)) | `lz4-sys` |
| `zstd`            | `zstd+tcp://` compression transport ([RFC](https://github.com/paddor/omq-zstd/blob/main/RFC.md)) | `zstd-safe` (vends `libzstd`; needs `cc`) |
| `priority`        | Strict per-pipe priority on `Socket::connect_with`| -                                |

> [!WARNING]
> **BLAKE3ZMQ has not been independently security audited.** It's an
> omq-native construction (Noise XX + BLAKE3 + X25519 + ChaCha20-BLAKE3)
> and should not be relied on for anything that matters until it has had
> third-party review. Use **CURVE** (RFC 26) for production / regulated
> workloads. Audits welcome - open an issue if you can help fund or
> conduct one.

## Design highlights

- **Sans-I/O ZMTP codec** ([`omq-proto`](omq-proto/)): byte-in / events-
  out state machine, no async, no traits on the hot path.
- **Per-socket HWM with work-stealing send pumps** on round-robin patterns
  (PUSH / DEALER / REQ / PAIR / CLIENT / CHANNEL / SCATTER); per-connection
  queues on fan-out (PUB / XPUB / RADIO) and identity-routed patterns
  (ROUTER / REP / SERVER / PEER).
- **Optional strict per-pipe priority** (experimental `priority` Cargo feature) on
  `Socket::connect_with(endpoint, ConnectOpts { priority })` - nanomsg-
  style 1..=255 (lower = higher priority). Round-robin send always
  prefers the highest-priority alive peer; lower tiers only run when
  higher are blocked or disconnected.
- **Multi-chunk frame payloads** (`Payload = SmallVec<[Bytes; 2]>`,
  `Message = SmallVec<[Payload; 3]>`): layers prepend static prefixes
  without copying, kernel stitches chunks via `writev` / `sendmsg`.
- **Patricia-trie subscription matcher** (`omq-proto/src/subscription.rs`):
  PUB-side filter is O(M) on topic length, not O(N×M) on subscription count —
  scales to millions of subscriptions without degrading per-message match cost.
- **zstd dictionary auto-training** (`zstd+tcp://`): trains an 8 KiB
  compression dictionary from the first 1 000 messages or 100 KiB of
  plaintext, ships it to the peer once, then drops the compression
  threshold from 512 B to 64 B — making tiny messages compress
  profitably for the rest of the connection.
- **Inproc bypasses ZMTP codec**: peers exchange a pre-computed
  `InprocPeerSnapshot` (socket type + identity) at connect, skipping
  READY command marshalling entirely — no serialization overhead on
  same-process message passing.
- **Identity collision detection**: duplicate identities on
  identity-routed sockets (ROUTER / SERVER / PEER) are rejected with
  `Error::IdentityCollision` rather than silently clobbering routing.
- **Encrypted inproc rejected at parse time**: `inproc://` combined
  with CURVE or BLAKE3ZMQ is a parse-time error, not a silent
  misconfiguration.
- **Monitor** as a socket-like `Stream` with owned `PeerInfo` context on
  every event.
- **Python binding** ([`bindings/pyomq`](bindings/pyomq/)): PyO3 wrapper
  over `omq-compio` with a sync API and an `asyncio`-compatible bridge.

## Hot path

- Single-peer wire send encodes directly into a per-peer outbound
  queue under a `try_lock`, skipping the codec's async mutex.
- Small frames (<32 KiB) pack contiguously into one `Bytes` chunk per
  drain — one iovec entry for a batch of N small messages instead of
  2N.
- Direct-recv on supported socket types reads the FD inline, skipping
  the driver's read-side task wake.
- Frame headers come from a per-connection scratch `BytesMut`,
  amortized to ~one allocation per 7 000 frames; payload chunks are
  `Bytes::clone` (Arc bump) all the way to `writev` / `sendmsg`.
- Under `lz4+tcp` / `zstd+tcp`, parts below the compression threshold
  use the same direct-encode path as plain TCP, with the 4-byte
  plaintext sentinel prepended.

## Tests

81 integration test files across `omq-proto`, `omq-compio`, and
`omq-tokio`; ~700 tests total. `cargo test --workspace` runs the
default subset in a few seconds.

- **Coverage matrix** (`tests/coverage_matrix.rs`): every socket type
  × every supported transport on each backend.
- **Cross-runtime interop** (`omq-tokio/tests/interop_compio.rs`):
  spawns the other backend and round-trips over the wire.
- **External interop**: against pyzmq (CURVE) and the author's
  pure-Ruby ZMTP impl
  ([OMQ Ruby](https://github.com/paddor/omq)) over plain TCP, lz4, and
  zstd transports.
- **Fuzz** (`tests/fuzz_*.rs`): ~1 M iterations of randomized socket
  actions and parser inputs per suite. Gated behind `fuzz`; run by
  `scripts/test-all.sh` unless `OMQ_SKIP_FUZZ=1`.
- **pyomq**: maturin build + pytest, sync + `asyncio` surfaces plus
  pyzmq drop-in compatibility.

`scripts/test-all.sh` runs every feature combination on both backends.

## Benchmarks

- [BENCHMARKS.md](BENCHMARKS.md): throughput / latency / compression tables
  across transports, message sizes, and backends (omq-compio vs omq-tokio).
- [COMPARISONS.md](COMPARISONS.md): two-process TCP benchmarks against
  libzmq and zmq.rs.

## Documentation

- [doc/architecture.md](doc/architecture.md): high-level tour of the
  three-layer split, the two-queue socket model, and how the two
  backends compare.
- [doc/compio.md](doc/compio.md): compio backend internals (default).
- [doc/tokio.md](doc/tokio.md): tokio backend internals.
- [doc/performance.md](doc/performance.md): how omq beat libzmq -- a
  technical article on the design choices and dead ends behind the
  benchmark numbers.

## Platform support

Linux first. `omq-compio` uses io_uring on Linux, kqueue on macOS.
`omq-tokio` uses mio / epoll / kqueue.

## Requirements

- Rust 1.93 or newer (edition 2024).
- `omq-compio`: Linux 6.0 or newer (io_uring multi-shot recv with
  provided buffers).

## License

ISC.

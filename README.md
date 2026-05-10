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

`omq` is a thin facade; pick one backend at build time:

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

- **Sans-I/O ZMTP codec** ([`omq-proto`](omq-proto/)): byte-in / events-out, no async.
- **Per-socket HWM** with work-stealing send pumps on round-robin patterns;
  per-connection queues on fan-out and identity-routed patterns.
- **Multi-chunk frame payloads**: layers prepend headers without copying;
  kernel stitches chunks via `writev`.
- **Inproc bypasses ZMTP codec**: exchanges `InprocPeerSnapshot` at connect, no serialization.
- **Identity collision detection**: duplicate identity on ROUTER/SERVER/PEER =>
  `Error::IdentityCollision`.
- **Strict per-pipe priority** (`priority` feature): nanomsg-style 1..=255 on
  `Socket::connect_with`.
- **Patricia-trie subscription matcher**: O(M) on topic length, not O(N×M).
- **zstd dictionary auto-training** (`zstd+tcp://`): trains from first 1k
  messages, ships to peer once, drops threshold from 512 B to 64 B.
- **Encrypted inproc rejected at parse time**: `inproc://` + CURVE/BLAKE3ZMQ is a parse error.
- **Monitor**: socket-like `Stream` with owned `PeerInfo` on every event.
- **Python binding** ([`bindings/pyomq`](bindings/pyomq/)): PyO3 over `omq-compio`, sync + asyncio.

## Hot path

- Single-peer wire send encodes directly into a per-peer outbound queue under a `try_lock`, skipping the codec's async mutex.
- Small frames (<32 KiB) pack contiguously into one `Bytes` chunk per drain: one iovec entry for N messages instead of 2N.
- Direct-recv on supported socket types reads the FD inline, skipping the driver's read-side task wake.
- Frame headers from a per-connection scratch `BytesMut`; payload chunks are `Bytes::clone` (Arc bump) all the way to `writev`.
- Under `lz4+tcp` / `zstd+tcp`, sub-threshold frames use the plain TCP path with a 4-byte sentinel prepended.

## Tests

81 integration test files, ~700 tests. `cargo test --workspace` runs the default subset in a few seconds.

- **Coverage matrix** (`tests/coverage_matrix.rs`): every socket type × every transport on each backend.
- **Cross-runtime interop** (`omq-tokio/tests/interop_compio.rs`): compio and tokio, cross-backend.
- **External interop**: pyzmq (CURVE) and [OMQ Ruby](https://github.com/paddor/omq) over TCP, lz4, zstd.
- **Fuzz** (`tests/fuzz_*.rs`): ~1M iterations of randomized socket actions and parser inputs. Gated behind `fuzz`; run by `scripts/test-all.sh` unless `OMQ_SKIP_FUZZ=1`.
- **pyomq**: maturin build + pytest, sync + asyncio + pyzmq drop-in.

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

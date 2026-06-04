# ØMQ.rs

[![CI](https://github.com/paddor/omq.rs/actions/workflows/ci.yml/badge.svg)](https://github.com/paddor/omq.rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/omq?color=e9573f)](https://crates.io/crates/omq)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-%3E%3D%201.93-orange?logo=rust&logoColor=white)](https://www.rust-lang.org)

> **15.2M msg/s** inproc | **23.5M msg/s** ipc | **23.7M msg/s** tcp
>
> **2.3 µs** inproc latency | **28.4 µs** ipc | **36.1 µs** tcp
>
> **~3x** libzmq TCP throughput | **2x** lower TCP latency

Pure Rust [ZeroMQ](https://zeromq.org): brokerless message passing for distributed and concurrent applications. Wire-compatible with libzmq, faster across all message sizes.

- Two async backends: **tokio** (default, Linux/macOS) and **compio** (io_uring, Linux)
- 20 socket types (11 standard + 9 draft), 8 transports (TCP, IPC, inproc, UDP, WS, WSS, `lz4+tcp://`, `zstd+tcp://`)
- 4 security mechanisms: NULL, PLAIN, CURVE, BLAKE3ZMQ
- No C compiler, no vendored C, no libzmq, no libsodium
- Python binding ([pyomq](bindings/pyomq/)), C API ([omq-libzmq](omq-libzmq/)), zmq.rs drop-in ([omq-zeromq](omq-zeromq/))

### vs libzmq and other implementations

[How to beat libzmq](doc/performance.md) | [Comparison tables](COMPARISONS.md)

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/comparison_tcp.svg" alt="PUSH/PULL throughput and REQ/REP latency: TCP loopback" width="850">
</p>

<details>
<summary>Compression throughput: omq-tokio (lz4 / zstd, dict 2 KiB)</summary>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/compression/tokio_2048.svg" alt="Compression throughput: omq-tokio" width="850">
</p>
</details>

<details>
<summary>Compression throughput: omq-compio (lz4 / zstd, dict 2 KiB)</summary>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/compression/compio_2048.svg" alt="Compression throughput: omq-compio" width="850">
</p>
</details>

## Install

> [!CAUTION]
> **Experimental.** The API is unstable and may change without notice. Not yet battle-tested in production. Bug reports and testing in real workloads are very welcome.

```sh
cargo add omq                     # tokio backend (default)
cargo add omq --no-default-features --features compio-backend
```

If you know ZeroMQ, you know OMQ. Same socket types, same connect/bind/send/recv — just async Rust:

```rust
use omq::{Message, Options, Socket, SocketType};

let push = Socket::new(SocketType::Push, Options::default());
push.connect("tcp://127.0.0.1:5555".parse()?).await?;
push.send(Message::single("hello")).await?;

let pull = Socket::new(SocketType::Pull, Options::default());
pull.bind("tcp://127.0.0.1:5555".parse()?).await?;
let msg = pull.recv().await?;
assert_eq!(&msg[0], b"hello");
```

`omq` is a thin facade; pick one backend at build time:

- `tokio-backend` (default): multi-thread tokio + mio ([`omq-tokio`](omq-tokio/))
- `compio-backend`: single-thread io_uring/IOCP ([`omq-compio`](omq-compio/))

Identical public `Socket` API on both, verified by `coverage_matrix` + `interop_compio` test suites.

## Cargo features

All optional. Default build is the smallest deploy: NULL mechanism +
TCP / IPC / inproc / UDP, no C compiler required. Enable any of:

| feature           | what it adds                                      | extra deps                       |
|-------------------|---------------------------------------------------|----------------------------------|
| `tokio-backend`   | (default) tokio multi-thread backend              | -                                |
| `compio-backend`  | compio io_uring/IOCP backend                      | -                                |
| `plain`           | PLAIN username/password auth (RFC 24)             | -                                |
| `curve`           | CURVE encrypted-handshake mechanism (RFC 26)      | `crypto_box`, `crypto_secretbox` |
| `blake3zmq`       | OMQ-native BLAKE3 + ChaCha20 mechanism ([RFC](https://github.com/paddor/omq-blake3zmq/blob/main/RFC.md)) | `blake3`, `chacha20-blake3`, `x25519-dalek` |
| `lz4`             | `lz4+tcp://` compression transport ([RFC](https://github.com/paddor/omq-lz4/blob/main/RFC.md)) | `lz4-sys` |
| `zstd`            | `zstd+tcp://` compression transport ([RFC](https://github.com/paddor/omq-zstd/blob/main/RFC.md)) | `zstd-safe` (vends `libzstd`; needs `cc`) |
| `ws`              | WebSocket (`ws://`) and secure WebSocket (`wss://`) transports | `rustls`, `rustls-native-certs` |

> [!WARNING]
> **BLAKE3ZMQ has not been independently security audited.** It's an
> omq-native construction (Noise XX + BLAKE3 + X25519 + ChaCha20-BLAKE3)
> and should not be relied on for anything that matters until it has had
> third-party review. Use **CURVE** (RFC 26) for production / regulated
> workloads. Audits welcome - open an issue if you can help fund or
> conduct one.

## Design highlights

| Feature | Details |
|---------|---------|
| **Sans-I/O ZMTP codec** ([`omq-proto`](omq-proto/)) | Byte-in / events-out; no async, no traits on the hot path. Mirrors `rustls::ConnectionCommon`. |
| **Per-socket HWM** | Work-stealing send pumps on round-robin patterns; per-connection queues on fan-out and identity-routed patterns. |
| **Contiguous frame payloads** | `&msg[0]` gives `&[u8]` directly; no fallible borrow, no coalesce step. |
| **Zero-copy send and recv** | Send: `Bytes` payloads reach the kernel `writev` without a single data copy. Recv: large frames read directly into a pre-allocated buffer, bypassing intermediate queues. |
| **Patricia-trie subscription matcher** | O(M) on topic length, not O(NxM). |
| **zstd dictionary auto-training** | Trains from first 1k messages, ships to peer once; drops effective compression threshold from 512 B to 64 B. |
| **Monitor events** | Socket-like `Stream` with owned `PeerInfo` on every connect / disconnect / handshake event. |

## Workspace

Nine crates, one repo. The facade re-exports one backend; the rest are
independent, versioned, and published separately.

| Crate | What it does |
|-------|-------------|
| [`omq`](omq/) | Facade: re-exports `omq-compio` or `omq-tokio` at build time |
| [`omq-proto`](omq-proto/) | Sans-I/O ZMTP 3.x core: codec, messages, mechanisms, subscriptions |
| [`omq-tokio`](omq-tokio/) | Default backend: multi-thread tokio |
| [`omq-compio`](omq-compio/) | io_uring backend: single-thread io_uring / IOCP |
| [`omq-libzmq`](omq-libzmq/) | libzmq-compatible C interface (`libomq_zmq.so` drop-in) |
| [`omq-zeromq`](omq-zeromq/) | Drop-in replacement for the [`zeromq`](https://crates.io/crates/zeromq) Rust crate |
| [`blume`](blume/) | Batching MPSC channel with swap-drain consumer |
| [`yring`](yring/) | Bounded SPSC ring buffer with ypipe-style batched flush / prefetch |
| [`pyomq`](bindings/pyomq/) | Python binding (PyO3 over omq-compio, sync + asyncio) |

## Testing

Every socket type, transport, mechanism, and feature combination is
covered by integration tests on both backends. The full suite:

- **750+ integration tests** across omq-compio and omq-tokio (every
  socket-type x transport x mechanism cell).
- **Protocol fuzzing** (~10M iterations per suite): hand-rolled fuzz of
  the wire parser and the socket-action state machine.
- **12 soak test scenarios** per backend: peer churn, reconnect storms,
  PUB/SUB churn, compression, PLAIN / CURVE / BLAKE3ZMQ auth
  large-message throughput, multi-socket. Each scenario samples
  RSS and file-descriptor counts to detect leaks.
- **Cross-runtime interop**: omq-compio <-> omq-tokio over TCP.
- **Wire interop** with libzmq (C), pyzmq, and
  [Pure Ruby OMQ](https://github.com/zeromq/omq.rb).

```sh
./scripts/test-all.sh          # full sweep, both backends
OMQ_FUZZ=1 ./scripts/test-all.sh   # include fuzz suites
```

## Further reading

- [BENCHMARKS.md](BENCHMARKS.md): throughput / latency tables across
  transports, message sizes, and backends.
- [BENCHMARKS_COMPRESSION.md](BENCHMARKS_COMPRESSION.md): lz4+tcp / zstd+tcp
  throughput on bandwidth-limited links with structured JSON payloads.
- [COMPARISONS.md](COMPARISONS.md): two-process benchmarks against
  libzmq and zmq.rs.
- [doc/architecture.md](doc/architecture.md): three-layer split, two-queue
  socket model, backend comparison.
- [doc/compio.md](doc/compio.md): compio backend internals.
- [doc/tokio.md](doc/tokio.md): tokio backend internals.
- [doc/performance.md](doc/performance.md): how omq beat libzmq.
- [doc/migration_from_rust_zmq.md](doc/migration_from_rust_zmq.md):
  API mapping and feature mapping for rust-zmq users.

## Platform and requirements

Linux and macOS (and likely other mio targets). `omq-tokio` uses mio /
epoll / kqueue. `omq-compio` uses io_uring (Linux 6.0+) and is not
available on macOS.

- Rust 1.93 or newer (edition 2024).
- `omq-compio`: Linux 6.0 or newer (io_uring multi-shot recv with
  provided buffers).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines and [DEVELOPMENT.md](DEVELOPMENT.md) for build, test, and benchmark commands.

## AI disclosure

This project was built with significant LLM assistance throughout: architecture, implementation, tests, benchmark infrastructure, and docs. It's an experiment in what LLM-assisted development can and can't do. The design decisions and direction are mine.

## License

ISC.

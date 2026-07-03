# ØMQ.rs

> Modern ZeroMQ for the hard parts: reconnection, back-pressure, churn, soak tests, and real benchmarks.

Pure Rust [ZeroMQ](https://zeromq.org): brokerless message passing for distributed and concurrent applications. OMQ gives you socket-level messaging patterns that work the same way in-process, between processes, and over the network.

- Two async backends: **tokio** (default, Linux/macOS/Windows) and **compio** (io_uring, Linux)
- 20 socket types: stable ZMQ patterns plus draft CLIENT/SERVER, RADIO/DISH, SCATTER/GATHER, CHANNEL/PEER, and DGRAM
- 9 transports: TCP, IPC, inproc, UDP, WS, WSS, `lz4+tcp://`, `lz4+ws://`, and `lz4+wss://`
- 3 security mechanisms: PLAIN, CURVE, BLAKE3ZMQ
- No C compiler, no libzmq, no libsodium
- Python binding ([pyomq](bindings/pyomq/)), C API ([omq-libzmq](omq-libzmq/))

## The hard parts

OMQ is designed for real ZMQ behavior, not just happy-path PUSH/PULL throughput. You get:

- ZeroMQ semantics without extra tuning: no topology-specific socket types, no user-visible batching API, no manual reconnection loop.
- Transport failures are normal: reconnect, connect-before-bind, peer churn, and bind-side restarts are part of the design.
- Peer failures do not become user errors: `send()` and `recv()` keep working through disconnects, reconnects, slow consumers, and bind-side restarts.
- HWM back-pressure and routing fairness under load, not only in empty-queue examples.
- The hot paths are size-aware and latency-conscious: tiny messages stay inline without allocation, inproc passes messages by value, and large payloads use zero-copy buffers where it matters.
- Memory-safe Rust for the public crates. `unsafe` is isolated and checked with Miri.
- Benchmarks cover the real shapes: CPU accounting, fan-in/fan-out, fairness, transport differences (see charts below).

### Benchmarks

[How to beat libzmq](doc/performance.md) | [Full comparison charts](COMPARISONS.md)

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/main_classic_tcp.svg" alt="PUSH/PULL throughput: classic TCP implementations" width="950">
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/main_iouring_tcp.svg" alt="PUSH/PULL throughput: io_uring TCP implementations" width="950">
</p>

<details>
<summary>PUSH/PULL by transport</summary>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/classic_tcp.svg" alt="PUSH/PULL throughput: classic TCP" width="850">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/classic_ipc.svg" alt="PUSH/PULL throughput: classic IPC" width="850">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/classic_inproc.svg" alt="PUSH/PULL throughput: classic inproc" width="850">
</p>
</details>

<details>
<summary>REQ/REP latency</summary>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/reqrep/classic_tcp.svg" alt="REQ/REP latency: classic TCP" width="850">
</p>
</details>

<details>
<summary>Fan-out and fan-in</summary>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/fanout/classic_tcp.svg" alt="PUSH fan-out: classic TCP" width="850">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/fanin/classic_tcp.svg" alt="PUSH fan-in: classic TCP" width="850">
</p>
</details>

<details>
<summary>PUB/SUB throughput</summary>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pubsub/classic_tcp.svg" alt="PUB/SUB throughput: classic TCP" width="850">
</p>
</details>

<details>
<summary>Compression: lz4+tcp://</summary>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pubsub/lz4_tcp.svg" alt="PUB/SUB lz4+tcp fan-out: projected throughput at link speed" width="850">
</p>
</details>

<details>
<summary>Mechanisms: PLAIN / CURVE / BLAKE3ZMQ</summary>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/mechanism/tokio.svg" alt="Mechanisms: omq-tokio" width="850">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/mechanism/compio.svg" alt="Mechanisms: omq-compio" width="850">
</p>
</details>

<details>
<summary>io_uring backend details</summary>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/iouring_tcp.svg" alt="PUSH/PULL throughput: io_uring TCP" width="850">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/reqrep/iouring_tcp.svg" alt="REQ/REP latency: io_uring TCP" width="850">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/fanout/iouring_tcp.svg" alt="PUSH fan-out: io_uring TCP" width="850">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/fanin/iouring_tcp.svg" alt="PUSH fan-in: io_uring TCP" width="850">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pubsub/iouring_tcp.svg" alt="PUB/SUB throughput: io_uring TCP" width="850">
</p>
</details>

## Install

> [!CAUTION]
> **Experimental.** The API is unstable and may change without notice. Not yet battle-tested in production. Bug reports and testing in real workloads are very welcome.

```sh
cargo add omq-tokio
```

Two backends with identical `Socket` APIs, verified by `coverage_matrix` + `interop_compio` test suites:

- [`omq-tokio`](omq-tokio/): tokio + mio backend (Linux/macOS/Windows). Works on single-thread and multi-thread tokio runtimes. Recommended backend.
- [`omq-compio`](omq-compio/): single-thread io_uring/IOCP (Linux; not yet on crates.io). **Experimental.**

If you know ZeroMQ, you know OMQ. Same socket types, same connect/bind/send/recv:

```rust
use omq_tokio::{Message, Options, Socket, SocketType};

let push = Socket::new(SocketType::Push, Options::default());
push.connect("tcp://127.0.0.1:5555".parse()?).await?;
push.send(Message::single("hello")).await?;

let pull = Socket::new(SocketType::Pull, Options::default());
pull.bind("tcp://127.0.0.1:5555".parse()?).await?;
let msg = pull.recv().await?;
assert_eq!(&msg[0], b"hello");
```

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
| `lz4`             | `lz4+tcp://` compression transport ([RFC](https://github.com/paddor/omq-lz4/blob/main/RFC.md)) | `lz4rip` |
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
| **Zero-copy send and recv** | Send: large `Bytes` payloads reach the kernel `writev` without a single data copy. Recv: large frames read directly into a pre-allocated buffer, bypassing intermediate queues. |
| **Patricia-trie subscription matcher** | O(M) on topic length, not O(NxM). |
| **LZ4 dictionary auto-training** | Off by default. When enabled, trains from first 100 messages, ships to peer once; drops effective compression threshold from 512 B to 64 B. |
| **Monitor events** | Socket-like `Stream` with owned `PeerInfo` on every connect / disconnect / handshake event. |

## Workspace

Seven crates, one repo.

| Crate | What it does | Unsafe policy |
|-------|--------------|---------------|
| [`omq-proto`](omq-proto/) | Sans-I/O ZMTP 3.x core: codec, messages, mechanisms, subscriptions | `#![forbid(unsafe_code)]` |
| [`omq-tokio`](omq-tokio/) | Multi-thread tokio backend (Linux/macOS/Windows) | `#![forbid(unsafe_code)]` |
| [`omq-compio`](omq-compio/) | Single-thread io_uring / IOCP backend (Linux) | Small unsafe wrappers for single-thread runtime internals |
| [`omq-libzmq`](omq-libzmq/) | libzmq-compatible C interface (`libomq_zmq.so` drop-in) | Unsafe C ABI boundary |
| [`blume`](blume/) | Batching MPSC channel with swap-drain consumer | `#![forbid(unsafe_code)]` |
| [`yring`](yring/) | Bounded SPSC ring buffer with ypipe-style batched flush / prefetch | Unsafe ring core, Miri-tested |
| [`pyomq`](bindings/pyomq/) | Python binding (PyO3 over omq-tokio, sync + asyncio) | PyO3 FFI boundary |

## Testing

Every socket type, transport, mechanism, and feature combination is
covered by integration tests on both backends. The full suite:

- **850+ integration tests** across omq-compio and omq-tokio (every
  socket-type x transport x mechanism cell).
- **Protocol fuzzing** (~10M iterations per suite): hand-rolled fuzz of
  the wire parser and the socket-action state machine.
- **29 soak test scenarios**: peer churn, reconnect storms, PUB/SUB
  churn, ROUTER/DEALER churn, HWM reconnect, cancel safety, compression
  (lz4), PLAIN / CURVE / BLAKE3ZMQ auth, mechanism reconnect, large-message
  throughput, multi-socket, inproc cross-thread, WebSocket throughput
  and reconnect. Each scenario samples RSS and FD counts to detect leaks.
- **Loom** coverage for lock-free inproc queue behavior.
- **Miri** on `yring` and `omq-compio`'s pure unsafe wrappers.
- **Strict SemVer** because it matters.
- **Cross-runtime interop**: omq-compio <-> omq-tokio over TCP.
- **Wire interop** with libzmq and pyzmq.

```sh
./scripts/test-all.sh             # full sweep, both backends
OMQ_FUZZ=1 ./scripts/test-all.sh  # include fuzz suites
```

## Further reading

- [COMPARISONS.md](COMPARISONS.md): cross-implementation comparison charts.
- [BENCHMARKS_COMPRESSION.md](BENCHMARKS_COMPRESSION.md): lz4+tcp
  throughput on bandwidth-limited links.
- [doc/architecture.md](doc/architecture.md): three-layer split, backend
  comparison.
- [doc/compio.md](doc/compio.md): compio backend internals.
- [doc/tokio.md](doc/tokio.md): tokio backend internals.
- [doc/performance.md](doc/performance.md): how omq beat libzmq.

## Platform and requirements

**Linux is the primary platform.** All development, testing, and
benchmarking happens on Linux. CI is Linux-only for required checks.

**macOS** should work (`omq-tokio` via mio / kqueue) but is
experimental. The test suite has not been run on macOS recently.

**Windows** support is substantially complete. `omq-tokio` fully
works with TCP, IPC (named pipes), inproc, UDP, and WebSocket
transports. Windows CI is required for merge. Known limitations:

- `omq-libzmq` is excluded (Unix-only C API surface).
- Some tests are flaky (timer-sensitive assertions).

`omq-compio` uses io_uring and is Linux-only (6.0+).

Requirements:

- Rust 1.93 or newer (edition 2024).
- `omq-compio`: Linux 6.0 or newer (io_uring multi-shot recv with
  provided buffers).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines and [DEVELOPMENT.md](DEVELOPMENT.md) for build, test, and benchmark commands.

## AI disclosure

This project was built with significant LLM assistance throughout: architecture, implementation, tests, benchmark infrastructure, and docs. It's an experiment in what LLM-assisted development can and can't do. The design decisions and direction are mine.

## License

ISC.

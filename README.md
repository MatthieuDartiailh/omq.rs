# ØMQ.rs

[![CI](https://github.com/paddor/omq.rs/actions/workflows/ci.yml/badge.svg)](https://github.com/paddor/omq.rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/omq?color=e9573f)](https://crates.io/crates/omq)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-%3E%3D%201.93-orange?logo=rust&logoColor=white)](https://www.rust-lang.org)

> **16.4M msg/s** inproc | **7.2M msg/s** ipc | **7.2M msg/s** tcp
>
> **2.5 µs** inproc latency | **14.7 µs** ipc | **21.4 µs** tcp

Pure Rust ZeroMQ. Wire-compatible with libzmq, equal or faster across all message sizes.

- Two async backends: **compio** (io_uring, default) and **tokio**
- 11 standard socket types
- 8 draft socket types
- inproc transport
- IPC transport (including Linux abstract namespace)
- TCP transport
- UDP transport (RADIO/DISH only)
- `lz4+tcp://` transport with blazing-fast LZ4 compression
- `zstd+tcp://` transport with Zstandard compression
- NULL mechanism
- PLAIN mechanism
- CURVE mechanism
- BLAKE3ZMQ mechanism

> **Wire-compatible with libzmq.** omq sockets interoperate with any libzmq peer - C, Python
> (pyzmq), Ruby, Node - on any shared transport. Same ZMTP 3.x framing, same socket types, same
> CURVE handshake.

### vs. libzmq (TCP loopback, two processes)

| Size | libzmq | omq-compio | × | omq-tokio | × |
|------|--------|------------|---|-----------|---|
| 512 B | 1.99M msg/s | 3.55M msg/s | **1.8×** | 3.85M msg/s | **1.9×** |
| 8 KiB | 188k msg/s | 607k msg/s | **3.2×** | 461k msg/s | **2.5×** |
| 2 MiB | 2.7k msg/s | 3.8k msg/s | **1.4×** | 3.0k msg/s | **1.1×** |

[Full tables across all sizes and transports](COMPARISONS.md)

## Install

```sh
cargo add omq                     # compio backend (default)
cargo add omq --no-default-features --features tokio-backend
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

### Accessing message frames

A ZMQ message is one or more frames delivered atomically. Frame payloads
are always contiguous - zero-copy `&[u8]` access with no fallback needed:

```rust
let msg = pull.recv().await?;

// Borrow frame bytes (zero-copy)
let first: &[u8] = &msg[0];           // panics on OOB
let maybe: Option<&[u8]> = msg.get(1); // None on OOB

// Owned Bytes (refcount bump, no copy)
let frame: Bytes = msg.part_bytes(0).unwrap();

// Iterate all frames
for frame in msg.iter() {
    println!("{} bytes", frame.len());
}

// Part count and total byte length
msg.len();       // number of frames
msg.byte_len();  // total bytes across all frames
```

### Multi-part send

```rust
use omq::Message;

let msg = Message::single("hello");
let msg = Message::multipart(["identity", "payload"]);
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
| `plain`           | PLAIN username/password auth (RFC 24)             | -                                |
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

| Feature | Details |
|---------|---------|
| **Sans-I/O ZMTP codec** ([`omq-proto`](omq-proto/)) | Byte-in / events-out; no async, no traits on the hot path. Mirrors `rustls::ConnectionCommon`. |
| **Per-socket HWM** | Work-stealing send pumps on round-robin patterns; per-connection queues on fan-out and identity-routed patterns. |
| **Contiguous frame payloads** | `&msg[0]` gives `&[u8]` directly - no fallible borrow, no coalesce step. Kernel stitches outbound frames via `writev`. |
| **Patricia-trie subscription matcher** | O(M) on topic length, not O(NxM). |
| **Strict per-pipe priority** | nanomsg-style 1-255 tiers with `Socket::connect_with` (`priority` feature). |
| **zstd dictionary auto-training** | Trains from first 1k messages, ships to peer once; drops effective compression threshold from 512 B to 64 B. |
| **Monitor events** | Socket-like `Stream` with owned `PeerInfo` on every connect / disconnect / handshake event. |
| **Python binding** | PyO3 over `omq-compio`, sync + asyncio API. [`bindings/pyomq`](bindings/pyomq/). |

## C API (omq-zmq)

[`omq-zmq`](omq-zmq/) is a libzmq-compatible C interface backed by omq-compio.
Link against `libomq_zmq.so` instead of `libzmq.so` and existing C/C++
code works without source changes. No dependency on libzmq, libsodium,
or any C libraries.

## zmq.rs compatibility (omq-zeromq)

[`omq-zeromq`](omq-zeromq/) is a drop-in replacement for the
[`zeromq`](https://crates.io/crates/zeromq) Rust crate, backed by
omq-tokio. Rename the dependency and existing zmq.rs code compiles
against omq.

## Python binding (pyomq)

```sh
pip install pyomq
```

```python
import pyomq

ctx = pyomq.Context()
push = ctx.socket(pyomq.PUSH)
push.connect("tcp://127.0.0.1:5555")
push.send(b"hello")

pull = ctx.socket(pyomq.PULL)
pull.bind("tcp://127.0.0.1:5555")
data = pull.recv()
```

Drop-in pyzmq replacement. 2.3-3.1x faster over TCP:

| Size | pyomq | pyzmq | x |
|------|-------|-------|---|
| 512 B | 1.28M msg/s | 460k msg/s | **2.8x** |
| 2 KiB | 902k msg/s | 347k msg/s | **2.6x** |
| 8 KiB | 331k msg/s | 105k msg/s | **3.1x** |

asyncio API available via `pyomq.asyncio`. Wheels for Linux x86_64 and aarch64, Python 3.9+.

## Benchmarks

- [BENCHMARKS.md](BENCHMARKS.md): throughput / latency / compression tables
  across transports, message sizes, and backends (omq-compio vs omq-tokio).
- [COMPARISONS.md](COMPARISONS.md): two-process TCP and IPC benchmarks against
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

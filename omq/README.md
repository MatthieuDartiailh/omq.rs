# omq

Pure Rust [ZeroMQ](https://zeromq.org). 3x libzmq throughput, 2x lower latency. Brokerless message
passing for distributed and concurrent applications, wire-compatible with libzmq.

> **15.2M msg/s** inproc | **23.5M msg/s** ipc | **23.7M msg/s** tcp
>
> **~3x** libzmq TCP throughput | **2x** lower TCP latency

- 20 socket types (11 standard + 9 draft), 8 transports (TCP, IPC, inproc, UDP, WS, WSS, `lz4+tcp://`, `zstd+tcp://`)
- 4 security mechanisms: NULL, PLAIN, CURVE, BLAKE3ZMQ
- Sans-I/O ZMTP codec, zero-copy send and recv, Patricia-trie subscription matching
- No C compiler, no vendored C, no libzmq, no libsodium

This is a facade crate. Pick one backend at build time:

| Feature | Backend | Crate |
|---------|---------|-------|
| `tokio-backend` (default) | Multi-threaded, tokio (Linux/macOS) | [omq-tokio](https://crates.io/crates/omq-tokio) |
| `compio-backend` | Single-threaded, io_uring (Linux) | [omq-compio](https://crates.io/crates/omq-compio) |

Mutually exclusive.

## Install

```sh
cargo add omq                     # tokio backend (default)
cargo add omq --no-default-features --features compio-backend
```

## Usage

```rust
use omq::{Message, Options, Socket, SocketType};

let push = Socket::new(SocketType::Push, Options::default());
push.bind("tcp://127.0.0.1:5555".parse()?).await?;

let pull = Socket::new(SocketType::Pull, Options::default());
pull.connect("tcp://127.0.0.1:5555".parse()?).await?;

push.send(Message::single("hello")).await?;
let msg = pull.recv().await?;
```

See the [workspace README](https://github.com/paddor/omq.rs) for benchmarks, feature matrix,
and interop details.

## License

ISC

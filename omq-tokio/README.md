# omq-tokio

Tokio backend for [omq](https://crates.io/crates/omq). Multi-threaded, actor-based.
Default backend when you `cargo add omq`. Works on Linux, macOS, and Windows.

Built on [omq-proto](https://crates.io/crates/omq-proto) and
[tokio](https://crates.io/crates/tokio).

## Highlights

| | |
|-|-|
| Multi-threaded | Concurrent `send`/`recv` from multiple tasks is safe |
| Actor with bypass | `SocketDriver` actor owns mutable state. Common message path bypasses it: `send` pushes into the routing strategy directly, `recv` pulls from the user channel directly. |
| Arena encoding | Small messages (< 96 KiB) packed into one `BytesMut`, one `write_all` per batch |
| Shared-queue work stealing | Round-robin types (PUSH/DEALER) share one `flume` queue. Each connection driver polls it in a `select!` arm, draining up to 256 messages per wakeup. |

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/main_classic_tcp.svg" alt="PUSH/PULL throughput: TCP implementations" width="850">
</p>

## Usage

```rust
use omq_tokio::{Context, SocketType, Options, Message};

let ctx = Context::new();

let push = ctx.socket(SocketType::Push, Options::default());
push.bind("tcp://127.0.0.1:5555".parse()?).await?;

let pull = ctx.socket(SocketType::Pull, Options::default());
pull.connect("tcp://127.0.0.1:5555".parse()?).await?;

push.send(Message::single("hello")).await?;
let msg = pull.recv().await?;
```

Use `Socket::new(...)` when you want the socket driver on the caller's
active tokio runtime. Use `ctx.socket(...)` when OMQ should own IO runtime
threads.

`cargo add omq` picks this backend by default.

## Internals

[`doc/architecture.md`](../doc/architecture.md) covers the actor shape,
send/recv bypass, routing strategies, and arena encoding threshold.

## License

ISC

# omq-tokio

Tokio backend for [omq](https://crates.io/crates/omq). Multi-threaded, actor-based.
Default backend when you `cargo add omq`. Works on Linux and macOS.

Built on [omq-proto](https://crates.io/crates/omq-proto) and
[tokio](https://crates.io/crates/tokio).

## Highlights

| | |
|-|-|
| Multi-threaded | Concurrent `send`/`recv` from multiple tasks is safe |
| Actor with bypass | `SocketDriver` actor owns mutable state. Common message path bypasses it: `send` pushes into the routing strategy directly, `recv` pulls from the user channel directly. |
| Flat-buf encoding | Small messages (< 48 KiB) packed into one `BytesMut`, one `write_all` per batch |
| Shared-queue work stealing | Round-robin types (PUSH/DEALER) share one `flume` queue. Each connection driver polls it in a `select!` arm, draining up to 256 messages per wakeup. |

## Usage

```rust
use omq_tokio::{Socket, SocketType, Options, Message};

let push = Socket::new(SocketType::Push, Options::default());
push.bind("tcp://127.0.0.1:5555".parse()?).await?;

let pull = Socket::new(SocketType::Pull, Options::default());
pull.connect("tcp://127.0.0.1:5555".parse()?).await?;

push.send(Message::single("hello")).await?;
let msg = pull.recv().await?;
```

Most users should depend on the `omq` facade crate instead of `omq-tokio` directly.
`cargo add omq` picks this backend by default.

## Internals

[`doc/tokio.md`](../doc/tokio.md) covers the actor shape, send/recv bypass, routing
strategies, and flat-buf encoding threshold.

## License

ISC

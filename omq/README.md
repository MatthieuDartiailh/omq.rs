# omq

Facade crate for [omq](https://github.com/paddor/omq.rs). Re-exports one backend:

| Feature | Backend | Crate |
|---------|---------|-------|
| `compio-backend` (default) | Single-threaded, io_uring | [omq-compio](https://crates.io/crates/omq-compio) |
| `tokio-backend` | Multi-threaded, tokio | [omq-tokio](https://crates.io/crates/omq-tokio) |

Mutually exclusive.

## Install

```sh
cargo add omq                     # compio backend (default)
cargo add omq --no-default-features --features tokio-backend
```

## Usage

```rust
use omq::Socket;
use omq::prelude::*;

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

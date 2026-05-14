# omq-compio

compio backend for [omq](https://crates.io/crates/omq). Single-threaded, io_uring-based.
Primary backend, used by default when you `cargo add omq`.

Built on [omq-proto](https://crates.io/crates/omq-proto) and
[compio](https://crates.io/crates/compio) (io_uring on Linux, IOCP on Windows).

## Highlights

| | |
|-|-|
| Thread-per-core | One compio runtime per thread, no cross-thread sync on the hot path |
| io_uring multi-shot recv | One persistent SQE per connection, no re-arming |
| Direct-encode send | Encodes ZMTP frames into `EncodedQueue` under a sync mutex, bypassing the driver. Small messages (< 32 KiB) packed into one flat buffer, one `writev` call. |
| Direct-recv | `Socket::recv` claims the read side from the driver and feeds the codec inline. Saves ~12 µs per round-trip. |
| Large-message recv | Payloads above 128 KiB accumulated into a pre-allocated `BytesMut`. Frames above pool capacity fall back to one-shot read. |

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

Most users should depend on the `omq` facade crate instead of `omq-compio` directly.

## Internals

[`doc/compio.md`](../doc/compio.md) covers the driver loop, `DirectIoState`, `EncodedQueue`,
recv-direct claim arbitration, and the memory model.

## License

ISC

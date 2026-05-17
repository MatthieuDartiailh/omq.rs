# omq-zeromq

Drop-in replacement for the [`zeromq`](https://crates.io/crates/zeromq) crate,
backed by [omq-tokio](https://crates.io/crates/omq-tokio).

Provides the same API surface as the `zeromq` crate so existing code can
switch implementations by changing the dependency without rewriting socket
setup or send/recv calls.

## Switching from zeromq

Change one line in `Cargo.toml`:

```toml
# before
zeromq = "0.6"

# after
zeromq = { package = "omq-zeromq", version = "0.2" }
```

All `use zeromq::...` imports and socket code stay the same:

```rust
use zeromq::{PushSocket, PullSocket, Socket, SocketSend, SocketRecv, ZmqMessage};

#[tokio::main]
async fn main() {
    let mut push = PushSocket::new();
    push.bind("tcp://127.0.0.1:5555").await.unwrap();

    let mut pull = PullSocket::new();
    pull.connect("tcp://127.0.0.1:5555").await.unwrap();

    push.send(ZmqMessage::from("hello")).await.unwrap();
    let msg = pull.recv().await.unwrap();
    println!("{}", String::from_utf8_lossy(msg.get(0).unwrap()));
}
```

## Performance vs zeromq

Two-process push/pull benchmarks (tokio multi-thread runtime on both sides).
Hardware: Linux 6.12, Intel i7-8700B 3.2 GHz. Full numbers in
[COMPARISONS.md](../COMPARISONS.md).

### TCP

| Message size | zeromq msg/s | omq-zeromq msg/s | speedup |
|-------------|-------------|-----------------|---------|
| 8 B | 483k | 4.52M | **9.4×** |
| 128 B | 342k | 5.05M | **14.7×** |
| 512 B | 324k | 4.16M | **12.8×** |
| 2 KiB | 295k | 1.57M | **5.3×** |
| 8 KiB | 238k | 470k | **2.0×** |
| 32 KiB | 128k | 156k | **1.2×** |

### IPC

| Message size | zeromq msg/s | omq-zeromq msg/s | speedup |
|-------------|-------------|-----------------|---------|
| 8 B | 741k | 4.34M | **5.9×** |
| 128 B | 741k | 5.02M | **6.8×** |
| 512 B | 677k | 3.90M | **5.8×** |
| 2 KiB | 619k | 1.21M | **2.0×** |
| 8 KiB | 380k | 607k | **1.6×** |

## License

ISC

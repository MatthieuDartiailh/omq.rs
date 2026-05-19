# Switching from rust-zmq

**No C compiler required.** rust-zmq depends on libzmq (a C++ library) and optionally libsodium. Building omq requires nothing beyond `rustc`. No vendored C, no CMake, no vcpkg, no cross-compile headaches.

**Native async.** rust-zmq is blocking. Async wrappers over it (`tmq`, `async-zmq`) work around `EAGAIN` with thread pools and impose backpressure by polling. omq is async from the socket level up; `send`/`recv` are `async fn`.

**Wire-compatible.** omq speaks ZMTP 3.x. Existing libzmq peers — C, Python, Ruby, Node — interoperate without changes.

## API mapping

```rust
// rust-zmq
let ctx = zmq::Context::new();
let socket = ctx.socket(zmq::PUSH)?;
socket.connect("tcp://127.0.0.1:5555")?;
socket.send("hello", 0)?;
let msg = socket.recv_msg(0)?;

// omq
let socket = Socket::new(SocketType::Push, Options::default());
socket.connect("tcp://127.0.0.1:5555".parse()?).await?;
socket.send(Message::single("hello")).await?;
let msg = socket.recv().await?;
```

No `Context`. No send/recv flags. Endpoints are parsed and typed. Multi-part messages are a `Message` value, not repeated `SNDMORE` calls.

## Feature mapping

| rust-zmq | omq |
|----------|-----|
| `socket(zmq::PUSH)` | `Socket::new(SocketType::Push, ...)` |
| `socket(zmq::SUB)` + `set_subscribe` | `Socket::new(SocketType::Sub, ...)` + `subscribe` |
| `socket(zmq::ROUTER)` | `Socket::new(SocketType::Router, ...)` |
| `send_multipart` / `recv_multipart` | `Message::multipart(...)` / `msg[i]` |
| feature `"curve"` + `set_curve_*` | feature `curve` + `Options { mechanism: Curve(...) }` |
| feature `"plain"` | feature `plain` |
| feature `"vendored"` | not needed — no C dependency |

All 11 standard socket types are supported. PLAIN and CURVE mechanisms work the same way at the protocol level.

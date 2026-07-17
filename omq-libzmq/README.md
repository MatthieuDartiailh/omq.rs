# omq-libzmq

libzmq-compatible C interface backed by [omq-tokio](https://crates.io/crates/omq-tokio).

Exposes `zmq_socket`, `zmq_bind`, `zmq_connect`, `zmq_send`, `zmq_recv`, and
friends with the same ABI as libzmq, allowing C/C++ programs (and FFI bindings
in other languages) to link against omq instead of libzmq.

## Features

- **Transports:** `inproc://`, `tcp://`, `ipc://` (including Windows named pipes)
- **Socket Types:** All standard ZMQ types (PUSH/PULL, PUB/SUB, REQ/REP, DEALER/ROUTER, etc.)
- **Security:** PLAIN, CURVE
- **Compression:** LZ4 over TCP
- **Cross-Platform:** Linux, macOS, Windows, BSD
- **API Compatibility:** Drop-in libzmq replacement with identical ABI

## Build

Produces `libomq_zmq.so` / `libomq_zmq.a` / `libomq_zmq.dylib`.

```sh
cargo build -p omq-libzmq --release
```

## License

ISC

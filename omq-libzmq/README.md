# omq-libzmq

libzmq-compatible C interface backed by [omq-tokio](https://crates.io/crates/omq-tokio).

Exposes `zmq_socket`, `zmq_bind`, `zmq_connect`, `zmq_send`, `zmq_recv`, and
friends with the same ABI as libzmq, allowing C/C++ programs (and FFI bindings
in other languages) to link against omq instead of libzmq.

## Build

Produces `libomq_zmq.so` / `libomq_zmq.a` / `libomq_zmq.dylib`.

```sh
cargo build -p omq-libzmq --release
```

## License

ISC

# omq-zeromq

Drop-in replacement for the [`zeromq`](https://crates.io/crates/zeromq) crate,
backed by [omq-tokio](https://crates.io/crates/omq-tokio).

Provides the same API surface as the `zeromq` crate so existing code can
switch implementations by changing the dependency without rewriting socket
setup or send/recv calls.

## License

ISC

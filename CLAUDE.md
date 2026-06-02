# CLAUDE.md

## Workspace layout

Seven-crate Cargo workspace; `bindings/` is excluded and built
out-of-tree (maturin etc.).

- **`omq-proto`** -- sans-I/O ZMTP 3.x core. Codec (`Connection`),
  message/payload types, greeting + frame state machines, mechanism
  handshakes (NULL / PLAIN / CURVE / BLAKE3ZMQ), compression transforms
  (lz4 / zstd), endpoint parsing, options, subscription matcher.
  No async, no I/O. Mirrors `rustls::ConnectionCommon` / `quinn-proto`.
- **`omq-tokio`** -- multi-thread tokio backend. **Default backend.**
  Works on Linux and macOS (and likely other mio targets).
- **`omq-compio`** -- single-threaded compio backend (io_uring on
  Linux, IOCP on Windows). Not available on macOS.
- **`omq`** -- facade crate. Re-exports one backend via Cargo features:
  `tokio-backend` (default) or `compio-backend`. Mutually exclusive.
- **`blume`** -- batching MPSC channel for `omq-compio` inbound delivery.
- **`yring`** -- bounded SPSC ring buffer for inproc transport.
- **`omq-libzmq`** -- libzmq-compatible C interface (`libomq_zmq.so` /
  `.a`). Drop-in replacement: ships `zmq.h`, implements the `zmq_*`
  API. Backed by `omq-tokio`.
- **`bindings/pyomq`** -- PyO3 wrapper over `omq-compio`. Own `Cargo.lock`.
  Build: `cd bindings/pyomq && maturin develop --release`.

Both backends re-export `omq-proto`'s public API and share an identical
public `Socket` API. Verified by `tests/coverage_matrix.rs` (both) and
`omq-tokio/tests/interop_compio.rs`.

## Build / test / bench

See [`DEVELOPMENT.md`](DEVELOPMENT.md) for the full command reference
(unit tests, feature-gated tests, fuzz, soak, stress tests, benchmarks).

Quick reference:

```sh
cargo build --workspace
cargo fmt                                # pre-commit hook checks this
cargo clippy --workspace --all-targets   # pre-commit hook checks this
./scripts/test-all.sh                    # full sweep, both backends
```

**HARD RULE:** Clippy must pass under all three configurations before
pushing to GitHub. Never push code that produces clippy warnings or
errors. Run all three before every `git push`:

```sh
cargo clippy --workspace --all-targets                # default features
cargo clippy --workspace --all-targets --all-features # feature-gated paths (omq facade compile_error! is expected here: both backends are mutually exclusive)
(cd bindings/pyomq && cargo clippy --all-targets)     # separate workspace
```

`#[allow]` vs `#[expect]`: use `#[expect]` by default. Use `#[allow]`
only when the lint fires in some feature combinations but not others
(the expectation would be unfulfilled when the lint is silent).

Lints: `missing_debug_implementations` = **deny**,
`unsafe_op_in_unsafe_fn` = **deny**, clippy `pedantic` = **warn**.

## Comparison benchmarks

Cross-implementation throughput and latency benchmarks live in
`scripts/run_comparisons.py`. It drives standalone `bench_peer`
binaries, one per implementation:

| binary | source | impls |
|--------|--------|-------|
| `bench_peer_tokio` | `omq-tokio/src/bin/bench_peer_tokio.rs` | omq-tokio |
| `bench_peer_compio` | `omq-compio/src/bin/bench_peer_compio.rs` | omq-compio, omq-compio-st |
| `libzmq_bench_peer` | `scripts/libzmq_bench_peer.c` | libzmq, omq-libzmq |
| `zmqrs_bench_peer` | `scripts/zmqrs_bench_peer/` | zmq.rs |
| `rzmq_bench_peer` | `scripts/rzmq_bench_peer/` | rzmq |

Each binary speaks a subcommand protocol:

- `push <addr> <size>` -- bind PUSH, send forever
- `pull <addr> <size> <duration>` -- connect PULL, count for duration,
  print `<count> <elapsed> <size>` to stdout
- `pub <addr> <size>` -- bind PUB, send forever
- `sub <addr> <size> <duration>` -- connect SUB, subscribe(""),
  count for duration
- `inproc <name> <size> <duration>` -- in-process PUSH/PULL
- `inproc-pubsub <name> <size> <duration> <peers>` -- in-process
  PUB/SUB with N subscribers
- `rep <addr> <size>` / `req <addr> <size> <iters> <warmup>` -- latency

Results go to `~/.cache/omq/comparisons.jsonl`. Charts are generated
by `scripts/gen_comparison_chart.py` into `doc/charts/comparison_*.svg`.

Per-backend criterion-style benches (separate from comparisons) live in
`omq-tokio/benches/` and `omq-compio/benches/` with shared scaffolding
in `benches/common/mod.rs`. Custom harness (`harness = false`), no
external framework. Results go to `~/.cache/omq/results_{tokio,compio}.jsonl`.

## Charts and releasing

See [`DEVELOPMENT.md`](DEVELOPMENT.md) for:

- **Updating charts** -- comparison, compression, and pyomq bindings SVGs
- **Releasing** -- dep graph, semver rules, cascade, publish order, pyomq tag separation

**interop_compio dep constraint:** `omq-tokio/Cargo.toml`'s compio
dev-dep must use the same git rev as `omq-compio`'s dep. Different
revs link two `compio-runtime` instances -> TLS mismatch panic.

## Cargo features

| feature | adds | deps |
|---------|------|------|
| `plain` | PLAIN auth (RFC 24) | - |
| `curve` | CURVE handshake (RFC 26) | `crypto_box`, `crypto_secretbox` |
| `blake3zmq` | BLAKE3 + ChaCha20 mechanism | `blake3`, `chacha20-blake3` (git, AVX2), `x25519-dalek` |
| `lz4` | `lz4+tcp://` transform | `lz4-sys` (needs `cc`) |
| `zstd` | `zstd+tcp://` transform | `zstd-safe` (needs `cc`) |
| `fuzz` | fuzz test suites | - |
| `soak` | soak test suites | - |

## ZMQ fundamentals

ZMQ sockets are opaque message queues that abstract away the network.
The user sends and receives messages. The socket handles connections,
reconnections, framing, and multiplexing internally. The transport
(TCP, IPC, inproc, UDP) is chosen by endpoint URI and is transparent
to the application.

Core guarantees that omq must uphold:

- **Send/recv never fail due to peers.** A peer disconnecting, a TCP
  connection dropping, or a slow consumer does not cause `send` or
  `recv` to return an error. The socket reconnects automatically and
  resumes delivery. The only user-visible send errors are protocol
  violations (e.g. REQ sending twice without recv) or socket closed.
- **Connect-before-bind works.** `connect()` queues internally and
  waits for the bind to appear. Never suggest connection ordering as
  a cause for failures or hangs.
- **Automatic reconnection.** ZMTP peers reconnect on disconnect
  with configurable backoff. The application does not manage
  connection lifecycle.
- **Messages are atomic.** A multipart message is delivered in full
  or not at all. No partial delivery.
- **HWM back-pressure, not errors.** When the outbound queue is
  full, the socket either drops (PUB default) or blocks (PUSH
  default, configurable via OnMute). It does not return an error.
- **Transport-agnostic.** The same socket can bind on TCP and IPC
  simultaneously. Inproc is in-process (no kernel, no serialization).
- **Subscriptions are prefix-matched.** SUB subscribes to byte
  prefixes. Empty prefix = all messages. PUB filters per subscriber.
- **Thread safety contract.** A single socket must not be used from
  multiple threads concurrently (ZMQ's rule). omq-tokio relaxes this
  for async (send/recv serialize internally), but the principle holds
  for omq-libzmq's C API.

## Architecture and internals

Three-layer split: codec (omq-proto) is sans-I/O, backends own the
I/O loop. Two queues per socket: one inbound, one outbound.
See `doc/` for details:

- [`doc/architecture.md`](doc/architecture.md) -- diagrams, two-queue model, message types, transport/mechanism tables
- [`doc/compio.md`](doc/compio.md) -- compio internals: key types, DirectIoState, EncodedQueue, driver loop, recv-direct
- [`doc/tokio.md`](doc/tokio.md) -- tokio internals: actor shape, send/recv bypass, routing strategies
- [`doc/performance.md`](doc/performance.md) -- omq's performance journey: design decisions, dead ends, profiling. Technical and brief. No em-dashes, no frill.
- [`doc/libzmq/errors.md`](doc/libzmq/errors.md) -- libzmq error handling catalog
- [`doc/libzmq/gaps.md`](doc/libzmq/gaps.md) -- error handling gap analysis vs omq
- [`doc/libzmq/perf.md`](doc/libzmq/perf.md) -- libzmq performance internals reference

## Conventions

- Rust 2024 edition, MSRV **1.93**. ASCII-only source.
- `rustfmt.toml`: `edition = "2024"`, `max_width = 100`.
- `Cargo.lock` untracked at workspace root (library). `bindings/pyomq`
  ships its own.
- Per-crate versioning, tags are `<crate>-v<version>`.
- `main` is protected. All changes go through PRs.

## Adding new transport / mechanism

- **Transport:** `Endpoint` variant + parser in `omq-proto/src/endpoint.rs`,
  `transport/<name>.rs` in each backend. Compression transports are
  `transform/` layers on TCP, not separate transports.
- **Mechanism:** module under `omq-proto/src/proto/mechanism/`,
  feature-gate, register with greeting state machine, integration
  test in **both** `tests/<mechanism>.rs`.

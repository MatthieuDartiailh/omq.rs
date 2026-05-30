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
  API. Backed by `omq-compio`.
- **`bindings/pyomq`** -- PyO3 wrapper over `omq-compio`. Own `Cargo.lock`.
  Build: `cd bindings/pyomq && maturin develop --release`.

Both backends re-export `omq-proto`'s public API and share an identical
public `Socket` API. Verified by `tests/coverage_matrix.rs` (both) and
`omq-tokio/tests/interop_compio.rs`.

## Build / test / bench

See [`DEVELOPMENT.md`](DEVELOPMENT.md) for the full command reference
(unit tests, feature-gated tests, fuzz, soak, benchmarks).

Quick reference:

```sh
cargo build --workspace
cargo fmt                                # pre-commit hook checks this
cargo clippy --workspace --all-targets   # pre-commit hook checks this
./scripts/test-all.sh                    # full sweep, both backends
```

Clippy must pass under all three configurations before pushing:

```sh
cargo clippy --workspace --all-targets                # default features
cargo clippy --workspace --all-targets --all-features # feature-gated paths
(cd bindings/pyomq && cargo clippy --all-targets)     # separate workspace
```

`#[allow]` vs `#[expect]`: use `#[expect]` by default. Use `#[allow]`
only when the lint fires in some feature combinations but not others
(the expectation would be unfulfilled when the lint is silent).

Lints: `missing_debug_implementations` = **deny**,
`unsafe_op_in_unsafe_fn` = **deny**, clippy `pedantic` = **warn**.

## Charts, benchmarks, and releasing

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
| `priority` | per-pipe priority tiers | - |
| `fuzz` | fuzz test suites | - |
| `soak` | soak test suites | - |

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

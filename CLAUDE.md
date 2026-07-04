# CLAUDE.md

## Workspace layout

Five-crate Cargo workspace; `bindings/` is excluded and built
out-of-tree (maturin etc.).

- **`omq-proto`** -- sans-I/O ZMTP 3.x core. Codec (`Connection`),
  message/payload types, greeting + frame state machines, mechanism
  handshakes (NULL / PLAIN / CURVE / BLAKE3ZMQ), compression transforms
  (lz4), endpoint parsing, options, subscription matcher.
  No async, no I/O. Mirrors `rustls::ConnectionCommon` / `quinn-proto`.
- **`omq-tokio`** -- multi-thread tokio backend. **Default backend.**
  Works on Linux and macOS (and likely other mio targets).
- **`blume`** -- batching MPSC channel for same-thread inproc delivery.
- **`yring`** -- bounded SPSC ring buffer for inproc transport.
- **`omq-libzmq`** -- libzmq-compatible C interface (`libomq_zmq.so` /
  `.a`). Drop-in replacement: ships `zmq.h`, implements the `zmq_*`
  API. Backed by `omq-tokio`.
- **`bindings/pyomq`** -- PyO3 wrapper over `omq-tokio`. Own `Cargo.lock`.
  Build: `cd bindings/pyomq && maturin develop --release`.

`omq-tokio` re-exports `omq-proto`'s public API. Its public `Socket` API
is covered by `tests/coverage_matrix.rs`.

## Build / test / bench

See [`DEVELOPMENT.md`](DEVELOPMENT.md) for the full command reference
(unit tests, feature-gated tests, fuzz, soak, stress tests, benchmarks).

Quick reference:

```sh
cargo build --workspace
cargo fmt                                # pre-commit hook checks this
cargo clippy --workspace --all-targets   # pre-commit hook checks this
./scripts/test-all.sh                    # full sweep
```

**HARD RULE:** Clippy must pass under all three configurations before
pushing to GitHub. Never push code that produces clippy warnings or
errors. Run all three before every `git push`:

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

## Benchmarks, charts, releasing

See [`DEVELOPMENT.md`](DEVELOPMENT.md) for comparison benchmark infra,
chart generation, and release process.

## Cargo features

| feature | adds | deps |
|---------|------|------|
| `plain` | PLAIN auth (RFC 24) | - |
| `curve` | CURVE handshake (RFC 26) | `crypto_box`, `crypto_secretbox` |
| `blake3zmq` | BLAKE3 + ChaCha20 mechanism | `blake3`, `chacha20-blake3`, `x25519-dalek` |
| `lz4` | `lz4+tcp://` transform | `lz4rip` |
| `ws` | `ws://` / `wss://` WebSocket transport | `rustls`, `rustls-native-certs` (backend-level) |
| `fuzz` | fuzz test suites | - |
| `soak` | soak test suites | - |

## ZMQ fundamentals

ZMQ sockets are opaque message queues that abstract away the network.
The user sends and receives messages. The socket handles connections,
reconnections, framing, and multiplexing internally. The transport
(TCP, IPC, inproc, UDP) is chosen by endpoint URI and is transparent
to the application.

**Reliability is non-negotiable.** Self-healing, silent
back-pressure, no user-visible errors from peer failures. Never
propose a fix that weakens self-healing or trades reliability for
convenience. Core guarantees:

- **Send/recv never fail due to peers.** Peer disconnects,
  TCP drops, slow consumers: no errors. Reconnects automatically.
  Only user-visible send errors: protocol violations or socket closed.
- **Connect-before-bind works.** `connect()` retries until the
  remote `bind()` appears. Never blame connection ordering.
- **Automatic reconnection.** Configurable backoff. The
  application does not manage connection lifecycle.
- **Heartbeats detect dead peers, not slow ones.** A slow peer
  that still responds to PINGs is alive. Heartbeat timeout only
  fires when a peer stops responding entirely. Never assume
  heartbeat will resolve a slow-consumer backpressure situation.
- **Messages are atomic.** Delivered in full or not at all.
- **HWM back-pressure, not errors.** When the outbound queue is
  full, the socket either drops (PUB default) or blocks (PUSH
  default, configurable via OnMute). It does not return an error.
- **No peer starvation.** A slow peer must never be permanently
  starved. Round-robin (PUSH) waits for a full peer's slot to
  drain rather than skipping it indefinitely. A slow peer must
  also never block fast peers from making progress.
- **Fair fan-in.** Consumer fair-queues across all connections.
- **Transport-agnostic.** Bind TCP and IPC simultaneously. Inproc
  is in-process (no kernel, no serialization).
- **Subscriptions are prefix-matched.** Empty prefix = all
  messages. PUB filters per subscriber.
- **Thread safety.** One socket, one thread. omq-tokio relaxes
  this for async; omq-libzmq does not.

## Performance invariants

Fan-out sockets (PUB, XPUB, RADIO) send the same message to many
peers. The wire bytes are identical for all peers on the same
transport when no per-peer encryption is active. **Encode once,
distribute the encoded bytes.** Never encode, compress, or frame the
same message N times for N subscribers. The correct pattern: encode
into a scratch buffer, then memcpy the pre-encoded bytes into each
peer's `EncodedQueue` via `push_pre_encoded`. This applies equally
to ZMTP framing, compression transforms (lz4), and any future
wire-level transforms. Per-peer encoding is only justified when the
wire bytes genuinely differ per peer (e.g. per-peer encryption keys).

## Architecture summary

Three-layer split: `omq-proto` (sans-I/O ZMTP codec) -> `omq-tokio`
backend -> user `Socket` API. Two queues per socket: one inbound,
one outbound. Per-connection driver tasks bridge queues and wire.
Full detail in `doc/`:
[`architecture.md`](doc/architecture.md),
[`libzmq/`](doc/libzmq/).

**omq-proto key types.** `Connection`: ZMTP codec state machine
(`handle_input`/`poll_event`/`send_message`/`poll_transmit`).
`EncodedQueue`: arena (256 KiB) + entry-based encoder used by both
backends. Frame headers are always written into the arena. Small
messages (<96 KiB `ARENA_THRESHOLD`) go contiguously into the arena
(1 iovec per batch). Large payloads are tracked as external `Bytes`
entries (zero-copy gather-write). `Message`/`Payload`: 64 B
each (one cache line), inline variants (55 B / 62 B).

**omq-tokio hot path.** `SocketDriver` actor owns peer table and
type state. Send bypass: `Socket::send` skips actor for non-REQ/REP
via `SendSubmitter` (flume MPMC). Per-peer `PeerWireSlot`
(`EncodedQueue` under `std::sync::Mutex`, nanosecond hold): handle
encodes, driver flushes via `data_ready` select arm. `PeerSend` enum
(`Wire`/`Inbox`) dispatches fan-out/identity/exclusive to per-peer
slots without pump tasks. Recv bypass: `ConnectionDriver` pushes
straight to user `recv_tx` for PULL/SUB/REQ/etc. REP/ROUTER go
through actor for identity routing.

**Inproc.** No ZMTP, no driver. Cross-thread SPSC via `yring`
(lock-free ring buffer, 64 B `Message` by value). Same-thread via
`blume` (batching MPSC, swap-drain consumer).

## Conventions

- Rust 2024 edition, MSRV **1.93**. ASCII-only source.
- `rustfmt.toml`: `edition = "2024"`, `max_width = 100`.
- `Cargo.lock` untracked at workspace root (library). `bindings/pyomq`
  ships its own.
- Per-crate versioning, tags are `<crate>-v<version>`.
- `main` is protected. All changes go through PRs.

## Chart generation

**HARD RULE:** Chart subtitle configuration lives in `.chart_hw`
(gitignored, repo root). All `scripts/gen_*_chart.py` scripts read
it automatically via `scripts/chart_hw.py`. Never run chart gen
scripts without verifying `.chart_hw` exists. See `DEVELOPMENT.md`
for the exact commands.

## Adding new transport / mechanism

- **Transport:** `Endpoint` variant + parser in `omq-proto/src/endpoint.rs`,
  `transport/<name>.rs` in each backend. Compression transports are
  `transform/` layers on TCP, not separate transports.
- **Mechanism:** module under `omq-proto/src/proto/mechanism/`,
  feature-gate, register with greeting state machine, integration
  test in **both** `tests/<mechanism>.rs`.

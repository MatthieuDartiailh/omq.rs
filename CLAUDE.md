# CLAUDE.md

## Workspace layout

Five-crate Cargo workspace; `bindings/` is excluded and built
out-of-tree (maturin etc.).

- **`omq-proto`** -- sans-I/O ZMTP 3.x core. Codec (`Connection`),
  message/payload types, greeting + frame state machines, mechanism
  handshakes (NULL / PLAIN / CURVE / BLAKE3ZMQ), compression transforms
  (lz4), endpoint parsing, options, subscription matcher. No async, no I/O.
- **`omq-tokio`** -- multi-thread tokio backend. **Default backend.**
  Works on Linux and macOS (and likely other mio targets).
- **`blume`** -- batching MPSC channel for same-thread inproc delivery.
- **`yring`** -- bounded SPSC ring buffer for inproc transport based on
  libzmq's `ypipe_t`. One atomic per batch.
- **`omq-libzmq`** -- libzmq-compatible C interface (`libomq_zmq.so` /
  `.a`). Drop-in replacement: ships `zmq.h`, implements the `zmq_*` API.
- **`bindings/pyomq`** -- PyO3 wrapper over `omq-tokio`.

`omq-tokio` re-exports `omq-proto`'s public API. Its public `Socket` API
is covered by `tests/coverage_matrix.rs`.

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

## Architecture summary

Three-layer split: `omq-proto` (sans-I/O ZMTP codec) -> `omq-tokio`
backend -> user `Socket` API. Two queues per socket: one inbound,
one outbound. Per-connection driver tasks bridge queues and wire.
Full detail in `doc/`:
[`architecture.md`](doc/architecture.md),
[`libzmq/`](doc/libzmq/).

**omq-proto key types.** `Connection`: ZMTP codec state machine
(`handle_input`/`poll_event`/`send_message`/`poll_transmit`).
`FrameBuffer`: arena (256 KiB) + entry-based framer used by both
backends. Frame headers are always written into the arena. Small
messages (<8 KiB `ARENA_THRESHOLD`) go contiguously into the arena
(1 iovec per batch). Large payloads are tracked as external `Bytes`
entries (zero-copy gather-write). `Message`: 80 B, inline up to 71 B.
`Payload`: 64 B, inline up to 62 B.

**omq-tokio hot path.** `SocketDriver` actor owns peer table and
type state. Send bypass: `Socket::send` skips actor for non-REQ/REP
via `SendSubmitter` (flume MPMC). Per-peer `PeerTransmitSlot`
(`FrameBuffer` under `std::sync::Mutex`, nanosecond hold): handle
frames, driver flushes via `DataSignal` select arm. `PeerOutbound`
enum (`Wire`/`Inbox`) dispatches fan-out/identity/exclusive to
per-peer slots without pump tasks. Recv bypass: `ConnectionDriver`
pushes straight to user `recv_tx` for PULL/SUB/REQ/etc. REP/ROUTER
go through actor for identity routing. PUB fan-out shard workers
(`ShardWorker`) use split channels: a `yring` control channel
(drained unconditionally) and a `yring` data channel (drained up to
`DrainBudget::WORKER`). All producer-to-consumer signaling uses
`DataSignal` (transmit slot, send pipe, fallback queue, shard
workers).

**Inproc.** No ZMTP. Inproc and byte-stream round-robin peers both
register `yring` send pipes. Byte-stream consumers drain in
`ConnectionDriver`; inproc consumers drain in `inproc_peer_driver` and
forward to the socket inbound queue. Same-thread delivery still uses
`blume` where applicable. `FallbackQueue` is only the
no-peer/pre-connect fallback; peer tasks drain it before newer
pipe-fed sends.

## Build / test / bench / charts / releasing

See [`DEVELOPMENT.md`](DEVELOPMENT.md) for the full command reference
(unit tests, feature-gated tests, fuzz, soak, stress tests, benchmarks,
chart generation, release process).
Benchmark results are collected append-only in `~/.cache/omq/*.jsonl`.

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

## Conventions

- Rust 2024 edition, MSRV **1.93**. ASCII-only source.
- `main` branch is protected. All changes go through PRs.

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
  full, fan-out sockets (`PUB`, `XPUB`, `RADIO`) drop on mute
  unless `xpub_nodrop` is set. Round-robin/exclusive sockets
  default to blocking, configurable via `OnMute`. It does not
  return an error.
- **No peer starvation.** Fast peers MUST NOT starve slow peers,
  and slow peers MUST NOT block fast peers.
- **Control plane never starves.** Subscribe, cancel, add-peer,
  remove-peer, and shutdown commands must always be reachable
  within bounded time, regardless of data throughput. Never mix
  control commands into a data channel where they can be buried
  behind an unbounded backlog. Separate channels, drain control
  unconditionally every iteration.
- **Fair fan-in.** Consumer fair-queues across all connections.
- **Transport-agnostic.** Bind TCP and IPC simultaneously. Inproc
  is in-process (no kernel, no serialization).
- **Thread safety.** One socket, one thread. omq-tokio relaxes
  this for async; omq-libzmq does not.

## Socket types

20 types in 10 pairs. Compatibility is checked during ZMTP handshake.

**Pipeline (one-way, round-robin/fair-queue):**
- **PUSH/PULL** -- load distribution. PUSH round-robins across
  PULLs. PULL fair-queues from PUSHes.
- **SCATTER/GATHER** -- Same as PUSH/PULL but single-frame
  only (rejects multipart).

**Pub/sub (fan-out, topic-filtered):**
- **PUB/SUB** -- PUB fans out to all SUBs. SUB subscribes by
  prefix (`subscribe`/`unsubscribe`). PUB filters per subscriber.
  Mute subs drop by default (`OnMute`/`xpub_nodrop` control this).
- **XPUB/XSUB** -- raw pub/sub. XPUB surfaces subscribe/
  unsubscribe as receivable messages. XSUB sends subscribe commands
  explicitly. Used to build proxies (XSUB-XPUB).
- **RADIO/DISH** -- group-based pub/sub. RADIO requires a
  group on every message (`Message::with_group`). DISH joins/leaves
  groups (`join`/`leave`). UDP transport supported.

**Request/reply (strict alternation):**
- **REQ/REP** -- synchronous request/reply. REQ enforces
  send-recv-send-recv. REP enforces recv-send-recv-send. REQ
  prepends empty delimiter frame. REP strips it, saves envelope,
  restores on send.
- **DEALER/ROUTER** -- async request/reply. DEALER is REQ without
  the FSM (free send/recv). ROUTER prepends identity frame on recv,
  routes by identity frame on send. `router_mandatory`: error on
  unknown identity. Handover: new peer with same identity evicts
  old.
- **CLIENT/SERVER** CLIENT is DEALER without multipart.
  SERVER is ROUTER without multipart. Routing via `routing_id`
  field instead of identity frame.

**Exclusive (1:1):**
- **PAIR** -- bidirectional 1:1. Exactly one peer. No fan-out, no
  round-robin, no identity.
- **CHANNEL** Same as PAIR but single-frame only.
- **PEER** N:N bidirectional with identity routing (like
  ROUTER but peers are also PEER, not DEALER).

**Raw TCP:**
- **STREAM** -- raw TCP bridge. No ZMTP framing between peers.
  Recv prepends peer identity frame (like ROUTER). Send requires
  identity frame prefix to select target peer. TCP-only (rejects
  IPC/inproc). Accepts connections from non-ZMQ clients.

## Performance invariants

Fan-out sockets (PUB, XPUB, RADIO) send the same message to many
peers. The wire bytes are identical for all peers on the same
transport when no per-peer encryption is active.
**Encode/compress/frame once, distribute the encoded bytes.**

**Budget every drain loop.** Every loop that drains a channel or
queue must be capped by both message count AND byte count
(`DrainBudget`). Unbounded drains starve the tokio runtime and
other tasks.

**One wake per batch, not per push.** Producer-to-consumer signaling
uses `DataSignal` (atomic flag + `Notify`). `mark()` fires
`notify_one` only on the `false` to `true` transition of the
pending flag. Consumer `clear()`s the flag before draining, then
calls `rearm_if_nonempty()` to self-wake if data remains. For
budget-interrupted drains where the consumer knows data remains,
`reschedule()` fires `notify_one` unconditionally (bypasses the
coalescing check). Never replace `DataSignal` with bare
`Notify::notify_one()` per push.

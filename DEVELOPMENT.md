# Development

## Building and linting

```sh
cargo build --workspace
cargo clippy --workspace --all-targets   # run before every commit
```

Lints: `missing_debug_implementations` = **deny**,
`unsafe_op_in_unsafe_fn` = **deny**, clippy `pedantic` = **warn**.

## Unit and integration tests

```sh
cargo test  -p omq-compio               # default features
cargo test  -p omq-tokio
cargo test  -p omq-proto
cargo test  -p blume
cargo test  -p omq-tokio --test req_rep -- some_test_name
```

Feature-gated tests (mechanisms, compression, priority):

```sh
cargo test  -p omq-tokio  --features plain     --test plain
cargo test  -p omq-tokio  --features curve     --test curve
cargo test  -p omq-compio --features blake3zmq --test blake3zmq
cargo test  -p omq-tokio  --features lz4       --test lz4_tcp
cargo test  -p omq-tokio  --features zstd      --test zstd_tcp
cargo test  -p omq-tokio  --features priority  --test priority
```

Full sweep (all features, both backends):

```sh
./scripts/test-all.sh
OMQ_SKIP_FUZZ=1 ./scripts/test-all.sh    # skip fuzz for a faster run
```

## Fuzz tests

~1M random iterations per suite. Off by default; enable with the
`fuzz` feature. Takes a few minutes per backend.

```sh
cargo test -p omq-compio --features fuzz
cargo test -p omq-tokio  --features fuzz
```

## Soak tests

Long-running leak and stability scenarios. Both backends have
identical suites (12 scenarios each): peer churn, reconnect storm,
PUB/SUB churn, large-message throughput, compression (zstd),
compression (lz4), PLAIN auth, CURVE encryption, BLAKE3ZMQ
encryption, priority tiers, multi-socket, inproc cross-thread.

Pick the backend with `-p`, duration with `OMQ_SOAK_DURATION_SECS`.
Enable all feature-gated scenarios with the full feature set:

```sh
# compio backend, 10 min per scenario
OMQ_SOAK_DURATION_SECS=600 cargo test -p omq-compio \
  --features "soak lz4 zstd plain curve blake3zmq priority" \
  --release -- --nocapture

# tokio backend, same
OMQ_SOAK_DURATION_SECS=600 cargo test -p omq-tokio \
  --features "soak lz4 zstd plain curve blake3zmq priority" \
  --release -- --nocapture

# overnight (2h per scenario)
OMQ_SOAK_DURATION_SECS=7200 cargo test -p omq-compio \
  --features "soak lz4 zstd plain curve blake3zmq priority" \
  --release -- --nocapture
```

Each scenario monitors RSS, FD count, and (where applicable)
throughput stability. Failures print which metric tripped.

#### pyomq (Python binding)

Seven pytest scenarios under `bindings/pyomq/tests/soak/` exercise
the PyO3 binding layer: PUSH/PULL throughput, reconnect storm,
PUB/SUB churn, peer churn, REQ/REP cycles, context churn, and
large messages. Same `OMQ_SOAK_DURATION_SECS` knob:

```sh
cd bindings/pyomq
maturin develop --release
OMQ_SOAK_DURATION_SECS=120 python3 -m pytest tests/soak/ -v --tb=short
```

## Benchmarks

```sh
cargo bench -p omq-compio --bench push_pull
```

Env knobs: `OMQ_BENCH_TRANSPORTS`, `OMQ_BENCH_SIZES`,
`OMQ_BENCH_PEERS`, `OMQ_BENCH_ROUND_MS`, `OMQ_BENCH_ROUNDS`.
Full size sweep: `-- --all-sizes`.
Results append to `<crate>/benches/results.jsonl` unless
`OMQ_BENCH_NO_WRITE=1`.

## Constraints

**interop_compio dep constraint:** `omq-tokio/Cargo.toml`'s compio
dev-dep must use the same git rev as `omq-compio`'s dep. Different
revs link two `compio-runtime` instances -> TLS mismatch panic.

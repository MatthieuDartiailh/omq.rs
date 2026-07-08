# Development

## Building And Linting

```sh
cargo build --workspace
cargo clippy --workspace --all-targets
```

Release, soak, and benchmark builds use the local CPU:

```toml
# .cargo/config.toml
[build]
rustflags = ["-C", "target-cpu=native"]
```

Lints: `missing_debug_implementations` = **deny**,
`unsafe_op_in_unsafe_fn` = **deny**, clippy `pedantic` = **warn**.

## Unit And Integration Tests

```sh
cargo test -p omq-tokio
cargo test -p omq-proto
cargo test -p blume
cargo test -p yring
cargo test -p omq-tokio --test req_rep -- some_test_name
```

Feature-gated tests:

```sh
cargo test -p omq-tokio --features plain     --test plain
cargo test -p omq-tokio --features curve     --test curve
cargo test -p omq-tokio --features blake3zmq --test blake3zmq
cargo test -p omq-tokio --features lz4       --test lz4_tcp --test lz4_pub_sub
```

Miri target for unsafe internals:

```sh
cargo +nightly miri test -p yring
```

Full sweep:

```sh
./scripts/test-all.sh
OMQ_SKIP_FUZZ=1 ./scripts/test-all.sh
```

## Fuzz Tests

The hand-rolled fuzz suites are off by default. Enable with the `fuzz`
feature. Set `OMQ_FUZZ_ITERS=<n>` and `OMQ_FUZZ_SEED=<u64>` for long or
reproducible runs.

```sh
cargo test -p omq-tokio --features fuzz
OMQ_FUZZ_ITERS=500000000 cargo test -p omq-tokio --features fuzz --release -- --nocapture
```

## Soak Tests

Soak tests cover peer churn, reconnect storms, reconnect all types,
PUB/SUB churn, ROUTER/DEALER churn, HWM reconnect, WebSocket
throughput, WebSocket reconnect, large-message throughput, compression
with lz4, PLAIN, CURVE, BLAKE3ZMQ, multi-socket, inproc cross-thread,
and cancel safety.

Set duration with `OMQ_SOAK_DURATION_SECS` (default 600s). Set
`OMQ_SOAK_TOKIO_RUNTIME=multi_thread` or `current_thread` to select the
runtime flavor.

```sh
FEATURES="soak lz4 plain curve blake3zmq ws"
cargo test -p omq-tokio --features "$FEATURES" --release --no-run
OMQ_SOAK_DURATION_SECS=600 cargo test -p omq-tokio \
  --features "$FEATURES" --release --test soak_peer_churn -- --nocapture
```

### pyomq Soak Tests

```sh
cd bindings/pyomq
maturin develop --release
OMQ_SOAK_DURATION_SECS=120 python3 -m pytest tests/soak/ -v --tb=short
```

## Stress Tests

```sh
cargo test -p omq-tokio --test stress_connect_before_bind -- --test-threads=1
```

## Benchmarks

```sh
cargo bench -p omq-tokio --bench push_pull
```

Env knobs: `OMQ_BENCH_TRANSPORTS`, `OMQ_BENCH_SIZES`,
`OMQ_BENCH_PEERS`, `OMQ_BENCH_ROUND_MS`, `OMQ_BENCH_ROUNDS`.
Results append to `$XDG_CACHE_HOME/omq/` (default `~/.cache/omq/`)
unless `OMQ_BENCH_NO_WRITE=1`.

### Cross-implementation Comparison Benchmarks

`scripts/run_comparisons.py` drives standalone `bench_peer` binaries:

| binary | source | impls |
|--------|--------|-------|
| `bench_peer_tokio` | `omq-tokio/src/bin/bench_peer_tokio.rs` | omq-tokio, omq-tokio-mt |
| `libzmq_bench_peer` | `scripts/libzmq_bench_peer.c` | libzmq, omq-libzmq |
| `zmqrs_bench_peer` | `scripts/zmqrs_bench_peer/` | zmq.rs |
| `rzmq_bench_peer` | `scripts/rzmq_bench_peer/` | rzmq, rzmq-iouring |

Each binary speaks a subcommand protocol:

- `push <addr> <size>`: bind PUSH, send forever.
- `pull <addr> <size> <duration>`: connect PULL, count for duration.
- `pub <addr> <size>` / `sub <addr> <size> <duration>`: PUB/SUB throughput.
- `inproc <name> <size> <duration>`: in-process PUSH/PULL.
- `rep <addr> <size>` / `req <addr> <size> <iters> <warmup>`: latency.

Results go to `~/.cache/omq/comparisons.jsonl`.

## Updating Charts

Chart subtitles show hardware info auto-detected from `/proc/cpuinfo`
and sysfs. On machines where sysfs is absent, create `.chart_hw` in the
repo root:

```text
prefix=Linux VM on a 2018 Mac Mini
postfix=performance governor, turbo off
```

All `gen_*_chart.py` scripts read this file automatically via
`scripts/chart_hw.py`.

### Cross-library Comparison Charts

Produces `doc/charts/{pushpull,pubsub,reqrep}/*.svg`,
`doc/charts/pushpull/fan{out,in}/tcp.svg`,
`doc/charts/main_tcp.svg`:

```sh
python3 scripts/run_comparisons.py --omq
python3 scripts/gen_comparison_chart.py
```

Omit `--impl` to rebench all implementations when external baselines
are stale. Full refresh after omq/rzmq changes:

```sh
test -f .chart_hw
python3 scripts/run_comparisons.py --transport tcp --transport ipc --transport inproc \
  --fanout --fanin --pubsub-peers 1,8,32
python3 scripts/gen_comparison_chart.py
```

Stop if `run_comparisons.py` prints any warning or timeout. Fix the
bench peer or script first, then rerun before charting.

### Mechanism Chart

```sh
python3 scripts/bench_mechanism.py tokio --chart-sizes
python3 scripts/gen_mechanism_chart.py
```

### PUB/SUB LZ4 Compression Chart

```sh
python3 scripts/bench_pubsub_lz4.py --chart
python3 scripts/gen_pubsub_lz4_chart.py
```

### Compression Chart

```sh
python3 scripts/bench_compression_tokio.py --chart
python3 scripts/gen_compression_chart.py --backend tokio
```

### pyomq Bindings Charts

```sh
cd bindings/pyomq
export OMQ_HW_EXTRAS="performance governor, turbo off"
maturin develop --release
python scripts/update_perf.py --impl pyomq
python scripts/update_perf.py --chart-only
```

## Releasing

### Dependency Graph

```text
omq-proto ───────┬─ omq-tokio ──┬─ omq-libzmq
blume ───────────┤              └─ pyomq (maturin, not cargo)
yring ───────────┘
```

### Automation

`release-plz` runs on every push to `main`
(`.github/workflows/release-plz.yml`). It opens or updates a release PR,
creates annotated tags after merge, publishes to crates.io, and creates
GitHub releases. Configuration lives in `release-plz.toml`.

### Steps

1. **Review the release-plz PR.** Verify semver bumps.

2. **Curate changelogs.** For each bumped crate, insert a new
   `## [x.y.z]` section below `## [Unreleased]`. Never modify existing
   versioned sections.

3. **Update zguide examples.** Bump `omq-tokio` versions in
   `examples/zguide-tokio/*/Cargo.toml`.

4. **Merge the release PR.** release-plz tags and publishes to
   crates.io automatically.

5. **pyomq** if changed: bump `bindings/pyomq/Cargo.toml` and
   `bindings/pyomq/pyproject.toml`, run `cargo update -p pyomq` inside
   `bindings/pyomq`, add a changelog entry, then push a `pyomq-v*` tag.

### Crates To Check

`omq-proto`, `blume`, `yring`, `omq-tokio`, `omq-libzmq`, `pyomq`.

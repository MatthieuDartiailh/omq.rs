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
cargo test  -p omq-tokio                # default features
cargo test  -p omq-compio
cargo test  -p omq-proto
cargo test  -p blume
cargo test  -p omq-tokio --test req_rep -- some_test_name
```

Feature-gated tests (mechanisms, compression):

```sh
cargo test  -p omq-tokio  --features plain     --test plain
cargo test  -p omq-tokio  --features curve     --test curve
cargo test  -p omq-compio --features blake3zmq --test blake3zmq
cargo test  -p omq-tokio  --features lz4       --test lz4_tcp
```

Miri targets for unsafe internals:

```sh
cargo +nightly miri test -p yring
cargo +nightly miri test -p omq-compio unsafe_cell --lib
```

Full sweep (all features, both backends):

```sh
./scripts/test-all.sh
OMQ_SKIP_FUZZ=1 ./scripts/test-all.sh    # skip fuzz for a faster run
```

## Fuzz tests

~10M random iterations per suite. Off by default; enable with the
`fuzz` feature. Takes a few minutes per backend.

```sh
cargo test -p omq-compio --features fuzz
cargo test -p omq-tokio  --features fuzz
```

For long runs (e.g. 500M iterations), build with `--release` and set
`OMQ_FUZZ_ITERS`. Set `OMQ_FUZZ_SEED=<u64>` to reproduce a failure.

```sh
OMQ_FUZZ_ITERS=500000000 cargo test -p omq-compio --features fuzz --release -- --nocapture
OMQ_FUZZ_ITERS=500000000 cargo test -p omq-tokio  --features fuzz --release -- --nocapture
```

## Soak tests

Long-running leak and stability scenarios. omq-tokio has 21
scenarios; omq-compio has 11 (the ten newest are tokio-only for
now). Scenarios: peer churn, reconnect storm, reconnect all types,
PUB/SUB churn, PUB/SUB churn TCP, XPUB/XSUB churn,
ROUTER/DEALER churn, HWM reconnect (drop + block), WebSocket
throughput, WebSocket reconnect storm, large-message throughput,
compression (lz4), PLAIN auth, CURVE encryption, BLAKE3ZMQ
encryption, multi-socket, inproc cross-thread, cancel safety,
CURVE reconnect, BLAKE3ZMQ reconnect.

Set duration with `OMQ_SOAK_DURATION_SECS` (default 600s).
Enable all feature-gated scenarios with the full feature set.
For omq-tokio, `OMQ_SOAK_TOKIO_RUNTIME=multi_thread` (default) or
`OMQ_SOAK_TOKIO_RUNTIME=current_thread` selects the runtime flavor used
by each soak binary. Run both flavors when validating scheduler-sensitive
changes.

Each soak test is a separate binary, so `cargo test` runs them
sequentially. Launch scenarios in batches of 4 (8 processes) to
keep peak RSS bounded while still running in parallel:

```sh
FEATURES="soak lz4 plain curve blake3zmq ws"

# build first (avoid parallel compilation conflicts)
cargo test -p omq-compio --features "$FEATURES" --release --no-run
cargo test -p omq-tokio  --features "$FEATURES" --release --no-run

# run in batches of 4 scenarios (8 processes), 10 min each
TESTS=(blake3zmq cancel_safety compression compression_lz4 curve
       hwm_reconnect inproc_cross_thread large_throughput
       mechanism_reconnect multi_socket peer_churn plain
       pub_sub_churn pub_sub_churn_tcp reconnect_all_types
       reconnect_storm router_dealer_churn ws
       xpub_xsub_churn)

batch=()
for test in "${TESTS[@]}"; do
  OMQ_SOAK_DURATION_SECS=600 cargo test -p omq-compio \
    --features "$FEATURES" --release --test "soak_${test}" \
    -- --nocapture &
  OMQ_SOAK_DURATION_SECS=600 cargo test -p omq-tokio \
    --features "$FEATURES" --release --test "soak_${test}" \
    -- --nocapture &
  batch+=("$test")
  if [[ ${#batch[@]} -eq 4 ]]; then
    wait
    batch=()
  fi
done
wait
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

## Stress tests

Connect-before-bind and reconnection stress: 200 rounds per
socket-type/transport/bind-role combo. Covers PUSH/PULL, REQ/REP,
PUB/SUB, PAIR, DEALER/ROUTER across TCP, IPC, and inproc with both
sides taking the bind role. Single-threaded to catch hangs.

```sh
cargo test -p omq-compio --test stress_connect_before_bind -- --test-threads=1
cargo test -p omq-tokio  --test stress_connect_before_bind -- --test-threads=1
```

## Benchmarks

```sh
cargo bench -p omq-compio --bench push_pull
```

Env knobs: `OMQ_BENCH_TRANSPORTS`, `OMQ_BENCH_SIZES`,
`OMQ_BENCH_PEERS`, `OMQ_BENCH_ROUND_MS`, `OMQ_BENCH_ROUNDS`.
Full size sweep: `-- --all-sizes`.
Results append to `$XDG_CACHE_HOME/omq/` (default `~/.cache/omq/`)
unless `OMQ_BENCH_NO_WRITE=1`.

PUB/SUB compression benchmarks (`scripts/bench_pubsub_lz4.py`) run
at full CPU speed and project to realistic link speeds. See the
"PUB/SUB LZ4 compression chart" section below.

### Cross-implementation comparison benchmarks

`scripts/run_comparisons.py` drives standalone `bench_peer` binaries:

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
by `scripts/gen_comparison_chart.py` into
`doc/charts/{pushpull,pubsub,reqrep}/{classic,iouring}_*.svg`.

Per-backend benches (separate from comparisons) live in
`omq-tokio/benches/` and `omq-compio/benches/` with shared scaffolding
in `benches/common/mod.rs`. Custom harness (`harness = false`), no
external framework.

## Updating charts

After performance-relevant changes, regenerate the charts before
releasing. Chart subtitles show hardware info auto-detected from
`/proc/cpuinfo` and sysfs. On machines where sysfs is absent (VMs),
create a `.chart_hw` file in the repo root (gitignored):

```
prefix=Linux VM on a 2018 Mac Mini
postfix=performance governor, turbo off
```

All `gen_*_chart.py` scripts read this file automatically via
`scripts/chart_hw.py`. Environment variables `OMQ_HW_PREFIX` and
`OMQ_HW_POSTFIX` still work and take precedence over the file.

### Cross-library comparison charts

Produces `doc/charts/{pushpull,pubsub,reqrep}/{classic,iouring}_*.svg`,
`doc/charts/pushpull/fan{out,in}/{classic,iouring}_tcp.svg`, and
`doc/charts/main_{classic,iouring}_tcp.svg`:

```sh
python3 scripts/run_comparisons.py --impl omq-compio --impl omq-tokio   # omq only, reuse existing libzmq/zmq.rs baselines
python3 scripts/gen_comparison_chart.py                                # JSONL → SVG
```

Omit `--impl` to rebench all implementations when libzmq or zmq.rs
baselines are stale.

### Mechanism charts

Produces `doc/charts/mechanism/{tokio,compio}.svg`:

```sh
cargo bench -p omq-tokio  --bench mechanism --features plain,curve,blake3zmq
cargo bench -p omq-compio --bench mechanism --features plain,curve,blake3zmq
python3 scripts/gen_mechanism_chart.py
```

### PUB/SUB LZ4 compression chart

Produces `doc/charts/pubsub/lz4_tcp.svg`:

```sh
python3 scripts/bench_pubsub_lz4.py --chart   # full sweep + chart
python3 scripts/gen_pubsub_lz4_chart.py        # chart only (reuse existing JSONL)
```

### pyomq bindings charts

Produces `doc/charts/throughput_bindings.svg` and
`doc/charts/latency_bindings.svg`:

```sh
cd bindings/pyomq
maturin develop --release
python scripts/update_perf.py --impl pyomq   # bench pyomq only, reuse existing pyzmq baseline
python scripts/update_perf.py --chart-only   # regenerate SVG from existing JSONL
```

Omit `--impl` to rebench both pyomq and pyzmq.

## Releasing

### Dependency graph (publish order)

```
omq-proto ─────────────────────────────┐
blume ──────────────┐                  │
yring ──────────────┤                  │
                    ├─ omq-compio      │
                    │                  │
                    └─ omq-tokio ──┬─ omq-libzmq
                                   └─ pyomq (maturin, not cargo)
```

### Automation (release-plz)

`release-plz` runs on every push to `main`
(`.github/workflows/release-plz.yml`). It:

- Opens/updates a release PR with version bumps for changed crates,
  including cascading dep updates across the workspace.
- After the PR merges: creates annotated tags, publishes to crates.io
  in dependency order, creates GitHub releases.

Configuration: `release-plz.toml`. Changelogs are hand-curated, not
generated.

Uses trusted publishing (OIDC) for crates.io authentication. Each
crate must be configured as a trusted publisher on crates.io.

### Steps

1. **Review the release-plz PR.** Verify the semver bumps are correct
   (patch for bug fixes, minor for features/perf/refactors).

2. **Curate changelogs.** For each bumped crate, insert a new
   `## [x.y.z]` section below `## [Unreleased]` in `CHANGELOG.md`.
   Never modify existing versioned sections.

3. **Update zguide examples.** Bump `omq-tokio`/`omq-compio` version in
   `examples/zguide-*/*/Cargo.toml`.

4. **Merge the release PR.** release-plz tags and publishes to
   crates.io automatically.

5. **pyomq** (if changed): bump version in **both**
   `bindings/pyomq/Cargo.toml` and `bindings/pyomq/pyproject.toml`
   (maturin reads `pyproject.toml` for the wheel version), run
   `cargo update -p pyomq` to update `Cargo.lock`, add a changelog
   entry in `bindings/pyomq/CHANGELOG.md`, then push a `pyomq-v*`
   tag to trigger the wheel build/publish workflow.

### Crates to check

`omq-proto`, `blume`, `yring`, `omq-compio`, `omq-tokio`,
`omq-libzmq`, `pyomq`. Don't skip the small ones.

## Constraints

**Backend API parity:** `omq-compio` and `omq-tokio` must expose an
identical public `Socket` API. Adding or changing a method on one
backend requires the same change on the other. Parity is enforced by
`tests/coverage_matrix.rs` (both backends) and
`tests/interop/` (cross-runtime TCP and WS tests).

**interop dep constraint:** `tests/interop/Cargo.toml`'s compio
dep must use the same version as `omq-compio`'s dep. Different
versions link two `compio-runtime` instances -> TLS mismatch panic.

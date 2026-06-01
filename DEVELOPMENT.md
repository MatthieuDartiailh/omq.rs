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
cargo test  -p omq-tokio  --features zstd      --test zstd_tcp
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

Long-running leak and stability scenarios. Both backends have
identical suites (12 scenarios each): peer churn, reconnect storm,
PUB/SUB churn, large-message throughput, compression (zstd),
compression (lz4), PLAIN auth, CURVE encryption, BLAKE3ZMQ
encryption, multi-socket, inproc cross-thread.

Set duration with `OMQ_SOAK_DURATION_SECS` (default 600s).
Enable all feature-gated scenarios with the full feature set.

Each soak test is a separate binary, so `cargo test` runs them
sequentially. Launch scenarios in batches of 4 (8 processes) to
keep peak RSS bounded while still running in parallel:

```sh
FEATURES="soak lz4 zstd plain curve blake3zmq"

# build first (avoid parallel compilation conflicts)
cargo test -p omq-compio --features "$FEATURES" --release --no-run
cargo test -p omq-tokio  --features "$FEATURES" --release --no-run

# run in batches of 4 scenarios (8 processes), 10 min each
TESTS=(blake3zmq compression compression_lz4 curve
       inproc_cross_thread large_throughput multi_socket peer_churn
       plain pub_sub_churn reconnect_storm)

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

## Benchmarks

```sh
cargo bench -p omq-compio --bench push_pull
```

Env knobs: `OMQ_BENCH_TRANSPORTS`, `OMQ_BENCH_SIZES`,
`OMQ_BENCH_PEERS`, `OMQ_BENCH_ROUND_MS`, `OMQ_BENCH_ROUNDS`.
Full size sweep: `-- --all-sizes`.
Results append to `$XDG_CACHE_HOME/omq/` (default `~/.cache/omq/`)
unless `OMQ_BENCH_NO_WRITE=1`.

Compression benchmarks run separately with bandwidth limiting.
See [BENCHMARKS_COMPRESSION.md](BENCHMARKS_COMPRESSION.md) for commands and results.

## Updating charts

After performance-relevant changes, regenerate the charts before
releasing.

### Cross-library comparison charts

Produces `doc/charts/comparison_{tcp,ipc,inproc}.svg`:

```sh
python3 scripts/run_comparisons.py --scope omq   # bench omq-compio + omq-tokio only, reuse existing libzmq/zmq.rs baselines
python3 scripts/gen_comparison_chart.py           # JSONL → doc/charts/comparison_*.svg
```

Use `--scope all` (default) to rebench all implementations when
libzmq or zmq.rs baselines are stale.

### Compression charts

Produces `doc/charts/compression/compio_2048.svg` and `doc/charts/compression/tokio_2048.svg`:

```sh
cargo bench -p omq-compio --bench compression --features lz4,zstd  # → ~/.cache/omq/results_compression_compio.jsonl
cargo bench -p omq-tokio  --bench compression --features lz4,zstd  # → ~/.cache/omq/results_compression_tokio.jsonl
python3 scripts/gen_compression_chart.py --backend compio           # JSONL → doc/charts/compression/compio_*.svg
python3 scripts/gen_compression_chart.py --backend tokio            # JSONL → doc/charts/compression/tokio_*.svg
```

### pyomq bindings chart

Produces `bindings/pyomq/doc/charts/bindings.svg`:

```sh
cd bindings/pyomq
maturin develop --release
python scripts/update_perf.py --scope pyomq  # bench pyomq only, reuse existing pyzmq baseline
python scripts/update_perf.py --chart-only   # regenerate SVG from existing JSONL
```

Use `--scope all` (default) to rebench both pyomq and pyzmq.

## Releasing

### Dependency graph (publish order)

```
omq-proto ─────────────────────────────────┐
blume ──────────────┐                      │
yring ──────────────┤                      │
                    ├─ omq-compio ─┬─ omq ─┤
                    │              ├─ omq-libzmq
                    │              └─ pyomq (maturin, not cargo)
                    └─ omq-tokio ──┬─ omq
                                   └─ omq-zeromq
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

3. **Update zguide examples.** Bump `omq` version in
   `examples/zguide-*/*/Cargo.toml`.

4. **Merge the release PR.** release-plz tags and publishes to
   crates.io automatically.

5. **pyomq** (if changed): bump version in
   `bindings/pyomq/Cargo.toml` and `bindings/pyomq/pyproject.toml`,
   push a `pyomq-v*` tag to trigger the wheel build/publish workflow.

### Crates to check

`omq-proto`, `blume`, `yring`, `omq-compio`, `omq-tokio`, `omq`,
`omq-libzmq`, `omq-zeromq`, `pyomq`. Don't skip the small ones.

## Constraints

**Backend API parity:** `omq-compio` and `omq-tokio` must expose an
identical public `Socket` API. Adding or changing a method on one
backend requires the same change on the other. Parity is enforced by
`tests/coverage_matrix.rs` (both backends) and
`omq-tokio/tests/interop_compio.rs`.

**interop_compio dep constraint:** `omq-tokio/Cargo.toml`'s compio
dev-dep must use the same git rev as `omq-compio`'s dep. Different
revs link two `compio-runtime` instances -> TLS mismatch panic.

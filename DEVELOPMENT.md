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
encryption, priority tiers, multi-socket, inproc cross-thread.

Set duration with `OMQ_SOAK_DURATION_SECS` (default 600s).
Enable all feature-gated scenarios with the full feature set.

Each soak test is a separate binary, so `cargo test` runs them
sequentially. Launch scenarios in batches of 4 (8 processes) to
keep peak RSS bounded while still running in parallel:

```sh
FEATURES="soak lz4 zstd plain curve blake3zmq priority"

# build first (avoid parallel compilation conflicts)
cargo test -p omq-compio --features "$FEATURES" --release --no-run
cargo test -p omq-tokio  --features "$FEATURES" --release --no-run

# run in batches of 4 scenarios (8 processes), 10 min each
TESTS=(blake3zmq compression compression_lz4 curve
       inproc_cross_thread large_throughput multi_socket peer_churn
       plain priority pub_sub_churn reconnect_storm)

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

### Steps

1. **Identify changed crates.** For each crate, compare against its
   last release tag:

   ```sh
   git log <crate>-v<last>..HEAD --no-merges -- <crate>/src/ <crate>/Cargo.toml
   ```

2. **Determine semver bump.** Bug fixes only: patch. Anything else
   (features, perf, refactors): minor. Apply the cascade rule: if
   any dependency is bumped, bump every dependent too (even if only
   the dep version changed).

3. **Update each crate:**
   - Bump `version` in `Cargo.toml`.
   - Update dep versions in all dependents' `Cargo.toml` files.
   - Insert a new `## [x.y.z]` section below `## [Unreleased]` in
     `CHANGELOG.md`. Never modify existing versioned sections.
   - Update `omq` version in `examples/zguide-*/*/Cargo.toml`.
   - For pyomq: bump version in both `Cargo.toml` and
     `pyproject.toml`.

4. **Verify.** `cargo check --workspace` and
   `cargo clippy --workspace --all-targets`.

5. **Commit and PR.** One commit for the entire release wave. Push
   to a branch, create a PR, wait for CI, merge.

6. **Tag.** After merge, create annotated tags on the merge commit:

   ```sh
   for t in omq-proto-v0.X.0 blume-v0.X.0 ...; do
     git tag -a "$t" -m "$t"
   done
   ```

7. **Push tags.** Push workspace tags (everything except pyomq):

   ```sh
   git push origin omq-proto-v0.X.0 blume-v0.X.0 ...
   ```

   Push the pyomq tag **separately** so its release workflow
   triggers independently:

   ```sh
   git push origin pyomq-v0.X.0
   ```

8. **Publish to crates.io** in dependency order:

   ```sh
   cargo publish -p omq-proto
   cargo publish -p blume
   cargo publish -p omq-compio
   cargo publish -p omq-tokio
   cargo publish -p omq
   cargo publish -p omq-libzmq
   cargo publish -p omq-zeromq
   ```

   Each `cargo publish` waits for the previous crate to propagate
   before proceeding. pyomq is published by the `release-pyomq.yml`
   GitHub Actions workflow triggered by the `pyomq-v*` tag.

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

# CLAUDE.md — bindings/pyomq

## Purpose

PyO3 binding for `omq-tokio`. Drop-in pyzmq API for Python: sync
(`pyomq`) and async (`pyomq.asyncio`). Single stable-ABI wheel
(`abi3-py39`, Python 3.9+) via maturin. Linux and macOS.

See [`doc/architecture.md`](doc/architecture.md) for internals:
threading model, queue relay, send/recv paths, zero-copy conversions,
proxy, authentication, error mapping, and known limitations.

See [`doc/performance.md`](doc/performance.md) for the performance
journey: dead ends, profiling results, and the decisions behind the
current design. Read it before changing hot paths.

## Build / test / lint

```sh
cd bindings/pyomq
uv venv && source .venv/bin/activate
uv pip install maturin pytest pyzmq pytest-asyncio
maturin develop --release          # rebuild after every Rust change
pytest -v                          # soak tests excluded by default
cargo clippy --all-targets         # separate workspace, not --workspace
```

Maturin enables all features (`plain`, `curve`, `blake3zmq`, `lz4`,
`zstd`). Runtime check: `pyomq.has("curve")`.

Own `Cargo.lock` and `uv.lock` (both committed). Not part of the
workspace root lock file.

## Benchmarks

```sh
maturin develop --release
python scripts/update_perf.py                # full (pyomq + pyzmq)
python scripts/update_perf.py --impl pyomq   # reuse latest pyzmq baseline
python scripts/update_perf.py --chart-only   # regenerate SVG from JSONL
```

Results in `~/.cache/omq/bindings.jsonl` (latest `run_id` per impl wins).
Regenerates `doc/charts/bindings.svg` and the proxy table in `README.md`.

The proxy PUSH/PULL benchmark uses a native omq-compio client
(`bench_proxy_client`) to saturate the proxy without Python
sender/receiver overhead. Build it before running benchmarks:

```sh
cargo build --release -p omq-compio --bin bench_proxy_client
```

If the binary is missing, the proxy PUSH/PULL bench falls back to
Python sender/receiver (slower, measures Python overhead not proxy
throughput).

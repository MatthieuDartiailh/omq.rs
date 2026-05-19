# pyomq

Python binding for [omq.rs](https://github.com/paddor/omq.rs), a Rust libzmq
port. Drop-in pyzmq replacement on the common path.

## Install

```sh
uv pip install pyomq
# Optional extras (built into the wheel via cargo features):
uv pip install 'pyomq[curve]'
uv pip install 'pyomq[blake3zmq,lz4,zstd]'
uv pip install 'pyomq[test]'   # adds pytest, pyzmq for the interop suite
```

## Usage

```python
import pyomq as zmq  # drop-in for `import zmq` from pyzmq

ctx = zmq.Context()
push = ctx.socket(zmq.PUSH)
push.connect("tcp://127.0.0.1:5555")
push.send(b"hello")
push.close()
ctx.term()
```

For asynchronous code:

```python
import pyomq.asyncio as zmq_async

ctx = zmq_async.Context()
sock = ctx.socket(pyomq.PUSH)
await sock.connect("tcp://127.0.0.1:5555")
await sock.send(b"hello")
await sock.close()
```

## Status

Sync and `asyncio` APIs both ship in this release. All 19 ZMTP socket types are wired:

- **Standard (RFC 28 + 47)**: PAIR, PUB, SUB, REQ, REP, DEALER, ROUTER, PULL, PUSH, XPUB, XSUB.
- **Draft**: SERVER, CLIENT (RFC 41), RADIO, DISH (RFC 48), GATHER, SCATTER (RFC 49), PEER, CHANNEL (RFC 51).

Transports: `tcp://`, `ipc://`, `inproc://`, and `udp://` (RADIO/DISH only).
Optional features built into the wheel: `curve`, `blake3zmq`, `lz4`, `zstd`.

DISH groups: use `socket.join(b"group")` / `socket.leave(b"group")` to manage
subscriptions; messages are sent as multipart `[group, body]`.

## Backend

pyomq is built on `omq-compio` (single-threaded io_uring on Linux). The runtime
runs on a dedicated background thread; every Python call releases the GIL
across the runtime trip. This is the only backend pyomq supports — the
`omq-tokio` backend exists in the upstream Rust workspace for callers that need
a multi-thread tokio integration, but pyomq's per-call overhead is shaped
around compio's single-thread invariant.

## Performance

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/comparison_chart_bindings.svg" alt="PUSH/PULL throughput: Python bindings" width="850">
</p>

Loopback PUSH/PULL throughput vs pyzmq, on a Linux 6.12 (Debian 13) VM on an
Intel Mac Mini 2018 (i7-8700B, 3.2 GHz), Rust 1.95.0, default features:

<!-- PERF:START -->
| Size    | inproc pyomq | inproc pyzmq | ratio     | tcp pyomq | tcp pyzmq | ratio     |
|---------|-------------:|-------------:|----------:|----------:|----------:|----------:|
| 8 B     |     1.30 M/s |      627 k/s | **2.08×** |  1.36 M/s |   565 k/s | **2.41×** |
| 32 B    |     1.29 M/s |      620 k/s | **2.08×** |  1.36 M/s |   576 k/s | **2.37×** |
| 128 B   |     1.31 M/s |      516 k/s | **2.54×** |  1.29 M/s |   496 k/s | **2.61×** |
| 512 B   |     1.29 M/s |      480 k/s | **2.69×** |  1.21 M/s |   461 k/s | **2.62×** |
| 2 KiB   |     1.17 M/s |      461 k/s | **2.54×** |   908 k/s |   342 k/s | **2.65×** |
| 8 KiB   |     1.04 M/s |      368 k/s | **2.83×** |   349 k/s |   102 k/s | **3.41×** |
| 32 KiB  |      622 k/s |      196 k/s | **3.17×** |   116 k/s |    46 k/s | **2.50×** |
| 128 KiB |      203 k/s |       70 k/s | **2.91×** |    32 k/s |    24 k/s | **1.32×** |
<!-- PERF:END -->

### `zmq.proxy()` forwarding (128 B, TCP)

<!-- PROXY_PERF:START -->
|                    | pyomq     | pyzmq     | ratio     |
|--------------------|----------:|----------:|----------:|
| PUSH/PULL msg/s    |   963 k/s |   520 k/s | **1.85×** |
| REQ/REP rt/s       |     8,764/s |     6,521/s | **1.34×** |
<!-- PROXY_PERF:END -->

pyomq's `proxy()` runs as a native Rust async loop on the compio thread — no
Python per-message overhead. pyzmq's `zmq.proxy()` calls libzmq's C-level
`zmq_proxy`. PUSH/PULL forwarding is throughput-bound and pyomq is ~1.9× faster.
REQ/REP is latency-bound (4 TCP hops per round-trip) so both are similar.

Run `scripts/update_perf.py` (after `maturin develop --release`) to re-measure and update the tables above.

## Develop

```sh
cd bindings/pyomq
uv venv && source .venv/bin/activate
uv pip install maturin pytest pyzmq
maturin develop --release
pytest -v
```

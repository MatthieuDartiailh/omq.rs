# pyomq

Python binding for [omq.rs](https://github.com/paddor/omq.rs), a Rust libzmq
port. Drop-in pyzmq replacement on the common path.

## Install

```sh
uv pip install pyomq
uv pip install 'pyomq[test]'   # adds pytest, pyzmq for the interop suite
```

The published wheel includes all optional features (curve, blake3zmq, lz4, zstd).
Use `pyomq.has("curve")` at runtime to check availability.

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
import pyomq
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

See [BENCHMARKS.md](https://github.com/paddor/omq.rs/blob/main/BENCHMARKS.md) for full tables.

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

## Compression transports

OMQ.rs adds two transparent compression transports on top of TCP: `lz4+tcp://`
(fast, low-latency) and `zstd+tcp://` (higher ratio, better for large or
structured payloads). Swap the scheme in your endpoint string and everything
else stays the same:

```python
push = ctx.socket(zmq.PUSH)
push.bind("lz4+tcp://127.0.0.1:5555")   # or zstd+tcp://

pull = ctx.socket(zmq.PULL)
pull.connect("lz4+tcp://127.0.0.1:5555")
```

Both peers must use a matching compression endpoint. Payloads below ~512 B are
sent as-is (the codec detects that compression would expand them). For
realistic JSON payloads at 2 KiB, lz4 yields ~3.8× and zstd ~4.5× on a
bandwidth-limited link.

`zstd+tcp://` also auto-trains a dictionary: it samples the first 1000
outbound messages (or 100 KiB of plaintext, whichever comes first), builds an
8 KiB dict, and ships it to the peer once. After that the compression threshold
drops from 512 B to 64 B, so small structured messages start compressing too.
`lz4+tcp://` does not auto-train (LZ4 has no standard dict trainer).

Virtual throughput on bandwidth-limited links (JSON payloads, compio backend):

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/compression_chart_1g.svg" alt="Compression throughput at 1 Gbps" width="850">
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/compression_chart_100m.svg" alt="Compression throughput at 100 Mbps" width="850">
</p>

See [BENCHMARKS_COMPRESSION.md](https://github.com/paddor/omq.rs/blob/main/BENCHMARKS_COMPRESSION.md) for full tables including dict-trained ratios.

## Develop

```sh
cd bindings/pyomq
uv venv && source .venv/bin/activate
uv pip install maturin pytest pyzmq
maturin develop --release
pytest -v
```

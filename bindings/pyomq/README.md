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

Loopback PUSH/PULL throughput vs pyzmq, on a Linux 6.12 (Debian 13) VM on an
Intel Mac Mini 2018 (i7-8700B, 3.2 GHz), Rust 1.95.0, default features:

| Size   | inproc pyomq | inproc pyzmq | ratio     | tcp pyomq | tcp pyzmq | ratio     |
|--------|-------------:|-------------:|----------:|----------:|----------:|----------:|
| 128 B  | 1.64 M/s     | 544 k/s      | **3.02×** | 1.14 M/s  | 483 k/s   | **2.37×** |
| 512 B  | 1.63 M/s     | 513 k/s      | **3.18×** | 914 k/s   | 462 k/s   | **1.98×** |
| 2 KiB  | 1.61 M/s     | 456 k/s      | **3.53×** | 623 k/s   | 361 k/s   | **1.73×** |
| 8 KiB  | 1.44 M/s     | 378 k/s      | **3.81×** | 267 k/s   | 106 k/s   | **2.52×** |
| 32 KiB | 773 k/s      | 192 k/s      | **4.04×** | 86 k/s    | 47 k/s    | **1.85×** |

Run `pytest tests/test_perf.py -v -s` (after `maturin develop --release`) to reproduce on your hardware.

At small sizes, the per-call PyO3 + flume hop is shorter than pyzmq's libzmq
round-trip, so pyomq pulls ahead by a wide margin. At 32 KiB the two
implementations both hit memory-bandwidth and converge (small lead from
compio's writev + io_uring batching).

## Develop

```sh
cd bindings/pyomq
uv venv && source .venv/bin/activate
uv pip install maturin pytest pyzmq
maturin develop --release
pytest -v
```

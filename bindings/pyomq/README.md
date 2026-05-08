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

| Size    | inproc pyomq | inproc pyzmq | ratio     | tcp pyomq | tcp pyzmq | ratio     |
|---------|-------------:|-------------:|----------:|----------:|----------:|----------:|
| 8 B     | 1.49 M/s     | 603 k/s      | **2.47×** | 1.34 M/s  | 519 k/s   | **2.59×** |
| 32 B    | 1.41 M/s     | 603 k/s      | **2.34×** | 1.37 M/s  | 567 k/s   | **2.41×** |
| 128 B   | 1.71 M/s     | 530 k/s      | **3.22×** | 1.37 M/s  | 509 k/s   | **2.70×** |
| 512 B   | 1.57 M/s     | 492 k/s      | **3.19×** | 1.30 M/s  | 463 k/s   | **2.81×** |
| 2 KiB   | 1.44 M/s     | 422 k/s      | **3.41×** | 875 k/s   | 363 k/s   | **2.41×** |
| 8 KiB   | 1.23 M/s     | 370 k/s      | **3.33×** | 324 k/s   | 104 k/s   | **3.12×** |
| 32 KiB  | 645 k/s      | 183 k/s      | **3.52×** | 111 k/s   | 45 k/s    | **2.46×** |
| 128 KiB | 218 k/s      | 68 k/s       | **3.20×** | 31 k/s    | 26 k/s    | **1.23×** |

Run `pytest tests/test_perf.py -v -s` (after `maturin develop --release`) to reproduce on your hardware.

At small sizes, the per-call PyO3 + flume hop is shorter than pyzmq's libzmq
round-trip, so pyomq pulls ahead by a wide margin. The lead holds through
32 KiB (3.5× inproc, 2.5× TCP). At 128 KiB TCP both implementations saturate
memory bandwidth and the gap narrows to ~1.2×; inproc still shows 3.2× because
there is no kernel copy.

## Develop

```sh
cd bindings/pyomq
uv venv && source .venv/bin/activate
uv pip install maturin pytest pyzmq
maturin develop --release
pytest -v
```

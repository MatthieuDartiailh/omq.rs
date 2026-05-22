# pyomq

Python binding for [omq.rs](https://github.com/paddor/omq.rs), a Rust libzmq
port. Drop-in pyzmq replacement on the common path.

## Install

```sh
uv pip install pyomq
uv pip install 'pyomq[test]'   # adds pytest, pyzmq for the interop suite
```

The published wheel includes optional features: plain, curve, lz4, zstd.
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
Optional features built into the wheel: `plain`, `curve`, `lz4`, `zstd`.

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
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/comparison_chart_bindings.svg" alt="PUSH/PULL throughput: Python bindings" width="850">
</p>

Loopback PUSH/PULL throughput vs pyzmq, on a Linux 6.12 (Debian 13) VM on an
Intel Mac Mini 2018 (i7-8700B, 3.2 GHz), Rust 1.95.0, default features:

<!-- PERF:START -->
| Size    | inproc pyomq | inproc pyzmq | ratio     | tcp pyomq | tcp pyzmq | ratio     |
|---------|-------------:|-------------:|----------:|----------:|----------:|----------:|
| 8 B     |     1.65 M/s |      632 k/s | **2.61×** |  1.57 M/s |   585 k/s | **2.69×** |
| 16 B    |     1.65 M/s |      616 k/s | **2.69×** |  1.60 M/s |   599 k/s | **2.67×** |
| 32 B    |     1.63 M/s |      622 k/s | **2.62×** |  1.60 M/s |   603 k/s | **2.65×** |
| 64 B    |     1.62 M/s |      584 k/s | **2.77×** |  1.55 M/s |   540 k/s | **2.87×** |
| 128 B   |     1.61 M/s |      523 k/s | **3.08×** |  1.57 M/s |   528 k/s | **2.98×** |
| 256 B   |     1.62 M/s |      527 k/s | **3.08×** |  1.52 M/s |   509 k/s | **3.00×** |
| 512 B   |     1.60 M/s |      488 k/s | **3.27×** |  1.39 M/s |   498 k/s | **2.79×** |
| 1 KiB   |     1.50 M/s |      500 k/s | **2.99×** |  1.31 M/s |   477 k/s | **2.75×** |
| 2 KiB   |     1.47 M/s |      475 k/s | **3.09×** |   962 k/s |   365 k/s | **2.63×** |
| 4 KiB   |     1.47 M/s |      429 k/s | **3.43×** |   618 k/s |   206 k/s | **3.01×** |
| 8 KiB   |     1.32 M/s |      379 k/s | **3.48×** |   363 k/s |   108 k/s | **3.37×** |
| 16 KiB  |      991 k/s |      268 k/s | **3.70×** |   190 k/s |    56 k/s | **3.38×** |
| 32 KiB  |      765 k/s |      186 k/s | **4.11×** |   113 k/s |    46 k/s | **2.45×** |
| 64 KiB  |      548 k/s |      127 k/s | **4.30×** |    56 k/s |    38 k/s | **1.50×** |
| 128 KiB |      268 k/s |       69 k/s | **3.87×** |    26 k/s |    25 k/s | **1.07×** |
| 256 KiB |      128 k/s |       38 k/s | **3.37×** |    15 k/s |    15 k/s | **1.00×** |
<!-- PERF:END -->

### REQ/REP latency (TCP loopback)

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/comparison_latency_chart_bindings.svg" alt="REQ/REP latency: pyomq vs pyzmq" width="850">
</p>

Serial ping-pong: 1000 warmup + 10000 measured iterations per cell. Lower is better;
ratio = pyzmq / pyomq.

<!-- LATENCY_PERF:START -->
| Size    | pyomq p50 | pyzmq p50 | ratio     | pyomq p99 | pyzmq p99 | ratio     |
|---------|----------:|----------:|----------:|----------:|----------:|----------:|
| 8 B     |   61.8 µs |   70.4 µs | **1.14×** |   84.7 µs |   89.6 µs |     1.06× |
| 32 B    |   63.0 µs |   72.4 µs | **1.15×** |   80.8 µs |   88.1 µs |     1.09× |
| 128 B   |   62.5 µs |   73.0 µs | **1.17×** |   80.7 µs |   91.6 µs | **1.13×** |
| 512 B   |   62.1 µs |   71.7 µs | **1.15×** |   76.4 µs |    101 µs | **1.33×** |
| 2 KiB   |   65.9 µs |   74.4 µs | **1.13×** |   88.2 µs |   92.3 µs |     1.05× |
| 8 KiB   |   68.9 µs |   90.1 µs | **1.31×** |   94.7 µs |    112 µs | **1.19×** |
| 32 KiB  |   80.7 µs |    104 µs | **1.28×** |    109 µs |    139 µs | **1.28×** |
| 128 KiB |    145 µs |    143 µs |     0.99× |    168 µs |    185 µs | **1.10×** |
<!-- LATENCY_PERF:END -->

### `zmq.proxy()` forwarding (128 B, TCP)

<!-- PROXY_PERF:START -->
|                    | pyomq     | pyzmq     | ratio     |
|--------------------|----------:|----------:|----------:|
| PUSH/PULL msg/s    |  1.34 M/s |   540 k/s | **2.48×** |
| REQ/REP rt/s       |  11,222/s |   6,599/s | **1.70×** |
<!-- PROXY_PERF:END -->

pyomq's `proxy()` forwards directly between sockets on the compio thread —
no rings, no Python per-message overhead. pyzmq's `zmq.proxy()` calls libzmq's
C-level `zmq_proxy`. PUSH/PULL forwarding is throughput-bound and pyomq is ~2.5×
faster. REQ/REP proxy is latency-bound (4 TCP hops per round-trip); pyomq is
~1.7× faster thanks to direct socket forwarding.

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
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/compression_chart_1g.svg" alt="Compression throughput at 1 Gbps" width="850">
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/compression_chart_100m.svg" alt="Compression throughput at 100 Mbps" width="850">
</p>

See [BENCHMARKS_COMPRESSION.md](https://github.com/paddor/omq.rs/blob/main/BENCHMARKS_COMPRESSION.md) for full tables including dict-trained ratios.

## CURVE authentication

CURVE encrypts traffic and authenticates the server to the client. To also
authenticate clients to the server, call `set_curve_auth()` before
`bind()`/`connect()`:

```python
server_pub, server_sec = zmq.curve_keypair()
client_pub, client_sec = zmq.curve_keypair()

pull = ctx.socket(zmq.PULL)
pull.curve_server = 1
pull.curve_publickey = server_pub
pull.curve_secretkey = server_sec

# Option 1: allow specific client keys (checked in Rust, no GIL overhead)
pull.set_curve_auth([client_pub])

# Option 2: custom callback receiving a PeerInfo with a .public_key (Z85 bytes)
pull.set_curve_auth(lambda peer: peer.public_key in allowed_keys)

# Option 3: accept any valid CURVE client (the default)
pull.set_curve_auth(None)
```

No ZAP, no filesystem key management. The callback runs during the CURVE
handshake; returning a falsy value rejects the client.

## Develop

```sh
cd bindings/pyomq
uv venv && source .venv/bin/activate
uv pip install maturin pytest pyzmq
maturin develop --release
pytest -v
```

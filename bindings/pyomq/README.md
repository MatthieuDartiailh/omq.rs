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
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/throughput_bindings.svg" alt="PUSH/PULL throughput: Python bindings" width="850">
</p>

Loopback PUSH/PULL throughput vs pyzmq, on a Linux 6.12 (Debian 13) VM on an
Intel Mac Mini 2018 (i7-8700B, 3.2 GHz), Rust 1.95.0, default features:

<!-- PERF:START -->
| Size    | inproc pyomq | inproc pyzmq | ratio     | tcp pyomq | tcp pyzmq | ratio     |
|---------|-------------:|-------------:|----------:|----------:|----------:|----------:|
| 8 B     |     1.68 M/s |      602 k/s | **2.79×** |  1.58 M/s |   569 k/s | **2.79×** |
| 16 B    |     1.47 M/s |      614 k/s | **2.39×** |  1.60 M/s |   517 k/s | **3.09×** |
| 32 B    |     1.67 M/s |      618 k/s | **2.70×** |  1.60 M/s |   546 k/s | **2.92×** |
| 64 B    |     1.66 M/s |      566 k/s | **2.94×** |  1.57 M/s |   538 k/s | **2.92×** |
| 128 B   |     1.67 M/s |      526 k/s | **3.18×** |  1.57 M/s |   497 k/s | **3.15×** |
| 256 B   |     1.67 M/s |      522 k/s | **3.20×** |  1.55 M/s |   498 k/s | **3.10×** |
| 512 B   |     1.67 M/s |      503 k/s | **3.31×** |  1.41 M/s |   479 k/s | **2.94×** |
| 1 KiB   |     1.55 M/s |      465 k/s | **3.34×** |  1.34 M/s |   464 k/s | **2.90×** |
| 2 KiB   |     1.52 M/s |      460 k/s | **3.31×** |   998 k/s |   364 k/s | **2.74×** |
| 4 KiB   |     1.49 M/s |      389 k/s | **3.81×** |   582 k/s |   203 k/s | **2.87×** |
| 8 KiB   |     1.32 M/s |      361 k/s | **3.67×** |   336 k/s |   104 k/s | **3.24×** |
| 16 KiB  |     1.02 M/s |      256 k/s | **3.97×** |   176 k/s |    56 k/s | **3.13×** |
| 32 KiB  |      748 k/s |      188 k/s | **3.98×** |   111 k/s |    46 k/s | **2.40×** |
| 64 KiB  |      541 k/s |      117 k/s | **4.64×** |    55 k/s |    37 k/s | **1.46×** |
| 128 KiB |      304 k/s |       71 k/s | **4.28×** |    26 k/s |    24 k/s | **1.06×** |
| 256 KiB |      131 k/s |       37 k/s | **3.50×** |    15 k/s |    15 k/s | **1.00×** |
<!-- PERF:END -->

### REQ/REP latency (TCP loopback)

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/latency_bindings.svg" alt="REQ/REP latency: pyomq vs pyzmq" width="850">
</p>

Serial ping-pong: 1000 warmup + 10000 measured iterations per cell. Lower is better;
ratio = pyzmq / pyomq.

<!-- LATENCY_PERF:START -->
| Size    | pyomq p50 | pyzmq p50 | ratio     | pyomq p99 | pyzmq p99 | ratio     |
|---------|----------:|----------:|----------:|----------:|----------:|----------:|
| 8 B     |   63.1 µs |   67.9 µs |     1.08× |   96.9 µs |    103 µs |     1.06× |
| 16 B    |   63.3 µs |   69.5 µs |     1.10× |   82.3 µs |   91.6 µs | **1.11×** |
| 32 B    |   63.7 µs |   69.9 µs |     1.10× |   79.2 µs |   90.4 µs | **1.14×** |
| 64 B    |   64.0 µs |   70.2 µs |     1.10× |   84.1 µs |   96.9 µs | **1.15×** |
| 128 B   |   62.8 µs |   69.4 µs | **1.10×** |   80.3 µs |   92.5 µs | **1.15×** |
| 256 B   |   62.2 µs |   70.6 µs | **1.14×** |   87.3 µs |   99.0 µs | **1.13×** |
| 512 B   |   64.0 µs |   69.0 µs |     1.08× |   90.1 µs |    103 µs | **1.15×** |
| 1 KiB   |   64.1 µs |   70.8 µs | **1.10×** |   83.4 µs |   96.8 µs | **1.16×** |
| 2 KiB   |   66.6 µs |   71.2 µs |     1.07× |   87.7 µs |   99.7 µs | **1.14×** |
| 4 KiB   |   66.0 µs |   76.9 µs | **1.16×** |   93.9 µs |    109 µs | **1.16×** |
| 8 KiB   |   73.6 µs |   89.5 µs | **1.22×** |    102 µs |    116 µs | **1.14×** |
| 16 KiB  |   75.6 µs |   92.6 µs | **1.23×** |   93.5 µs |    115 µs | **1.23×** |
| 32 KiB  |   81.2 µs |    100 µs | **1.23×** |    128 µs |    143 µs | **1.11×** |
| 64 KiB  |    109 µs |    115 µs |     1.06× |    146 µs |    150 µs |     1.02× |
| 128 KiB |    149 µs |    146 µs |     0.98× |    211 µs |    191 µs |     0.90× |
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
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/compression_1g.svg" alt="Compression throughput at 1 Gbps" width="850">
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/compression_100m.svg" alt="Compression throughput at 100 Mbps" width="850">
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

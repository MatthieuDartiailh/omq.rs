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
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/bindings/pyomq/doc/charts/bindings.svg" alt="pyomq vs pyzmq performance" width="850">
</p>

Loopback PUSH/PULL throughput vs pyzmq, on a Linux 6.12 (Debian 13) VM on an
Intel Mac Mini 2018 (i7-8700B, 3.2 GHz), Rust 1.95.0, default features:

<!-- PERF:START -->
| Size    | inproc pyomq | inproc pyzmq | ratio     | tcp pyomq | tcp pyzmq | ratio     |
|---------|-------------:|-------------:|----------:|----------:|----------:|----------:|
| 8 B     |     1.60 M/s |      567 k/s | **2.82×** |  1.49 M/s |   563 k/s | **2.65×** |
| 16 B    |     1.63 M/s |      581 k/s | **2.82×** |  1.49 M/s |   530 k/s | **2.82×** |
| 32 B    |     1.60 M/s |      566 k/s | **2.83×** |  1.48 M/s |   543 k/s | **2.72×** |
| 64 B    |     1.61 M/s |      511 k/s | **3.16×** |  1.45 M/s |   511 k/s | **2.84×** |
| 128 B   |     1.60 M/s |      487 k/s | **3.29×** |  1.44 M/s |   468 k/s | **3.07×** |
| 256 B   |     1.60 M/s |      491 k/s | **3.27×** |  1.43 M/s |   472 k/s | **3.04×** |
| 512 B   |     1.58 M/s |      495 k/s | **3.20×** |  1.36 M/s |   458 k/s | **2.98×** |
| 1 KiB   |     1.50 M/s |      457 k/s | **3.28×** |  1.25 M/s |   450 k/s | **2.79×** |
| 2 KiB   |     1.47 M/s |      431 k/s | **3.42×** |   880 k/s |   344 k/s | **2.56×** |
| 4 KiB   |     1.41 M/s |      408 k/s | **3.45×** |   592 k/s |   199 k/s | **2.98×** |
| 8 KiB   |     1.31 M/s |      353 k/s | **3.70×** |   334 k/s |   106 k/s | **3.16×** |
| 16 KiB  |      984 k/s |      262 k/s | **3.76×** |   170 k/s |    56 k/s | **3.01×** |
| 32 KiB  |      725 k/s |      200 k/s | **3.63×** |   107 k/s |    47 k/s | **2.29×** |
| 64 KiB  |      472 k/s |      120 k/s | **3.93×** |    54 k/s |    37 k/s | **1.46×** |
<!-- PERF:END -->

### REQ/REP latency (TCP loopback)

Serial ping-pong: 1000 warmup + 10000 measured iterations per cell. Lower is better;
ratio = pyzmq / pyomq.

<!-- LATENCY_PERF:START -->
| Size    | pyomq p50 | pyzmq p50 | ratio     | pyomq p99 | pyzmq p99 | ratio     |
|---------|----------:|----------:|----------:|----------:|----------:|----------:|
| 8 B     |   62.3 µs |   69.6 µs | **1.12×** |   78.5 µs |   88.8 µs | **1.13×** |
| 16 B    |   62.0 µs |   70.2 µs | **1.13×** |   85.3 µs |   91.9 µs |     1.08× |
| 32 B    |   63.4 µs |   69.9 µs | **1.10×** |   79.1 µs |    104 µs | **1.32×** |
| 64 B    |   62.6 µs |   71.5 µs | **1.14×** |   77.7 µs |   92.1 µs | **1.19×** |
| 128 B   |   62.0 µs |   72.8 µs | **1.18×** |   94.5 µs |   88.9 µs |     0.94× |
| 256 B   |   64.4 µs |   72.9 µs | **1.13×** |   81.3 µs |   89.9 µs | **1.11×** |
| 512 B   |   64.3 µs |   71.4 µs | **1.11×** |   79.4 µs |   89.1 µs | **1.12×** |
| 1 KiB   |   66.2 µs |   73.0 µs | **1.10×** |   80.8 µs |   90.1 µs | **1.12×** |
| 2 KiB   |   63.5 µs |   73.7 µs | **1.16×** |   91.1 µs |   90.2 µs |     0.99× |
| 4 KiB   |   67.1 µs |   75.1 µs | **1.12×** |   97.8 µs |   92.4 µs |     0.95× |
| 8 KiB   |   67.3 µs |   91.0 µs | **1.35×** |    101 µs |    122 µs | **1.22×** |
| 16 KiB  |   75.3 µs |   95.2 µs | **1.26×** |   93.0 µs |    110 µs | **1.19×** |
| 32 KiB  |   82.8 µs |    106 µs | **1.28×** |    100 µs |    123 µs | **1.23×** |
| 64 KiB  |    116 µs |    116 µs |     1.00× |    136 µs |    140 µs |     1.03× |
<!-- LATENCY_PERF:END -->

### `zmq.proxy()` forwarding (128 B, TCP)

<!-- PROXY_PERF:START -->
|                    | pyomq     | pyzmq     | ratio     |
|--------------------|----------:|----------:|----------:|
| PUSH/PULL msg/s    |   867 k/s |   501 k/s | **1.73×** |
| REQ/REP rt/s       |  11,816/s |   6,259/s | **1.89×** |
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

## BLAKE3ZMQ authentication

BLAKE3ZMQ is an omq-native encryption mechanism using BLAKE3 key
derivation and ChaCha20 encryption. Keys are raw 32-byte X25519 keypairs
(not Z85-encoded like CURVE). Setup mirrors CURVE:

```python
server_pub, server_sec = zmq.blake3zmq_keypair()
client_pub, client_sec = zmq.blake3zmq_keypair()

pull = ctx.socket(zmq.PULL)
pull.blake3zmq_server = 1
pull.blake3zmq_publickey = server_pub
pull.blake3zmq_secretkey = server_sec

push = ctx.socket(zmq.PUSH)
push.blake3zmq_serverkey = server_pub
push.blake3zmq_publickey = client_pub
push.blake3zmq_secretkey = client_sec

# Client authentication (same three options as CURVE)
pull.set_blake3zmq_auth([client_pub])                         # allow list
pull.set_blake3zmq_auth(lambda peer: peer.public_key in ok)   # callback
pull.set_blake3zmq_auth(None)                                 # accept all
```

The callback receives a `PeerInfo` with a `.public_key` attribute (raw
32-byte bytes). Requires the `blake3zmq` feature (`pyomq.has("blake3zmq")`).

> [!WARNING]
> **BLAKE3ZMQ has not been independently security audited.** It's an
> omq-native construction (Noise XX + BLAKE3 + X25519 + ChaCha20-BLAKE3)
> and should not be relied on for anything that matters until it has had
> third-party review. Use **CURVE** (RFC 26) for production / regulated
> workloads.

## Develop

```sh
cd bindings/pyomq
uv venv && source .venv/bin/activate
uv pip install maturin pytest pyzmq
maturin develop --release
pytest -v
```

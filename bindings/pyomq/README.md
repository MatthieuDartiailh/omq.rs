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

2-process loopback throughput and latency vs pyzmq, measured on Linux 6.12
(Debian 13), Intel i7-8700B 3.2 GHz, Rust 1.95.0.

### `zmq.proxy()` forwarding (128 B, TCP)

<!-- PROXY_PERF:START -->
|                    | pyomq     | pyzmq     | ratio     |
|--------------------|----------:|----------:|----------:|
| PUSH/PULL msg/s    |   986 k/s |   522 k/s | **1.89×** |
| REQ/REP rt/s       |  11,406/s |   6,221/s | **1.83×** |
<!-- PROXY_PERF:END -->

pyomq's `proxy()` forwards directly between sockets on the compio thread —
no rings, no Python per-message overhead. pyzmq's `zmq.proxy()` calls libzmq's
C-level `zmq_proxy`. PUSH/PULL forwarding is throughput-bound and pyomq is ~2.5×
faster. REQ/REP proxy is latency-bound (4 TCP hops per round-trip); pyomq is
~1.7× faster thanks to direct socket forwarding.

Run `scripts/update_perf.py` (after `maturin develop --release`) to re-measure, regenerate the chart, and update the proxy table.

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

Virtual throughput on bandwidth-limited links (JSON payloads, dict 2 KiB):

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/compression/compio_2048.svg" alt="Compression throughput at 1 Gbps, 100 Mbps, and 10 Mbps" width="850">
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

# Compression Transport Benchmarks

Realistic JSON event-log payloads over TCP loopback (2-process setup).
LZ4 default compression. Dictionary auto-training is off by default
(2 KiB capacity when enabled).

Virtual throughput = msg/s x uncompressed size (effective app data rate
on a constrained link). Charts show projected throughput at 1 Gbps,
100 Mbps, and 10 Mbps.

- **Auto-dict off by default:** when enabled, the encoder trains a
  2 KiB dict from the first 100 messages, ships it once, then uses it
  for all subsequent compression. Small-message throughput improves
  significantly with dict.
- **Pure Rust:** lz4+tcp uses lz4rip, no C compiler required.

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pubsub/lz4_tcp.svg" alt="PUB/SUB lz4+tcp fan-out: projected throughput at link speed" width="850">
</p>

### Compression thresholds

Messages below a minimum size skip compression entirely and pass
through as plaintext. The defaults reflect extensive benchmarking
across link speeds and dict sizes:

| Transport | No dict | With dict |
|-----------|---------|-----------|
| lz4+tcp   | 512 B   | 64 B      |

Operators on high-bandwidth links who send many small messages can
raise the threshold further via `Options::compression_threshold()` to
avoid the CPU overhead of compressing messages that already fit in a
single packet.

### Dict size

When enabled, auto-trained dict capacity defaults to 2 KiB. Benchmarks across dict
sizes (256 B to 8 KiB) show that a 2 KiB dict captures most of the
compressible structure in typical JSON payloads. Larger dicts (4-8 KiB)
produce marginally better wire ratios at 2 KiB+ message sizes but hurt
throughput at smaller sizes due to L1/L2 cache pressure during
compression. The default max accepted dict size from a peer is 8 KiB.


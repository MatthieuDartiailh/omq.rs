# Performance verification

Run the fast TCP core-path gate with:

```text
cargo run --release -p omq-tokio --bin perf_verify
```

The verifier measures CT REQ/REP latency at 256B, canonical 1-IO
PUSH/PULL at 16B, 1KiB, and 16KiB, and canonical 1-IO PUB/SUB with four
subscribers at 16B and 4KiB. It uses separate OMQ contexts and loopback TCP.
Warmup and measurement windows are bounded. The normal run must finish in
under 10 seconds.

Thresholds are machine-specific. Create the ignored `.perf_hw` file in the
repository root. Keys match the measurement names printed by the verifier:

```text
[reqrep_ct]
p50_256b_us=50

[pushpull_1io]
16b_msgs_s=9500000
1k_msgs_s=3000000
16k_msgs_s=250000

[pushpull_2io]
16b_msgs_s=8000000
1k_msgs_s=3000000
16k_msgs_s=250000

[pubsub_1io]
16b_msgs_s=1500000
4k_msgs_s=200000

[pubsub_2io]
16b_msgs_s=1100000
4k_msgs_s=430000
```

Use measured local baselines for the ten throughput values. A missing file
prints measurements without applying thresholds.

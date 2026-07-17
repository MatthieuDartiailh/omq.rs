# omq-bench: Rust rewrite plan

Replace all Python benchmark and chart scripts with a single Rust
binary `omq-bench`. Two phases: bench runners first, chart generation
second.

## Design decisions

- Hand-rolled SVG. No plotters or external chart crate. Pixel-level
  control, minimal output, same visual language as today.
- No more triple Y-axis. CPU% becomes a single averaged number shown
  next to each series in the legend (e.g. "omq-tokio (1T) 98%").
  Eliminates the left CPU% axis, the dashed CPU lines, and the
  line-type legend row.
- Non-main charts compare omq vs libzmq only. Main chart keeps all
  impls (zmq.rs, rzmq).
- Append-only JSONL cache in `~/.cache/omq/` (unchanged).
- Single binary, subcommand CLI via clap.

## 1. Crate structure

```
omq-bench/
  Cargo.toml          # [[bin]] name = "omq-bench"
  src/
    main.rs           # clap dispatch
    cli.rs            # Args / subcommand enums
    hw.rs             # hardware label detection
    jsonl.rs          # JSONL read/append, row types
    process.rs        # subprocess spawn, lifetime guard, CPU reading
    parse.rs          # throughput/latency/multi-socket output parsers
    bench/
      mod.rs
      comparisons.rs  # replaces run_comparisons.py
      mechanism.rs    # replaces bench_mechanism.py
      pubsub_lz4.rs   # replaces bench_pubsub_lz4.py
      compression.rs  # replaces bench_compression_tokio.py
    chart/
      mod.rs
      svg.rs          # shared SVG primitives module
      comparison.rs   # replaces gen_comparison_chart.py
      main_tcp.rs     # replaces gen_main_chart.py
      mechanism.rs    # replaces gen_mechanism_chart.py
      pubsub_lz4.rs   # replaces gen_pubsub_lz4_chart.py
      compression.rs  # replaces gen_compression_chart.py
```

Add to workspace `Cargo.toml` members. The crate is `path = "omq-bench"`,
not under `omq-tokio` or `omq-proto`. It has no dependency on the library
crates (it shells out to `bench_peer_tokio`). This also means it
doesn't slow down the main workspace build.

## 2. Dependencies

```toml
[dependencies]
clap = { version = "4", features = ["derive"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

No async runtime, no tokio. The bench runner is synchronous subprocess
management. Everything is `std::process::Command`,
`std::thread::spawn` for the watchdog, `std::io` for pipe reading.

## 3. Shared modules

### 3.1 `hw.rs` — hardware detection

Replaces `scripts/chart_hw.py` (82 lines).

```rust
pub fn detect_hardware() -> Option<String>
```

Read `/proc/cpuinfo` for model name, `num_cpus::get()` or
`std::thread::available_parallelism()` for core count. Read sysfs
for governor and turbo state. Override via `.chart_hw` file and env
vars (`OMQ_HW_PREFIX`, `OMQ_HW_POSTFIX`, `OMQ_HW_EXTRAS`). Same
precedence as Python.

### 3.2 `jsonl.rs` — row types and I/O

Replaces the scattered JSONL logic in all Python scripts.

```rust
/// Row in comparisons.jsonl
#[derive(Serialize, Deserialize)]
pub struct ComparisonRow {
    pub run_id: String,
    pub impl_name: String,       // "impl" in JSON (rename)
    pub kind: String,            // throughput | latency | pub_sub | fan_out | fan_in
    pub transport: String,
    pub msg_size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msgs_s: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mbps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub push_cpu_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pull_cpu_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pub_cpu_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub req_cpu_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p50_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p99_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p999_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_max: Option<f64>,
    // ... other optional fields
}

/// Row in results_{backend}.jsonl (mechanism bench)
#[derive(Serialize, Deserialize)]
pub struct MechanismRow { ... }

/// Row in results_pubsub_lz4.jsonl
#[derive(Serialize, Deserialize)]
pub struct PubsubLz4Row { ... }

/// Row in results_compression_{backend}.jsonl
#[derive(Serialize, Deserialize)]
pub struct CompressionRow { ... }

pub fn load_jsonl<T: DeserializeOwned>(path: &Path) -> Vec<(usize, T)>
pub fn append_jsonl<T: Serialize>(path: &Path, row: &T)
```

Each JSONL file uses its own row type. `load_jsonl` returns `(seq, row)`
tuples so the "last-writer-wins" dedup logic uses file-order sequence
numbers, matching the Python `_seq` pattern.

### 3.3 `process.rs` — subprocess management

Replaces `run_comparisons.py` lines 35-330: process lifetime guard,
watchdog thread, spawn/kill/capture helpers.

```rust
pub struct ProcessGuard { ... }  // RAII: kills on drop
pub struct Reaper { ... }        // watchdog thread, MAX_PROC_LIFETIME

pub fn spawn(cmd: &[&str], env: &[(&str, &str)], cpu: Option<&str>) -> ProcessGuard
pub fn read_bound_port(proc: &mut ProcessGuard, timeout: Duration) -> Option<u16>
pub fn capture_with_cpu(cmd: &[&str], ...) -> (String, f64)
pub fn read_proc_cpu(pid: u32) -> f64
```

Key behavior from Python to preserve:
- `start_new_session=True` → `pre_exec` with `setsid()`
- Watchdog kills any process alive > 60s
- `atexit` reaper kills all registered processes
- Optional `taskset -c` CPU pinning via `OMQ_BENCH_TASKSET=1` and the
  `MEASURED_CPU` / `OTHER_CPU` masks

### 3.4 `parse.rs` — output parsers

Replaces `parse_throughput`, `parse_latency`, `parse_multi_throughput`
from `run_comparisons.py` lines 378-430.

```rust
pub struct ThroughputResult {
    pub msgs_s: f64,
    pub mbps: f64,
    pub elapsed: f64,
    pub pull_cpu: Option<f64>,
}

pub struct MultiThroughputResult {
    pub msgs_s: f64,       // per-peer mean
    pub mbps: f64,         // aggregate
    pub elapsed: f64,
    pub pull_cpu: Option<f64>,
    pub peer_min: Option<f64>,
    pub peer_max: Option<f64>,
}

pub struct LatencyResult {
    pub p50_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub max_us: f64,
    pub iterations: u64,
    pub req_cpu: Option<f64>,
    pub elapsed: Option<f64>,
}

pub fn parse_throughput(output: &str, size: u64) -> Option<ThroughputResult>
pub fn parse_multi_throughput(output: &str, size: u64, peers: u64) -> Option<MultiThroughputResult>
pub fn parse_latency(output: &str) -> Option<LatencyResult>
```

## 4. Phase 1: bench runner modules

### 4.1 `bench/comparisons.rs`

Replaces `scripts/run_comparisons.py` (~1770 lines). The largest
module.

#### Impl registry

Replaces `IMPLS` dict (lines 896-1057). Use a static slice of structs:

```rust
pub struct ImplDef {
    pub name: &'static str,
    pub binary_from: Option<&'static str>,  // shares binary with this impl
    pub prefix: &'static str,
    pub class: ImplClass,                    // Classic, IoUring, Curve
    pub transports: &'static [Transport],
    pub inproc_tput_subcmd: &'static str,    // default "inproc"
    pub inproc_lat_subcmd: &'static str,     // default "inproc-latency"
    pub inproc_pubsub_subcmd: &'static str,  // default "inproc-pubsub"
    pub pub_needs_peer_count: bool,
    pub fanout_push_subcmd: &'static str,    // default "push"
    pub fanio_needs_peer_count: bool,
    pub supports_pubsub: bool,
    pub env: &'static [(&'static str, &'static str)],
}
```

#### Build step

Replaces `build_peers()` (lines 1064-1113). Run `cargo build
--release` for omq-tokio, `gcc` for libzmq, `cargo build` for
zmq.rs/rzmq subdirectories. Return `HashMap<String, PathBuf>`.

#### Cell functions

Each returns the best-of-N result:

- `run_throughput_cell()` — 1 push + 1 pull, 2-process. Lines 453-540.
- `run_pubsub_cell()` — 1 pub + 1 multi-sub process. Lines 543-657.
- `run_fanout_cell()` — 1 push + 1 multi-pull process. Lines 632-755.
- `run_fanin_cell()` — 1 multi-push + 1 pull-bind process. Lines 758-773.
- `run_latency_cell()` — 1 rep + 1 req, 2-process. Lines 776-908.

#### Measurement integrity

Replaces `MEASUREMENT_ISSUES` list and `_note`/`_flush_issues` (lines
342-376). Track missing CPU fields per cell, abort the run if any
best-of-N winner is incomplete.

#### Top-level orchestration

Replaces `run_benchmarks()` (lines 1116-1574) and `main()` (lines
1578-1774). Iterate impls × transports × sizes, call cell functions,
append JSONL, print table.

### 4.2 `bench/mechanism.rs`

Replaces `scripts/bench_mechanism.py` (240 lines). Simple 2-process
PUSH/PULL with `OMQ_BENCH_MECHANISM` env var selecting PLAIN, CURVE,
or CURVE. Writes to `~/.cache/omq/results_tokio.jsonl`.

### 4.3 `bench/pubsub_lz4.rs`

Replaces `scripts/bench_pubsub_lz4.py` (275 lines). 1 PUB → 32 SUBs
with tcp vs lz4+tcp. Also runs `wire-size` and `train-dict`
subcommands of `bench_peer_tokio` for dict sweeps. Writes to
`~/.cache/omq/results_pubsub_lz4.jsonl`.

### 4.4 `bench/compression.rs`

Replaces `scripts/bench_compression_tokio.py` (241 lines). 2-process
PUSH/PULL with JSON payloads, tcp vs lz4+tcp, with and without dict.
Writes to `~/.cache/omq/results_compression_tokio.jsonl`.

## 5. Phase 2: chart modules

### 5.1 `chart/svg.rs` — shared SVG primitives

The core module. Eliminates the copy-pasted SVG helpers across 5
Python scripts.

```rust
pub struct SvgDoc {
    width: f64,
    height: f64,
    lines: Vec<String>,
}

impl SvgDoc {
    pub fn new(width: f64, height: f64) -> Self
    pub fn rect(&mut self, x: f64, y: f64, w: f64, h: f64, fill: &str)
    pub fn line(&mut self, x1: f64, y1: f64, x2: f64, y2: f64, style: LineStyle)
    pub fn polyline(&mut self, points: &[(f64, f64)], style: LineStyle)
    pub fn circle(&mut self, cx: f64, cy: f64, r: f64, fill: &str)
    pub fn x_mark(&mut self, cx: f64, cy: f64, r: f64, color: &str)
    pub fn text(&mut self, x: f64, y: f64, content: &str, style: TextStyle)
    pub fn whisker(&mut self, x: f64, y_lo: f64, y_hi: f64, color: &str)
    pub fn finish(self) -> String
}

pub struct LineStyle {
    pub color: &'static str,
    pub width: f64,
    pub dash: Option<&'static str>,  // e.g. "6,3"
    pub opacity: Option<f64>,
}

pub struct TextStyle {
    pub anchor: Anchor,      // Start, Middle, End
    pub fill: &'static str,
    pub size: f64,
    pub weight: Option<&'static str>,
    pub baseline: Option<&'static str>,
    pub rotate: Option<f64>,
}
```

Higher-level layout helpers:

```rust
/// Axis configuration
pub struct Axis {
    pub min: f64,
    pub max: f64,
    pub log: bool,
}

impl Axis {
    pub fn map(&self, value: f64, pixel_lo: f64, pixel_hi: f64) -> f64
    pub fn ticks(&self, target_count: usize) -> Vec<f64>
}

/// Draw gridlines + tick labels for a vertical axis
pub fn draw_y_axis(doc: &mut SvgDoc, axis: &Axis, ...)

/// Draw x-axis size labels
pub fn draw_x_labels(doc: &mut SvgDoc, sizes: &[u64], xs: &[f64], y: f64)

/// Draw impl legend with optional CPU% annotation
pub fn draw_legend(
    doc: &mut SvgDoc,
    items: &[LegendItem],
    mid_x: f64,
    y: f64,
) -> f64  // returns extra height consumed

pub struct LegendItem {
    pub color: &'static str,
    pub label: String,
    pub cpu_pct: Option<f64>,  // shown as "(98%)" suffix
}
```

The `nice_step()` function (used by all chart scripts) lives here:

```rust
pub fn nice_step(max_val: f64, target_lines: usize) -> f64
```

### 5.2 Chart design changes

**Before (triple axis):**
- Left axis: CPU% (dashed lines per series per size)
- Right axis 1: msg/s or GB/s (solid lines)
- Right axis 2: msg/s (dashed lines, outer)
- Line-type legend explaining dashed vs solid vs dotted

**After (single axis + legend CPU%):**
- Single metric axis (msg/s for small, GB/s for large)
- Solid lines with dot markers
- Whisker bars where applicable (fan-out)
- CPU% as a suffix in the legend: "omq-tokio (1T) 98%"
- No line-type legend needed

This simplification applies to:
- `pushpull/{tcp,ipc,inproc}.svg` — was 3-axis, becomes 1 metric axis
- `reqrep/{tcp,ipc,inproc}.svg` — was 2-axis (latency + CPU%), becomes latency only
- `pubsub/tcp.svg` — was 3-axis, becomes 1 metric axis
- `pushpull/fanout/tcp.svg`, `fanin/tcp.svg` — was 3-axis, becomes 1 metric axis
- `mechanism/tokio.svg` — was 2-axis (msg/s + GB/s, both log), keep both (dual Y, no CPU)
- `compression/tokio.svg`, `pubsub/lz4_tcp.svg` — link-speed projection panels, keep throughput + msg/s (dual Y, no CPU)

### 5.3 `chart/comparison.rs`

Replaces `scripts/gen_comparison_chart.py` (1914 lines). Produces:

| output | panel type |
|--------|-----------|
| `pushpull/{tcp,ipc,inproc}.svg` | split: small msg/s + large GB/s |
| `reqrep/{tcp,ipc,inproc}.svg` | single: p50 latency |
| `pubsub/tcp.svg` | multi-panel: 4p + 64p, split msg/s + GB/s |
| `pubsub/curve_tcp.svg` | multi-panel: 16p, split msg/s + GB/s |
| `pushpull/fanout/tcp.svg` | multi-panel: 4p + 64p, split msg/s + GB/s |
| `pushpull/fanin/tcp.svg` | multi-panel: 4p + 64p, split msg/s + GB/s |

Data loading: read `comparisons.jsonl`, dedup by `(impl, size)` keeping
the latest row by file position. Group by transport/kind/peers.

### 5.4 `chart/main_tcp.rs`

Replaces `scripts/gen_main_chart.py` (508 lines). Produces the three
focused main TCP charts: `main_pushpull_tcp.svg`,
`main_reqrep_tcp.svg`, and `main_pubsub_tcp.svg`. Each chart contains
only the implementations relevant to that workload.

### 5.5 `chart/mechanism.rs`

Replaces `scripts/gen_mechanism_chart.py` (347 lines). Dual log-scale
axes (msg/s left, GB/s right). Two series: PLAIN, CURVE.
No CPU% axis.

### 5.6 `chart/pubsub_lz4.rs`

Replaces `scripts/gen_pubsub_lz4_chart.py` (468 lines). Link-speed
projection panels (1G, 100M, 10M). Two axes: aggregate throughput
(log) + msg/s (log). Three series: tcp, lz4+tcp, lz4+tcp+dict.
CPU% becomes legend annotation.

### 5.7 `chart/compression.rs`

Replaces `scripts/gen_compression_chart.py` (571 lines). Four
link-speed panels (10G, 1G, 100M, 10M). Same axis structure as
pubsub_lz4. CPU% becomes legend annotation.

## 6. CLI design

```
omq-bench run comparisons [FLAGS]
    --impl <name>...          # filter impls
    --omq                     # shorthand: omq-tokio-ct + omq-tokio-2t
    --transport <t>...        # tcp, ipc, inproc, ws
    --sizes <list>
    --duration <secs>
    --rounds <n>
    --no-throughput
    --no-latency
    --no-pubsub
    --pubsub-peers <list>
    --fanout / --fanin
    --fanout-peers / --fanin-peers <list>
    --curve
    --quick-run

omq-bench run mechanism [--chart-sizes] [--sizes <list>]
omq-bench run pubsub-lz4 [--quick] [--chart]
omq-bench run compression [--chart]

omq-bench chart                    # regenerate all charts
omq-bench chart comparisons        # just comparison + main
omq-bench chart mechanism
omq-bench chart pubsub-lz4
omq-bench chart compression
```

## 7. Migration path

### Phase 1 (bench runners)

1. Create `omq-bench/` crate with shared modules (`hw`, `jsonl`,
   `process`, `parse`) and `bench/comparisons.rs`.
2. Validate: run side-by-side with Python, compare JSONL output.
3. Port `bench/mechanism.rs`, `bench/pubsub_lz4.rs`,
   `bench/compression.rs`.
4. Update `DEVELOPMENT.md`: replace `python3 scripts/run_comparisons.py`
   with `cargo run -p omq-bench --release -- run comparisons`.
5. Keep Python scripts in-tree until phase 2 is done (chart gen still
   needs them for `--chart` flags).

### Phase 2 (chart generation)

1. Implement `chart/svg.rs` with primitives. Test with a minimal chart.
2. Port `chart/comparison.rs` (the big one). Compare SVG pixel-diff
   against Python output.
3. Port remaining chart modules.
4. Update `DEVELOPMENT.md`: replace `python3 scripts/gen_*_chart.py`
   with `cargo run -p omq-bench --release -- chart`.
5. Delete all Python scripts: `run_comparisons.py`,
   `bench_mechanism.py`, `bench_pubsub_lz4.py`,
   `bench_compression_tokio.py`, `gen_comparison_chart.py`,
   `gen_main_chart.py`, `gen_mechanism_chart.py`,
   `gen_pubsub_lz4_chart.py`, `gen_compression_chart.py`,
   `chart_hw.py`.
6. Delete `scripts/zmqrs_bench_peer/`, `scripts/rzmq_bench_peer/`,
   `scripts/libzmq_bench_peer.c` and `scripts/libzmq_bench_peer` only
   if they are no longer needed (they are bench *peers*, not the
   runner). They stay.

## 8. JSONL file inventory

| file | writer | reader |
|------|--------|--------|
| `comparisons.jsonl` | `bench/comparisons.rs` | `chart/comparison.rs`, `chart/main_tcp.rs` |
| `results_tokio.jsonl` | `bench/mechanism.rs` | `chart/mechanism.rs` |
| `results_pubsub_lz4.jsonl` | `bench/pubsub_lz4.rs` | `chart/pubsub_lz4.rs` |
| `results_compression_tokio.jsonl` | `bench/compression.rs` | `chart/compression.rs` |

All files are append-only. The chart reader always takes the latest row
per dedup key (typically `(impl, msg_size)` or `(transport, msg_size)`
by highest file-position sequence number). Existing Python-written
rows remain readable; the Rust writer produces identical JSON field
names.

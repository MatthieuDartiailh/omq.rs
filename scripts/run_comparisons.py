#!/usr/bin/env python3
"""Consolidated benchmark comparison runner.

Runs PUSH/PULL throughput and REQ/REP latency benchmarks across
implementations (omq-compio, omq-tokio, libzmq, zmq.rs) and writes
results to benchmarks/comparisons.jsonl.

Usage:
  scripts/run_comparisons.py                        # all impls, tcp+inproc+ipc, latency on
  scripts/run_comparisons.py --quick-run            # 3 sizes only
  scripts/run_comparisons.py --impl rzmq            # single impl
  scripts/run_comparisons.py --impl omq-compio --impl libzmq  # subset
  scripts/run_comparisons.py --transport tcp         # TCP only
  scripts/run_comparisons.py --no-latency           # skip REQ/REP latency
  scripts/run_comparisons.py --update-markdown      # update COMPARISONS.md tables
"""

import argparse
import atexit
import glob
import json
import os
import random
import re
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _cleanup_ipc_sockets():
    """Remove stale IPC socket files left by benchmark peers."""
    for p in glob.glob(str(ROOT / "@omq-bench-cmp-*")):
        try:
            os.unlink(p)
        except OSError:
            pass
CACHE_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq"
JSONL_PATH = CACHE_DIR / "comparisons.jsonl"
COMPARISONS_MD = ROOT / "COMPARISONS.md"

FULL_SIZES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
QUICK_SIZES = [32, 1024, 4096]
TABLE_SIZES = [32, 1024, 4096]

DEFAULT_DURATION = float(os.environ.get("OMQ_BENCH_DURATION", "2.0"))
QUICK_DURATION = 1.5
DEFAULT_ROUNDS = int(os.environ.get("OMQ_BENCH_ROUNDS", "3"))
QUICK_ROUNDS = 1
LATENCY_ITERATIONS = 5_000
LATENCY_WARMUP = 500
LATENCY_TIMEOUT = 15


# ── formatting ────────────────────────────────────────────────────

def size_label(n: int) -> str:
    if n >= 1024 * 1024:
        return f"{n // (1024 * 1024)} MiB"
    if n >= 1024:
        return f"{n // 1024} KiB"
    return f"{n} B"


def format_si(v: float | None) -> str:
    if v is None or v <= 0:
        return "—"
    if v >= 1e6:
        return f"{v / 1e6:.2f}M"
    if v >= 100e3:
        return f"{v / 1e3:.0f}k"
    if v >= 1e3:
        return f"{v / 1e3:.1f}k"
    return f"{v:.0f}"


def format_mbps(v: float | None) -> str:
    if v is None or v <= 0:
        return "—"
    if v >= 1000:
        return f"{v / 1000:.1f} GB/s"
    return f"{v:.0f} MB/s"


def format_us(v: float | None) -> str:
    if v is None or v <= 0:
        return "—"
    if v >= 10_000:
        return f"{v / 1000:.0f} ms"
    if v >= 1_000:
        return f"{v / 1000:.1f} ms"
    if v >= 100:
        return f"{v:.0f} µs"
    if v >= 10:
        return f"{v:.1f} µs"
    return f"{v:.2f} µs"


def speedup_str(val: float | None, ref: float | None) -> str:
    if not val or not ref or ref <= 0:
        return "—"
    r = val / ref
    if r >= 1.1:
        return f"**{r:.1f}×**"
    return f"{r:.2f}×"


def latency_speedup_str(ref: float | None, val: float | None) -> str:
    if not val or not ref or val <= 0:
        return "—"
    r = ref / val
    if r >= 1.1:
        return f"**{r:.1f}×**"
    return f"{r:.2f}×"


# ── build ─────────────────────────────────────────────────────────

def cargo_build(crate: str, binary: str, features: list[str] | None = None):
    cmd = ["cargo", "build", "--release", "-p", crate, "--bin", binary, "-q"]
    if features:
        cmd += ["--features", ",".join(features)]
    subprocess.run(cmd, cwd=ROOT, check=True)


def gcc_build(src: Path, out: Path):
    subprocess.run(
        ["gcc", "-O2", "-o", str(out), str(src), "-lzmq", "-lpthread"],
        check=True,
    )


def cargo_version(crate: str, manifest: Path | None = None) -> str:
    cmd = ["cargo", "metadata", "--format-version", "1", "--no-deps"]
    if manifest:
        cmd += ["--manifest-path", str(manifest)]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=True, cwd=ROOT,
        )
        pkgs = json.loads(result.stdout)["packages"]
        for p in pkgs:
            if p["name"] == crate:
                return p["version"]
    except Exception:
        pass
    return "?"


def libzmq_version() -> str:
    try:
        result = subprocess.run(
            ["pkg-config", "--modversion", "libzmq"],
            capture_output=True, text=True,
        )
        v = result.stdout.strip()
        return v if v else "?"
    except Exception:
        return "?"


# ── process management ────────────────────────────────────────────

def spawn_process(binary: str, *args: str) -> subprocess.Popen:
    return subprocess.Popen(
        [binary, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def read_bound_port(proc: subprocess.Popen, timeout: float = 5.0) -> int | None:
    """Read 'PORT <n>' from the process's first stdout line."""
    import selectors
    sel = selectors.DefaultSelector()
    sel.register(proc.stdout, selectors.EVENT_READ)
    ready = sel.select(timeout=timeout)
    sel.close()
    if not ready:
        return None
    line = proc.stdout.readline().strip()
    if line.startswith("PORT "):
        return int(line.split()[1])
    return None


def capture_process(binary: str, *args: str, timeout: int = 15) -> str:
    proc = subprocess.Popen(
        [binary, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    try:
        stdout, _ = proc.communicate(timeout=timeout)
        return stdout
    except subprocess.TimeoutExpired:
        print(f"WARNING: timeout: {binary} {' '.join(args)}", file=sys.stderr)
        proc.kill()
        proc.wait()
        return ""


def kill_process(proc: subprocess.Popen):
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=5)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            proc.kill()
            proc.wait(timeout=2)
        except Exception:
            pass


# ── measurement parsing ──────────────────────────────────────────

def parse_throughput(output: str, size: int) -> dict | None:
    parts = output.strip().split()
    if len(parts) < 2:
        return None
    count = float(parts[0])
    elapsed = float(parts[1])
    if elapsed <= 0:
        return None
    msgs_s = count / elapsed
    mbps = (count * size) / elapsed / 1e6
    return {"msgs_s": msgs_s, "mbps": mbps}


def parse_latency(output: str) -> dict | None:
    parts = output.strip().split()
    if len(parts) < 5:
        return None
    return {
        "p50_us": float(parts[0]),
        "p99_us": float(parts[1]),
        "p999_us": float(parts[2]),
        "max_us": float(parts[3]),
        "iterations": int(parts[4]),
    }


# ── benchmark cells ──────────────────────────────────────────────

def run_throughput_cell(
    binary: str, transport: str, addr: str, size: int,
    inproc_subcmd: str = "inproc",
    duration: float = DEFAULT_DURATION,
    rounds: int = DEFAULT_ROUNDS,
) -> dict | None:
    best = None
    # Extra retries for inproc: compio cross-thread waker bug causes
    # intermittent hangs. With 5s timeout per attempt, retries are cheap.
    effective_rounds = max(rounds, 5) if transport == "inproc" else rounds
    for _ in range(effective_rounds):
        result = _run_throughput_once(binary, transport, addr, size,
                                      inproc_subcmd, duration)
        if result and (best is None or result["msgs_s"] > best["msgs_s"]):
            best = result
    return best


def _run_throughput_once(
    binary: str, transport: str, addr: str, size: int,
    inproc_subcmd: str, duration: float,
) -> dict | None:
    dur = str(duration)
    if transport == "inproc":
        # Shorter timeout: inproc finishes in ~2.5s or hangs forever
        # (compio cross-thread waker bug). 5s catches hangs faster.
        output = capture_process(binary, inproc_subcmd, addr, str(size), dur,
                                 timeout=5)
        return parse_throughput(output, size)

    push = spawn_process(binary, "push", addr, str(size))
    port = read_bound_port(push)
    if port is None:
        kill_process(push)
        return None
    connect_addr = str(port)
    try:
        output = capture_process(binary, "pull", connect_addr, str(size), dur)
    finally:
        kill_process(push)
    return parse_throughput(output, size)


def run_pubsub_cell(
    binary: str, transport: str, addr: str, size: int, peers: int,
    inproc_subcmd: str = "inproc-pubsub",
    duration: float = DEFAULT_DURATION,
    rounds: int = DEFAULT_ROUNDS,
) -> dict | None:
    best = None
    for _ in range(rounds):
        result = _run_pubsub_once(binary, transport, addr, size, peers,
                                  inproc_subcmd, duration)
        if result and (best is None or result["msgs_s"] > best["msgs_s"]):
            best = result
    return best


def _run_pubsub_once(
    binary: str, transport: str, addr: str, size: int, peers: int,
    inproc_subcmd: str, duration: float,
) -> dict | None:
    dur = str(duration)
    if transport == "inproc":
        output = capture_process(binary, inproc_subcmd, addr, str(size), dur,
                                 str(peers))
        result = parse_throughput(output, size)
    else:
        pub_ = spawn_process(binary, "pub", addr, str(size))
        time.sleep(0.15)
        if pub_.poll() is not None:
            print(f"WARNING: pub died (rc={pub_.returncode}) for {binary} {addr}",
                  file=sys.stderr)
            return None
        drain_subs = []
        try:
            for _ in range(peers - 1):
                drain_subs.append(spawn_process(binary, "sub", addr, str(size), dur))
            time.sleep(0.05)
            output = capture_process(binary, "sub", addr, str(size), dur)
        finally:
            kill_process(pub_)
            for s in drain_subs:
                kill_process(s)
        result = parse_throughput(output, size)
    if result and peers > 1:
        result["mbps"] *= peers
    return result


def run_latency_cell(
    binary: str, transport: str, addr: str, size: int,
    inproc_subcmd: str = "inproc-latency",
    iterations: int = LATENCY_ITERATIONS,
    warmup: int = LATENCY_WARMUP,
    timeout: int = LATENCY_TIMEOUT,
) -> dict | None:
    if transport == "inproc":
        output = capture_process(
            binary, inproc_subcmd, addr, str(size),
            str(iterations), str(warmup),
            timeout=timeout,
        )
        return parse_latency(output)

    rep = spawn_process(binary, "rep", addr, str(size))
    port = read_bound_port(rep)
    if port is None:
        kill_process(rep)
        return None
    connect_addr = str(port)
    try:
        output = capture_process(
            binary, "req", connect_addr, str(size),
            str(iterations), str(warmup),
            timeout=timeout,
        )
    finally:
        kill_process(rep)
    return parse_latency(output)


# ── address generation ────────────────────────────────────────────

def addr_for(transport: str, prefix: str, idx: int, base_port: int) -> str:
    if transport == "tcp":
        offsets = {"c": 0, "t": 100, "z": 200, "q": 300, "s": 400, "r": 1000, "m": 1200}
        return str(base_port + offsets.get(prefix, 0) + idx)
    if transport == "ws":
        offsets = {"c": 500, "t": 600, "z": 700, "q": 800, "s": 900, "r": 1100, "m": 1300}
        return f"ws://127.0.0.1:{base_port + offsets.get(prefix, 500) + idx}/"
    if transport == "ipc":
        return f"ipc://@omq-bench-cmp-{prefix}-{idx}"
    if transport == "inproc":
        return f"bench-cmp-{prefix}-{idx}"
    return str(base_port + idx)


# ── JSONL I/O ─────────────────────────────────────────────────────

def append_jsonl(row: dict):
    JSONL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(JSONL_PATH, "a") as f:
        f.write(json.dumps(row, separators=(",", ":")) + "\n")


def load_jsonl() -> list[dict]:
    if not JSONL_PATH.exists():
        return []
    rows = []
    for line in JSONL_PATH.read_text().splitlines():
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def latest_by_key(rows: list[dict], key_fields: list[str]) -> dict[tuple, dict]:
    result: dict[tuple, dict] = {}
    for row in rows:
        key = tuple(row.get(f) for f in key_fields)
        prev = result.get(key)
        if prev is None or row.get("run_id", "") >= prev.get("run_id", ""):
            result[key] = row
    return result


# ── markdown table update ─────────────────────────────────────────

def replace_block(text: str, marker: str, content: str) -> str:
    b = f"<!-- BEGIN {marker} -->"
    e = f"<!-- END {marker} -->"
    pattern = re.compile(re.escape(b) + r".*?" + re.escape(e), re.DOTALL)
    if not pattern.search(text):
        print(f"WARNING: marker {b} not found in COMPARISONS.md", file=sys.stderr)
        return text
    return pattern.sub(f"{b}\n{content}{e}", text)


def build_throughput_table(
    latest: dict[tuple, dict],
    ref_impl: str,
    impls: list[tuple[str, str]],
) -> str:
    cols = ["Size", f"{ref_impl} msg/s", f"{ref_impl} MB/s"]
    for _, label in impls:
        cols += [f"{label} msg/s", f"{label} MB/s", f"{label} ×"]

    md = "| " + " | ".join(cols) + " |\n"
    md += "|" + "|".join(["---"] * len(cols)) + "|\n"

    for size in TABLE_SIZES:
        ref_key = (ref_impl, "throughput", size)
        ref_row = latest.get(ref_key)
        ref_msgs = format_si(ref_row["msgs_s"] if ref_row else None)
        ref_bw = format_mbps(ref_row["mbps"] if ref_row else None)

        cells = [size_label(size), ref_msgs, ref_bw]
        for impl_name, _ in impls:
            key = (impl_name, "throughput", size)
            row = latest.get(key)
            cells.append(format_si(row["msgs_s"] if row else None))
            cells.append(format_mbps(row["mbps"] if row else None))
            val = row["msgs_s"] if row else None
            ref_val = ref_row["msgs_s"] if ref_row else None
            cells.append(speedup_str(val, ref_val))

        md += "| " + " | ".join(cells) + " |\n"

    md += "\n"
    return md


def build_latency_table(
    latest: dict[tuple, dict],
    ref_impl: str,
    impls: list[tuple[str, str]],
) -> str:
    cols = ["Size", f"{ref_impl} p50", f"{ref_impl} p99"]
    for _, label in impls:
        cols += [f"{label} p50", f"{label} p99", f"{label} ×"]

    md = "| " + " | ".join(cols) + " |\n"
    md += "|" + "|".join(["---"] * len(cols)) + "|\n"

    for size in TABLE_SIZES:
        ref_key = (ref_impl, "latency", size)
        ref_row = latest.get(ref_key)
        ref_p50 = format_us(ref_row["p50_us"] if ref_row else None)
        ref_p99 = format_us(ref_row["p99_us"] if ref_row else None)

        cells = [size_label(size), ref_p50, ref_p99]
        for impl_name, _ in impls:
            key = (impl_name, "latency", size)
            row = latest.get(key)
            cells.append(format_us(row["p50_us"] if row else None))
            cells.append(format_us(row["p99_us"] if row else None))
            ref_p50_val = ref_row["p50_us"] if ref_row else None
            val_p50 = row["p50_us"] if row else None
            cells.append(latency_speedup_str(ref_p50_val, val_p50))

        md += "| " + " | ".join(cells) + " |\n"

    md += "\n"
    return md


def update_comparisons_md(transports: list[str]):
    rows = load_jsonl()
    if not rows:
        print("No JSONL data to update from", file=sys.stderr)
        return

    text = COMPARISONS_MD.read_text()

    for transport in transports:
        t_rows = [r for r in rows if r.get("transport") == transport]
        if not t_rows:
            continue

        latest = latest_by_key(t_rows, ["impl", "kind", "msg_size"])
        data = {(r["impl"], r["kind"], r["msg_size"]): r for r in latest.values()}

        available_impls = {r.get("impl") for r in t_rows}

        # libzmq comparison tables
        if "libzmq" not in available_impls:
            print(
                f"WARNING: no libzmq data for {transport}, tables will have gaps",
                file=sys.stderr,
            )

        if transport == "inproc":
            compio_impls = [
                ("omq-compio", "compio-mt"),
                ("omq-compio-st", "compio-st"),
            ]
        else:
            compio_impls = [("omq-compio", "omq-compio")]

        tput_compio = build_throughput_table(data, "libzmq", compio_impls)
        text = replace_block(text, f"libzmq_comparison_{transport}_compio", tput_compio)

        tput_tokio = build_throughput_table(
            data, "libzmq", [("omq-tokio", "omq-tokio")],
        )
        text = replace_block(text, f"libzmq_comparison_{transport}_tokio", tput_tokio)

        # zmq.rs comparison tables (TCP and IPC, not inproc/ws)
        if transport in ("tcp", "ipc"):
            if "zmq.rs" not in available_impls and transport in ("tcp", "ipc"):
                print(
                    f"WARNING: no zmq.rs data for {transport}, tables will have gaps",
                    file=sys.stderr,
                )

            zmqrs_compio = build_throughput_table(
                data, "zmq.rs", [("omq-compio", "omq-compio")],
            )
            text = replace_block(text, f"zmqrs_comparison_{transport}_compio", zmqrs_compio)

            zmqrs_tokio = build_throughput_table(
                data, "zmq.rs", [("omq-tokio", "omq-tokio")],
            )
            text = replace_block(text, f"zmqrs_comparison_{transport}_tokio", zmqrs_tokio)

        # latency tables
        lat_table = build_latency_table(
            data, "libzmq",
            [("omq-compio", "omq-compio"), ("omq-tokio", "omq-tokio")],
        )
        text = replace_block(text, f"libzmq_latency_{transport}", lat_table)

        if transport in ("tcp", "ipc"):
            zmqrs_lat = build_latency_table(
                data, "zmq.rs",
                [("omq-compio", "omq-compio"), ("omq-tokio", "omq-tokio")],
            )
            text = replace_block(text, f"zmqrs_latency_{transport}", zmqrs_lat)

    COMPARISONS_MD.write_text(text)
    print(f"Updated {COMPARISONS_MD}", file=sys.stderr)


# ── impl definitions ─────────────────────────────────────────────

IMPLS = {
    "omq-compio": {
        "crate": "omq-compio",
        "bin": "bench_peer_compio",
        "prefix": "c",
        "transports": ["tcp", "inproc", "ipc", "ws"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "inproc_pubsub_subcmd": "inproc-pubsub",
        "supports_pubsub": True,
    },
    "omq-compio-st": {
        "binary_from": "omq-compio",
        "prefix": "s",
        "transports": ["inproc"],
        "inproc_tput_subcmd": "inproc-st",
        "inproc_lat_subcmd": "inproc-st-latency",
    },
    "omq-tokio": {
        "crate": "omq-tokio",
        "bin": "bench_peer_tokio",
        "prefix": "t",
        "transports": ["tcp", "inproc", "ipc", "ws"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "inproc_pubsub_subcmd": "inproc-pubsub",
        "supports_pubsub": True,
    },
    "libzmq": {
        "prefix": "z",
        "transports": ["tcp", "inproc", "ipc", "ws"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "inproc_pubsub_subcmd": "inproc-pubsub",
        "supports_pubsub": True,
    },
    "zmq.rs": {
        "prefix": "q",
        "transports": ["tcp"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "supports_pubsub": True,
    },
    "rzmq": {
        "prefix": "r",
        "transports": ["tcp", "inproc", "ipc"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "supports_pubsub": True,
    },
    "omq-libzmq": {
        "prefix": "m",
        "transports": ["tcp", "inproc", "ipc"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "inproc_pubsub_subcmd": "inproc-pubsub",
        "supports_pubsub": True,
    },
}

PUBSUB_PEER_COUNTS = [1, 8, 64]


def build_peers(impl_names: set[str], ws_needed: bool):
    binaries = {}
    features = ["ws"] if ws_needed else []

    if "omq-compio" in impl_names or "omq-compio-st" in impl_names:
        print("==> building omq-compio bench_peer...", file=sys.stderr)
        cargo_build("omq-compio", "bench_peer_compio", features=features or None)
        compio_bin = str(ROOT / "target" / "release" / "bench_peer_compio")
        if "omq-compio" in impl_names:
            binaries["omq-compio"] = compio_bin
        if "omq-compio-st" in impl_names:
            binaries["omq-compio-st"] = compio_bin

    if "omq-tokio" in impl_names:
        print("==> building omq-tokio bench_peer...", file=sys.stderr)
        cargo_build("omq-tokio", "bench_peer_tokio", features=features or None)
        binaries["omq-tokio"] = str(ROOT / "target" / "release" / "bench_peer_tokio")

    if "libzmq" in impl_names:
        print("==> building libzmq bench_peer...", file=sys.stderr)
        src = ROOT / "scripts" / "libzmq_bench_peer.c"
        out = ROOT / "scripts" / "libzmq_bench_peer"
        gcc_build(src, out)
        binaries["libzmq"] = str(out)

    if "zmq.rs" in impl_names:
        print("==> building zmq.rs bench_peer...", file=sys.stderr)
        zmqrs_dir = ROOT / "scripts" / "zmqrs_bench_peer"
        subprocess.run(
            ["cargo", "build", "--release", "-q"],
            cwd=zmqrs_dir, check=True,
        )
        binaries["zmq.rs"] = str(zmqrs_dir / "target" / "release" / "zmqrs_bench_peer")

    if "rzmq" in impl_names:
        print("==> building rzmq bench_peer...", file=sys.stderr)
        rzmq_dir = ROOT / "scripts" / "rzmq_bench_peer"
        subprocess.run(
            ["cargo", "build", "--release", "-q"],
            cwd=rzmq_dir, check=True,
        )
        binaries["rzmq"] = str(rzmq_dir / "target" / "release" / "rzmq_bench_peer")

    if "omq-libzmq" in impl_names:
        print("==> building omq-libzmq bench_peer...", file=sys.stderr)
        subprocess.run(
            ["cargo", "build", "--release", "-p", "omq-libzmq", "-q"],
            cwd=ROOT, check=True,
        )
        src = ROOT / "scripts" / "libzmq_bench_peer.c"
        out = ROOT / "scripts" / "omq_libzmq_bench_peer"
        inc = ROOT / "omq-libzmq" / "include"
        lib_dir = ROOT / "target" / "release"
        subprocess.run(
            ["gcc", "-O2", "-o", str(out), str(src),
             f"-I{inc}", f"-L{lib_dir}", "-lomq_zmq", "-lpthread",
             f"-Wl,-rpath,{lib_dir}"],
            check=True,
        )
        binaries["omq-libzmq"] = str(out)

    return binaries


def run_benchmarks(
    binaries: dict[str, str],
    transports: list[str],
    sizes: list[int],
    run_latency: bool,
    run_pubsub: bool,
    pubsub_peers: list[int],
    base_port: int,
    run_id: str,
    duration: float = DEFAULT_DURATION,
    rounds: int = DEFAULT_ROUNDS,
    latency_iterations: int = LATENCY_ITERATIONS,
    latency_warmup: int = LATENCY_WARMUP,
    latency_timeout: int = LATENCY_TIMEOUT,
):
    _cleanup_ipc_sockets()
    atexit.register(_cleanup_ipc_sockets)
    for transport in transports:
        active = {
            name: path for name, path in binaries.items()
            if transport in IMPLS[name]["transports"]
        }
        if not active:
            continue

        # throughput
        print(f"\n── throughput: {transport} ──", file=sys.stderr)
        header = "".join(f"  {name:>22s}" for name in active)
        print(f"{'size':>10s}{header}", file=sys.stderr)

        for idx, size in enumerate(sizes):
            cells = {}
            for name, binary in active.items():
                impl_def = IMPLS[name]
                prefix = impl_def["prefix"]
                addr = addr_for(transport, prefix, idx, base_port)
                subcmd = impl_def.get("inproc_tput_subcmd", "inproc")
                result = run_throughput_cell(binary, transport, addr, size,
                                            inproc_subcmd=subcmd,
                                            duration=duration, rounds=rounds)
                cells[name] = result
                if result:
                    append_jsonl({
                        "run_id": run_id,
                        "impl": name,
                        "kind": "throughput",
                        "transport": transport,
                        "msg_size": size,
                        "msgs_s": round(result["msgs_s"], 1),
                        "mbps": round(result["mbps"], 1),
                    })

            line = f"{size_label(size):>10s}"
            for name in active:
                r = cells.get(name)
                if r:
                    line += f"  {r['msgs_s']:>9.0f} msg/s {r['mbps']:>6.1f} MB/s"
                else:
                    line += f"  {'—':>9s} msg/s {'—':>6s} MB/s"
            print(line, file=sys.stderr)

        # latency
        if run_latency:
            print(f"\n── latency: {transport} ──", file=sys.stderr)
            header = "".join(f"  {name:>24s}" for name in active)
            print(f"{'size':>10s}{header}", file=sys.stderr)

            for idx, size in enumerate(sizes):
                cells = {}
                for name, binary in active.items():
                    impl_def = IMPLS[name]
                    prefix = impl_def["prefix"]
                    addr = addr_for(transport, prefix, idx + len(sizes), base_port)
                    subcmd = impl_def.get("inproc_lat_subcmd", "inproc-latency")
                    result = run_latency_cell(binary, transport, addr, size,
                                             inproc_subcmd=subcmd,
                                             iterations=latency_iterations,
                                             warmup=latency_warmup,
                                             timeout=latency_timeout)
                    cells[name] = result
                    if result:
                        append_jsonl({
                            "run_id": run_id,
                            "impl": name,
                            "kind": "latency",
                            "transport": transport,
                            "msg_size": size,
                            "p50_us": round(result["p50_us"], 3),
                            "p99_us": round(result["p99_us"], 3),
                            "p999_us": round(result["p999_us"], 3),
                            "max_us": round(result["max_us"], 3),
                            "iterations": result["iterations"],
                        })

                line = f"{size_label(size):>10s}"
                for name in active:
                    r = cells.get(name)
                    if r:
                        line += f"    p50={r['p50_us']:>7.1f} µs  p99={r['p99_us']:>7.1f} µs"
                    else:
                        line += f"    {'—':>24s}"
                print(line, file=sys.stderr)

        # pub/sub throughput
        if not run_pubsub:
            continue
        pubsub_active = {
            name: path for name, path in active.items()
            if IMPLS[name].get("supports_pubsub")
        }
        if pubsub_active:
            for peers in pubsub_peers:
                print(f"\n── pub/sub {peers}p: {transport} ──", file=sys.stderr)
                header = "".join(f"  {name:>22s}" for name in pubsub_active)
                print(f"{'size':>10s}{header}", file=sys.stderr)

                for idx, size in enumerate(sizes):
                    cells = {}
                    for name, binary in pubsub_active.items():
                        impl_def = IMPLS[name]
                        prefix = impl_def["prefix"]
                        port_offset = 200 + peers * 50 + idx
                        addr = addr_for(transport, prefix, port_offset, base_port)
                        subcmd = impl_def.get("inproc_pubsub_subcmd",
                                              "inproc-pubsub")
                        result = run_pubsub_cell(
                            binary, transport, addr, size, peers,
                            inproc_subcmd=subcmd,
                            duration=duration, rounds=rounds,
                        )
                        cells[name] = result
                        if result:
                            append_jsonl({
                                "run_id": run_id,
                                "impl": name,
                                "kind": "pub_sub",
                                "transport": transport,
                                "peers": peers,
                                "msg_size": size,
                                "msgs_s": round(result["msgs_s"], 1),
                                "mbps": round(result["mbps"], 1),
                            })

                    line = f"{size_label(size):>10s}"
                    for name in pubsub_active:
                        r = cells.get(name)
                        if r:
                            line += (f"  {r['msgs_s']:>9.0f} msg/s"
                                     f" {r['mbps']:>6.1f} MB/s")
                        else:
                            line += f"  {'—':>9s} msg/s {'—':>6s} MB/s"
                    print(line, file=sys.stderr)

    print(file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Run comparison benchmarks")
    parser.add_argument(
        "--impl", action="append", dest="impls",
        choices=list(IMPLS.keys()),
        help="implementation(s) to benchmark (default: all)",
    )
    parser.add_argument(
        "--transport", action="append",
        choices=["tcp", "inproc", "ipc", "ws"],
        help="transport(s) to benchmark (default: tcp + inproc + ipc)",
    )
    parser.add_argument(
        "--quick-run", action="store_true",
        help=f"3 sizes, {QUICK_ROUNDS} round of {QUICK_DURATION}s (unless overridden)",
    )
    parser.add_argument(
        "--duration", type=float, default=None,
        help=f"seconds per throughput round (default: {DEFAULT_DURATION}, quick: {QUICK_DURATION})",
    )
    parser.add_argument(
        "--rounds", type=int, default=None,
        help=f"throughput rounds per cell, best-of-N (default: {DEFAULT_ROUNDS}, quick: {QUICK_ROUNDS})",
    )
    parser.add_argument(
        "--no-latency", action="store_true",
        help="skip REQ/REP latency benchmarks (on by default)",
    )
    parser.add_argument(
        "--no-pubsub", action="store_true",
        help="skip PUB/SUB throughput benchmarks",
    )
    parser.add_argument(
        "--pubsub-peers", type=str, default=None,
        help=f"comma-separated peer counts for PUB/SUB (default: {','.join(str(p) for p in PUBSUB_PEER_COUNTS)})",
    )
    parser.add_argument(
        "--latency-iterations", type=int, default=LATENCY_ITERATIONS,
        help=f"measured round-trips per latency cell (default: {LATENCY_ITERATIONS})",
    )
    parser.add_argument(
        "--latency-warmup", type=int, default=LATENCY_WARMUP,
        help=f"warmup round-trips before measuring (default: {LATENCY_WARMUP})",
    )
    parser.add_argument(
        "--latency-timeout", type=int, default=LATENCY_TIMEOUT,
        help=f"timeout in seconds for latency subprocess (default: {LATENCY_TIMEOUT})",
    )
    parser.add_argument(
        "--update-markdown", action="store_true",
        help="update COMPARISONS.md tables from JSONL",
    )
    parser.add_argument(
        "--base-port", type=int, default=0,
        help="base TCP port (default: random ephemeral)",
    )
    parser.add_argument(
        "--id", type=str, default=None,
        help="override run_id (default: ISO timestamp)",
    )
    args = parser.parse_args()

    transports = args.transport or ["tcp", "inproc", "ipc"]
    sizes = QUICK_SIZES if args.quick_run else FULL_SIZES
    if args.quick_run:
        duration = args.duration if args.duration is not None else QUICK_DURATION
        rounds = args.rounds if args.rounds is not None else QUICK_ROUNDS
    else:
        duration = args.duration if args.duration is not None else DEFAULT_DURATION
        rounds = args.rounds if args.rounds is not None else DEFAULT_ROUNDS
    run_id = args.id or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    run_latency = not args.no_latency
    run_pubsub = not args.no_pubsub
    pubsub_peers = (
        [int(x) for x in args.pubsub_peers.split(",")]
        if args.pubsub_peers else PUBSUB_PEER_COUNTS
    )
    ws_needed = "ws" in transports

    impl_names = set(args.impls) if args.impls else set(IMPLS.keys())

    binaries = build_peers(impl_names, ws_needed)

    versions = []
    if impl_names & {"omq-compio", "omq-compio-st", "omq-tokio"}:
        versions.append(f"omq {cargo_version('omq-compio')}")
    if "libzmq" in impl_names:
        versions.append(f"libzmq {libzmq_version()}")
    if "zmq.rs" in impl_names:
        versions.append(f"zmq.rs {cargo_version('zeromq', manifest=ROOT / 'scripts' / 'zmqrs_bench_peer' / 'Cargo.toml')}")
    if "rzmq" in impl_names:
        versions.append(f"rzmq {cargo_version('rzmq', manifest=ROOT / 'scripts' / 'rzmq_bench_peer' / 'Cargo.toml')}")
    if "omq-libzmq" in impl_names:
        versions.append(f"omq-libzmq {cargo_version('omq-libzmq')}")
    print(" vs ".join(versions), file=sys.stderr)

    base_port = args.base_port or random.randint(20_000, 40_000)
    run_benchmarks(binaries, transports, sizes, run_latency,
                   run_pubsub, pubsub_peers, base_port, run_id,
                   duration=duration, rounds=rounds,
                   latency_iterations=args.latency_iterations,
                   latency_warmup=args.latency_warmup,
                   latency_timeout=args.latency_timeout)

    if args.update_markdown:
        update_comparisons_md(transports)


if __name__ == "__main__":
    main()

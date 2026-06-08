#!/usr/bin/env python3
"""2-process mechanism benchmark for omq-tokio and omq-compio.

Spawns separate PUSH (bind) and PULL (connect) processes per cell,
each configured via OMQ_BENCH_MECHANISM env var. Results go to
~/.cache/omq/results_{backend}.jsonl.

Usage:
  scripts/bench_mechanism.py tokio                  # default 3 sizes
  scripts/bench_mechanism.py compio --chart-sizes    # all 16 chart sizes
  OMQ_BENCH_SIZES=128,2048 scripts/bench_mechanism.py tokio
"""

import json
import os
import selectors
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq"

DEFAULT_SIZES = [2_048, 8_192, 32_768]
CHART_SIZES = [
    8, 16, 32, 64, 128, 256, 512, 1_024, 2_048, 4_096,
    8_192, 16_384, 32_768, 65_536, 131_072, 262_144,
]
MECHANISMS = ["PLAIN", "CURVE", "BLAKE3ZMQ"]

DURATION = float(os.environ.get("OMQ_BENCH_DURATION", "2.0"))
ROUNDS = int(os.environ.get("OMQ_BENCH_ROUNDS", "3"))


def sizes() -> list[int]:
    if s := os.environ.get("OMQ_BENCH_SIZES"):
        return [int(x) for x in s.split(",") if x.strip()]
    if "--chart-sizes" in sys.argv:
        return CHART_SIZES
    return DEFAULT_SIZES


def fmt_size(b: int) -> str:
    if b >= 1024:
        return f"{b // 1024} KiB"
    return f"{b} B"


def cargo_build(backend: str):
    pkg = f"omq-{backend}"
    bin_name = f"bench_peer_{backend}"
    print(f"==> building {bin_name}...", file=sys.stderr)
    subprocess.run(
        ["cargo", "build", "--release", "-p", pkg,
         "--bin", bin_name,
         "--features", "plain,curve,blake3zmq", "-q"],
        cwd=ROOT, check=True,
    )


def spawn(binary: str, *args: str, mechanism: str = "null") -> subprocess.Popen:
    env = os.environ.copy()
    env["OMQ_BENCH_MECHANISM"] = mechanism
    return subprocess.Popen(
        [binary, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env=env,
        text=True,
    )


def read_bound_port(proc: subprocess.Popen, timeout: float = 5.0) -> int | None:
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


def kill(proc: subprocess.Popen):
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=5)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            proc.kill()
            proc.wait(timeout=2)
        except Exception:
            pass


def capture(binary: str, *args: str, mechanism: str = "null",
            timeout: int = 15) -> str:
    env = os.environ.copy()
    env["OMQ_BENCH_MECHANISM"] = mechanism
    proc = subprocess.Popen(
        [binary, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env=env,
        text=True,
    )
    try:
        stdout, _ = proc.communicate(timeout=timeout)
        return stdout
    except subprocess.TimeoutExpired:
        print(f"WARNING: timeout: {mechanism} {' '.join(args)}", file=sys.stderr)
        kill(proc)
        return ""


def run_cell(binary: str, mechanism: str, size: int) -> dict | None:
    best = None
    for _ in range(ROUNDS):
        result = run_once(binary, mechanism, size)
        if result and (best is None or result["msgs_s"] > best["msgs_s"]):
            best = result
    return best


def run_once(binary: str, mechanism: str, size: int) -> dict | None:
    push = spawn(binary, "push", "tcp://127.0.0.1:0", str(size),
                 mechanism=mechanism)
    port = read_bound_port(push)
    if port is None:
        kill(push)
        return None
    try:
        timeout_s = max(int(DURATION) + 10, 15)
        output = capture(binary, "pull", str(port), str(size), str(DURATION),
                         mechanism=mechanism, timeout=timeout_s)
    finally:
        kill(push)
    return parse_throughput(output, size)


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


def run_id() -> str:
    return os.environ.get(
        "OMQ_BENCH_RUN_ID",
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


def append_jsonl(jsonl_path: Path, rid: str, mechanism: str, size: int, result: dict):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    row = json.dumps({
        "run_id": rid,
        "pattern": "mechanism",
        "transport": mechanism,
        "peers": 1,
        "msg_size": size,
        "msg_count": int(result["msgs_s"] * DURATION),
        "elapsed": DURATION,
        "mbps": result["mbps"],
        "msgs_s": result["msgs_s"],
    })
    with open(jsonl_path, "a") as f:
        f.write(row + "\n")


def main():
    backends = [a for a in sys.argv[1:] if not a.startswith("-")]
    if not backends:
        print("usage: bench_mechanism.py <tokio|compio> [--chart-sizes]",
              file=sys.stderr)
        sys.exit(1)

    for backend in backends:
        if backend not in ("tokio", "compio"):
            print(f"unknown backend: {backend}", file=sys.stderr)
            sys.exit(1)

        jsonl_path = CACHE_DIR / f"results_{backend}.jsonl"
        cargo_build(backend)
        binary = str(ROOT / "target" / "release" / f"bench_peer_{backend}")
        sz = sizes()
        rid = run_id()

        print(f"mechanism bench (2-process, TCP, omq-{backend}) | {len(sz)} sizes | "
              f"rounds={ROUNDS} duration={DURATION}s", file=sys.stderr)
        print(file=sys.stderr)

        header = f"  {'size':>6}"
        for m in MECHANISMS:
            header += f" | {'msg/s':>10}  {'MB/s':>8}"
        print(header, file=sys.stderr)
        print(f"  {'-' * (len(header) - 2)}", file=sys.stderr)

        for size in sz:
            line = f"  {fmt_size(size):>6}"
            for mechanism in MECHANISMS:
                result = run_cell(binary, mechanism.lower(), size)
                if result:
                    append_jsonl(jsonl_path, rid, mechanism, size, result)
                    line += f" | {result['msgs_s']:>10,.0f}  {result['mbps']:>8.1f}"
                else:
                    line += f" | {'—':>10}  {'—':>8}"
            print(line, file=sys.stderr)

        print(file=sys.stderr)
        print(f"Results appended to {jsonl_path}", file=sys.stderr)


if __name__ == "__main__":
    main()

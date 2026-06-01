#!/usr/bin/env python3
"""Two-process compression benchmark for omq-tokio.

Runs push and pull in separate processes (separate tokio runtimes),
trains a dict for dict-benchmarks, writes JSONL for chart generation.

Usage:
    python3 scripts/bench_compression_tokio.py
    python3 scripts/bench_compression_tokio.py --chart-only
    python3 scripts/bench_compression_tokio.py --transports lz4+tcp --sizes 8192,131072
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PEER = REPO / "target" / "release" / "bench_peer_tokio"
CACHE_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq"
JSONL = CACHE_DIR / "results_compression_tokio.jsonl"
CHART_SCRIPT = REPO / "scripts" / "gen_compression_chart.py"

CHART_SIZES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096,
               8192, 16384, 32768, 65536, 131072, 262144]
DEFAULT_TRANSPORTS = ["tcp", "lz4+tcp", "zstd+tcp"]
DICT_TRANSPORTS = ["lz4+tcp", "zstd+tcp"]
DEFAULT_DICT_SIZES = [2048]


def build_peer():
    if PEER.exists():
        return
    print("Building bench_peer_tokio...", file=sys.stderr)
    subprocess.run(
        ["cargo", "build", "--release", "-p", "omq-tokio",
         "--bin", "bench_peer_tokio", "--features", "lz4,zstd"],
        cwd=REPO, check=True, capture_output=True,
    )


def run_peer(*args, env=None):
    merged = {**os.environ, **(env or {})}
    return subprocess.Popen(
        [str(PEER)] + list(args),
        env=merged, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )


def peer_output(*args, env=None):
    merged = {**os.environ, **(env or {})}
    r = subprocess.run(
        [str(PEER)] + list(args),
        env=merged, capture_output=True, text=True, check=True,
    )
    return r.stdout.strip()


def get_wire_size(transport, size, dict_file=None):
    port = 19999
    ep = f"{transport}://127.0.0.1:{port}"
    env = {"OMQ_BENCH_PAYLOAD": "json"}
    if dict_file:
        env["OMQ_BENCH_DICT_FILE"] = str(dict_file)
    return int(peer_output("wire-size", ep, str(size), env=env))


def train_dict(path, capacity=2048):
    peer_output("train-dict", str(path), str(capacity))


def run_cell(transport, size, port, duration, dict_file=None):
    ep = f"{transport}://127.0.0.1:{port}"
    env = {"OMQ_BENCH_PAYLOAD": "json"}
    if dict_file:
        env["OMQ_BENCH_DICT_FILE"] = str(dict_file)

    push = run_peer("push", ep, str(size), env=env)
    time.sleep(0.15)

    try:
        pull = run_peer("pull", ep, str(size), str(duration), env=env)
        stdout, _ = pull.communicate(timeout=duration + 10)
        output = stdout.decode().strip()
    except Exception:
        push.kill()
        push.wait()
        return None
    finally:
        push.send_signal(signal.SIGTERM)
        push.wait()

    if not output or pull.returncode != 0:
        return None

    parts = output.split()
    count, elapsed = int(parts[0]), float(parts[1])
    msgs_s = count / elapsed
    mbps = count * size / elapsed / 1_000_000
    return {"count": count, "elapsed": elapsed, "msgs_s": msgs_s, "mbps": mbps}


def run_sweep(transports, sizes, duration, run_id, dict_file=None,
              pattern="compression_json", dict_size=None):
    port = 17200
    rows = []

    for transport in transports:
        label = transport
        if dict_file:
            ds = dict_size or 2048
            label = f"{transport} (dict {ds}B)"
        print(f"--- {label} (1 peer, 2-process) ---")

        for size in sizes:
            port += 1
            wire_bytes = get_wire_size(transport, size, dict_file)
            cell = run_cell(transport, size, port, duration, dict_file)

            if cell is None:
                print(f"  ~{size:>6}B  FAILED")
                continue

            wire_mbps = cell["msgs_s"] * wire_bytes / 1_000_000
            print(
                f"  ~{size:>6}B  {cell['msgs_s']:>9.0f} msg/s"
                f"  {wire_mbps:>9.1f} wireMB/s"
                f"  {cell['mbps']:>9.1f} virtMB/s"
                f"  ({cell['elapsed']:.2f}s, n={cell['count']})"
            )

            row = {
                "run_id": run_id,
                "pattern": pattern,
                "transport": transport,
                "peers": 1,
                "msg_size": size,
                "wire_bytes": wire_bytes,
                "msg_count": cell["count"],
                "elapsed": cell["elapsed"],
                "cpu_time": 0,
                "mbps": cell["mbps"],
                "msgs_s": cell["msgs_s"],
            }
            if dict_size is not None:
                row["dict_size"] = dict_size
            rows.append(row)

        print()

    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--transports", default=",".join(DEFAULT_TRANSPORTS))
    parser.add_argument("--sizes", default=",".join(str(s) for s in CHART_SIZES))
    parser.add_argument("--duration", type=float, default=2.0)
    parser.add_argument("--dict-sizes", default=",".join(str(s) for s in DEFAULT_DICT_SIZES))
    parser.add_argument("--chart", action="store_true",
                        help="also regenerate the tokio compression chart")
    args = parser.parse_args()

    build_peer()

    transports = args.transports.split(",")
    sizes = [int(s) for s in args.sizes.split(",")]
    dict_sizes = [int(s) for s in args.dict_sizes.split(",")]
    run_id = f"ts-{int(time.time())}"

    print(f"--- 2-process compression bench (omq-tokio) ---")
    print(f"run_id: {run_id}")
    print()

    all_rows = []

    # Non-dict sweep
    all_rows.extend(
        run_sweep(transports, sizes, args.duration, run_id)
    )

    # Dict sweep
    for ds in dict_sizes:
        with tempfile.NamedTemporaryFile(suffix=".dict", delete=False) as f:
            dict_path = f.name

        try:
            train_dict(dict_path, ds)
            print(f"Trained {ds}B dict -> {dict_path}", file=sys.stderr)

            for transport in DICT_TRANSPORTS:
                if transport not in transports and transport.replace("+tcp", "") not in transports:
                    continue
                all_rows.extend(
                    run_sweep(
                        [transport], sizes, args.duration, run_id,
                        dict_file=dict_path,
                        pattern="compression_json_dict",
                        dict_size=ds,
                    )
                )
        finally:
            os.unlink(dict_path)

    # Write JSONL
    with open(JSONL, "a") as f:
        for row in all_rows:
            f.write(json.dumps(row) + "\n")
    print(f"Appended {len(all_rows)} rows to {JSONL}", file=sys.stderr)

    if args.chart:
        subprocess.run(
            [sys.executable, str(CHART_SCRIPT), "--backend", "tokio"],
            check=True,
        )


if __name__ == "__main__":
    main()

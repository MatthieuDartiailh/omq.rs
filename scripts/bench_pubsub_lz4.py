#!/usr/bin/env python3
"""PUB/SUB lz4+tcp fan-out benchmark.

Measures CPU-limited PUB/SUB throughput with 1 PUB -> 32 SUBs using
JSON payloads, captures sender CPU%, then projects throughput at
various link speeds.

Usage:
    scripts/bench_pubsub_lz4.py                        # full sweep
    scripts/bench_pubsub_lz4.py --quick                 # 3 sizes, 1 round
    scripts/bench_pubsub_lz4.py --chart                 # generate chart after
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
JSONL = CACHE_DIR / "results_pubsub_lz4.jsonl"

CHART_SIZES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096,
               8192, 16384, 32768, 65536, 131072, 262144]
QUICK_SIZES = [128, 1024, 8192]
DEFAULT_TRANSPORTS = ["tcp", "lz4+tcp"]
N_SUBS = 32
DEFAULT_DICT_SIZES = [2048]
DEFAULT_DURATION = 2.0
DEFAULT_ROUNDS = 3
QUICK_DURATION = 1.5
QUICK_ROUNDS = 1

_port_counter = 17500


def next_port():
    global _port_counter
    _port_counter += 1
    return _port_counter


def build_peer():
    print("Building bench_peer_tokio (--features lz4)...", file=sys.stderr)
    subprocess.run(
        ["cargo", "build", "--release", "-p", "omq-tokio",
         "--bin", "bench_peer_tokio", "--features", "lz4"],
        cwd=REPO, check=True,
    )


def run_peer(*args, env=None):
    merged = {**os.environ, **(env or {})}
    return subprocess.Popen(
        [str(PEER)] + list(args),
        env=merged, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    )


def kill_peer(proc):
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=3)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            proc.kill()
            proc.wait(timeout=2)
        except Exception:
            pass


def peer_output(*args, env=None):
    merged = {**os.environ, **(env or {})}
    r = subprocess.run(
        [str(PEER)] + list(args),
        env=merged, capture_output=True, text=True, check=True,
    )
    return r.stdout.strip()


def get_wire_size(transport, size, dict_file=None):
    ep = f"{transport}://127.0.0.1:19999"
    env = {"OMQ_BENCH_PAYLOAD": "json"}
    if dict_file:
        env["OMQ_BENCH_DICT_FILE"] = str(dict_file)
    return int(peer_output("wire-size", ep, str(size), env=env))


def train_dict(path, capacity=2048):
    peer_output("train-dict", str(path), str(capacity))


def read_proc_cpu(pid):
    try:
        fields = open(f"/proc/{pid}/stat").read().split()
        utime = int(fields[13])
        stime = int(fields[14])
        return (utime + stime) / os.sysconf("SC_CLK_TCK")
    except (OSError, IndexError):
        return 0.0


def run_cell(transport, size, peers, duration, dict_file=None):
    port = next_port()
    ep = f"{transport}://127.0.0.1:{port}"
    env = {"OMQ_BENCH_PAYLOAD": "json"}
    if dict_file:
        env["OMQ_BENCH_DICT_FILE"] = str(dict_file)

    pub = run_peer("pub", ep, str(size), env=env)
    time.sleep(0.25)

    # Drain SUBs use bare tcp:// for lz4+tcp transports: they receive
    # the compressed frames and discard without decompressing, simulating
    # subscribers on separate machines whose CPU doesn't compete with
    # the PUB.
    drain_ep = f"tcp://127.0.0.1:{port}" if "lz4" in transport else ep
    drain_dur = str(duration + 3)
    drains = []
    for _ in range(peers - 1):
        drains.append(run_peer("sub", drain_ep, str(size), drain_dur,
                               env=env))

    if peers > 1:
        time.sleep(0.15)

    try:
        measured = run_peer("sub", ep, str(size), str(duration), env=env)
        stdout, _ = measured.communicate(timeout=duration + 15)
        output = stdout.decode().strip()
    except Exception:
        output = ""

    pub_cpu = read_proc_cpu(pub.pid)

    kill_peer(pub)
    for d in drains:
        kill_peer(d)

    if not output:
        return None

    parts = output.split()
    if len(parts) < 2:
        return None
    count, elapsed = int(parts[0]), float(parts[1])
    if elapsed <= 0:
        return None
    msgs_s = count / elapsed
    mbps = count * size / elapsed / 1e6
    return {"count": count, "elapsed": elapsed, "msgs_s": msgs_s,
            "mbps": mbps * peers, "cpu_time": pub_cpu}


def main():
    parser = argparse.ArgumentParser(
        description="PUB/SUB lz4+tcp fan-out benchmark")
    parser.add_argument("--transports",
                        default=",".join(DEFAULT_TRANSPORTS))
    parser.add_argument("--sizes",
                        default=",".join(str(s) for s in CHART_SIZES))
    parser.add_argument("--duration", type=float, default=DEFAULT_DURATION)
    parser.add_argument("--rounds", type=int, default=DEFAULT_ROUNDS)
    parser.add_argument("--quick", action="store_true",
                        help="3 sizes, 1 round, shorter duration")
    parser.add_argument("--dict-sizes",
                        default=",".join(str(s) for s in DEFAULT_DICT_SIZES))
    parser.add_argument("--chart", action="store_true",
                        help="regenerate chart after bench")
    args = parser.parse_args()

    build_peer()

    transports = args.transports.split(",")
    sizes = [int(s) for s in args.sizes.split(",")]
    dict_sizes = [int(s) for s in args.dict_sizes.split(",")]

    if args.quick:
        sizes = QUICK_SIZES
        duration = QUICK_DURATION
        rounds = QUICK_ROUNDS
    else:
        duration = args.duration
        rounds = args.rounds

    run_id = f"ts-{int(time.time())}"
    print(f"--- PUB/SUB lz4 bench: 1 PUB -> {N_SUBS} SUBs"
          f" (omq-tokio, JSON) ---")
    print(f"run_id: {run_id}")
    print()

    all_rows = []

    for transport in transports:
        print(f"--- {transport}, {N_SUBS} subscribers ---")
        for size in sizes:
            best = None
            for _ in range(rounds):
                cell = run_cell(transport, size, N_SUBS, duration)
                if cell and (best is None
                             or cell["msgs_s"] > best["msgs_s"]):
                    best = cell
            if best is None:
                print(f"  ~{size:>6}B  FAILED")
                continue
            wire_bytes = get_wire_size(transport, size)
            agg_gbs = best["mbps"] / 1000
            cpu_pct = (best["cpu_time"] / best["elapsed"] * 100
                       if best["elapsed"] > 0 else 0)
            print(f"  ~{size:>6}B  {best['msgs_s']:>9.0f} msg/s"
                  f"  {agg_gbs:>7.2f} agg GB/s"
                  f"  cpu {cpu_pct:>5.1f}%"
                  f"  (wire {wire_bytes}B)")
            all_rows.append({
                "run_id": run_id,
                "pattern": "pubsub_lz4",
                "transport": transport,
                "peers": N_SUBS,
                "msg_size": size,
                "wire_bytes": wire_bytes,
                "msg_count": best["count"],
                "elapsed": best["elapsed"],
                "cpu_time": best["cpu_time"],
                "msgs_s": best["msgs_s"],
                "mbps": best["mbps"],
            })
        print()

    # Wire-size sweep for dict series (projected, not measured)
    for ds in dict_sizes:
        with tempfile.NamedTemporaryFile(suffix=".dict",
                                         delete=False) as f:
            dict_path = f.name
        try:
            train_dict(dict_path, ds)
            ds_label = f"{ds // 1024}K" if ds >= 1024 else f"{ds}B"
            print(f"--- wire sizes: lz4+tcp + {ds_label} dict ---")
            for size in sizes:
                wire_bytes = get_wire_size("lz4+tcp", size, dict_path)
                ratio = size / wire_bytes if wire_bytes else 0
                print(f"  ~{size:>6}B  wire {wire_bytes}B ({ratio:.1f}x)")
                all_rows.append({
                    "run_id": run_id,
                    "pattern": "pubsub_lz4_dict",
                    "transport": "lz4+tcp",
                    "peers": N_SUBS,
                    "msg_size": size,
                    "wire_bytes": wire_bytes,
                    "dict_size": ds,
                })
            print()
        finally:
            os.unlink(dict_path)

    JSONL.parent.mkdir(parents=True, exist_ok=True)
    with open(JSONL, "a") as f:
        for row in all_rows:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")
    print(f"Appended {len(all_rows)} rows to {JSONL}", file=sys.stderr)

    if args.chart:
        chart_script = REPO / "scripts" / "gen_pubsub_lz4_chart.py"
        subprocess.run([sys.executable, str(chart_script)], check=True)


if __name__ == "__main__":
    main()

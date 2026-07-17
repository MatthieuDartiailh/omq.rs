#!/usr/bin/env python3
"""Consolidated benchmark comparison runner.

Runs PUSH/PULL throughput and REQ/REP latency benchmarks across
implementations (omq-tokio, libzmq, zmq.rs, rzmq) and writes
results to ~/.cache/omq/comparisons.jsonl.

Usage:
  scripts/run_comparisons.py                        # all impls, tcp+inproc+ipc, latency on
  scripts/run_comparisons.py --quick-run            # 3 sizes only
  scripts/run_comparisons.py --impl rzmq            # single impl
  scripts/run_comparisons.py --impl omq-tokio --impl libzmq  # subset
  scripts/run_comparisons.py --transport tcp         # TCP only
  scripts/run_comparisons.py --no-latency           # skip REQ/REP latency
"""

import argparse
import atexit
import glob
import json
import os
import random
import selectors
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ── process lifetime guard ────────────────────────────────────────
# Hard rule: NO bench peer process ever outlives MAX_PROC_LIFETIME, and no
# process is ever orphaned. Every peer is spawned in its own session/process
# group (start_new_session=True) and registered here. A daemon "suicide"
# thread SIGKILLs the whole group of any peer older than the cap, and every
# exit path (normal, integrity abort via sys.exit, Ctrl-C, crash) reaps all
# registered groups. This is the backstop: per-cell timeouts kill peers in
# seconds, but if any timeout path is missed, the watchdog still guarantees
# death within the cap.
MAX_PROC_LIFETIME = 60.0
_LIVE_PROCS: dict[int, tuple] = {}
_PROCS_LOCK = threading.Lock()


def _register_proc(proc: subprocess.Popen) -> subprocess.Popen:
    with _PROCS_LOCK:
        _LIVE_PROCS[proc.pid] = (proc, time.monotonic())
    return proc


def _deregister_proc(proc: subprocess.Popen) -> None:
    with _PROCS_LOCK:
        _LIVE_PROCS.pop(proc.pid, None)


def _hard_kill(proc: subprocess.Popen) -> None:
    """SIGKILL the process's whole group, then reap. Never raises."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.kill()
        except Exception:
            pass
    try:
        proc.wait(timeout=2)
    except Exception:
        pass
    _deregister_proc(proc)


def _reap_all() -> None:
    with _PROCS_LOCK:
        procs = [p for (p, _) in _LIVE_PROCS.values()]
    for p in procs:
        _hard_kill(p)


def _watchdog() -> None:
    while True:
        time.sleep(1.0)
        now = time.monotonic()
        with _PROCS_LOCK:
            stale = [p for (p, t) in _LIVE_PROCS.values()
                     if now - t > MAX_PROC_LIFETIME]
        for p in stale:
            print(f"WATCHDOG: hard-killing bench pid {p.pid} "
                  f"(alive > {MAX_PROC_LIFETIME:.0f}s)", file=sys.stderr)
            _hard_kill(p)


def _install_reaper() -> None:
    atexit.register(_reap_all)

    def _on_signal(signum, _frame):
        _reap_all()
        signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)

    for s in (signal.SIGINT, signal.SIGTERM):
        signal.signal(s, _on_signal)
    threading.Thread(target=_watchdog, daemon=True).start()


def _cleanup_ipc_sockets():
    """Remove stale IPC socket files left by benchmark peers."""
    for p in glob.glob(str(ROOT / "@omq-bench-cmp-*")):
        try:
            os.unlink(p)
        except OSError:
            pass
CACHE_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq"
JSONL_PATH = CACHE_DIR / "comparisons.jsonl"
COMPARISON_CHART_SIZES = [16, 64, 256, 1024, 4096, 16384]
MAIN_EXTRA_CHART_SIZES = [32, 128, 512, 2048, 8192, 32768, 262144, 4194304]
QUICK_SIZES = [64, 1024, 4096]

# Physical sanity ceiling for a single TCP loopback stream (MB/s). Measured
# loopback peaks around 6-7 GB/s here; anything above this for ONE peer is a
# measurement glitch, not a result. Per-stream (not aggregate), so legit
# fan-out aggregate bandwidth (pub/sub counts bytes per subscriber) is safe.
SINGLE_STREAM_MBPS = 12000
DEFAULT_DURATION = float(os.environ.get("OMQ_BENCH_DURATION", "3.0"))
QUICK_DURATION = 1.5
DEFAULT_ROUNDS = int(os.environ.get("OMQ_BENCH_ROUNDS", "2"))
QUICK_ROUNDS = 1
PEER_WARMUP_SECS = 0.5
LATENCY_ITERATIONS = 5_000
LATENCY_WARMUP = 500
LATENCY_TIMEOUT = 15
RUN_ID_TS_LEN = len("YYYY-MM-DDTHH:MM:SS")


# ── formatting ────────────────────────────────────────────────────

def size_label(n: int) -> str:
    if n >= 1024 * 1024:
        return f"{n // (1024 * 1024)} MiB"
    if n >= 1024:
        return f"{n // 1024} KiB"
    return f"{n} B"


def make_run_id(name: str | None) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    if not name:
        return ts
    if len(name) >= RUN_ID_TS_LEN:
        try:
            datetime.strptime(name[:RUN_ID_TS_LEN], "%Y-%m-%dT%H:%M:%S")
            return name
        except ValueError:
            pass
    return f"{ts}-{name}"


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

MEASURED_CPU = "0,1,2,3"
OTHER_CPU = "3,4,5"
SINK_IO_THREADS = "3"


def spawn_process(binary: str, *args: str, env: dict | None = None,
                  cpu: str | None = None) -> subprocess.Popen:
    merged = {**os.environ, **(env or {})} if env else None
    cmd = [binary, *args]
    if cpu is not None:
        cmd = ["taskset", "-c", cpu] + cmd
    return _register_proc(subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        env=merged,
        start_new_session=True,
    ))


def read_bound_port(proc: subprocess.Popen, timeout: float = 5.0) -> int | None:
    """Read 'PORT <n>' from the process's first stdout line."""
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


def capture_with_cpu(binary: str, *args: str, timeout: int = 15,
                     env: dict | None = None,
                     cpu: str | None = None) -> tuple[str, float]:
    """Run a single-process bench and return (stdout, cpu_seconds)."""
    merged = {**os.environ, **(env or {})} if env else None
    cmd = [binary, *args]
    if cpu is not None:
        cmd = ["taskset", "-c", cpu] + cmd
    proc = _register_proc(subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=False,
        env=merged,
        start_new_session=True,
    ))
    sel = selectors.DefaultSelector()
    sel.register(proc.stdout, selectors.EVENT_READ)
    chunks = []
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            print(f"WARNING: timeout: {binary} {' '.join(args)}", file=sys.stderr)
            _hard_kill(proc)
            sel.close()
            return "", 0.0
        ready = sel.select(timeout=remaining)
        if ready:
            data = os.read(proc.stdout.fileno(), 65536)
            if data:
                chunks.append(data)
            else:
                break
    sel.close()
    cpu = read_proc_cpu(proc.pid)
    proc.wait()
    _deregister_proc(proc)
    return b"".join(chunks).decode("utf-8", errors="replace"), cpu


def capture_process(binary: str, *args: str, timeout: int = 15,
                    env: dict | None = None,
                    cpu: str | None = None) -> str:
    merged = {**os.environ, **(env or {})} if env else None
    cmd = [binary, *args]
    if cpu is not None:
        cmd = ["taskset", "-c", cpu] + cmd
    proc = _register_proc(subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        env=merged,
        start_new_session=True,
    ))
    try:
        stdout, _ = proc.communicate(timeout=timeout)
        _deregister_proc(proc)
        return stdout
    except subprocess.TimeoutExpired:
        print(f"WARNING: timeout: {binary} {' '.join(args)}", file=sys.stderr)
        _hard_kill(proc)
        return ""


def cleanup_ipc_socket(addr: str):
    if addr.startswith("ipc://") and not addr.startswith("ipc://@"):
        path = addr[len("ipc://"):]
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


def kill_process(proc: subprocess.Popen):
    # Signal the whole process group so any grandchildren die too, then
    # escalate to SIGKILL and always deregister so nothing is orphaned.
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
    except (ProcessLookupError, OSError):
        pass
    except subprocess.TimeoutExpired:
        _hard_kill(proc)
        return
    _deregister_proc(proc)


# ── measurement parsing ──────────────────────────────────────────

def read_proc_cpu(pid: int) -> float:
    """Read user+sys CPU time in seconds from /proc/[pid]/stat."""
    try:
        fields = open(f"/proc/{pid}/stat").read().split()
        utime = int(fields[13])
        stime = int(fields[14])
        return (utime + stime) / os.sysconf("SC_CLK_TCK")
    except (OSError, IndexError):
        return 0.0


# ── measurement integrity ────────────────────────────────────────
# A benchmark that silently records an undercounted number is worse than one
# that fails: it produces a plausible-looking chart that lies. Every CPU
# component we fold into `cpu_time` must actually be present. When a peer
# fails to report one (e.g. a receiver that never prints its CPU field), we
# record the gap here, warn on the spot, and abort the run at the end rather
# than chart a quietly-wrong CPU line.
MEASUREMENT_ISSUES: list[str] = []


def _note(issues: list, present: bool, impl: str, kind: str, transport: str,
          size: int, peers: int, what: str) -> bool:
    """Record a missing measurement component on this round's `issues` list.

    `what` names the missing piece (e.g. "pull CPU (peer stdout)"). Returns
    `present` so callers can gate `cpu_time` on completeness and avoid
    recording a known-undercounted number. Issues are per-round; only the
    best-of-N result that gets recorded is promoted to the run log (see
    `_flush_issues`), so a transient bad round that loses selection is not
    treated as a measurement failure."""
    if not present:
        issues.append(
            f"{impl} {kind}/{transport} size={size} peers={peers}: missing {what}")
    return present


def _flush_issues(result: dict | None) -> None:
    """Promote the recorded (best-of-N) result's issues to the run-level log."""
    if not result:
        return
    for msg in result.get("_issues", ()):
        print(f"  !! INCOMPLETE MEASUREMENT: {msg}", file=sys.stderr)
        MEASUREMENT_ISSUES.append(msg)


def parse_throughput(output: str, size: int) -> dict | None:
    parts = output.strip().split()
    if len(parts) < 2:
        return None
    count = float(parts[0])
    elapsed = float(parts[1])
    if count <= 0 or elapsed <= 0:
        return None
    msgs_s = count / elapsed
    mbps = (count * size) / elapsed / 1e6
    result = {"msgs_s": msgs_s, "mbps": mbps, "elapsed": elapsed}
    if len(parts) >= 4:
        try:
            result["pull_cpu"] = float(parts[3])
        except ValueError:
            pass
    return result


def parse_multi_throughput(output: str, size: int, peers: int) -> dict | None:
    """Parse output from multi-pull / multi-sub.

    Format: total_count elapsed size cpu_secs socket_count per_min_rate per_max_rate
    """
    parts = output.strip().split()
    if len(parts) < 4:
        return None
    count = float(parts[0])
    elapsed = float(parts[1])
    if count <= 0 or elapsed <= 0:
        return None
    cpu = float(parts[3])
    per_peer_msgs = count / elapsed / peers
    result = {
        "msgs_s": per_peer_msgs,
        "mbps": count * size / elapsed / 1e6,
        "elapsed": elapsed,
        "peers_measured": peers,
    }
    if cpu > 0:
        result["pull_cpu"] = cpu
    if len(parts) >= 7:
        result["peer_min"] = float(parts[5])
        result["peer_max"] = float(parts[6])
    return result


def zero_tput_result(duration: float) -> dict:
    return {
        "msgs_s": 0.0,
        "mbps": 0.0,
        "elapsed": duration,
        "zero_transport": True,
        "_issues": [],
    }


def parse_latency(output: str) -> dict | None:
    parts = output.strip().split()
    if len(parts) < 5:
        return None
    result = {
        "p50_us": float(parts[0]),
        "p99_us": float(parts[1]),
        "p999_us": float(parts[2]),
        "max_us": float(parts[3]),
        "iterations": int(parts[4]),
    }
    if len(parts) >= 6:
        try:
            result["req_cpu"] = float(parts[5])
        except ValueError:
            pass
    if len(parts) >= 7:
        try:
            result["elapsed"] = float(parts[6])
        except ValueError:
            pass
    return result


# ── benchmark cells ──────────────────────────────────────────────

def run_throughput_cell(
    binary: str, transport: str, addr: str, size: int,
    inproc_subcmd: str = "inproc",
    duration: float = DEFAULT_DURATION,
    rounds: int = DEFAULT_ROUNDS,
    env: dict | None = None,
    impl: str = "?",
) -> dict | None:
    best = None
    for _ in range(rounds):
        result = _run_throughput_once(binary, transport, addr, size,
                                      inproc_subcmd, duration, env=env, impl=impl)
        if result and (best is None or result["msgs_s"] > best["msgs_s"]):
            best = result
    _flush_issues(best)
    return best


def _fresh_addr(addr: str) -> str:
    """Return a unique variant of an IPC address to avoid kernel cleanup races."""
    if addr.startswith("ipc://"):
        return f"{addr}-{next_addr_id()}"
    return addr


def _run_throughput_once(
    binary: str, transport: str, addr: str, size: int,
    inproc_subcmd: str, duration: float, env: dict | None = None,
    impl: str = "?",
) -> dict | None:
    dur = str(duration)
    issues: list = []
    cell_env = {**(env or {}), "OMQ_BENCH_START_AT": f"{time.time() + 2.0:.6f}"}
    recv_env = {**cell_env, "OMQ_IO_THREADS": "1"}
    if transport == "inproc":
        fresh_name = f"{addr}-{next_addr_id()}"
        timeout_s = max(int(duration) + 5, 8)
        output, cpu = capture_with_cpu(binary, inproc_subcmd, fresh_name,
                                       str(size), dur,
                                       timeout=timeout_s, env=cell_env)
        result = parse_throughput(output, size)
        if result:
            if _note(issues, cpu > 0, impl, "throughput", transport, size, 1,
                     "process CPU (/proc)"):
                result["cpu_time"] = cpu
            result["_issues"] = issues
        return result

    addr = _fresh_addr(addr)
    cleanup_ipc_socket(addr)
    push = spawn_process(binary, "push", addr, str(size), env=cell_env,
                         cpu=MEASURED_CPU)
    if transport in ("ipc", "ws"):
        time.sleep(0.2)
        connect_addr = addr
    else:
        port = read_bound_port(push)
        if port is None:
            kill_process(push)
            return None
        connect_addr = str(port)
    try:
        output = capture_process(binary, "pull", connect_addr, str(size), dur,
                                 env=recv_env, cpu=OTHER_CPU)
        push_cpu = read_proc_cpu(push.pid)
    finally:
        _hard_kill(push)
        cleanup_ipc_socket(addr)
    result = parse_throughput(output, size)
    if result:
        push_ok = _note(issues, push_cpu > 0, impl, "throughput", transport,
                        size, 1, "push CPU (/proc)")
        pull_ok = _note(issues, "pull_cpu" in result, impl, "throughput",
                        transport, size, 1, "pull CPU (peer stdout)")
        if push_ok and pull_ok:
            result["cpu_time"] = push_cpu + result["pull_cpu"]
        if push_ok:
            result["push_cpu_time"] = push_cpu
        result["_issues"] = issues
    return result


def run_pubsub_cell(
    binary: str, transport: str, addr: str, size: int, peers: int,
    inproc_subcmd: str = "inproc-pubsub",
    pub_needs_peers: bool = False,
    duration: float = DEFAULT_DURATION,
    rounds: int = DEFAULT_ROUNDS,
    env: dict | None = None,
    impl: str = "?",
) -> dict | None:
    best = None
    for _ in range(rounds):
        result = _run_pubsub_once(binary, transport, addr, size, peers,
                                  inproc_subcmd, pub_needs_peers, duration,
                                  env=env, impl=impl)
        if result and (best is None or result["msgs_s"] > best["msgs_s"]):
            best = result
    _flush_issues(best)
    return best


def _run_pubsub_once(
    binary: str, transport: str, addr: str, size: int, peers: int,
    inproc_subcmd: str, pub_needs_peers: bool, duration: float,
    env: dict | None = None, impl: str = "?",
) -> dict | None:
    dur = str(duration)
    issues: list = []
    cell_env = {**(env or {}), "OMQ_BENCH_START_AT": f"{time.time() + 2.0:.6f}"}
    if transport == "inproc":
        fresh_name = f"{addr}-{next_addr_id()}"
        timeout_s = max(int(duration) + 5, 8)
        output, cpu = capture_with_cpu(binary, inproc_subcmd, fresh_name,
                                       str(size), dur, str(peers),
                                       timeout=timeout_s, env=cell_env)
        result = parse_throughput(output, size)
        if result:
            if _note(issues, cpu > 0, impl, "pubsub", transport, size, peers,
                     "process CPU (/proc)"):
                result["cpu_time"] = cpu
    else:
        addr = _fresh_addr(addr)
        cleanup_ipc_socket(addr)
        recv_env = {**cell_env,
                    "OMQ_IO_THREADS": SINK_IO_THREADS,
                    "ZMQ_IO_THREADS": SINK_IO_THREADS}
        pub_args = [binary, "pub", addr, str(size)]
        if pub_needs_peers:
            pub_args.append(str(peers))
        pub_ = spawn_process(*pub_args, env=cell_env, cpu=MEASURED_CPU)
        if transport in ("ipc", "ws"):
            time.sleep(0.2)
            connect_addr = addr
        else:
            port = read_bound_port(pub_)
            if port is None:
                kill_process(pub_)
                return None
            connect_addr = str(port)
        try:
            timeout_s = max(int(duration) + 8, 10)
            output, _ = capture_with_cpu(
                binary, "multi-sub", connect_addr, str(size), dur,
                str(peers), timeout=timeout_s, env=recv_env, cpu=OTHER_CPU)
            pub_cpu = read_proc_cpu(pub_.pid)
        finally:
            _hard_kill(pub_)
            cleanup_ipc_socket(addr)
        result = parse_multi_throughput(output, size, peers)
        if not result:
            result = zero_tput_result(duration)
        else:
            pub_ok = _note(issues, pub_cpu > 0, impl, "pubsub", transport, size,
                           peers, "pub CPU (/proc)")
            sub_ok = _note(issues, "pull_cpu" in result, impl, "pubsub",
                           transport, size, peers, "subscriber CPU (multi-sub stdout)")
            if pub_ok and sub_ok:
                result["cpu_time"] = pub_cpu + result["pull_cpu"]
            if pub_ok:
                result["pub_cpu_time"] = pub_cpu
    if result:
        result["_issues"] = issues
    return result


def run_fanout_cell(
    binary: str, transport: str, addr: str, size: int, peers: int,
    duration: float = DEFAULT_DURATION,
    rounds: int = DEFAULT_ROUNDS,
    fanout_subcmd: str = "push",
    fanout_needs_peers: bool = False,
    env: dict | None = None,
    impl: str = "?",
) -> dict | None:
    best = None
    for _ in range(max(1, rounds)):
        result = _run_fanout_once(binary, transport, addr, size, peers, duration,
                                  fanout_subcmd, fanout_needs_peers, env=env, impl=impl)
        if result and (best is None or result["msgs_s"] > best["msgs_s"]):
            best = result
    _flush_issues(best)
    return best


def _run_fanout_once(
    binary: str, transport: str, addr: str, size: int, peers: int,
    duration: float,
    fanout_subcmd: str = "push",
    fanout_needs_peers: bool = False,
    env: dict | None = None,
    impl: str = "?",
) -> dict | None:
    addr = _fresh_addr(addr)
    cleanup_ipc_socket(addr)
    issues: list = []
    cell_env = {**(env or {}), "OMQ_BENCH_START_AT": f"{time.time() + 2.0:.6f}"}
    recv_env = {**cell_env,
                "OMQ_IO_THREADS": SINK_IO_THREADS,
                "ZMQ_IO_THREADS": SINK_IO_THREADS}
    push_args = [binary, fanout_subcmd, addr, str(size)]
    if fanout_needs_peers:
        push_args.append(str(peers))
    push = spawn_process(*push_args, env=cell_env, cpu=MEASURED_CPU)
    if transport in ("ipc", "ws"):
        time.sleep(0.2)
        connect_addr = addr
    else:
        port = read_bound_port(push)
        if port is None:
            print(f"   !! {impl} fan_out size={size} peers={peers}: "
                  "pusher did not report a bound port", file=sys.stderr)
            kill_process(push)
            return None
        connect_addr = str(port)
    try:
        timeout_s = max(int(duration) + 8, 10)
        output, _ = capture_with_cpu(
            binary, "multi-pull", connect_addr, str(size), str(duration),
            str(peers), timeout=timeout_s, env=recv_env, cpu=OTHER_CPU)
        push_cpu = read_proc_cpu(push.pid)
    finally:
        _hard_kill(push)
        cleanup_ipc_socket(addr)
    result = parse_multi_throughput(output, size, peers)
    if not result:
        return zero_tput_result(duration)
    push_ok = _note(issues, push_cpu > 0, impl, "fan_out", transport, size, peers,
                    "push CPU (/proc)")
    if push_ok:
        result["cpu_time"] = push_cpu
        result["push_cpu_time"] = push_cpu
    if "pull_cpu" in result:
        result["pull_cpu_time"] = result["pull_cpu"]
    result["_issues"] = issues
    return result


def run_fanin_cell(
    binary: str, transport: str, addr: str, size: int, peers: int,
    duration: float = DEFAULT_DURATION,
    rounds: int = DEFAULT_ROUNDS,
    pull_subcmd: str = "pull-bind",
    pull_needs_peers: bool = False,
    env: dict | None = None,
    impl: str = "?",
) -> dict | None:
    best = None
    for _ in range(max(1, rounds)):
        result = _run_fanin_once(binary, transport, addr, size, peers, duration,
                                 pull_subcmd, pull_needs_peers, env=env, impl=impl)
        if result and (best is None or result["msgs_s"] > best["msgs_s"]):
            best = result
    _flush_issues(best)
    return best


def _run_fanin_once(
    binary: str, transport: str, addr: str, size: int, peers: int,
    duration: float,
    pull_subcmd: str = "pull-bind",
    pull_needs_peers: bool = False,
    env: dict | None = None,
    impl: str = "?",
) -> dict | None:
    addr = _fresh_addr(addr)
    cleanup_ipc_socket(addr)
    dur = str(duration)
    issues: list = []
    cell_env = {**(env or {}), "OMQ_BENCH_START_AT": f"{time.time() + 2.0:.6f}"}
    pull_args = [binary, pull_subcmd, addr, str(size), dur]
    if pull_needs_peers:
        pull_args.append(str(peers))
    pull = spawn_process(*pull_args, env=cell_env, cpu=MEASURED_CPU)
    if transport in ("ipc", "ws"):
        time.sleep(0.2)
        connect_addr = addr
    else:
        port = read_bound_port(pull)
        if port is None:
            kill_process(pull)
            return None
        connect_addr = str(port)
    multi_push = spawn_process(binary, "multi-push", connect_addr,
                               str(size), str(peers), env=cell_env,
                               cpu=OTHER_CPU)
    try:
        stdout, _ = pull.communicate(timeout=max(int(duration) + 10, 15))
        _deregister_proc(pull)
        push_cpu = read_proc_cpu(multi_push.pid)
    except subprocess.TimeoutExpired:
        _hard_kill(pull)
        stdout = ""
        push_cpu = 0.0
    finally:
        _hard_kill(multi_push)
        cleanup_ipc_socket(addr)
    result = parse_throughput(stdout, size)
    if result:
        push_ok = _note(issues, push_cpu > 0, impl, "fan_in", transport,
                        size, peers, "pusher CPU (/proc multi-push)")
        pull_ok = _note(issues, "pull_cpu" in result, impl, "fan_in", transport,
                        size, peers, "collector CPU (peer stdout)")
        if push_ok and pull_ok:
            result["cpu_time"] = push_cpu + result["pull_cpu"]
        if pull_ok:
            result["pull_cpu_time"] = result["pull_cpu"]
        result["_issues"] = issues
    return result


def run_latency_cell(
    binary: str, transport: str, addr: str, size: int,
    inproc_subcmd: str = "inproc-latency",
    iterations: int = LATENCY_ITERATIONS,
    warmup: int = LATENCY_WARMUP,
    timeout: int = LATENCY_TIMEOUT,
    env: dict | None = None,
    impl: str = "?",
) -> dict | None:
    issues: list = []
    if transport == "inproc":
        fresh_name = f"{addr}-{next_addr_id()}"
        output, cpu = capture_with_cpu(
            binary, inproc_subcmd, fresh_name, str(size),
            str(iterations), str(warmup),
            timeout=timeout, env=env,
        )
        result = parse_latency(output)
        if result:
            if _note(issues, cpu > 0, impl, "latency", transport, size, 1,
                     "process CPU (/proc)"):
                result["cpu_time"] = cpu
            result["_issues"] = issues
            _flush_issues(result)
        return result

    addr = _fresh_addr(addr)
    cleanup_ipc_socket(addr)
    rep = spawn_process(binary, "rep", addr, str(size), env=env,
                        cpu=OTHER_CPU)
    if transport in ("ipc", "ws"):
        time.sleep(0.2)
        connect_addr = addr
    else:
        port = read_bound_port(rep)
        if port is None:
            kill_process(rep)
            return None
        connect_addr = str(port)
    try:
        output = capture_process(
            binary, "req", connect_addr, str(size),
            str(iterations), str(warmup),
            timeout=timeout, env=env, cpu=MEASURED_CPU,
        )
        rep_cpu = read_proc_cpu(rep.pid)
    finally:
        _hard_kill(rep)
        cleanup_ipc_socket(addr)
    result = parse_latency(output)
    if result:
        rep_ok = _note(issues, rep_cpu > 0, impl, "latency", transport, size, 1,
                       "rep CPU (/proc)")
        req_ok = _note(issues, "req_cpu" in result, impl, "latency", transport,
                       size, 1, "req CPU (peer stdout)")
        if rep_ok and req_ok:
            result["cpu_time"] = rep_cpu + result["req_cpu"]
        if req_ok:
            result["req_cpu_time"] = result["req_cpu"]
        result["_issues"] = issues
        _flush_issues(result)
    return result


# ── address generation ────────────────────────────────────────────

_addr_counter = 0

def next_addr_id() -> int:
    global _addr_counter
    _addr_counter += 1
    return _addr_counter

def addr_for(transport: str, prefix: str, idx: int, base_port: int,
             *, impl_name: str = "") -> str:
    uid = next_addr_id()
    if transport == "tcp":
        return "0"
    if transport == "ws":
        offsets = {"c": 500, "t": 600, "z": 700, "q": 800, "s": 900, "r": 1100, "m": 1300}
        return f"ws://127.0.0.1:{base_port + offsets.get(prefix, 500) + idx}/"
    if transport == "ipc":
        if impl_name in ("zmq.rs", "rzmq", "rust-zmq"):
            return f"ipc:///tmp/omq-bench-cmp-{prefix}-{uid}"
        return f"ipc://@omq-bench-cmp-{prefix}-{uid}"
    if transport == "inproc":
        return f"bench-cmp-{prefix}-{uid}"
    return "0"


# ── JSONL I/O ─────────────────────────────────────────────────────

def append_jsonl(row: dict):
    JSONL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(JSONL_PATH, "a") as f:
        f.write(json.dumps(row, separators=(",", ":")) + "\n")
        f.flush()
        os.fsync(f.fileno())


def append_zero_tput_row(
    run_id: str,
    impl: str,
    kind: str,
    transport: str,
    size: int,
    peers: int | None = None,
):
    row = {
        "run_id": run_id,
        "impl": impl,
        "kind": kind,
        "transport": transport,
        "msg_size": size,
        "msgs_s": 0.0,
        "mbps": 0.0,
        "zero_transport": True,
    }
    if peers is not None:
        row["peers"] = peers
    append_jsonl(row)


# ── impl definitions ─────────────────────────────────────────────

IMPLS = {
    "omq-tokio": {
        "crate": "omq-tokio",
        "bin": "bench_peer_tokio",
        "prefix": "t",
        "class": "classic",
        "transports": ["tcp", "inproc", "ipc", "ws"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "inproc_pubsub_subcmd": "inproc-pubsub",
        "pub_needs_peer_count": True,
        "fanout_subcmd": "pub-fanout",
        "fanio_needs_peer_count": True,
        "supports_pubsub": True,
    },
    "omq-tokio-2t": {
        "binary_from": "omq-tokio",
        "prefix": "u",
        "class": "classic",
        "transports": ["tcp", "inproc", "ipc", "ws"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "inproc_pubsub_subcmd": "inproc-pubsub",
        "pub_needs_peer_count": True,
        "fanout_subcmd": "push",
        "fanio_needs_peer_count": False,
        "supports_pubsub": True,
        "env": {"OMQ_IO_THREADS": "2"},
    },
    "libzmq": {
        "prefix": "z",
        "class": "classic",
        "transports": ["tcp", "inproc", "ipc", "ws"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "inproc_pubsub_subcmd": "inproc-pubsub",
        "supports_pubsub": True,
    },
    "libzmq-2t": {
        "binary_from": "libzmq",
        "prefix": "Y",
        "class": "classic",
        "transports": ["tcp", "ipc", "ws"],
        "supports_pubsub": True,
        "env": {"ZMQ_IO_THREADS": "2"},
    },
    "libzmq-mt": {
        "binary_from": "libzmq",
        "prefix": "Z",
        "class": "classic",
        "transports": ["tcp", "ipc", "ws"],
        "supports_pubsub": True,
        "env": {"ZMQ_IO_THREADS": "4"},
    },
    "zmq.rs": {
        "prefix": "q",
        "class": "classic",
        "transports": ["tcp", "ipc"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "supports_pubsub": True,
    },
    "rzmq": {
        "prefix": "r",
        "class": "classic",
        "transports": ["tcp", "inproc", "ipc"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "supports_pubsub": True,
    },
    "rzmq-iouring": {
        "binary_from": "rzmq",
        "prefix": "R",
        "class": "iouring",
        "transports": ["tcp", "inproc", "ipc"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "supports_pubsub": True,
        "env": {"RZMQ_IO_URING": "1"},
    },
    "omq-tokio-1t": {
        "binary_from": "omq-tokio",
        "prefix": "s1",
        "transports": ["tcp", "ipc"],
        "pub_needs_peer_count": True,
        "fanout_subcmd": "pub-fanout",
        "fanio_needs_peer_count": True,
        "supports_pubsub": True,
        "env": {"OMQ_IO_THREADS": "1"},
    },
    "omq-tokio-4t": {
        "binary_from": "omq-tokio",
        "prefix": "s4",
        "transports": ["tcp", "ipc"],
        "pub_needs_peer_count": True,
        "fanout_subcmd": "pub-fanout",
        "fanio_needs_peer_count": True,
        "supports_pubsub": True,
        "env": {"OMQ_IO_THREADS": "4"},
    },
    "libzmq-curve-1t": {
        "binary_from": "libzmq",
        "prefix": "lc1",
        "class": "curve",
        "transports": ["tcp"],
        "supports_pubsub": True,
        "env": {"ZMQ_IO_THREADS": "1", "ZMQ_BENCH_CURVE": "1"},
    },
    "libzmq-curve-2t": {
        "binary_from": "libzmq",
        "prefix": "lc2",
        "class": "curve",
        "transports": ["tcp"],
        "supports_pubsub": True,
        "env": {"ZMQ_IO_THREADS": "2", "ZMQ_BENCH_CURVE": "1"},
    },
    "libzmq-curve-4t": {
        "binary_from": "libzmq",
        "prefix": "lc4",
        "class": "curve",
        "transports": ["tcp"],
        "supports_pubsub": True,
        "env": {"ZMQ_IO_THREADS": "4", "ZMQ_BENCH_CURVE": "1"},
    },
    "omq-curve-1t": {
        "binary_from": "omq-tokio",
        "prefix": "oc1",
        "class": "curve",
        "transports": ["tcp"],
        "pub_needs_peer_count": True,
        "supports_pubsub": True,
        "env": {"OMQ_IO_THREADS": "1", "OMQ_BENCH_MECHANISM": "curve"},
    },
    "omq-curve-2t": {
        "binary_from": "omq-tokio",
        "prefix": "oc2",
        "class": "curve",
        "transports": ["tcp"],
        "pub_needs_peer_count": True,
        "supports_pubsub": True,
        "env": {"OMQ_IO_THREADS": "2", "OMQ_BENCH_MECHANISM": "curve"},
    },
    "omq-curve-4t": {
        "binary_from": "omq-tokio",
        "prefix": "oc4",
        "class": "curve",
        "transports": ["tcp"],
        "pub_needs_peer_count": True,
        "supports_pubsub": True,
        "env": {"OMQ_IO_THREADS": "4", "OMQ_BENCH_MECHANISM": "curve"},
    },
}

PUBSUB_PEER_COUNTS = [4, 64]
FANOUT_PEER_COUNTS = [4, 64]
FANIN_PEER_COUNTS = [4, 64]


def build_peers(impl_names: set[str], ws_needed: bool):
    binaries = {}
    features = ["ws"] if ws_needed else []

    omq_io_names = {"omq-tokio-1t", "omq-tokio-2t", "omq-tokio-4t"}
    curve_omq_names = {"omq-curve-1t", "omq-curve-2t", "omq-curve-4t"}
    omq_all = {"omq-tokio", "omq-tokio-2t"} | omq_io_names | curve_omq_names
    if impl_names & omq_all:
        print("==> building omq-tokio bench_peer...", file=sys.stderr)
        tokio_features = list(features) if features else []
        if impl_names & curve_omq_names:
            tokio_features.append("curve")
        cargo_build("omq-tokio", "bench_peer_tokio", features=tokio_features)
        tokio_bin = str(ROOT / "target" / "release" / "bench_peer_tokio")
        for name in omq_all & impl_names:
            binaries[name] = tokio_bin

    curve_libzmq_names = {"libzmq-curve-1t", "libzmq-curve-2t", "libzmq-curve-4t"}
    if impl_names & ({"libzmq", "libzmq-2t", "libzmq-mt"} | curve_libzmq_names):
        print("==> building libzmq bench_peer...", file=sys.stderr)
        src = ROOT / "scripts" / "libzmq_bench_peer.c"
        out = ROOT / "scripts" / "libzmq_bench_peer"
        gcc_build(src, out)
        for ln in ({"libzmq", "libzmq-2t", "libzmq-mt"} | curve_libzmq_names) & impl_names:
            binaries[ln] = str(out)

    if "zmq.rs" in impl_names:
        print("==> building zmq.rs bench_peer...", file=sys.stderr)
        zmqrs_dir = ROOT / "scripts" / "zmqrs_bench_peer"
        subprocess.run(
            ["cargo", "build", "--release", "-q"],
            cwd=zmqrs_dir, check=True,
        )
        binaries["zmq.rs"] = str(zmqrs_dir / "target" / "release" / "zmqrs_bench_peer")

    if impl_names & {"rzmq", "rzmq-iouring"}:
        print("==> building rzmq bench_peer...", file=sys.stderr)
        rzmq_dir = ROOT / "scripts" / "rzmq_bench_peer"
        subprocess.run(
            ["cargo", "build", "--release", "-q"],
            cwd=rzmq_dir, check=True,
        )
        rzmq_bin = str(rzmq_dir / "target" / "release" / "rzmq_bench_peer")
        # Same binary; rzmq-iouring flips on the io_uring path via its env entry.
        if "rzmq" in impl_names:
            binaries["rzmq"] = rzmq_bin
        if "rzmq-iouring" in impl_names:
            binaries["rzmq-iouring"] = rzmq_bin

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
    run_throughput: bool = True,
    duration: float = DEFAULT_DURATION,
    rounds: int = DEFAULT_ROUNDS,
    latency_iterations: int = LATENCY_ITERATIONS,
    latency_warmup: int = LATENCY_WARMUP,
    latency_timeout: int = LATENCY_TIMEOUT,
    run_fanout: bool = False,
    fanout_peers: list[int] | None = None,
    run_fanin: bool = False,
    fanin_peers: list[int] | None = None,
    run_curve: bool = False,
    curve_peers: int = 16,
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
        if run_throughput:
            print(f"\n── throughput: {transport} ──", file=sys.stderr)
            header = "".join(f"  {name:>22s}" for name in active)
            print(f"{'size':>10s}{header}", file=sys.stderr)

            for idx, size in enumerate(sizes):
                cells = {}
                for name, binary in active.items():
                    impl_def = IMPLS[name]
                    prefix = impl_def["prefix"]
                    addr = addr_for(transport, prefix, idx, base_port,
                                   impl_name=name)
                    subcmd = impl_def.get("inproc_tput_subcmd", "inproc")
                    impl_env = impl_def.get("env")
                    result = run_throughput_cell(binary, transport, addr, size,
                                                inproc_subcmd=subcmd,
                                                duration=duration, rounds=rounds,
                                                env=impl_env, impl=name)
                    cells[name] = result
                    if result:
                        row = {
                            "run_id": run_id,
                            "impl": name,
                            "kind": "throughput",
                            "transport": transport,
                            "msg_size": size,
                            "msgs_s": round(result["msgs_s"], 1),
                            "mbps": round(result["mbps"], 1),
                        }
                        if "elapsed" in result:
                            row["elapsed"] = round(result["elapsed"], 6)
                        if "cpu_time" in result:
                            row["cpu_time"] = round(result["cpu_time"], 6)
                        if "push_cpu_time" in result:
                            row["push_cpu_time"] = round(result["push_cpu_time"], 6)
                        if result.get("zero_transport"):
                            row["zero_transport"] = True
                        append_jsonl(row)
                    else:
                        append_zero_tput_row(run_id, name, "throughput",
                                             transport, size)

                line = f"{size_label(size):>10s}"
                for name in active:
                    r = cells.get(name)
                    if r and not r.get("zero_transport"):
                        line += f"  {r['msgs_s']:>9.0f} msg/s {r['mbps']:>6.1f} MB/s"
                    else:
                        line += f"  {0:>9.0f} msg/s {0:>6.1f} MB/s ZERO"
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
                    addr = addr_for(transport, prefix, idx + len(sizes), base_port,
                                   impl_name=name)
                    subcmd = impl_def.get("inproc_lat_subcmd", "inproc-latency")
                    impl_env = impl_def.get("env")
                    result = run_latency_cell(binary, transport, addr, size,
                                             inproc_subcmd=subcmd,
                                             iterations=latency_iterations,
                                             warmup=latency_warmup,
                                             timeout=latency_timeout,
                                             env=impl_env, impl=name)
                    cells[name] = result
                    if result:
                        row = {
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
                        }
                        if "cpu_time" in result:
                            row["cpu_time"] = round(result["cpu_time"], 6)
                        if "req_cpu_time" in result:
                            row["req_cpu_time"] = round(result["req_cpu_time"], 6)
                        if "elapsed" in result:
                            row["elapsed"] = round(result["elapsed"], 6)
                        append_jsonl(row)

                line = f"{size_label(size):>10s}"
                for name in active:
                    r = cells.get(name)
                    if r:
                        line += f"    p50={r['p50_us']:>7.1f} µs  p99={r['p99_us']:>7.1f} µs"
                    else:
                        line += f"    {'—':>24s}"
                print(line, file=sys.stderr)

        # pub/sub throughput
        if run_pubsub:
            pubsub_active = {
                name: path for name, path in active.items()
                if IMPLS[name].get("supports_pubsub")
                and IMPLS[name].get("class") != "curve"
            }
        else:
            pubsub_active = {}
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
                            pub_needs_peers=impl_def.get("pub_needs_peer_count", False),
                            duration=duration, rounds=rounds,
                            env=impl_def.get("env"), impl=name,
                        )
                        cells[name] = result
                        if result:
                            row = {
                                "run_id": run_id,
                                "impl": name,
                                "kind": "pub_sub",
                                "transport": transport,
                                "peers": peers,
                                "msg_size": size,
                                "msgs_s": round(result["msgs_s"], 1),
                                "mbps": round(result["mbps"], 1),
                            }
                            if "elapsed" in result:
                                row["elapsed"] = round(result["elapsed"], 6)
                            if "cpu_time" in result:
                                row["cpu_time"] = round(result["cpu_time"], 6)
                            if "pub_cpu_time" in result:
                                row["pub_cpu_time"] = round(result["pub_cpu_time"], 6)
                            if result.get("zero_transport"):
                                row["zero_transport"] = True
                            append_jsonl(row)
                        else:
                            append_zero_tput_row(run_id, name, "pub_sub",
                                                 transport, size, peers)

                    line = f"{size_label(size):>10s}"
                    for name in pubsub_active:
                        r = cells.get(name)
                        if r and not r.get("zero_transport"):
                            line += (f"  {r['msgs_s']:>9.0f} msg/s"
                                     f" {r['mbps']:>6.1f} MB/s")
                        else:
                            line += f"  {0:>9.0f} msg/s {0:>6.1f} MB/s ZERO"
                    print(line, file=sys.stderr)

        # fan-out (1 PUSH → N PULL)
        if run_fanout and transport == "tcp":
            for peers in (fanout_peers or FANOUT_PEER_COUNTS):
                print(f"\n── fan-out {peers}p: {transport} ──", file=sys.stderr)
                header = "".join(f"  {name:>22s}" for name in active)
                print(f"{'size':>10s}{header}", file=sys.stderr)

                for idx, size in enumerate(sizes):
                    cells = {}
                    for name, binary in active.items():
                        impl_def = IMPLS[name]
                        prefix = impl_def["prefix"]
                        port_offset = 300 + peers * 50 + idx
                        addr = addr_for(transport, prefix, port_offset,
                                        base_port, impl_name=name)
                        result = run_fanout_cell(
                            binary, transport, addr, size, peers,
                            duration=duration, rounds=rounds,
                            fanout_subcmd=impl_def.get("fanout_subcmd", "push"),
                            fanout_needs_peers=impl_def.get("fanio_needs_peer_count", False),
                            env=impl_def.get("env"), impl=name,
                        )
                        cells[name] = result
                        if result:
                            row = {
                                "run_id": run_id,
                                "impl": name,
                                "kind": "fan_out",
                                "transport": transport,
                                "peers": peers,
                                "msg_size": size,
                                "msgs_s": round(result["msgs_s"], 1),
                                "mbps": round(result["mbps"], 1),
                            }
                            if "elapsed" in result:
                                row["elapsed"] = round(result["elapsed"], 6)
                            if "cpu_time" in result:
                                row["cpu_time"] = round(result["cpu_time"], 6)
                            if "push_cpu_time" in result:
                                row["push_cpu_time"] = round(
                                    result["push_cpu_time"], 6)
                            if "pull_cpu_time" in result:
                                row["pull_cpu_time"] = round(
                                    result["pull_cpu_time"], 6)
                            if "peer_min" in result:
                                row["peer_min"] = round(result["peer_min"], 1)
                                row["peer_max"] = round(result["peer_max"], 1)
                            if result.get("zero_transport"):
                                row["zero_transport"] = True
                            append_jsonl(row)
                        else:
                            append_zero_tput_row(run_id, name, "fan_out",
                                                 transport, size, peers)

                    line = f"{size_label(size):>10s}"
                    for name in active:
                        r = cells.get(name)
                        if r and not r.get("zero_transport"):
                            spread = (r["peer_max"] / r["peer_min"]
                                      if r.get("peer_min") else 1.0)
                            line += (f"  {r['msgs_s']:>9.0f} msg/s"
                                     f" {r['mbps']:>6.1f} MB/s"
                                     f" [{spread:.2f}x]")
                        else:
                            line += f"  {0:>9.0f} msg/s {0:>6.1f} MB/s ZERO"
                    print(line, file=sys.stderr)

        # fan-in (N PUSH → 1 PULL)
        if run_fanin and transport == "tcp":
            for peers in (fanin_peers or FANIN_PEER_COUNTS):
                print(f"\n── fan-in {peers}p: {transport} ──", file=sys.stderr)
                header = "".join(f"  {name:>22s}" for name in active)
                print(f"{'size':>10s}{header}", file=sys.stderr)

                for idx, size in enumerate(sizes):
                    cells = {}
                    for name, binary in active.items():
                        impl_def = IMPLS[name]
                        prefix = impl_def["prefix"]
                        port_offset = 400 + peers * 50 + idx
                        addr = addr_for(transport, prefix, port_offset,
                                        base_port, impl_name=name)
                        result = run_fanin_cell(
                            binary, transport, addr, size, peers,
                            duration=duration, rounds=rounds,
                            pull_subcmd=impl_def.get("fanin_pull_subcmd", "pull-bind"),
                            pull_needs_peers=impl_def.get("fanio_needs_peer_count", False),
                            env=impl_def.get("env"), impl=name,
                        )
                        cells[name] = result
                        if result:
                            row = {
                                "run_id": run_id,
                                "impl": name,
                                "kind": "fan_in",
                                "transport": transport,
                                "peers": peers,
                                "msg_size": size,
                                "msgs_s": round(result["msgs_s"], 1),
                                "mbps": round(result["mbps"], 1),
                            }
                            if "elapsed" in result:
                                row["elapsed"] = round(result["elapsed"], 6)
                            if "cpu_time" in result:
                                row["cpu_time"] = round(result["cpu_time"], 6)
                            if "pull_cpu_time" in result:
                                row["pull_cpu_time"] = round(result["pull_cpu_time"], 6)
                            if result.get("zero_transport"):
                                row["zero_transport"] = True
                            append_jsonl(row)
                        else:
                            append_zero_tput_row(run_id, name, "fan_in",
                                                 transport, size, peers)

                    line = f"{size_label(size):>10s}"
                    for name in active:
                        r = cells.get(name)
                        if r and not r.get("zero_transport"):
                            line += (f"  {r['msgs_s']:>9.0f} msg/s"
                                     f" {r['mbps']:>6.1f} MB/s")
                        else:
                            line += f"  {0:>9.0f} msg/s {0:>6.1f} MB/s ZERO"
                    print(line, file=sys.stderr)

    # ── CURVE PUB/SUB ─────────────────────────────────────────────
    if run_curve:
        curve_active = {
            name: path for name, path in binaries.items()
            if IMPLS[name].get("class") == "curve"
            and "tcp" in IMPLS[name]["transports"]
        }
        if curve_active:
            peers = curve_peers
            print(f"\n── CURVE pub/sub {peers}p: tcp ──", file=sys.stderr)
            header = "".join(f"  {name:>22s}" for name in curve_active)
            print(f"{'size':>10s}{header}", file=sys.stderr)
            for idx, size in enumerate(sizes):
                cells = {}
                for name, binary in curve_active.items():
                    impl_def = IMPLS[name]
                    prefix = impl_def["prefix"]
                    port_offset = 500 + idx
                    addr = addr_for("tcp", prefix, port_offset, base_port)
                    result = run_pubsub_cell(
                        binary, "tcp", addr, size, peers,
                        pub_needs_peers=impl_def.get("pub_needs_peer_count", False),
                        duration=duration, rounds=rounds,
                        env=impl_def.get("env"), impl=name,
                    )
                    cells[name] = result
                    if result:
                        row = {
                            "run_id": run_id,
                            "impl": name,
                            "kind": "pub_sub",
                            "transport": "tcp",
                            "peers": peers,
                            "msg_size": size,
                            "msgs_s": round(result["msgs_s"], 1),
                            "mbps": round(result["mbps"], 1),
                        }
                        if "elapsed" in result:
                            row["elapsed"] = round(result["elapsed"], 6)
                        if "cpu_time" in result:
                            row["cpu_time"] = round(result["cpu_time"], 6)
                        if "pub_cpu_time" in result:
                            row["pub_cpu_time"] = round(result["pub_cpu_time"], 6)
                        if result.get("zero_transport"):
                            row["zero_transport"] = True
                        append_jsonl(row)
                    else:
                        append_zero_tput_row(run_id, name, "pub_sub",
                                             "tcp", size, peers)

                line = f"{size_label(size):>10s}"
                for name in curve_active:
                    r = cells.get(name)
                    if r and not r.get("zero_transport"):
                        line += (f"  {r['msgs_s']:>9.0f} msg/s"
                                 f" {r['mbps']:>6.1f} MB/s")
                    else:
                        line += f"  {0:>9.0f} msg/s {0:>6.1f} MB/s ZERO"
                print(line, file=sys.stderr)

    print(file=sys.stderr)


def main():
    _install_reaper()
    parser = argparse.ArgumentParser(description="Run comparison benchmarks")
    parser.add_argument(
        "--impl", action="append", dest="impls",
        choices=list(IMPLS.keys()),
        help="implementation(s) to benchmark (default: all)",
    )
    parser.add_argument(
        "--omq", action="store_true",
        help="rebench only this project's backends (omq-tokio, omq-tokio-2t). "
             "Competitor data is external and stable, so it is "
             "reused from the JSONL cache. The fast iteration path.",
    )
    parser.add_argument(
        "--sizes", type=str, default=None,
        help="comma-separated message sizes; must be comparison or main chart "
             "sizes unless --allow-non-chart-sizes is set",
    )
    parser.add_argument(
        "--allow-non-chart-sizes", action="store_true",
        help="allow --sizes values outside the comparison chart size set",
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
        "--no-throughput", action="store_true",
        help="skip PUSH/PULL throughput benchmarks (e.g. to refresh only fan-out/fan-in)",
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
        "--fanout", action="store_true",
        help="run PUSH fan-out benchmarks (1 PUSH → N PULL, TCP only)",
    )
    parser.add_argument(
        "--fanout-peers", type=str, default=None,
        help=f"comma-separated peer counts for fan-out (default: {','.join(str(p) for p in FANOUT_PEER_COUNTS)})",
    )
    parser.add_argument(
        "--fanin", action="store_true",
        help="run PUSH fan-in benchmarks (N PUSH → 1 PULL, TCP only)",
    )
    parser.add_argument(
        "--fanin-peers", type=str, default=None,
        help=f"comma-separated peer counts for fan-in (default: {','.join(str(p) for p in FANIN_PEER_COUNTS)})",
    )
    parser.add_argument(
        "--curve", action="store_true",
        help="run CURVE PUB/SUB benchmarks (libzmq + omq, 1T/2T/4T, 16 subscribers)",
    )
    parser.add_argument(
        "--curve-peers", type=int, default=16,
        help="subscriber count for CURVE benchmarks (default: 16)",
    )
    parser.add_argument(
        "--base-port", type=int, default=0,
        help="base TCP port (default: random ephemeral)",
    )
    parser.add_argument(
        "--id", type=str, default=None,
        help="run name suffix; non-ISO values are prefixed with an ISO timestamp",
    )
    args = parser.parse_args()

    transports = args.transport or ["tcp", "inproc", "ipc"]
    sizes = QUICK_SIZES if args.quick_run else COMPARISON_CHART_SIZES
    if args.sizes:
        sizes = [int(x) for x in args.sizes.split(",")]
        chart_sizes = set(COMPARISON_CHART_SIZES) | set(MAIN_EXTRA_CHART_SIZES)
        non_chart = sorted(set(sizes) - chart_sizes)
        if non_chart and not args.allow_non_chart_sizes:
            parser.error(
                "--sizes includes non-chart sizes "
                f"{','.join(str(s) for s in non_chart)}; "
                "pass --allow-non-chart-sizes for exploratory sweeps"
            )
    if args.quick_run:
        duration = args.duration if args.duration is not None else QUICK_DURATION
        rounds = args.rounds if args.rounds is not None else QUICK_ROUNDS
    else:
        duration = args.duration if args.duration is not None else DEFAULT_DURATION
        rounds = args.rounds if args.rounds is not None else DEFAULT_ROUNDS
    run_id = make_run_id(args.id)
    run_latency = not args.no_latency
    run_pubsub = not args.no_pubsub
    pubsub_peers = (
        [int(x) for x in args.pubsub_peers.split(",")]
        if args.pubsub_peers else PUBSUB_PEER_COUNTS
    )
    ws_needed = "ws" in transports

    impl_names = set(args.impls) if args.impls else set()
    if args.omq:
        impl_names |= {"omq-tokio", "omq-tokio-2t"}
    curve_impl_names = {"libzmq-curve-1t", "libzmq-curve-2t", "libzmq-curve-4t",
                        "omq-curve-1t", "omq-curve-2t", "omq-curve-4t"}
    if args.curve and not impl_names & curve_impl_names:
        impl_names |= curve_impl_names
    if not impl_names:
        impl_names = set(IMPLS.keys())

    binaries = build_peers(impl_names, ws_needed)

    versions = []
    if impl_names & {"omq-tokio", "omq-tokio-2t"}:
        versions.append(f"omq {cargo_version('omq-tokio')}")
    if "libzmq" in impl_names:
        versions.append(f"libzmq {libzmq_version()}")
    if "zmq.rs" in impl_names:
        versions.append(f"zmq.rs {cargo_version('zeromq', manifest=ROOT / 'scripts' / 'zmqrs_bench_peer' / 'Cargo.toml')}")
    if impl_names & {"rzmq", "rzmq-iouring"}:
        versions.append(f"rzmq {cargo_version('rzmq', manifest=ROOT / 'scripts' / 'rzmq_bench_peer' / 'Cargo.toml')}")
    print(" vs ".join(versions), file=sys.stderr)

    base_port = args.base_port or random.randint(20_000, 40_000)
    fanout_peers = (
        [int(x) for x in args.fanout_peers.split(",")]
        if args.fanout_peers else None
    )
    fanin_peers = (
        [int(x) for x in args.fanin_peers.split(",")]
        if args.fanin_peers else None
    )
    run_benchmarks(binaries, transports, sizes, run_latency,
                   run_pubsub, pubsub_peers, base_port, run_id,
                   run_throughput=not args.no_throughput,
                   duration=duration, rounds=rounds,
                   latency_iterations=args.latency_iterations,
                   latency_warmup=args.latency_warmup,
                   latency_timeout=args.latency_timeout,
                   run_fanout=args.fanout,
                   fanout_peers=fanout_peers,
                   run_fanin=args.fanin,
                   fanin_peers=fanin_peers,
                   run_curve=args.curve,
                   curve_peers=args.curve_peers)

    # A run that quietly undercounted CPU is worse than a failed one: it ships a
    # plausible chart that lies. Abort loudly so the operator fixes the peer
    # (and re-runs) instead of charting partial data.
    if MEASUREMENT_ISSUES:
        n = len(MEASUREMENT_ISSUES)
        impls = sorted({m.split()[0] for m in MEASUREMENT_ISSUES})
        print("\n" + "=" * 70, file=sys.stderr)
        print(f"MEASUREMENT INTEGRITY FAILURE: {n} incomplete cell(s); "
              f"affected impls: {', '.join(impls)}", file=sys.stderr)
        for m in MEASUREMENT_ISSUES[:20]:
            print(f"  - {m}", file=sys.stderr)
        if n > 20:
            print(f"  ... and {n - 20} more", file=sys.stderr)
        print("These cells recorded NO cpu_time (not a wrong one). Fix the "
              "peer's CPU reporting and re-run before charting.", file=sys.stderr)
        print("=" * 70, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

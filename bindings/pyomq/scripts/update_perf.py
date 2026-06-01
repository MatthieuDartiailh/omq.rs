#!/usr/bin/env python3
"""Measure pyomq vs pyzmq throughput and latency (sync + async).

Run from the pyomq root (bindings/pyomq/) after `maturin develop --release`.
Results are appended to doc/charts/bindings.jsonl (latest run_id wins per impl).
Generates doc/charts/bindings.svg and updates the proxy table in README.md.
"""

import argparse
import asyncio
import json
import math
import os
import re
import subprocess
import sys
import threading
import time

SIZES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
TARGET_RUNTIME_S = 0.4
N_ROUNDS = 3
LATENCY_WARMUP = 1000
LATENCY_ITERS = 10000
README = os.path.join(os.path.dirname(__file__), "..", "README.md")
CHART_DIR = os.path.join(os.path.dirname(__file__), "..", "doc", "charts")
_CACHE_DIR = os.path.join(os.environ.get("XDG_CACHE_HOME", os.path.join(os.path.expanduser("~"), ".cache")), "omq")
JSONL_FILE = os.path.join(_CACHE_DIR, "bindings.jsonl")


def load_jsonl():
    rows = []
    try:
        with open(JSONL_FILE) as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    except FileNotFoundError:
        pass
    return rows


def append_jsonl(rows):
    os.makedirs(os.path.dirname(JSONL_FILE), exist_ok=True)
    with open(JSONL_FILE, "a") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def save_results(run_id, impl, tp_inproc, tp_tcp, atp_tcp, lat, alat, proxy_pp, proxy_rr):
    rows = []
    for i, size in enumerate(SIZES):
        rows.append({"run_id": run_id, "impl": impl, "kind": "throughput",
                      "mode": "sync", "transport": "inproc",
                      "msg_size": size, "msgs_s": tp_inproc[i]})
        rows.append({"run_id": run_id, "impl": impl, "kind": "throughput",
                      "mode": "sync", "transport": "tcp",
                      "msg_size": size, "msgs_s": tp_tcp[i]})
        rows.append({"run_id": run_id, "impl": impl, "kind": "throughput",
                      "mode": "async", "transport": "tcp",
                      "msg_size": size, "msgs_s": atp_tcp[i]})
        rows.append({"run_id": run_id, "impl": impl, "kind": "latency",
                      "mode": "sync", "msg_size": size,
                      "p50_us": lat[i][0], "p99_us": lat[i][1]})
        rows.append({"run_id": run_id, "impl": impl, "kind": "latency",
                      "mode": "async", "msg_size": size,
                      "p50_us": alat[i][0], "p99_us": alat[i][1]})
    rows.append({"run_id": run_id, "impl": impl, "kind": "proxy",
                  "pattern": "pushpull", "msgs_s": proxy_pp})
    rows.append({"run_id": run_id, "impl": impl, "kind": "proxy",
                  "pattern": "reqrep", "msgs_s": proxy_rr})
    append_jsonl(rows)
    print(f"  appended {len(rows)} rows to {JSONL_FILE}")


def chart_data_from_jsonl():
    rows = load_jsonl()

    latest = {}
    for r in rows:
        impl = r.get("impl")
        kind = r.get("kind")
        mode = r.get("mode", "")
        transport = r.get("transport", "")
        size = r.get("msg_size", 0)
        pattern = r.get("pattern", "")
        key = (impl, kind, mode, transport, size, pattern)
        prev = latest.get(key)
        if prev is None or r.get("run_id", "") >= prev.get("run_id", ""):
            latest[key] = r

    def get_tp(mode, impl, transport, size):
        r = latest.get((impl, "throughput", mode, transport, size, ""))
        return r["msgs_s"] if r else 0.0

    def get_lat(mode, impl, size):
        r = latest.get((impl, "latency", mode, "", size, ""))
        return r["p50_us"] if r else 0.0

    sync_omq_tp = [get_tp("sync", "pyomq", "tcp", s) for s in SIZES]
    sync_pz_tp = [get_tp("sync", "pyzmq", "tcp", s) for s in SIZES]
    async_omq_tp = [get_tp("async", "pyomq", "tcp", s) for s in SIZES]
    async_pz_tp = [get_tp("async", "pyzmq", "tcp", s) for s in SIZES]
    sync_omq_lat = [get_lat("sync", "pyomq", s) for s in SIZES]
    sync_pz_lat = [get_lat("sync", "pyzmq", s) for s in SIZES]
    async_omq_lat = [get_lat("async", "pyomq", s) for s in SIZES]
    async_pz_lat = [get_lat("async", "pyzmq", s) for s in SIZES]

    return {
        "sync_omq_tp": sync_omq_tp, "sync_pz_tp": sync_pz_tp,
        "async_omq_tp": async_omq_tp, "async_pz_tp": async_pz_tp,
        "sync_omq_lat": sync_omq_lat, "sync_pz_lat": sync_pz_lat,
        "async_omq_lat": async_omq_lat, "async_pz_lat": async_pz_lat,
    }


# ── helpers ──────────────────────────────────────────────────────────

def free_tcp():
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"tcp://127.0.0.1:{port}"


def fmt_rate(rate):
    if rate >= 1_000_000:
        return f"{rate / 1_000_000:.2f} M/s"
    return f"{rate / 1_000:.0f} k/s"


def fmt_size(size):
    if size >= 1024:
        return f"{size // 1024} KiB"
    return f"{size} B"


def fmt_int(n):
    return f"{n:,.0f}"


# ── subprocess runner ────────────────────────────────────────────────

def _run_subprocess(code, label, timeout=30, retries=2):
    for attempt in range(1 + retries):
        try:
            r = subprocess.run([sys.executable, "-c", code],
                               capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            sys.stderr.write(f"  [{label} timeout, attempt {attempt + 1}]\n")
            continue
        if r.returncode != 0:
            sys.stderr.write(f"  [{label} failed, attempt {attempt + 1}]\n")
            continue
        return json.loads(r.stdout.strip())
    return None


def _measure_throughput_subprocess(lib_name, transport, size, n_target_per_s=200_000):
    """Run a throughput measurement in a subprocess to isolate libzmq state."""
    code = f"""
import threading, time, json, socket as sock
def free_tcp():
    s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return f'tcp://127.0.0.1:{{port}}'
if '{lib_name}' == 'pyzmq':
    import zmq as lib
else:
    import pyomq as lib
size = {size}
n = max(int({n_target_per_s} * {TARGET_RUNTIME_S}), 100)
payload = b'x' * size
if '{transport}' == 'inproc':
    ep = f'inproc://bench-{{time.monotonic_ns()}}'
else:
    ep = free_tcp()
ctx = lib.Context()
pull = ctx.socket(lib.PULL)
push = ctx.socket(lib.PUSH)
pull.linger = 0
push.linger = 0
pull.bind(ep)
push.connect(ep)
def sender():
    for _ in range(n):
        push.send(payload)
t = threading.Thread(target=sender)
start = time.monotonic()
t.start()
for _ in range(n):
    pull.recv()
elapsed = time.monotonic() - start
t.join()
push.close()
pull.close()
print(json.dumps(n / elapsed))
import sys; sys.stdout.flush(); import os; os._exit(0)
"""
    result = _run_subprocess(code, f"{lib_name} {transport} {size}B")
    return result if result is not None else 0.0


def run_throughput(lib_name):
    inproc_results = []
    tcp_results = []
    for size in SIZES:
        label = fmt_size(size)
        sys.stdout.write(f"  {label:>7} ...")
        sys.stdout.flush()

        _measure_throughput_subprocess(lib_name, "inproc", size)
        _measure_throughput_subprocess(lib_name, "tcp", size)

        inproc = max(
            _measure_throughput_subprocess(lib_name, "inproc", size)
            for _ in range(N_ROUNDS)
        )
        tcp = max(
            _measure_throughput_subprocess(lib_name, "tcp", size)
            for _ in range(N_ROUNDS)
        )
        inproc_results.append(inproc)
        tcp_results.append(tcp)
        print(f" inproc {fmt_rate(inproc):>10}  tcp {fmt_rate(tcp):>10}")

    return inproc_results, tcp_results


# ── async PUSH/PULL throughput ───────────────────────────────────────

def _measure_async_subprocess(lib_name, size, n_target_per_s=200_000):
    """Async throughput: sync sender thread + async recv, single subprocess."""
    n = min(max(int(n_target_per_s * TARGET_RUNTIME_S), 100), 20_000)
    if lib_name == "pyzmq":
        code = f"""
import asyncio, threading, time, json, sys, socket as sock
import zmq, zmq.asyncio
def free_tcp():
    s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return f'tcp://127.0.0.1:{{port}}'
async def run():
    ep = free_tcp()
    ctx = zmq.asyncio.Context()
    pull = ctx.socket(zmq.PULL); pull.linger = 0
    pull.bind(ep)
    sctx = zmq.Context()
    push = sctx.socket(zmq.PUSH); push.linger = 0
    push.connect(ep)
    payload = b'x' * {size}
    n = {n}
    def sender():
        for _ in range(n):
            push.send(payload)
    t = threading.Thread(target=sender)
    t.start()
    count = 0; start = None
    for _ in range(n):
        await pull.recv()
        if start is None:
            start = time.monotonic()
        count += 1
    elapsed = time.monotonic() - start
    t.join()
    push.close(); pull.close()
    print(json.dumps(count / elapsed))
    sys.stdout.flush(); import os; os._exit(0)
asyncio.run(run())
"""
    else:
        code = f"""
import asyncio, threading, time, json, sys, socket as sock
import pyomq, pyomq.asyncio as zmq_async
def free_tcp():
    s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return f'tcp://127.0.0.1:{{port}}'
async def run():
    ep = free_tcp()
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    pull.bind(ep)
    push = pyomq.Context().socket(pyomq.PUSH)
    push.connect(ep)
    payload = b'x' * {size}
    n = {n}
    def sender():
        for _ in range(n):
            push.send(payload)
    t = threading.Thread(target=sender)
    t.start()
    count = 0; start = None
    for _ in range(n):
        await pull.recv()
        if start is None:
            start = time.monotonic()
        count += 1
    elapsed = time.monotonic() - start
    t.join()
    push.close()
    pull.close()
    print(json.dumps(count / elapsed))
    sys.stdout.flush(); import os; os._exit(0)
asyncio.run(run())
"""
    result = _run_subprocess(code, f"{lib_name} async tcp {size}B")
    return result if result is not None else 0.0


def run_async_throughput(lib_name):
    results = []
    for size in SIZES:
        label = fmt_size(size)
        sys.stdout.write(f"  {label:>7} ...")
        sys.stdout.flush()

        tcp = max(_measure_async_subprocess(lib_name, size)
                  for _ in range(N_ROUNDS + 1))
        results.append(tcp)
        print(f" {fmt_rate(tcp):>10}")

    return results


# ── sync REQ/REP latency ────────────────────────────────────────────

def _measure_latency_subprocess(lib_name, size, warmup, iters):
    code = f"""
import time, threading, json, socket as sock
def free_tcp():
    s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return f'tcp://127.0.0.1:{{port}}'
if '{lib_name}' == 'pyzmq':
    import zmq as lib
else:
    import pyomq as lib
payload = b'x' * {size}
ep = free_tcp()
ctx = lib.Context()
rep = ctx.socket(lib.REP)
req = ctx.socket(lib.REQ)
rep.linger = 0
req.linger = 0
rep.bind(ep)
req.connect(ep)
time.sleep(0.05)
def echo():
    try:
        for _ in range({warmup} + {iters} + 100):
            rep.send(rep.recv())
    except Exception:
        pass
t = threading.Thread(target=echo, daemon=True)
t.start()
for _ in range({warmup}):
    req.send(payload)
    req.recv()
rtts = []
for _ in range({iters}):
    t0 = time.monotonic()
    req.send(payload)
    req.recv()
    rtts.append(time.monotonic() - t0)
req.close()
rep.close()
rtts.sort()
p50 = rtts[len(rtts)*50//100]*1e6
p99 = rtts[len(rtts)*99//100]*1e6
print(json.dumps([p50, p99]))
import sys; sys.stdout.flush(); import os; os._exit(0)
"""
    result = _run_subprocess(code, f"{lib_name} lat {size}B", timeout=60)
    return tuple(result) if result is not None else (999999.0, 999999.0)


def run_latency(lib_name):
    results = []
    for size in SIZES:
        label = fmt_size(size)
        sys.stdout.write(f"  {label:>7} ...")
        sys.stdout.flush()

        _measure_latency_subprocess(lib_name, size, 200, 200)

        runs = [_measure_latency_subprocess(lib_name, size, LATENCY_WARMUP, LATENCY_ITERS)
                for _ in range(N_ROUNDS)]
        p50 = min(r[0] for r in runs)
        p99 = min(r[1] for r in runs)
        results.append((p50, p99))
        print(f" p50 {p50:.1f} µs  p99 {p99:.1f} µs")

    return results


# ── async REQ/REP latency ───────────────────────────────────────────

def _measure_async_latency_subprocess(lib_name, size, warmup, iters):
    if lib_name == "pyzmq":
        lib_import = "import zmq; import zmq.asyncio; lib = zmq; actx = zmq.asyncio"
        close_expr = "sock.close()"
    else:
        lib_import = "import pyomq; import pyomq.asyncio as actx; lib = pyomq"
        close_expr = "sock.close()"

    send_await = "await " if lib_name == "pyzmq" else ""
    code = f"""
import asyncio, time, json, socket as sock
def free_tcp():
    s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return f'tcp://127.0.0.1:{{port}}'
{lib_import}
async def run():
    payload = b'x' * {size}
    ep = free_tcp()
    ctx = actx.Context()
    rep = ctx.socket(lib.REP)
    req = ctx.socket(lib.REQ)
    rep.bind(ep)
    req.connect(ep)
    await asyncio.sleep(0.05)
    async def echo():
        try:
            for _ in range({warmup} + {iters} + 100):
                msg = await rep.recv()
                {send_await}rep.send(msg)
        except Exception:
            pass
    task = asyncio.create_task(echo())
    for _ in range({warmup}):
        {send_await}req.send(payload)
        await req.recv()
    rtts = []
    for _ in range({iters}):
        t0 = time.monotonic()
        {send_await}req.send(payload)
        await req.recv()
        rtts.append(time.monotonic() - t0)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    rtts.sort()
    p50 = rtts[len(rtts)*50//100]*1e6
    p99 = rtts[len(rtts)*99//100]*1e6
    print(json.dumps([p50, p99]))
    import sys; sys.stdout.flush(); import os; os._exit(0)
asyncio.run(run())
"""
    result = _run_subprocess(code, f"{lib_name} async lat {size}B", timeout=60)
    return tuple(result) if result is not None else (999999.0, 999999.0)


def run_async_latency(lib_name):
    results = []
    for size in SIZES:
        label = fmt_size(size)
        sys.stdout.write(f"  {label:>7} ...")
        sys.stdout.flush()

        _measure_async_latency_subprocess(lib_name, size, 200, 200)

        runs = [_measure_async_latency_subprocess(lib_name, size, LATENCY_WARMUP, LATENCY_ITERS)
                for _ in range(N_ROUNDS)]
        p50 = min(r[0] for r in runs)
        p99 = min(r[1] for r in runs)
        results.append((p50, p99))
        print(f" p50 {p50:.1f} µs  p99 {p99:.1f} µs")

    return results


# ── proxy forwarding ─────────────────────────────────────────────────

def _quiet_proxy(lib, fe, be):
    try:
        lib.proxy(fe, be)
    except Exception:
        pass


def measure_proxy_pushpull(lib, n=200_000):
    payload = b"x" * 128
    ctx = lib.Context()
    frontend = ctx.socket(lib.PULL)
    backend = ctx.socket(lib.PUSH)
    fe_ep = free_tcp()
    be_ep = free_tcp()
    frontend.bind(fe_ep)
    backend.bind(be_ep)

    sender = ctx.socket(lib.PUSH)
    sender.connect(fe_ep)
    receiver = ctx.socket(lib.PULL)
    receiver.connect(be_ep)

    proxy_t = threading.Thread(
        target=_quiet_proxy, args=(lib, frontend, backend), daemon=True,
    )
    proxy_t.start()
    time.sleep(0.05)

    for _ in range(200):
        sender.send(b"w")
        receiver.recv()

    def send_all():
        for _ in range(n):
            sender.send(payload)

    t = threading.Thread(target=send_all)
    start = time.monotonic()
    t.start()
    for _ in range(n):
        receiver.recv()
    elapsed = time.monotonic() - start
    t.join()

    sender.close()
    receiver.close()
    frontend.close()
    backend.close()
    return n / elapsed


def measure_proxy_reqrep(lib, n=10_000):
    payload = b"x" * 128
    ctx = lib.Context()
    frontend = ctx.socket(lib.ROUTER)
    backend = ctx.socket(lib.DEALER)
    fe_ep = free_tcp()
    be_ep = free_tcp()
    frontend.bind(fe_ep)
    backend.bind(be_ep)

    worker = ctx.socket(lib.REP)
    worker.connect(be_ep)
    client = ctx.socket(lib.REQ)
    client.connect(fe_ep)

    proxy_t = threading.Thread(
        target=_quiet_proxy, args=(lib, frontend, backend), daemon=True,
    )
    proxy_t.start()
    time.sleep(0.05)

    for _ in range(100):
        client.send(b"w")
        worker.recv()
        worker.send(b"w")
        client.recv()

    start = time.monotonic()
    for _ in range(n):
        client.send(payload)
        worker.recv()
        worker.send(payload)
        client.recv()
    elapsed = time.monotonic() - start

    client.close()
    worker.close()
    frontend.close()
    backend.close()
    return n / elapsed


def _measure_proxy_pyzmq_subprocess(pattern, n):
    code = f"""
import threading, time, json, socket as sock
def free_tcp():
    s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return f'tcp://127.0.0.1:{{port}}'
import zmq
ctx = zmq.Context()
"""
    if pattern == "pushpull":
        code += f"""
frontend = ctx.socket(zmq.PULL)
backend = ctx.socket(zmq.PUSH)
fe_ep = free_tcp()
be_ep = free_tcp()
frontend.bind(fe_ep)
backend.bind(be_ep)
sender = ctx.socket(zmq.PUSH)
sender.connect(fe_ep)
receiver = ctx.socket(zmq.PULL)
receiver.connect(be_ep)
def proxy():
    try:
        zmq.proxy(frontend, backend)
    except Exception:
        pass
t = threading.Thread(target=proxy, daemon=True)
t.start()
time.sleep(0.05)
payload = b'x' * 128
for _ in range(200):
    sender.send(b'w')
    receiver.recv()
n = {n}
def send_all():
    for _ in range(n):
        sender.send(payload)
st = threading.Thread(target=send_all)
start = time.monotonic()
st.start()
for _ in range(n):
    receiver.recv()
elapsed = time.monotonic() - start
st.join()
sender.close()
receiver.close()
frontend.close()
backend.close()
print(json.dumps(n / elapsed))
import sys; sys.stdout.flush(); import os; os._exit(0)
"""
    else:
        code += f"""
frontend = ctx.socket(zmq.ROUTER)
backend = ctx.socket(zmq.DEALER)
fe_ep = free_tcp()
be_ep = free_tcp()
frontend.bind(fe_ep)
backend.bind(be_ep)
worker = ctx.socket(zmq.REP)
worker.connect(be_ep)
client = ctx.socket(zmq.REQ)
client.connect(fe_ep)
def proxy():
    try:
        zmq.proxy(frontend, backend)
    except Exception:
        pass
t = threading.Thread(target=proxy, daemon=True)
t.start()
time.sleep(0.05)
for _ in range(100):
    client.send(b'w')
    worker.recv()
    worker.send(b'w')
    client.recv()
n = {n}
payload = b'x' * 128
start = time.monotonic()
for _ in range(n):
    client.send(payload)
    worker.recv()
    worker.send(payload)
    client.recv()
elapsed = time.monotonic() - start
client.close()
worker.close()
frontend.close()
backend.close()
print(json.dumps(n / elapsed))
import sys; sys.stdout.flush(); import os; os._exit(0)
"""
    result = _run_subprocess(code, f"pyzmq proxy {pattern}", timeout=30)
    return result if result is not None else 0.0


def run_proxy(lib_name):
    if lib_name == "pyomq":
        import pyomq as lib
        sys.stdout.write("  PUSH/PULL ...")
        sys.stdout.flush()
        pp = max(measure_proxy_pushpull(lib) for _ in range(N_ROUNDS))
        print(f" {fmt_rate(pp)}")

        sys.stdout.write("  REQ/REP ...")
        sys.stdout.flush()
        rr = max(measure_proxy_reqrep(lib) for _ in range(N_ROUNDS))
        print(f" {fmt_rate(rr)}")
    else:
        sys.stdout.write("  PUSH/PULL ...")
        sys.stdout.flush()
        pp = max(_measure_proxy_pyzmq_subprocess("pushpull", 200_000)
                 for _ in range(N_ROUNDS))
        print(f" {fmt_rate(pp)}")

        sys.stdout.write("  REQ/REP ...")
        sys.stdout.flush()
        rr = max(_measure_proxy_pyzmq_subprocess("reqrep", 10_000)
                 for _ in range(N_ROUNDS))
        print(f" {fmt_rate(rr)}")

    return pp, rr


# ── SVG chart generation ────────────────────────────────────────────

# Colors: warm = pyomq, cool = pyzmq
C_PYOMQ = "#dc2626"
C_PYOMQ_ASYNC = "#f97316"
C_PYZMQ = "#2563eb"
C_PYZMQ_ASYNC = "#8b5cf6"

def _nice_ceil(v):
    if v <= 0:
        return 1
    exp = math.floor(math.log10(v))
    base = 10 ** exp
    for m in [1, 2, 5, 10]:
        candidate = m * base
        if candidate >= v:
            return candidate
    return 10 * base


def _fmt_y_rate(val):
    if val >= 1_000_000:
        return f"{val / 1_000_000:g}M"
    if val >= 1_000:
        return f"{val / 1_000:g}k"
    return f"{val:g}"


def _fmt_y_us(val):
    if val >= 1000:
        return f"{val / 1000:g} ms"
    return f"{val:g} µs"


def _fmt_mbps(val):
    if val >= 1000:
        return f"{val / 1000:.1f} GB/s"
    if val >= 10:
        return f"{val:.0f} MB/s"
    return f"{val:.1f} MB/s"


def _detect_hardware():
    try:
        cpu = None
        for line in open("/proc/cpuinfo"):
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                cpu = cpu.replace("(R)", "").replace("(TM)", "").replace("CPU ", "")
                break
        cores = os.cpu_count()
        if cpu and cores:
            return f"{cpu}, {cores} cores"
    except OSError:
        pass
    return None


def gen_combined_chart(data, path):
    n = len(SIZES)
    hw_label = _detect_hardware()
    hw_offset = 14 if hw_label else 0
    svg_w = 850
    svg_h = 670 + hw_offset
    x_left, x_right = 90, 760
    plot_w = x_right - x_left

    t1_top = 35 + hw_offset
    t1_bot = 370 + hw_offset
    t1_h = t1_bot - t1_top
    t2_top = t1_bot + 80
    t2_bot = t2_top + 120
    t2_h = t2_bot - t2_top

    xs = [x_left + i * plot_w / max(n - 1, 1) for i in range(n)]
    mid_x = (x_left + x_right) / 2

    sync_omq_tp = data["sync_omq_tp"]
    sync_pz_tp = data["sync_pz_tp"]
    async_omq_tp = data["async_omq_tp"]
    async_pz_tp = data["async_pz_tp"]

    msg_max = 2_000_000
    mbps_max = 5_000

    def y_msg(v):
        frac = v / msg_max if msg_max > 0 else 0
        return t1_bot - frac * t1_h

    def y_mbps(v):
        frac = v / mbps_max if mbps_max > 0 else 0
        return t1_bot - frac * t1_h

    lat_max = 200.0
    lat_step = 20

    def y_lat(v):
        return t2_bot - (v / lat_max) * t2_h

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    # ── TOP PANEL: THROUGHPUT ──────────────────────────────────────

    L.append(
        f'  <text x="{mid_x}" y="{t1_top - 17}" text-anchor="middle" fill="#111827"'
        f' font-size="13" font-weight="700">'
        f'PUSH/PULL throughput: 2-process, TCP loopback (higher is better)</text>'
    )
    if hw_label:
        L.append(
            f'  <text x="{mid_x}" y="{t1_top - 3}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    n_l_ticks = 4
    for i in range(n_l_ticks + 1):
        val = i * msg_max / n_l_ticks
        yy = y_msg(val)
        L.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )
        L.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#374151"'
            f' font-size="10">{_fmt_y_rate(val)}</text>'
        )

    n_r_ticks = 5
    for i in range(n_r_ticks + 1):
        mbps_val = i * mbps_max / n_r_ticks
        yy = y_mbps(mbps_val)
        L.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#e5e7eb" stroke-width="1" stroke-dasharray="3,6"/>'
        )
        L.append(
            f'  <text x="{x_right + 8}" y="{yy:.1f}" text-anchor="start"'
            f' dominant-baseline="middle" fill="#6b7280"'
            f' font-size="10">{_fmt_mbps(mbps_val)}</text>'
        )

    for x in xs:
        L.append(
            f'  <line x1="{x:.1f}" y1="{t1_top}" x2="{x:.1f}" y2="{t1_bot}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )

    L.append(
        f'  <line x1="{x_left}" y1="{t1_top}" x2="{x_left}" y2="{t1_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )
    L.append(
        f'  <line x1="{x_right}" y1="{t1_top}" x2="{x_right}" y2="{t1_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )
    L.append(
        f'  <line x1="{x_left}" y1="{t1_bot}" x2="{x_right}" y2="{t1_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )

    t1_mid = (t1_top + t1_bot) / 2
    L.append(
        f'  <text x="40" y="{t1_mid:.0f}" text-anchor="middle"'
        f' dominant-baseline="middle" fill="#374151" font-size="10" font-weight="600"'
        f' transform="rotate(-90,40,{t1_mid:.0f})">msg/s</text>'
    )

    tp_series = [
        ("pyomq", C_PYOMQ, sync_omq_tp),
        ("pyomq async", C_PYOMQ_ASYNC, async_omq_tp),
        ("pyzmq", C_PYZMQ, sync_pz_tp),
        ("pyzmq async", C_PYZMQ_ASYNC, async_pz_tp),
    ]

    for _, color, vals in tp_series:
        pts = " ".join(f"{xs[i]:.1f},{y_msg(v):.1f}" for i, v in enumerate(vals))
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{color}"'
            f' stroke-width="2" stroke-dasharray="6,4"/>'
        )

    for _, color, vals in tp_series:
        mbps = [v * SIZES[i] / 1e6 for i, v in enumerate(vals)]
        pts = " ".join(f"{xs[i]:.1f},{y_mbps(v):.1f}" for i, v in enumerate(mbps))
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{color}"'
            f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
        )
        for i, v in enumerate(mbps):
            yy = y_mbps(v)
            L.append(
                f'  <circle cx="{xs[i]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{color}" stroke="white" stroke-width="1"/>'
            )

    for i in range(n):
        L.append(
            f'  <text x="{xs[i]:.1f}" y="{t1_bot + 14}" text-anchor="middle"'
            f' fill="#374151" font-size="8.5">{fmt_size(SIZES[i])}</text>'
        )

    # ── BOTTOM PANEL: LATENCY ─────────────────────────────────────

    L.append(
        f'  <text x="{mid_x}" y="{t2_top - 17}" text-anchor="middle" fill="#111827"'
        f' font-size="13" font-weight="700">'
        f'REQ/REP latency: 2-process, TCP loopback, p50 µs (lower is better)</text>'
    )

    sync_omq_lat = data["sync_omq_lat"]
    sync_pz_lat = data["sync_pz_lat"]
    async_omq_lat = data["async_omq_lat"]
    async_pz_lat = data["async_pz_lat"]

    for v in range(int(lat_step), int(lat_max) + 1, int(lat_step)):
        yy = y_lat(v)
        L.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )
        L.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#374151" font-size="10">'
            f'{_fmt_y_us(v)}</text>'
        )

    for x in xs:
        L.append(
            f'  <line x1="{x:.1f}" y1="{t2_top}" x2="{x:.1f}" y2="{t2_bot}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )

    L.append(
        f'  <line x1="{x_left}" y1="{t2_top}" x2="{x_left}" y2="{t2_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )
    L.append(
        f'  <line x1="{x_left}" y1="{t2_bot}" x2="{x_right}" y2="{t2_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )

    t2_mid = (t2_top + t2_bot) / 2
    L.append(
        f'  <text x="40" y="{t2_mid:.0f}" text-anchor="middle"'
        f' dominant-baseline="middle" fill="#374151" font-size="10" font-weight="600"'
        f' transform="rotate(-90,40,{t2_mid:.0f})">p50 latency (µs)</text>'
    )

    lat_series = [
        ("pyomq", C_PYOMQ, sync_omq_lat),
        ("pyomq async", C_PYOMQ_ASYNC, async_omq_lat),
        ("pyzmq", C_PYZMQ, sync_pz_lat),
        ("pyzmq async", C_PYZMQ_ASYNC, async_pz_lat),
    ]

    for _, color, vals in lat_series:
        pts = " ".join(f"{xs[i]:.1f},{y_lat(v):.1f}" for i, v in enumerate(vals))
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{color}"'
            f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
        )
        for i, v in enumerate(vals):
            yy = y_lat(v)
            L.append(
                f'  <circle cx="{xs[i]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{color}" stroke="white" stroke-width="1"/>'
            )

    for i in range(n):
        L.append(
            f'  <text x="{xs[i]:.1f}" y="{t2_bot + 14}" text-anchor="middle"'
            f' fill="#374151" font-size="8.5">{fmt_size(SIZES[i])}</text>'
        )

    # ── LEGEND ────────────────────────────────────────────────────

    leg_y = t2_bot + 40
    legend_items = [
        ("pyomq", C_PYOMQ), ("pyomq async", C_PYOMQ_ASYNC),
        ("pyzmq", C_PYZMQ), ("pyzmq async", C_PYZMQ_ASYNC),
    ]
    item_w = 140
    total_w = len(legend_items) * item_w
    start_x = mid_x - total_w / 2

    for idx, (label, color) in enumerate(legend_items):
        lx = start_x + idx * item_w
        L.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y}" x2="{lx + 14:.0f}" y2="{leg_y}"'
            f' stroke="{color}" stroke-width="2.5"/>'
        )
        L.append(
            f'  <circle cx="{lx + 7:.0f}" cy="{leg_y}" r="2.5" fill="{color}"/>'
        )
        L.append(
            f'  <text x="{lx + 20:.0f}" y="{leg_y + 4}" fill="#374151"'
            f' font-size="11" font-weight="500">{label}</text>'
        )

    footer_y = leg_y + 18
    L.append(
        f'  <text x="{mid_x:.1f}" y="{footer_y}" text-anchor="middle"'
        f' fill="#9ca3af" font-size="9">'
        f'dashed = msg/s (left) · solid = throughput (right)</text>'
    )

    L.append("</svg>")

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("\n".join(L))
        f.write("\n")
    print(f"  wrote {path}")


# ── README tables ────────────────────────────────────────────────────

def build_proxy_table():
    rows = load_jsonl()
    latest = {}
    for r in rows:
        if r.get("kind") != "proxy":
            continue
        key = (r["impl"], r["pattern"])
        prev = latest.get(key)
        if prev is None or r.get("run_id", "") >= prev.get("run_id", ""):
            latest[key] = r

    pp_omq = latest.get(("pyomq", "pushpull"), {}).get("msgs_s", 0)
    pp_pz = latest.get(("pyzmq", "pushpull"), {}).get("msgs_s", 0)
    rr_omq = latest.get(("pyomq", "reqrep"), {}).get("msgs_s", 0)
    rr_pz = latest.get(("pyzmq", "reqrep"), {}).get("msgs_s", 0)
    pp_ratio = pp_omq / pp_pz if pp_pz > 0 else 0
    rr_ratio = rr_omq / rr_pz if rr_pz > 0 else 0

    return "\n".join([
        "|                    | pyomq     | pyzmq     | ratio     |",
        "|--------------------|----------:|----------:|----------:|",
        f"| PUSH/PULL msg/s    | {fmt_rate(pp_omq):>9} "
        f"| {fmt_rate(pp_pz):>9} | **{pp_ratio:.2f}×** |",
        f"| REQ/REP rt/s       | {fmt_int(rr_omq) + '/s':>9} "
        f"| {fmt_int(rr_pz) + '/s':>9} | **{rr_ratio:.2f}×** |",
    ])


# ── README update ────────────────────────────────────────────────────

def update_marker(content, marker, table):
    pattern = rf"<!-- {marker}:START -->\n.*?\n<!-- {marker}:END -->"
    replacement = f"<!-- {marker}:START -->\n{table}\n<!-- {marker}:END -->"
    new_content, count = re.subn(pattern, replacement, content, flags=re.DOTALL)
    if count == 0:
        print(f"ERROR: <!-- {marker}:START -->...<!-- {marker}:END --> "
              f"markers not found in README.md")
        sys.exit(1)
    return new_content


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", choices=["all", "pyomq"],
                        default="all",
                        help="all: bench both impls. pyomq: bench pyomq only, "
                             "chart uses latest pyzmq from JSONL")
    parser.add_argument("--chart-only", action="store_true",
                        help="regenerate SVG from existing JSONL, no benchmarking")
    args = parser.parse_args()

    if args.chart_only:
        print("Generating chart from existing JSONL...")
        data = chart_data_from_jsonl()
        gen_combined_chart(data, os.path.join(CHART_DIR, "bindings.svg"))
        return

    run_id = time.strftime("%Y-%m-%dT%H:%M:%S")
    impls = ["pyomq", "pyzmq"] if args.scope == "all" else ["pyomq"]

    for impl in impls:
        print(f"\n{'=' * 40}")
        print(f"Benchmarking {impl}")
        print(f"{'=' * 40}")

        print("\nSync PUSH/PULL throughput...")
        tp_inproc, tp_tcp = run_throughput(impl)

        print("\nAsync PUSH/PULL throughput...")
        atp_tcp = run_async_throughput(impl)

        print("\nSync REQ/REP latency (TCP)...")
        lat = run_latency(impl)

        print("\nAsync REQ/REP latency (TCP)...")
        alat = run_async_latency(impl)

        print("\nzmq.proxy() forwarding...")
        proxy_pp, proxy_rr = run_proxy(impl)

        print("\nSaving results...")
        save_results(run_id, impl, tp_inproc, tp_tcp, atp_tcp, lat, alat,
                     proxy_pp, proxy_rr)

    proxy_table = build_proxy_table()
    with open(README) as f:
        content = f.read()
    content = update_marker(content, "PROXY_PERF", proxy_table)
    with open(README, "w") as f:
        f.write(content)
    print(f"\nUpdated {README}")

    print("\nGenerating chart...")
    data = chart_data_from_jsonl()
    gen_combined_chart(data, os.path.join(CHART_DIR, "bindings.svg"))


if __name__ == "__main__":
    main()

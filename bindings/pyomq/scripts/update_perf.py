#!/usr/bin/env python3
"""Measure pyomq vs pyzmq throughput and latency (sync + async).

Run from the pyomq root (bindings/pyomq/) after `maturin develop --release`.
Generates SVG charts and updates the README tables.
"""

import asyncio
import json
import math
import os
import re
import subprocess
import sys
import threading
import time

SIZES = [128, 512, 2048, 8192, 32768]
LATENCY_SIZES = [128, 512, 2048, 8192, 32768]
TARGET_RUNTIME_S = 0.4
N_ROUNDS = 3
LATENCY_WARMUP = 1000
LATENCY_ITERS = 10000
README = os.path.join(os.path.dirname(__file__), "..", "README.md")
CHART_DIR = os.path.join(os.path.dirname(__file__), "..", "doc", "charts")


# ── helpers ──────────────────────────────────────────────────────────

def free_inproc(label):
    return f"inproc://perf-{label}-{time.monotonic_ns()}"


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


def fmt_us(v):
    if v >= 1000:
        return f"{v / 1000:.1f} ms"
    if v >= 100:
        return f"{v:.0f} µs"
    if v >= 10:
        return f"{v:.1f} µs"
    return f"{v:.2f} µs"


# ── sync PUSH/PULL throughput ────────────────────────────────────────

def measure(lib, endpoint, size, n_target_per_s=200_000):
    payload = b"x" * size
    ctx = lib.Context() if hasattr(lib, "Context") else lib.Context.instance()
    pull = ctx.socket(lib.PULL)
    push = ctx.socket(lib.PUSH)
    pull.linger = 0
    push.linger = 0
    pull.bind(endpoint)
    push.connect(endpoint)

    n = max(int(n_target_per_s * TARGET_RUNTIME_S), 100)

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
    return n / elapsed


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
    try:
        r = subprocess.run([sys.executable, "-c", code],
                           capture_output=True, text=True, timeout=15)
    except subprocess.TimeoutExpired:
        sys.stderr.write(f"  [{lib_name} {transport} {size}B timeout]\n")
        return 0.0
    if r.returncode != 0:
        sys.stderr.write(f"  [{lib_name} {transport} {size}B failed]\n")
        return 0.0
    return json.loads(r.stdout.strip())


def run_throughput():
    import pyomq

    results = []
    for size in SIZES:
        label = fmt_size(size)
        sys.stdout.write(f"  {label:>7} ...")
        sys.stdout.flush()

        # warmup
        measure(pyomq, free_inproc(f"w-omq-{size}"), size)
        _measure_throughput_subprocess("pyzmq", "inproc", size)
        measure(pyomq, free_tcp(), size)
        _measure_throughput_subprocess("pyzmq", "tcp", size)

        inproc_omq = max(
            measure(pyomq, free_inproc(f"omq-{size}-{i}"), size)
            for i in range(N_ROUNDS)
        )
        inproc_pz = max(
            _measure_throughput_subprocess("pyzmq", "inproc", size)
            for _ in range(N_ROUNDS)
        )
        tcp_omq = max(measure(pyomq, free_tcp(), size) for _ in range(N_ROUNDS))
        tcp_pz = max(
            _measure_throughput_subprocess("pyzmq", "tcp", size)
            for _ in range(N_ROUNDS)
        )

        inproc_ratio = inproc_omq / inproc_pz if inproc_pz > 0 else 0
        tcp_ratio = tcp_omq / tcp_pz if tcp_pz > 0 else 0

        results.append((label, inproc_omq, inproc_pz, inproc_ratio,
                         tcp_omq, tcp_pz, tcp_ratio))

        print(f" inproc {inproc_ratio:.2f}x  tcp {tcp_ratio:.2f}x")

    return results


# ── async PUSH/PULL throughput ───────────────────────────────────────

async def _measure_async_pyomq(endpoint, size, n):
    import pyomq
    import pyomq.asyncio as zmq_async

    payload = b"x" * size
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    await pull.bind(endpoint)
    await push.connect(endpoint)
    await asyncio.sleep(0.05)

    async def sender():
        for _ in range(n):
            await push.send(payload)

    async def receiver():
        for _ in range(n):
            await pull.recv()

    start = time.monotonic()
    await asyncio.gather(sender(), receiver())
    elapsed = time.monotonic() - start

    await push.close()
    await pull.close()
    return n / elapsed


def measure_async_pyomq(endpoint, size, n_target_per_s=200_000):
    n = max(int(n_target_per_s * TARGET_RUNTIME_S), 100)
    return asyncio.run(_measure_async_pyomq(endpoint, size, n))


def measure_async_pyzmq(_endpoint, size, n_target_per_s=200_000):
    n = max(int(n_target_per_s * TARGET_RUNTIME_S), 100)
    code = f"""
import asyncio, time, json, socket as sock
def free_tcp():
    s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return f'tcp://127.0.0.1:{{port}}'
import zmq, zmq.asyncio
async def run():
    payload = b'x' * {size}
    ctx = zmq.asyncio.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    pull.linger = 0
    push.linger = 0
    ep = free_tcp()
    pull.bind(ep)
    push.connect(ep)
    await asyncio.sleep(0.05)
    async def sender():
        for _ in range({n}):
            await push.send(payload)
    async def receiver():
        for _ in range({n}):
            await pull.recv()
    start = time.monotonic()
    await asyncio.gather(sender(), receiver())
    elapsed = time.monotonic() - start
    push.close()
    pull.close()
    print(json.dumps({n} / elapsed))
    import sys; sys.stdout.flush(); import os; os._exit(0)
asyncio.run(run())
"""
    try:
        r = subprocess.run([sys.executable, "-c", code],
                           capture_output=True, text=True, timeout=15)
    except subprocess.TimeoutExpired:
        return 0.0
    if r.returncode != 0:
        return 0.0
    return json.loads(r.stdout.strip())


def run_async_throughput():
    results = []
    for size in SIZES:
        label = fmt_size(size)
        sys.stdout.write(f"  {label:>7} ...")
        sys.stdout.flush()

        measure_async_pyomq(free_tcp(), size)
        measure_async_pyzmq(free_tcp(), size)

        tcp_omq = max(measure_async_pyomq(free_tcp(), size)
                      for _ in range(N_ROUNDS))
        tcp_pz = max(measure_async_pyzmq(free_tcp(), size)
                     for _ in range(N_ROUNDS))

        ratio = tcp_omq / tcp_pz if tcp_pz > 0 else 0
        results.append((label, tcp_omq, tcp_pz, ratio))
        print(f" pyomq {fmt_rate(tcp_omq):>10}  pyzmq {fmt_rate(tcp_pz):>10}  {ratio:.2f}x")

    return results


# ── sync REQ/REP latency ────────────────────────────────────────────

def measure_latency(lib, endpoint, size, warmup=LATENCY_WARMUP, iters=LATENCY_ITERS):
    payload = b"x" * size
    ctx = lib.Context() if hasattr(lib, "Context") else lib.Context.instance()
    rep = ctx.socket(lib.REP)
    req = ctx.socket(lib.REQ)
    rep.linger = 0
    req.linger = 0
    rep.bind(endpoint)
    req.connect(endpoint)
    time.sleep(0.05)

    def echo():
        try:
            for _ in range(warmup + iters + 100):
                msg = rep.recv()
                rep.send(msg)
        except Exception:
            pass

    t = threading.Thread(target=echo, daemon=True)
    t.start()

    for _ in range(warmup):
        req.send(payload)
        req.recv()

    rtts = []
    for _ in range(iters):
        t0 = time.monotonic()
        req.send(payload)
        req.recv()
        rtts.append(time.monotonic() - t0)

    req.close()
    rep.close()
    try:
        ctx.term()
    except Exception:
        pass

    rtts.sort()
    p50 = rtts[len(rtts) * 50 // 100] * 1e6
    p99 = rtts[len(rtts) * 99 // 100] * 1e6
    return p50, p99


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
    try:
        r = subprocess.run([sys.executable, "-c", code],
                           capture_output=True, text=True, timeout=60)
    except subprocess.TimeoutExpired:
        return (999999.0, 999999.0)
    if r.returncode != 0:
        return (999999.0, 999999.0)
    return tuple(json.loads(r.stdout.strip()))


def run_latency():
    import pyomq

    results = []
    for size in LATENCY_SIZES:
        label = fmt_size(size)
        sys.stdout.write(f"  {label:>7} ...")
        sys.stdout.flush()

        measure_latency(pyomq, free_tcp(), size, warmup=200, iters=200)
        _measure_latency_subprocess("pyzmq", size, 200, 200)

        omq_runs = [measure_latency(pyomq, free_tcp(), size) for _ in range(N_ROUNDS)]
        pz_runs = [_measure_latency_subprocess("pyzmq", size, LATENCY_WARMUP, LATENCY_ITERS)
                    for _ in range(N_ROUNDS)]
        omq_p50 = min(r[0] for r in omq_runs)
        omq_p99 = min(r[1] for r in omq_runs)
        pz_p50 = min(r[0] for r in pz_runs)
        pz_p99 = min(r[1] for r in pz_runs)

        p50_ratio = pz_p50 / omq_p50 if omq_p50 > 0 else 0
        p99_ratio = pz_p99 / omq_p99 if omq_p99 > 0 else 0

        results.append((label, omq_p50, pz_p50, p50_ratio, omq_p99, pz_p99, p99_ratio))
        print(f" p50 {p50_ratio:.2f}x  p99 {p99_ratio:.2f}x")

    return results


# ── async REQ/REP latency ───────────────────────────────────────────

def _measure_async_latency_subprocess(lib_name, size, warmup, iters):
    if lib_name == "pyzmq":
        lib_import = "import zmq; import zmq.asyncio; lib = zmq; actx = zmq.asyncio"
        close_expr = "sock.close()"
    else:
        lib_import = "import pyomq; import pyomq.asyncio as actx; lib = pyomq"
        close_expr = "await sock.close()"

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
    {"" if lib_name == "pyzmq" else "await "}rep.bind(ep)
    {"" if lib_name == "pyzmq" else "await "}req.connect(ep)
    await asyncio.sleep(0.05)
    async def echo():
        try:
            for _ in range({warmup} + {iters} + 100):
                msg = await rep.recv()
                await rep.send(msg)
        except Exception:
            pass
    task = asyncio.create_task(echo())
    for _ in range({warmup}):
        await req.send(payload)
        await req.recv()
    rtts = []
    for _ in range({iters}):
        t0 = time.monotonic()
        await req.send(payload)
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
    try:
        r = subprocess.run([sys.executable, "-c", code],
                           capture_output=True, text=True, timeout=60)
    except subprocess.TimeoutExpired:
        sys.stderr.write(f"  [{lib_name} async lat {size}B timeout]\n")
        return (999999.0, 999999.0)
    if r.returncode != 0:
        sys.stderr.write(f"  [{lib_name} async lat {size}B failed: {r.stderr[:200]}]\n")
        return (999999.0, 999999.0)
    return tuple(json.loads(r.stdout.strip()))


def run_async_latency():
    results = []
    for size in LATENCY_SIZES:
        label = fmt_size(size)
        sys.stdout.write(f"  {label:>7} ...")
        sys.stdout.flush()

        _measure_async_latency_subprocess("pyomq", size, 200, 200)
        _measure_async_latency_subprocess("pyzmq", size, 200, 200)

        omq_runs = [_measure_async_latency_subprocess("pyomq", size, LATENCY_WARMUP, LATENCY_ITERS)
                    for _ in range(N_ROUNDS)]
        pz_runs = [_measure_async_latency_subprocess("pyzmq", size, LATENCY_WARMUP, LATENCY_ITERS)
                   for _ in range(N_ROUNDS)]

        omq_p50 = min(r[0] for r in omq_runs)
        omq_p99 = min(r[1] for r in omq_runs)
        pz_p50 = min(r[0] for r in pz_runs)
        pz_p99 = min(r[1] for r in pz_runs)

        p50_ratio = pz_p50 / omq_p50 if omq_p50 > 0 else 0
        p99_ratio = pz_p99 / omq_p99 if omq_p99 > 0 else 0

        results.append((label, omq_p50, pz_p50, p50_ratio, omq_p99, pz_p99, p99_ratio))
        print(f" p50 {p50_ratio:.2f}x  p99 {p99_ratio:.2f}x")

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


def run_proxy():
    import pyomq
    import zmq as pyzmq

    sys.stdout.write("  PUSH/PULL ...")
    sys.stdout.flush()
    pp_omq = max(measure_proxy_pushpull(pyomq) for _ in range(N_ROUNDS))
    pp_pz = max(measure_proxy_pushpull(pyzmq) for _ in range(N_ROUNDS))
    pp_ratio = pp_omq / pp_pz
    print(f" {pp_ratio:.2f}x")

    sys.stdout.write("  REQ/REP ...")
    sys.stdout.flush()
    rr_omq = max(measure_proxy_reqrep(pyomq) for _ in range(N_ROUNDS))
    rr_pz = max(measure_proxy_reqrep(pyzmq) for _ in range(N_ROUNDS))
    rr_ratio = rr_omq / rr_pz
    print(f" {rr_ratio:.2f}x")

    return pp_omq, pp_pz, pp_ratio, rr_omq, rr_pz, rr_ratio


# ── SVG chart generation ────────────────────────────────────────────

# Colors: warm = pyomq, cool = pyzmq
C_PYOMQ = "#dc2626"
C_PYOMQ_ASYNC = "#f97316"
C_PYZMQ = "#2563eb"
C_PYZMQ_ASYNC = "#8b5cf6"

PLOT_LEFT = 90
PLOT_RIGHT = 760
PLOT_TOP = 45
PLOT_BOT = 350
PLOT_W = PLOT_RIGHT - PLOT_LEFT
PLOT_H = PLOT_BOT - PLOT_TOP


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


def _y_pos(val, y_max):
    frac = val / y_max if y_max > 0 else 0
    return PLOT_BOT - frac * PLOT_H


def _y_pos_log(val, log_min, log_max):
    if val <= 0:
        return PLOT_BOT
    lv = math.log10(val)
    frac = (lv - log_min) / (log_max - log_min) if log_max > log_min else 0
    return PLOT_BOT - frac * PLOT_H


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


def _svg_header(title):
    return [
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 850 440"'
        ' font-family="system-ui, -apple-system, sans-serif">',
        '  <rect width="850" height="440" fill="white"/>',
    ]


def _svg_x_grid_and_labels(xs, x_labels):
    lines = []
    for x in xs:
        lines.append(f'  <line x1="{x:.1f}" y1="{PLOT_TOP}" x2="{x:.1f}"'
                     f' y2="{PLOT_BOT}" stroke="#e5e7eb" stroke-width="1"/>')
    for i, label in enumerate(x_labels):
        lines.append(f'  <text x="{xs[i]:.1f}" y="366" text-anchor="middle"'
                     f' fill="#374151" font-size="9.5">{label}</text>')
    return lines


def _svg_axes_border():
    return [
        f'  <line x1="{PLOT_LEFT}" y1="{PLOT_TOP}" x2="{PLOT_LEFT}"'
        f' y2="{PLOT_BOT}" stroke="#9ca3af" stroke-width="1.5"/>',
        f'  <line x1="{PLOT_RIGHT}" y1="{PLOT_TOP}" x2="{PLOT_RIGHT}"'
        f' y2="{PLOT_BOT}" stroke="#9ca3af" stroke-width="1.5"/>',
        f'  <line x1="{PLOT_LEFT}" y1="{PLOT_BOT}" x2="{PLOT_RIGHT}"'
        f' y2="{PLOT_BOT}" stroke="#9ca3af" stroke-width="1.5"/>',
    ]


def _svg_legend(series_info, y=392):
    """series_info: [(name, color, dash), ...]"""
    lines = []
    cx = (PLOT_LEFT + PLOT_RIGHT) / 2
    n = len(series_info)
    total_w = n * 120
    x0 = cx - total_w / 2
    for idx, (name, color, dash) in enumerate(series_info):
        lx = x0 + idx * 120
        da = f' stroke-dasharray="{dash}"' if dash else ""
        lines.append(f'  <line x1="{lx:.0f}" y1="{y}" x2="{lx + 14:.0f}" y2="{y}"'
                     f' stroke="{color}" stroke-width="2.5"{da}/>')
        lines.append(f'  <text x="{lx + 18:.0f}" y="{y + 4}" fill="#374151"'
                     f' font-size="10" font-weight="500">{name}</text>')
    return lines


def _svg_polyline(xs, vals, y_fn, color, dash, with_dots=True, width="2.5"):
    lines = []
    pts = " ".join(f"{xs[i]:.1f},{y_fn(v):.1f}" for i, v in enumerate(vals))
    da = f' stroke-dasharray="{dash}"' if dash else ""
    lines.append(f'  <polyline points="{pts}" fill="none" stroke="{color}"'
                 f' stroke-width="{width}" stroke-linecap="round"'
                 f' stroke-linejoin="round"{da}/>')
    if with_dots:
        for i, v in enumerate(vals):
            y = y_fn(v)
            lines.append(f'  <circle cx="{xs[i]:.1f}" cy="{y:.1f}" r="3"'
                         f' fill="{color}" stroke="white" stroke-width="1"/>')
    return lines


def render_throughput_svg(title, x_labels, series_msg, sizes):
    """Dual-axis throughput chart.

    Left axis:  log-scale msg/s (solid lines with dots)
    Right axis: linear MB/s     (dashed lines, no dots)

    series_msg: [(name, color, msg_s_values)]
    sizes:      message sizes in bytes (parallel to values)
    """
    n = len(x_labels)
    xs = [PLOT_LEFT + i * PLOT_W / (n - 1) for i in range(n)]

    all_rates = [v for _, _, vals in series_msg for v in vals if v > 0]
    log_min = math.floor(math.log10(min(all_rates)))
    log_max = math.ceil(math.log10(max(all_rates)))

    all_mbps = [v * sizes[i] / 1e6
                for _, _, vals in series_msg
                for i, v in enumerate(vals) if v > 0]
    mbps_max = _nice_ceil(max(all_mbps)) if all_mbps else 1

    lines = _svg_header(title)

    # Left Y grid (log msg/s) — decades + intermediate ticks (2× and 5×)
    for decade in range(log_min, log_max + 1):
        for mult in [1, 2, 5]:
            val = mult * 10 ** decade
            if val < 10 ** log_min or val > 10 ** log_max:
                continue
            y = _y_pos_log(val, log_min, log_max)
            is_decade = mult == 1
            lines.append(f'  <line x1="{PLOT_LEFT}" y1="{y:.1f}" x2="{PLOT_RIGHT}"'
                         f' y2="{y:.1f}" stroke="{"#e5e7eb" if is_decade else "#f0f0f0"}"'
                         f' stroke-width="{"1" if is_decade else "0.5"}"/>')
            lines.append(f'  <text x="{PLOT_LEFT - 8}" y="{y:.1f}" text-anchor="end"'
                         f' dominant-baseline="middle"'
                         f' fill="{"#374151" if is_decade else "#9ca3af"}"'
                         f' font-size="{"10" if is_decade else "8"}">'
                         f'{_fmt_y_rate(val)}</text>')

    # Right Y labels (linear MB/s) — 5 evenly spaced ticks
    n_r_ticks = 5
    for i in range(n_r_ticks + 1):
        mbps_val = i * mbps_max / n_r_ticks
        frac = mbps_val / mbps_max if mbps_max > 0 else 0
        y = PLOT_BOT - frac * PLOT_H
        lines.append(f'  <line x1="{PLOT_LEFT}" y1="{y:.1f}" x2="{PLOT_RIGHT}"'
                     f' y2="{y:.1f}" stroke="#e5e7eb" stroke-width="1"'
                     f' stroke-dasharray="3,6"/>')
        lines.append(f'  <text x="{PLOT_RIGHT + 8}" y="{y:.1f}" text-anchor="start"'
                     f' dominant-baseline="middle" fill="#6b7280"'
                     f' font-size="10">{_fmt_mbps(mbps_val)}</text>')

    lines += _svg_x_grid_and_labels(xs, x_labels)
    lines += _svg_axes_border()

    # Y-axis labels
    mid_y = (PLOT_TOP + PLOT_BOT) / 2
    lines.append(f'  <text x="40" y="{mid_y:.0f}" text-anchor="middle"'
                 f' dominant-baseline="middle" fill="#374151" font-size="11"'
                 f' font-weight="600" transform="rotate(-90,40,{mid_y:.0f})">'
                 f'msg/s (log)</text>')
    lines.append(f'  <text x="830" y="{mid_y:.0f}" text-anchor="middle"'
                 f' dominant-baseline="middle" fill="#6b7280" font-size="11"'
                 f' font-weight="600" transform="rotate(90,830,{mid_y:.0f})">'
                 f'throughput</text>')

    cx = (PLOT_LEFT + PLOT_RIGHT) / 2
    lines.append(f'  <text x="{cx:.1f}" y="22" text-anchor="middle" fill="#111827"'
                 f' font-size="14" font-weight="700">{title}</text>')

    # Plot: dashed MB/s first (behind), then solid msg/s on top
    for name, color, vals in series_msg:
        mbps = [v * sizes[i] / 1e6 for i, v in enumerate(vals)]

        def mbps_y(v):
            frac = v / mbps_max if mbps_max > 0 else 0
            return PLOT_BOT - frac * PLOT_H

        lines += _svg_polyline(xs, mbps, mbps_y, color, "6,4",
                               with_dots=False, width="2")

    for name, color, vals in series_msg:
        def log_y(v, _lmin=log_min, _lmax=log_max):
            return _y_pos_log(v, _lmin, _lmax)

        lines += _svg_polyline(xs, vals, log_y, color, None,
                               with_dots=True, width="2.5")

    legend = [(name, color, None) for name, color, _ in series_msg]
    lines += _svg_legend(legend, y=388)

    lines.append(f'  <text x="{cx:.1f}" y="418" text-anchor="middle"'
                 f' fill="#9ca3af" font-size="9">'
                 f'solid = msg/s (left, log) · dashed = throughput (right)</text>')

    lines.append("</svg>")
    return "\n".join(lines)


def render_latency_svg(title, x_labels, series, y_label="p50 latency"):
    """Simple linear-axis latency chart."""
    n = len(x_labels)
    xs = [PLOT_LEFT + i * PLOT_W / (n - 1) for i in range(n)]

    y_max = 200
    y_step = 20

    lines = _svg_header(title)

    n_y_ticks = int(y_max / y_step)
    for i in range(n_y_ticks + 1):
        val = i * y_step
        y = _y_pos(val, y_max)
        lines.append(f'  <line x1="{PLOT_LEFT}" y1="{y:.1f}" x2="{PLOT_RIGHT}"'
                     f' y2="{y:.1f}" stroke="#e5e7eb" stroke-width="1"/>')
        lines.append(f'  <text x="{PLOT_LEFT - 8}" y="{y:.1f}" text-anchor="end"'
                     f' dominant-baseline="middle" fill="#374151"'
                     f' font-size="10">{_fmt_y_us(val)}</text>')

    lines += _svg_x_grid_and_labels(xs, x_labels)
    lines += _svg_axes_border()

    mid_y = (PLOT_TOP + PLOT_BOT) / 2
    lines.append(f'  <text x="30" y="{mid_y:.0f}" text-anchor="middle"'
                 f' dominant-baseline="middle" fill="#374151" font-size="11"'
                 f' font-weight="600" transform="rotate(-90,30,{mid_y:.0f})">'
                 f'{y_label}</text>')

    cx = (PLOT_LEFT + PLOT_RIGHT) / 2
    lines.append(f'  <text x="{cx:.1f}" y="22" text-anchor="middle" fill="#111827"'
                 f' font-size="14" font-weight="700">{title}</text>')

    for name, color, dash, vals in series:
        def lin_y(v, _ymax=y_max):
            return _y_pos(v, _ymax)

        lines += _svg_polyline(xs, vals, lin_y, color, dash,
                               with_dots=True, width="2.5")

    legend = [(name, color, dash) for name, color, dash, _ in series]
    lines += _svg_legend(legend)

    lines.append("</svg>")
    return "\n".join(lines)


def gen_throughput_chart(sync_results, async_results, path):
    x_labels = [fmt_size(s) for s in SIZES]
    sync_omq = [r[4] for r in sync_results]
    sync_pz = [r[5] for r in sync_results]
    async_omq = [r[1] for r in async_results]
    async_pz = [r[2] for r in async_results]

    series_msg = [
        ("pyomq", C_PYOMQ, sync_omq),
        ("pyomq async", C_PYOMQ_ASYNC, async_omq),
        ("pyzmq", C_PYZMQ, sync_pz),
        ("pyzmq async", C_PYZMQ_ASYNC, async_pz),
    ]

    svg = render_throughput_svg(
        title="PUSH/PULL throughput: TCP loopback (Python bindings)",
        x_labels=x_labels,
        series_msg=series_msg,
        sizes=SIZES,
    )

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(svg)
        f.write("\n")
    print(f"  wrote {path}")


def gen_latency_chart(sync_results, async_results, path):
    x_labels = [fmt_size(s) for s in LATENCY_SIZES]
    sync_omq = [r[1] for r in sync_results]
    sync_pz = [r[2] for r in sync_results]
    async_omq = [r[1] for r in async_results]
    async_pz = [r[2] for r in async_results]

    series = [
        ("pyomq", C_PYOMQ, None, sync_omq),
        ("pyomq async", C_PYOMQ_ASYNC, "6,4", async_omq),
        ("pyzmq", C_PYZMQ, None, sync_pz),
        ("pyzmq async", C_PYZMQ_ASYNC, "6,4", async_pz),
    ]

    svg = render_latency_svg(
        title="REQ/REP latency: TCP loopback, p50 (Python bindings)",
        x_labels=x_labels,
        series=series,
    )

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(svg)
        f.write("\n")
    print(f"  wrote {path}")


# ── README tables ────────────────────────────────────────────────────

def build_throughput_table(results):
    lines = [
        "| Size    | inproc pyomq | inproc pyzmq | ratio     "
        "| tcp pyomq | tcp pyzmq | ratio     |",
        "|---------|-------------:|-------------:|----------:"
        "|----------:|----------:|----------:|",
    ]
    for label, i_omq, i_pz, i_r, t_omq, t_pz, t_r in results:
        lines.append(
            f"| {label:<7} | {fmt_rate(i_omq):>12} | {fmt_rate(i_pz):>12} "
            f"| **{i_r:.2f}×** "
            f"| {fmt_rate(t_omq):>9} | {fmt_rate(t_pz):>9} "
            f"| **{t_r:.2f}×** |"
        )
    return "\n".join(lines)


def build_latency_table(results):
    lines = [
        "| Size    | pyomq p50 | pyzmq p50 | ratio     "
        "| pyomq p99 | pyzmq p99 | ratio     |",
        "|---------|----------:|----------:|----------:"
        "|----------:|----------:|----------:|",
    ]
    for label, op50, pp50, r50, op99, pp99, r99 in results:
        r50s = f"**{r50:.2f}×**" if r50 >= 1.1 else f"{r50:.2f}×"
        r99s = f"**{r99:.2f}×**" if r99 >= 1.1 else f"{r99:.2f}×"
        lines.append(
            f"| {label:<7} | {fmt_us(op50):>9} | {fmt_us(pp50):>9} "
            f"| {r50s:>9} "
            f"| {fmt_us(op99):>9} | {fmt_us(pp99):>9} "
            f"| {r99s:>9} |"
        )
    return "\n".join(lines)


def build_proxy_table(pp_omq, pp_pz, pp_ratio, rr_omq, rr_pz, rr_ratio):
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
    print("Measuring sync PUSH/PULL throughput...")
    tp_results = run_throughput()
    tp_table = build_throughput_table(tp_results)

    print("\nMeasuring async PUSH/PULL throughput...")
    atp_results = run_async_throughput()

    print("\nMeasuring sync REQ/REP latency (TCP)...")
    lat_results = run_latency()
    lat_table = build_latency_table(lat_results)

    print("\nMeasuring async REQ/REP latency (TCP)...")
    alat_results = run_async_latency()

    print("\nMeasuring zmq.proxy() forwarding...")
    proxy_results = run_proxy()
    proxy_table = build_proxy_table(*proxy_results)

    print()
    print(tp_table)
    print()
    print(lat_table)
    print()
    print(proxy_table)

    with open(README) as f:
        content = f.read()

    content = update_marker(content, "PERF", tp_table)
    content = update_marker(content, "LATENCY_PERF", lat_table)
    content = update_marker(content, "PROXY_PERF", proxy_table)

    with open(README, "w") as f:
        f.write(content)
    print(f"\nUpdated {README}")

    print("\nGenerating charts...")
    gen_throughput_chart(tp_results, atp_results,
                        os.path.join(CHART_DIR, "throughput_bindings.svg"))
    gen_latency_chart(lat_results, alat_results,
                      os.path.join(CHART_DIR, "latency_bindings.svg"))


if __name__ == "__main__":
    main()

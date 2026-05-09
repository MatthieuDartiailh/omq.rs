#!/usr/bin/env python3
"""Measure pyomq vs pyzmq throughput and update the README tables.

Run from the pyomq root (bindings/pyomq/) after `maturin develop --release`.
"""

import os
import re
import sys
import threading
import time

SIZES = [8, 32, 128, 512, 2048, 8192, 32768, 131072]
TARGET_RUNTIME_S = 0.4
N_ROUNDS = 3
README = os.path.join(os.path.dirname(__file__), "..", "README.md")


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


# ── PUSH/PULL throughput ─────────────────────────────────────────────

def measure(lib, endpoint, size, n_target_per_s=200_000):
    payload = b"x" * size
    ctx = lib.Context() if hasattr(lib, "Context") else lib.Context.instance()
    pull = ctx.socket(lib.PULL)
    push = ctx.socket(lib.PUSH)
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


def run_throughput():
    import pyomq
    import zmq as pyzmq

    results = []
    for size in SIZES:
        label = fmt_size(size)
        sys.stdout.write(f"  {label:>7} ...")
        sys.stdout.flush()

        # warmup
        measure(pyomq, free_inproc(f"w-omq-{size}"), size)
        measure(pyzmq, f"inproc://w-pz-{size}-{time.monotonic_ns()}", size)
        measure(pyomq, free_tcp(), size)
        measure(pyzmq, free_tcp(), size)

        # measure
        inproc_omq = max(
            measure(pyomq, free_inproc(f"omq-{size}-{i}"), size)
            for i in range(N_ROUNDS)
        )
        inproc_pz = max(
            measure(pyzmq, f"inproc://pz-{size}-{i}-{time.monotonic_ns()}", size)
            for i in range(N_ROUNDS)
        )
        tcp_omq = max(measure(pyomq, free_tcp(), size) for _ in range(N_ROUNDS))
        tcp_pz = max(measure(pyzmq, free_tcp(), size) for _ in range(N_ROUNDS))

        inproc_ratio = inproc_omq / inproc_pz
        tcp_ratio = tcp_omq / tcp_pz

        results.append((label, inproc_omq, inproc_pz, inproc_ratio,
                         tcp_omq, tcp_pz, tcp_ratio))

        print(f" inproc {inproc_ratio:.2f}x  tcp {tcp_ratio:.2f}x")

    return results


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


# ── proxy forwarding ─────────────────────────────────────────────────

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
        target=lib.proxy, args=(frontend, backend), daemon=True,
    )
    proxy_t.start()
    time.sleep(0.05)

    # warmup
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
        target=lib.proxy, args=(frontend, backend), daemon=True,
    )
    proxy_t.start()
    time.sleep(0.05)

    # warmup
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
    print("Measuring PUSH/PULL throughput...")
    tp_results = run_throughput()
    tp_table = build_throughput_table(tp_results)

    print("\nMeasuring zmq.proxy() forwarding...")
    proxy_results = run_proxy()
    proxy_table = build_proxy_table(*proxy_results)

    print()
    print(tp_table)
    print()
    print(proxy_table)

    with open(README) as f:
        content = f.read()

    content = update_marker(content, "PERF", tp_table)
    content = update_marker(content, "PROXY_PERF", proxy_table)

    with open(README, "w") as f:
        f.write(content)

    print(f"\nUpdated {README}")


if __name__ == "__main__":
    main()

"""Performance measurement: pyomq PUSH/PULL throughput vs pyzmq.

No assertions — just measure and print. Run `scripts/update_perf.py`
to re-measure and update the README table automatically.
"""

import threading
import time

import pytest

zmq_pyzmq = pytest.importorskip("zmq")  # pyzmq

import pyomq

SIZES = [8, 32, 128, 512, 2048, 8192, 32768, 131072]
TARGET_RUNTIME_S = 0.4


def _measure_pyomq(endpoint: str, size: int, n_target_per_s: int = 200_000) -> float:
    payload = b"x" * size
    ctx = pyomq.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    pull.bind(endpoint)
    push.connect(endpoint)

    def sender(n):
        for _ in range(n):
            push.send(payload)

    n = max(int(n_target_per_s * TARGET_RUNTIME_S), 100)
    t = threading.Thread(target=sender, args=(n,))
    start = time.monotonic()
    t.start()
    received = 0
    while received < n:
        pull.recv()
        received += 1
    elapsed = time.monotonic() - start
    t.join()

    push.close()
    pull.close()
    ctx.term()
    return n / elapsed


def _measure_pyzmq(endpoint: str, size: int, n_target_per_s: int = 200_000) -> float:
    payload = b"x" * size
    ctx = zmq_pyzmq.Context.instance()
    pull = ctx.socket(zmq_pyzmq.PULL)
    push = ctx.socket(zmq_pyzmq.PUSH)
    pull.bind(endpoint)
    push.connect(endpoint)

    def sender(n):
        for _ in range(n):
            push.send(payload)

    n = max(int(n_target_per_s * TARGET_RUNTIME_S), 100)
    t = threading.Thread(target=sender, args=(n,))
    start = time.monotonic()
    t.start()
    received = 0
    while received < n:
        pull.recv()
        received += 1
    elapsed = time.monotonic() - start
    t.join()

    push.close()
    pull.close()
    return n / elapsed


def _free_inproc(label: str) -> str:
    return f"inproc://perf-{label}-{time.monotonic_ns()}"


@pytest.mark.parametrize("size", SIZES)
def test_perf_inproc(size):
    _measure_pyomq(_free_inproc(f"warm-pyomq-{size}"), size)
    _measure_pyzmq(f"inproc://warm-pyzmq-{size}-{time.monotonic_ns()}", size)
    runs_omq = [_measure_pyomq(_free_inproc(f"omq-{size}-{i}"), size) for i in range(2)]
    runs_pz = [
        _measure_pyzmq(f"inproc://pz-{size}-{i}-{time.monotonic_ns()}", size)
        for i in range(2)
    ]
    omq = max(runs_omq)
    pz = max(runs_pz)
    ratio = omq / pz
    print(
        f"[perf inproc {size:>5}B]  pyomq {omq:>10,.0f} msg/s  "
        f"pyzmq {pz:>10,.0f} msg/s  ratio {ratio:.2f}x"
    )


def _free_tcp_port_local() -> int:
    import socket as _so
    s = _so.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _new_tcp_ep() -> str:
    return f"tcp://127.0.0.1:{_free_tcp_port_local()}"


@pytest.mark.parametrize("size", SIZES)
def test_perf_tcp(size):
    _measure_pyomq(_new_tcp_ep(), size)
    _measure_pyzmq(_new_tcp_ep(), size)
    omq_runs = [_measure_pyomq(_new_tcp_ep(), size) for _ in range(2)]
    pz_runs = [_measure_pyzmq(_new_tcp_ep(), size) for _ in range(2)]
    omq = max(omq_runs)
    pz = max(pz_runs)
    ratio = omq / pz
    print(
        f"[perf tcp    {size:>5}B]  pyomq {omq:>10,.0f} msg/s  "
        f"pyzmq {pz:>10,.0f} msg/s  ratio {ratio:.2f}x"
    )

"""Smoke test: inproc PUSH/PULL throughput stays above 500k msg/s at 64B."""

import threading
import time

import pyomq as zmq


def test_inproc_throughput_above_500k():
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    ep = f"inproc://tp-{time.monotonic_ns()}"
    pull.bind(ep)
    push.connect(ep)

    n = 40_000
    payload = b"x" * 64

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

    rate = n / elapsed
    assert rate > 800_000, f"inproc throughput {rate/1e6:.2f}M msg/s, expected >0.8M"

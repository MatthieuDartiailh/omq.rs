"""Soak: reconnect storm.

PUSH connects to a TCP port. PULL binds, exchanges one message,
closes. New PULL rebinds, PUSH reconnects. Repeated for the full
duration. Exercises the PyO3 socket creation/teardown path.
"""

import time

import pyomq as zmq

from conftest import ResourceMonitor, free_tcp_port, soak_duration


def test_reconnect_storm():
    duration = soak_duration()
    monitor = ResourceMonitor()

    port = free_tcp_port()
    ep = f"tcp://127.0.0.1:{port}"

    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    push.setsockopt(zmq.SNDTIMEO, 5000)
    push.setsockopt(zmq.RECONNECT_IVL, 10)
    push.connect(ep)

    start = time.monotonic()
    cycles = 0
    delivered = 0
    last_log = start

    while time.monotonic() - start < duration:
        pull = ctx.socket(zmq.PULL)
        pull.setsockopt(zmq.RCVTIMEO, 5000)

        bound = False
        for _ in range(40):
            try:
                pull.bind(ep)
                bound = True
                break
            except zmq.ZMQError:
                time.sleep(0.025)
        if not bound:
            pull.close()
            continue

        tag = f"c-{cycles}".encode()
        try:
            push.send(tag)
        except zmq.Again:
            pull.close()
            cycles += 1
            continue

        try:
            msg = pull.recv()
            assert msg == tag
            delivered += 1
        except zmq.Again:
            pass

        pull.close()
        cycles += 1

        if time.monotonic() - last_log >= 30:
            elapsed = time.monotonic() - start
            print(
                f"[reconnect_storm] {elapsed:.0f}s, "
                f"cycles {cycles}, delivered {delivered}"
            )
            last_log = time.monotonic()

    push.close()
    ctx.term()

    pct = delivered / cycles * 100 if cycles else 100
    print(
        f"[reconnect_storm] done: {delivered}/{cycles} "
        f"delivered ({pct:.1f}%) in {duration:.1f}s"
    )
    report = monitor.stop()
    report.assert_no_leak("reconnect_storm")

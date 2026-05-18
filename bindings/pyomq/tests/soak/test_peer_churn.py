"""Soak: peer churn under sustained PUSH load.

PUSH bound on TCP, continuous send. PULL peers connect, receive
briefly, disconnect. Varies 0-20 concurrent peers.
"""

import random
import time

import pyomq as zmq

from conftest import ResourceMonitor, soak_duration, tcp_ep


def test_peer_churn():
    duration = soak_duration()
    monitor = ResourceMonitor()

    ep = tcp_ep()
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    push.setsockopt(zmq.SNDTIMEO, 1)
    push.setsockopt(zmq.SNDHWM, 1024)
    push.bind(ep)

    initial = ctx.socket(zmq.PULL)
    initial.connect(ep)
    time.sleep(0.1)

    peers = [initial]
    sent = 0
    start = time.monotonic()
    last_log = start

    while time.monotonic() - start < duration:
        action = random.randrange(10)
        if action < 3 and len(peers) < 20:
            p = ctx.socket(zmq.PULL)
            p.connect(ep)
            peers.append(p)
        elif action < 5 and len(peers) > 1:
            idx = random.randrange(len(peers))
            peers[idx].close()
            peers.pop(idx)

        for _ in range(100):
            try:
                push.send(b"soak")
                sent += 1
            except zmq.Again:
                break

        for p in peers:
            p.setsockopt(zmq.RCVTIMEO, 0)
            try:
                while True:
                    p.recv()
            except zmq.Again:
                pass

        now = time.monotonic()
        if now - last_log >= 30:
            print(
                f"[peer_churn] {now - start:.0f}s, "
                f"sent {sent}, peers {len(peers)}"
            )
            last_log = now

    for p in peers:
        p.close()
    push.close()
    ctx.term()

    print(f"[peer_churn] done: {sent} messages in {duration:.1f}s")

    report = monitor.stop()
    report.assert_no_leak("peer_churn")

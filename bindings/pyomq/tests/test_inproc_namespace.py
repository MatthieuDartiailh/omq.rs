"""Context-local inproc namespace isolation."""

import pyomq as zmq


def test_two_contexts_same_inproc_name():
    ctx1 = zmq.Context()
    ctx2 = zmq.Context()
    try:
        s1 = ctx1.socket(zmq.PUSH)
        s2 = ctx2.socket(zmq.PUSH)
        s1.bind("inproc://shared-name")
        s2.bind("inproc://shared-name")
    finally:
        s1.close()
        s2.close()
        ctx1.term()
        ctx2.term()


def test_inproc_namespaced_roundtrip():
    ctx = zmq.Context()
    try:
        push = ctx.socket(zmq.PUSH)
        pull = ctx.socket(zmq.PULL)
        pull.bind("inproc://ns-test")
        push.connect("inproc://ns-test")
        push.send(b"namespaced")
        assert pull.recv() == b"namespaced"
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_inproc_cross_context_isolation():
    """Messages sent in ctx1's inproc don't leak to ctx2's same-name inproc."""
    ctx1 = zmq.Context()
    ctx2 = zmq.Context()
    try:
        push1 = ctx1.socket(zmq.PUSH)
        pull1 = ctx1.socket(zmq.PULL)
        pull1.bind("inproc://isolated")
        push1.connect("inproc://isolated")

        push2 = ctx2.socket(zmq.PUSH)
        pull2 = ctx2.socket(zmq.PULL)
        pull2.bind("inproc://isolated")
        push2.connect("inproc://isolated")

        push1.send(b"ctx1")
        push2.send(b"ctx2")

        assert pull1.recv() == b"ctx1"
        assert pull2.recv() == b"ctx2"
    finally:
        for s in (push1, pull1, push2, pull2):
            s.close()
        ctx1.term()
        ctx2.term()

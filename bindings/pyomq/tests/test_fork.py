"""Verify sockets re-materialize correctly after fork."""

import multiprocessing
import os
import time

import pyomq as zmq


def _child_recv(ep, result_fd):
    """Run in forked child: connect, recv one message, write result."""
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    pull.connect(ep)
    msg = pull.recv()
    os.write(result_fd, msg)
    pull.close()
    os._exit(0)


def test_socket_works_after_fork():
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    ep = push.bind("tcp://127.0.0.1:0")

    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(r)
        _child_recv(ep, w)
    else:
        os.close(w)
        time.sleep(0.1)
        push.send(b"after-fork")
        data = os.read(r, 1024)
        os.close(r)
        os.waitpid(pid, 0)
        push.close()
        assert data == b"after-fork"


def test_pre_materialized_socket_works_after_fork():
    """Socket materialized before fork gets fresh state in child."""
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    ep = pull.bind("tcp://127.0.0.1:0")
    push.connect(ep)
    time.sleep(0.05)

    # Materialize by sending a message before fork.
    push.send(b"pre-fork")
    assert pull.recv() == b"pre-fork"

    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(r)
        # Child: the parent's push socket is stale. Create a fresh one.
        child_ctx = zmq.Context()
        child_push = child_ctx.socket(zmq.PUSH)
        child_push.connect(ep)
        time.sleep(0.05)
        child_push.send(b"child-msg")
        child_push.close()
        os._exit(0)
    else:
        os.close(w)
        msg = pull.recv()
        os.waitpid(pid, 0)
        os.close(r)
        push.close()
        pull.close()
        assert msg == b"child-msg"

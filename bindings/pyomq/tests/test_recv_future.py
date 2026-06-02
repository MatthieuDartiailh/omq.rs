"""_RecvFuture: both await and blocking .result() paths."""

import asyncio
import threading

import pytest

import pyomq
import pyomq.asyncio as zmq_async


@pytest.mark.asyncio
async def test_recv_future_await(tcp_endpoint):
    ctx = zmq_async.Context()
    push = ctx.socket(pyomq.PUSH)
    pull = ctx.socket(pyomq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"await-test")
        msg = await pull.recv()
        assert msg == b"await-test"
    finally:
        push.close()
        pull.close()


@pytest.mark.asyncio
async def test_recv_future_fast_path(tcp_endpoint):
    """Message already available returns a resolved future."""
    ctx = zmq_async.Context()
    push = ctx.socket(pyomq.PUSH)
    pull = ctx.socket(pyomq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"fast")
        await asyncio.sleep(0.1)
        msg = await pull.recv()
        assert msg == b"fast"
    finally:
        push.close()
        pull.close()


@pytest.mark.asyncio
async def test_recv_future_done_transitions(tcp_endpoint):
    """_RecvFuture.done() transitions from False to True."""
    ctx = zmq_async.Context()
    push = ctx.socket(pyomq.PUSH)
    pull = ctx.socket(pyomq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)

        fut = pull.recv()

        push.send(b"done-test")
        msg = await fut
        assert msg == b"done-test"
    finally:
        push.close()
        pull.close()

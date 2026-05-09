"""Async wrapper parity tests."""

import pytest

import pyomq as zmq
import pyomq.asyncio as zmq_async


async def test_async_again_exception(tcp_endpoint):
    import asyncio
    ctx = zmq_async.Context()
    pull = ctx.socket(zmq.PULL)
    try:
        await pull.bind(tcp_endpoint)
        await pull.close()
        with pytest.raises(zmq.ContextTerminated):
            await pull.recv()
    finally:
        pass


async def test_async_send_recv_string(tcp_endpoint):
    ctx = zmq_async.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        await pull.bind(tcp_endpoint)
        await push.connect(tcp_endpoint)
        await push.send_string("hello")
        assert await pull.recv_string() == "hello"
    finally:
        await push.close()
        await pull.close()


async def test_async_send_recv_json(tcp_endpoint):
    ctx = zmq_async.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        await pull.bind(tcp_endpoint)
        await push.connect(tcp_endpoint)
        await push.send_json({"k": 1})
        assert await pull.recv_json() == {"k": 1}
    finally:
        await push.close()
        await pull.close()


async def test_async_send_recv_pyobj(tcp_endpoint):
    ctx = zmq_async.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        await pull.bind(tcp_endpoint)
        await push.connect(tcp_endpoint)
        await push.send_pyobj([1, 2, 3])
        assert await pull.recv_pyobj() == [1, 2, 3]
    finally:
        await push.close()
        await pull.close()


async def test_async_closed_property(tcp_endpoint):
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.PUSH)
    assert sock.closed is False
    await sock.close()
    assert sock.closed is True


async def test_async_context_property():
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.PUSH)
    assert sock.context is ctx
    await sock.close()


async def test_async_socket_type():
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.PUSH)
    assert sock.socket_type == zmq.PUSH
    await sock.close()

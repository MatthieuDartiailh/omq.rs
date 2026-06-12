"""_ShadowSocket and Socket.shadow() tests."""

import sys

import pytest

import pyomq as zmq
import pyomq.asyncio as zmq_async

# Shadow socket requires select() on file descriptors, which doesn't work
# with Windows socket handles. Shadow socket is a Tier 2 feature on Windows.
pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="Shadow socket not supported on Windows"
)


@pytest.mark.asyncio
async def test_shadow_of_sync_returns_self():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        result = zmq.Socket.shadow(sock)
        assert result is sock
    finally:
        sock.close()
        ctx.term()


@pytest.mark.asyncio
async def test_shadow_of_async_returns_shadow_socket(tcp_endpoint):
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        sock.bind(tcp_endpoint)
        shadow = zmq.Socket.shadow(sock)
        assert shadow is not sock
        assert not shadow.closed
    finally:
        shadow.close()
        sock.close()


@pytest.mark.asyncio
async def test_shadow_recv(tcp_endpoint):
    ctx = zmq_async.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"hello")

        shadow = zmq.Socket.shadow(pull)
        msg = shadow.recv()
        assert msg == b"hello"
    finally:
        shadow.close()
        push.close()
        pull.close()


@pytest.mark.asyncio
async def test_shadow_recv_multipart(tcp_endpoint):
    ctx = zmq_async.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send_multipart([b"a", b"b"])

        shadow = zmq.Socket.shadow(pull)
        parts = shadow.recv_multipart()
        assert parts == [b"a", b"b"]
    finally:
        shadow.close()
        push.close()
        pull.close()


@pytest.mark.asyncio
async def test_shadow_send(tcp_endpoint):
    ctx = zmq_async.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)

        shadow = zmq.Socket.shadow(push)
        shadow.send(b"from-shadow")

        msg = await pull.recv()
        assert msg == b"from-shadow"
    finally:
        shadow.close()
        push.close()
        pull.close()


@pytest.mark.asyncio
async def test_shadow_getsockopt_setsockopt(tcp_endpoint):
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        sock.bind(tcp_endpoint)
        shadow = zmq.Socket.shadow(sock)
        shadow.setsockopt(zmq.LINGER, 100)
        assert shadow.getsockopt(zmq.LINGER) == 100
    finally:
        shadow.close()
        sock.close()


@pytest.mark.asyncio
async def test_shadow_socket_type(tcp_endpoint):
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.PULL)
    try:
        sock.bind(tcp_endpoint)
        shadow = zmq.Socket.shadow(sock)
        assert shadow.socket_type == zmq.PULL
    finally:
        shadow.close()
        sock.close()

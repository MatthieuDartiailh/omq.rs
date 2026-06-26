"""_ShadowSocket, Socket.shadow(), and Context.shadow() tests."""

import pytest

import pyomq as zmq
import pyomq.asyncio as zmq_async


# ── Context.shadow ──────────────────────────────────────────────────


def test_context_shadow_shares_native():
    ctx = zmq.Context()
    try:
        shadow = zmq.Context.shadow(ctx)
        assert shadow is not ctx
        assert shadow._ctx is ctx._ctx
    finally:
        shadow.term()
        ctx.term()


def test_context_shadow_creates_sockets():
    ctx = zmq.Context()
    try:
        shadow = zmq.Context.shadow(ctx)
        sock = shadow.socket(zmq.PUSH)
        assert sock.socket_type == zmq.PUSH
        sock.close()
    finally:
        shadow.term()
        ctx.term()


def test_context_shadow_term_does_not_destroy_original():
    ctx = zmq.Context()
    shadow = zmq.Context.shadow(ctx)
    shadow.term()
    assert shadow.closed
    sock = ctx.socket(zmq.PUSH)
    sock.close()
    ctx.term()


def test_context_shadow_int_uses_instance():
    shadow = zmq.Context.shadow(0)
    assert shadow._ctx is zmq.Context.instance()._ctx
    shadow.term()


# ── Socket.shadow ───────────────────────────────────────────────────


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

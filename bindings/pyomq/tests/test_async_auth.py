"""Async API authentication tests for CURVE and BLAKE3ZMQ."""

import pytest

import pyomq
import pyomq.asyncio as zmq_async


# ── CURVE ────────────────────────────────────────────────────────────


@pytest.mark.skipif(not pyomq.has("curve"), reason="curve feature not compiled")
@pytest.mark.asyncio
async def test_async_curve_auth_allowed_keys(tcp_endpoint):
    server_pub, server_sec = pyomq.curve_keypair()
    client_pub, client_sec = pyomq.curve_keypair()

    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        pull.curve_server = 1
        pull.curve_publickey = server_pub
        pull.curve_secretkey = server_sec
        pull.set_curve_auth([client_pub])

        push.curve_serverkey = server_pub
        push.curve_publickey = client_pub
        push.curve_secretkey = client_sec

        ep = await pull.bind(tcp_endpoint)
        await push.connect(ep)
        await push.send(b"async-curve-ok")
        pull.setsockopt(pyomq.RCVTIMEO, 5000)
        assert await pull.recv() == b"async-curve-ok"
    finally:
        await push.close()
        await pull.close()


@pytest.mark.skipif(not pyomq.has("curve"), reason="curve feature not compiled")
@pytest.mark.asyncio
async def test_async_curve_auth_callback(tcp_endpoint):
    server_pub, server_sec = pyomq.curve_keypair()
    client_pub, client_sec = pyomq.curve_keypair()

    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        pull.curve_server = 1
        pull.curve_publickey = server_pub
        pull.curve_secretkey = server_sec
        pull.set_curve_auth(lambda peer: peer.public_key == client_pub)

        push.curve_serverkey = server_pub
        push.curve_publickey = client_pub
        push.curve_secretkey = client_sec

        ep = await pull.bind(tcp_endpoint)
        await push.connect(ep)
        await push.send(b"async-curve-cb")
        pull.setsockopt(pyomq.RCVTIMEO, 5000)
        assert await pull.recv() == b"async-curve-cb"
    finally:
        await push.close()
        await pull.close()


# ── BLAKE3ZMQ ────────────────────────────────────────────────────────


@pytest.mark.skipif(
    not pyomq.has("blake3zmq"), reason="blake3zmq feature not compiled"
)
@pytest.mark.asyncio
async def test_async_blake3zmq_auth_allowed_keys(tcp_endpoint):
    server_pub, server_sec = pyomq.blake3zmq_keypair()
    client_pub, client_sec = pyomq.blake3zmq_keypair()

    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        pull.blake3zmq_server = 1
        pull.blake3zmq_publickey = server_pub
        pull.blake3zmq_secretkey = server_sec
        pull.set_blake3zmq_auth([client_pub])

        push.blake3zmq_serverkey = server_pub
        push.blake3zmq_publickey = client_pub
        push.blake3zmq_secretkey = client_sec

        ep = await pull.bind(tcp_endpoint)
        await push.connect(ep)
        await push.send(b"async-blake3-ok")
        pull.setsockopt(pyomq.RCVTIMEO, 5000)
        assert await pull.recv() == b"async-blake3-ok"
    finally:
        await push.close()
        await pull.close()


@pytest.mark.skipif(
    not pyomq.has("blake3zmq"), reason="blake3zmq feature not compiled"
)
@pytest.mark.asyncio
async def test_async_blake3zmq_auth_callback(tcp_endpoint):
    server_pub, server_sec = pyomq.blake3zmq_keypair()
    client_pub, client_sec = pyomq.blake3zmq_keypair()

    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        pull.blake3zmq_server = 1
        pull.blake3zmq_publickey = server_pub
        pull.blake3zmq_secretkey = server_sec
        pull.set_blake3zmq_auth(lambda peer: peer.public_key == client_pub)

        push.blake3zmq_serverkey = server_pub
        push.blake3zmq_publickey = client_pub
        push.blake3zmq_secretkey = client_sec

        ep = await pull.bind(tcp_endpoint)
        await push.connect(ep)
        await push.send(b"async-blake3-cb")
        pull.setsockopt(pyomq.RCVTIMEO, 5000)
        assert await pull.recv() == b"async-blake3-cb"
    finally:
        await push.close()
        await pull.close()

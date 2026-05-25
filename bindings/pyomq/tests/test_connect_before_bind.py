"""Connect-before-bind: dialer connects before listener binds.

ZMQ sockets queue outbound messages and retry until the peer appears.
Tested across inproc, IPC, and TCP for both sync and async APIs,
with PUSH/PULL, REQ/REP, and PAIR.
"""

import time

import pytest

import pyomq as zmq
import pyomq.asyncio as zmq_async


BIND_DELAYS = [0, 0.05, 0.25]


# ── helpers ──────────────────────────────────────────────────────────

def _free_tcp():
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"tcp://127.0.0.1:{port}"


# ── sync PUSH/PULL ──────────────────────────────────────────────────

def _sync_push_pull_cbb(ep, delay):
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        push.connect(ep)
        time.sleep(delay)
        pull.bind(ep)
        push.send(b"late")
        assert pull.recv() == b"late"
    finally:
        push.close()
        pull.close()
        ctx.term()


@pytest.mark.parametrize("delay", BIND_DELAYS)
def test_sync_push_pull_cbb_inproc(inproc_endpoint, delay):
    _sync_push_pull_cbb(f"{inproc_endpoint}-{delay}", delay)


@pytest.mark.parametrize("delay", BIND_DELAYS)
def test_sync_push_pull_cbb_ipc(ipc_endpoint, delay):
    _sync_push_pull_cbb(ipc_endpoint, delay)


@pytest.mark.parametrize("delay", BIND_DELAYS)
def test_sync_push_pull_cbb_tcp(delay):
    _sync_push_pull_cbb(_free_tcp(), delay)


# ── sync REQ/REP ────────────────────────────────────────────────────

def _sync_req_rep_cbb(ep, delay):
    ctx = zmq.Context()
    req = ctx.socket(zmq.REQ)
    rep = ctx.socket(zmq.REP)
    try:
        req.connect(ep)
        time.sleep(delay)
        rep.bind(ep)
        req.send(b"q")
        assert rep.recv() == b"q"
        rep.send(b"a")
        assert req.recv() == b"a"
    finally:
        req.close()
        rep.close()
        ctx.term()


@pytest.mark.parametrize("delay", BIND_DELAYS)
def test_sync_req_rep_cbb_inproc(inproc_endpoint, delay):
    _sync_req_rep_cbb(f"{inproc_endpoint}-{delay}", delay)


@pytest.mark.parametrize("delay", BIND_DELAYS)
def test_sync_req_rep_cbb_ipc(ipc_endpoint, delay):
    _sync_req_rep_cbb(ipc_endpoint, delay)


@pytest.mark.parametrize("delay", BIND_DELAYS)
def test_sync_req_rep_cbb_tcp(delay):
    _sync_req_rep_cbb(_free_tcp(), delay)


# ── sync PAIR ───────────────────────────────────────────────────────

def _sync_pair_cbb(ep, delay):
    ctx = zmq.Context()
    a = ctx.socket(zmq.PAIR)
    b = ctx.socket(zmq.PAIR)
    try:
        a.connect(ep)
        time.sleep(delay)
        b.bind(ep)
        a.send(b"from-a")
        assert b.recv() == b"from-a"
        b.send(b"from-b")
        assert a.recv() == b"from-b"
    finally:
        a.close()
        b.close()
        ctx.term()


@pytest.mark.parametrize("delay", BIND_DELAYS)
def test_sync_pair_cbb_inproc(inproc_endpoint, delay):
    _sync_pair_cbb(f"{inproc_endpoint}-{delay}", delay)


@pytest.mark.parametrize("delay", BIND_DELAYS)
def test_sync_pair_cbb_ipc(ipc_endpoint, delay):
    _sync_pair_cbb(ipc_endpoint, delay)


@pytest.mark.parametrize("delay", BIND_DELAYS)
def test_sync_pair_cbb_tcp(delay):
    _sync_pair_cbb(_free_tcp(), delay)


# ── async PUSH/PULL ─────────────────────────────────────────────────

async def _async_push_pull_cbb(ep, delay):
    import asyncio
    ctx = zmq_async.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        push.connect(ep)
        await asyncio.sleep(delay)
        pull.bind(ep)
        push.send(b"late")
        assert await pull.recv() == b"late"
    finally:
        push.close()
        pull.close()


@pytest.mark.parametrize("delay", BIND_DELAYS)
@pytest.mark.asyncio
async def test_async_push_pull_cbb_inproc(inproc_endpoint, delay):
    await _async_push_pull_cbb(f"{inproc_endpoint}-{delay}", delay)


@pytest.mark.parametrize("delay", BIND_DELAYS)
@pytest.mark.asyncio
async def test_async_push_pull_cbb_ipc(ipc_endpoint, delay):
    await _async_push_pull_cbb(ipc_endpoint, delay)


@pytest.mark.parametrize("delay", BIND_DELAYS)
@pytest.mark.asyncio
async def test_async_push_pull_cbb_tcp(delay):
    await _async_push_pull_cbb(_free_tcp(), delay)


# ── async REQ/REP ───────────────────────────────────────────────────

async def _async_req_rep_cbb(ep, delay):
    import asyncio
    ctx = zmq_async.Context()
    req = ctx.socket(zmq.REQ)
    rep = ctx.socket(zmq.REP)
    try:
        req.connect(ep)
        await asyncio.sleep(delay)
        rep.bind(ep)
        req.send(b"q")
        assert await rep.recv() == b"q"
        rep.send(b"a")
        assert await req.recv() == b"a"
    finally:
        req.close()
        rep.close()


@pytest.mark.parametrize("delay", BIND_DELAYS)
@pytest.mark.asyncio
async def test_async_req_rep_cbb_inproc(inproc_endpoint, delay):
    await _async_req_rep_cbb(f"{inproc_endpoint}-{delay}", delay)


@pytest.mark.parametrize("delay", BIND_DELAYS)
@pytest.mark.asyncio
async def test_async_req_rep_cbb_ipc(ipc_endpoint, delay):
    await _async_req_rep_cbb(ipc_endpoint, delay)


@pytest.mark.parametrize("delay", BIND_DELAYS)
@pytest.mark.asyncio
async def test_async_req_rep_cbb_tcp(delay):
    await _async_req_rep_cbb(_free_tcp(), delay)


# ── async PAIR ──────────────────────────────────────────────────────

async def _async_pair_cbb(ep, delay):
    import asyncio
    ctx = zmq_async.Context()
    a = ctx.socket(zmq.PAIR)
    b = ctx.socket(zmq.PAIR)
    try:
        a.connect(ep)
        await asyncio.sleep(delay)
        b.bind(ep)
        a.send(b"from-a")
        assert await b.recv() == b"from-a"
        b.send(b"from-b")
        assert await a.recv() == b"from-b"
    finally:
        a.close()
        b.close()


@pytest.mark.parametrize("delay", BIND_DELAYS)
@pytest.mark.asyncio
async def test_async_pair_cbb_inproc(inproc_endpoint, delay):
    await _async_pair_cbb(f"{inproc_endpoint}-{delay}", delay)


@pytest.mark.parametrize("delay", BIND_DELAYS)
@pytest.mark.asyncio
async def test_async_pair_cbb_ipc(ipc_endpoint, delay):
    await _async_pair_cbb(ipc_endpoint, delay)


@pytest.mark.parametrize("delay", BIND_DELAYS)
@pytest.mark.asyncio
async def test_async_pair_cbb_tcp(delay):
    await _async_pair_cbb(_free_tcp(), delay)

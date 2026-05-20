"""Shared fixtures."""

import time

import pytest


import socket as _socket


@pytest.fixture
def tcp_endpoint() -> str:
    s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    s.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"tcp://127.0.0.1:{port}"


@pytest.fixture
def inproc_endpoint(request) -> str:
    # Unique per test so binds don't collide.
    return f"inproc://{request.node.name}"


@pytest.fixture
def ipc_endpoint(tmp_path_factory) -> str:
    # AF_UNIX paths cap near 108 bytes; pytest's nested tmp_path can
    # blow past that. Use a short unique path under /tmp.
    import tempfile, os
    fd, path = tempfile.mkstemp(suffix=".sock", prefix="pyomq-")
    os.close(fd)
    os.unlink(path)
    return f"ipc://{path}"


def wait_for(predicate, timeout: float = 2.0, interval: float = 0.01) -> bool:
    """Block-poll until predicate returns truthy or timeout. Returns the
    final predicate result."""
    deadline = time.monotonic() + timeout
    out = predicate()
    while not out and time.monotonic() < deadline:
        time.sleep(interval)
        out = predicate()
    return bool(out)

"""Shared fixtures."""

import time

import pytest


@pytest.fixture
def tcp_endpoint() -> str:
    return "tcp://127.0.0.1:0"


@pytest.fixture
def inproc_endpoint(request) -> str:
    # Unique per test so binds don't collide.
    return f"inproc://{request.node.name}"


@pytest.fixture
def ipc_endpoint(request) -> str:
    import os, hashlib
    # Linux abstract namespace: no filesystem entry, auto-cleaned on close.
    tag = hashlib.sha1(f"{os.getpid()}-{request.node.name}".encode()).hexdigest()[:16]
    return f"ipc://@pyomq-{tag}"


def wait_for(predicate, timeout: float = 2.0, interval: float = 0.01) -> bool:
    """Block-poll until predicate returns truthy or timeout. Returns the
    final predicate result."""
    deadline = time.monotonic() + timeout
    out = predicate()
    while not out and time.monotonic() < deadline:
        time.sleep(interval)
        out = predicate()
    return bool(out)

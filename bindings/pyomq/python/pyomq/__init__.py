"""pyomq - Python binding for omq.rs.

Drop-in pyzmq replacement on the common path. Use as::

    import pyomq as zmq

The Socket / Context API mirrors pyzmq's surface; constants
(``zmq.PUSH``, ``zmq.SUBSCRIBE``, ``zmq.LINGER`` ...) match libzmq's
integer values, so existing pyzmq code typically just works.

For asynchronous code::

    import pyomq.asyncio as zmq_async
"""

import json
import os
import pickle
import random
import threading

from . import _native  # type: ignore[attr-defined]
from . import error as error  # noqa: F401

from ._native import (  # type: ignore[attr-defined]
    backend_name,
    version,
    # Socket types
    PAIR,
    PUB,
    SUB,
    REQ,
    REP,
    DEALER,
    ROUTER,
    PULL,
    PUSH,
    XPUB,
    XSUB,
    # Draft socket types (RFC 41 / 48 / 49 / 51 + PEER)
    SERVER,
    CLIENT,
    RADIO,
    DISH,
    GATHER,
    SCATTER,
    PEER,
    CHANNEL,
    # Option constants
    AFFINITY,
    IDENTITY,
    SUBSCRIBE,
    UNSUBSCRIBE,
    RCVMORE,
    TYPE,
    LINGER,
    RECONNECT_IVL,
    RECONNECT_IVL_MAX,
    BACKLOG,
    MAXMSGSIZE,
    SNDHWM,
    RCVHWM,
    RCVTIMEO,
    SNDTIMEO,
    ROUTER_MANDATORY,
    IMMEDIATE,
    IPV6,
    HEARTBEAT_IVL,
    HEARTBEAT_TTL,
    HEARTBEAT_TIMEOUT,
    HANDSHAKE_IVL,
    CONFLATE,
    TCP_KEEPALIVE,
    TCP_KEEPALIVE_IDLE,
    TCP_KEEPALIVE_CNT,
    TCP_KEEPALIVE_INTVL,
    SNDMORE,
    NOBLOCK,
    DONTWAIT,
    # CURVE option ids
    CURVE_SERVER,
    CURVE_PUBLICKEY,
    CURVE_SECRETKEY,
    CURVE_SERVERKEY,
)

from .error import (  # noqa: F401  re-exports
    ZMQBaseError,
    ZMQError,
    Again,
    ContextTerminated,
    ZMQBindError,
    InterruptedSystemCall,
    NotImplementedError as ZMQNotImplementedError,
)

# ── Constants ─────────────────────────────────────────────────────────

POLLIN = 1
POLLOUT = 2
POLLERR = 4
POLLPRI = 32
STREAM = 11
HWM = 1

__version__ = version()
zmq_version_info = (4, 3, 4)


# ── Top-level functions ──────────────────────────────────────────────

def strerror(errnum):
    return os.strerror(errnum)


def has(capability):
    return capability in ("ipc", "inproc")


# ── Socket wrapper ───────────────────────────────────────────────────

class Socket:
    _sock: _native.Socket
    _context: "Context"
    _closed: bool

    def __init__(self, _sock, _context):
        self._sock = _sock
        self._context = _context
        self._closed = False

    @property
    def closed(self):
        return self._closed

    @property
    def context(self):
        return self._context

    @property
    def socket_type(self):
        return self._sock.getsockopt(TYPE)

    # ── I/O ──────────────────────────────────────────────────────────

    def bind(self, endpoint):
        try:
            return self._sock.bind(endpoint)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def connect(self, endpoint):
        try:
            return self._sock.connect(endpoint)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def unbind(self, endpoint):
        try:
            return self._sock.unbind(endpoint)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def disconnect(self, endpoint):
        try:
            return self._sock.disconnect(endpoint)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def send(self, data, flags=0, copy=True, track=False):
        if not copy:
            raise builtins.NotImplementedError(
                "copy=False requires Frame, which is not implemented"
            )
        if track:
            raise builtins.NotImplementedError(
                "track=True requires MessageTracker, which is not implemented"
            )
        try:
            return self._sock.send(data, flags)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def recv(self, flags=0, copy=True, track=False):
        if not copy:
            raise builtins.NotImplementedError(
                "copy=False requires Frame, which is not implemented"
            )
        try:
            return self._sock.recv(flags)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def send_multipart(self, parts, flags=0, copy=True, track=False):
        try:
            return self._sock.send_multipart(parts, flags)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def recv_multipart(self, flags=0, copy=True, track=False):
        try:
            return self._sock.recv_multipart(flags)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    # ── Serialization helpers ────────────────────────────────────────

    def send_string(self, u, flags=0, encoding="utf-8"):
        return self.send(u.encode(encoding), flags)

    def recv_string(self, flags=0, encoding="utf-8"):
        return self.recv(flags).decode(encoding)

    def send_json(self, obj, flags=0, **kwargs):
        return self.send(json.dumps(obj, **kwargs).encode("utf-8"), flags)

    def recv_json(self, flags=0, **kwargs):
        return json.loads(self.recv(flags), **kwargs)

    def send_pyobj(self, obj, flags=0, protocol=-1):
        return self.send(pickle.dumps(obj, protocol), flags)

    def recv_pyobj(self, flags=0):
        return pickle.loads(self.recv(flags))

    # ── Options ──────────────────────────────────────────────────────

    def setsockopt(self, option, value):
        try:
            return self._sock.setsockopt(option, value)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def getsockopt(self, option):
        try:
            return self._sock.getsockopt(option)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def set(self, option, value):
        return self.setsockopt(option, value)

    def get(self, option):
        return self.getsockopt(option)

    def setsockopt_string(self, option, value, encoding="utf-8"):
        return self.setsockopt(option, value.encode(encoding))

    def getsockopt_string(self, option, encoding="utf-8"):
        v = self.getsockopt(option)
        if isinstance(v, bytes):
            return v.decode(encoding)
        return str(v)

    # ── Subscriptions ────────────────────────────────────────────────

    def subscribe(self, prefix):
        try:
            return self._sock.subscribe(prefix)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def unsubscribe(self, prefix):
        try:
            return self._sock.unsubscribe(prefix)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def join(self, group):
        try:
            return self._sock.join(group)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def leave(self, group):
        try:
            return self._sock.leave(group)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    # ── Monitoring ───────────────────────────────────────────────────

    def monitor(self):
        return self._sock.monitor()

    def connections(self):
        return self._sock.connections()

    def connection_info(self, connection_id):
        return self._sock.connection_info(connection_id)

    # ── Lifecycle ────────────────────────────────────────────────────

    def close(self, linger=None):
        if not self._closed:
            self._closed = True
            self._sock.close(linger)

    def bind_to_random_port(self, addr, min_port=49152, max_port=65536,
                            max_tries=100):
        for _ in range(max_tries):
            port = random.randint(min_port, max_port - 1)
            try:
                self.bind(f"{addr}:{port}")
                return port
            except ZMQError:
                continue
        raise ZMQBindError(
            f"Could not bind socket to random port "
            f"(tried {max_tries} ports in [{min_port}, {max_port}))"
        )

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


# ── Context wrapper ──────────────────────────────────────────────────

class Context:
    _instance = None
    _instance_lock = threading.Lock()

    def __init__(self, io_threads=1):
        self._ctx = _native.Context(io_threads)
        self._closed = False

    def socket(self, socket_type, socket_class=None, **kwargs):
        native = self._ctx.socket(socket_type)
        cls = socket_class or Socket
        s = object.__new__(cls)
        s._sock = native
        s._context = self
        s._closed = False
        return s

    @classmethod
    def instance(cls, io_threads=1):
        with cls._instance_lock:
            if cls._instance is None or cls._instance._closed:
                cls._instance = cls(io_threads)
            return cls._instance

    def term(self):
        self._closed = True
        self._ctx.term()

    def destroy(self):
        self.term()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.term()
        return False


# ── Poller ───────────────────────────────────────────────────────────

class Poller:
    def __init__(self):
        self._sockets = {}  # native_socket_id -> (Socket, flags)

    def register(self, socket, flags=POLLIN):
        self._sockets[socket._sock.socket_id()] = (socket, flags)

    def unregister(self, socket):
        self._sockets.pop(socket._sock.socket_id(), None)

    def modify(self, socket, flags):
        k = socket._sock.socket_id()
        if k in self._sockets:
            self._sockets[k] = (socket, flags)

    def poll(self, timeout=None):
        if not self._sockets:
            return []
        pollin_socks = [
            s._sock
            for k, (s, f) in self._sockets.items()
            if f & POLLIN
        ]
        if not pollin_socks:
            return []
        t = None if (timeout is None or timeout < 0) else int(timeout)
        ready_ids = _native.wait_any(pollin_socks, t)
        return [
            (self._sockets[rid][0], POLLIN)
            for rid in ready_ids
            if rid in self._sockets
        ]


# ── proxy ────────────────────────────────────────────────────────────

def proxy(frontend, backend, capture=None):
    _native.native_proxy(
        frontend._sock, backend._sock,
        capture._sock if capture is not None else None,
    )


def device(device_type, frontend, backend):
    proxy(frontend, backend)


# ── builtins reference (for copy/track errors) ──────────────────────

import builtins  # noqa: E402

__all__ = [
    "Context",
    "Socket",
    "Poller",
    "ZMQBaseError",
    "ZMQError",
    "ZMQBindError",
    "Again",
    "ContextTerminated",
    "InterruptedSystemCall",
    "backend_name",
    "version",
    "proxy",
    "device",
    "strerror",
    "has",
    "error",
    # socket types
    "PAIR", "PUB", "SUB", "REQ", "REP", "DEALER", "ROUTER",
    "PULL", "PUSH", "XPUB", "XSUB",
    # draft socket types
    "SERVER", "CLIENT", "RADIO", "DISH", "GATHER", "SCATTER",
    "PEER", "CHANNEL",
    # options
    "AFFINITY", "IDENTITY", "SUBSCRIBE", "UNSUBSCRIBE", "RCVMORE",
    "TYPE", "LINGER", "RECONNECT_IVL", "RECONNECT_IVL_MAX", "BACKLOG",
    "MAXMSGSIZE", "SNDHWM", "RCVHWM", "RCVTIMEO", "SNDTIMEO",
    "ROUTER_MANDATORY", "IMMEDIATE", "IPV6",
    "HEARTBEAT_IVL", "HEARTBEAT_TTL", "HEARTBEAT_TIMEOUT",
    "HANDSHAKE_IVL", "CONFLATE",
    "TCP_KEEPALIVE", "TCP_KEEPALIVE_IDLE", "TCP_KEEPALIVE_CNT",
    "TCP_KEEPALIVE_INTVL",
    "SNDMORE", "NOBLOCK", "DONTWAIT",
    "CURVE_SERVER", "CURVE_PUBLICKEY", "CURVE_SECRETKEY", "CURVE_SERVERKEY",
    # poll / compat constants
    "POLLIN", "POLLOUT", "POLLERR", "POLLPRI", "STREAM", "HWM",
    # version
    "__version__", "zmq_version_info",
]

"""Async (asyncio) facade for pyomq.

Use::

    import pyomq
    import pyomq.asyncio as zmq_async

    ctx = zmq_async.Context()
    sock = ctx.socket(pyomq.PUSH)
    await sock.connect("tcp://127.0.0.1:5555")
    await sock.send(b"hello")
    msg = await sock.recv()
    await sock.close()
"""

import json
import pickle

from . import _native  # type: ignore[attr-defined]
from . import error


class Socket:
    _sock: _native.AsyncSocket
    _context: "Context"
    _closed: bool

    def __init__(self, _sock, _context):
        self._sock = _sock
        self._context = _context
        self._closed = False
        self._last_endpoint = None

    @property
    def closed(self):
        return self._closed

    @property
    def context(self):
        return self._context

    @property
    def last_endpoint(self):
        return self._last_endpoint

    @property
    def socket_type(self):
        return self._sock.getsockopt(_native.TYPE)

    # ── I/O ──────────────────────────────────────────────────────────

    async def bind(self, endpoint):
        try:
            ep = await self._sock.bind(endpoint)
            self._last_endpoint = ep.encode() if isinstance(ep, str) else ep
            return ep
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    async def connect(self, endpoint):
        try:
            await self._sock.connect(endpoint)
            self._last_endpoint = (
                endpoint.encode() if isinstance(endpoint, str) else endpoint
            )
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    async def unbind(self, endpoint):
        try:
            return await self._sock.unbind(endpoint)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    async def disconnect(self, endpoint):
        try:
            return await self._sock.disconnect(endpoint)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    async def send(self, data, flags=0, copy=True, track=False):
        try:
            return await self._sock.send(data, flags)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    async def recv(self, flags=0, copy=True, track=False):
        try:
            return await self._sock.recv(flags)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    async def send_multipart(self, parts, flags=0, copy=True, track=False):
        try:
            return await self._sock.send_multipart(parts, flags)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    async def recv_multipart(self, flags=0, copy=True, track=False):
        try:
            return await self._sock.recv_multipart(flags)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    # ── Serialization helpers ────────────────────────────────────────

    async def send_string(self, u, flags=0, encoding="utf-8"):
        return await self.send(u.encode(encoding), flags)

    async def recv_string(self, flags=0, encoding="utf-8"):
        return (await self.recv(flags)).decode(encoding)

    async def send_json(self, obj, flags=0, **kwargs):
        return await self.send(
            json.dumps(obj, **kwargs).encode("utf-8"), flags
        )

    async def recv_json(self, flags=0, **kwargs):
        return json.loads(await self.recv(flags), **kwargs)

    async def send_pyobj(self, obj, flags=0, protocol=-1):
        return await self.send(pickle.dumps(obj, protocol), flags)

    async def recv_pyobj(self, flags=0):
        return pickle.loads(await self.recv(flags))

    # ── Options (sync — matches pyzmq) ───────────────────────────────

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

    # ── Subscriptions ────────────────────────────────────────────────

    async def subscribe(self, prefix):
        try:
            return await self._sock.subscribe(prefix)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    async def unsubscribe(self, prefix):
        try:
            return await self._sock.unsubscribe(prefix)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    async def join(self, group):
        try:
            return await self._sock.join(group)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    async def leave(self, group):
        try:
            return await self._sock.leave(group)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    # ── Lifecycle ────────────────────────────────────────────────────

    async def close(self, linger=None):
        if not self._closed:
            self._closed = True
            await self._sock.close(linger)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()
        return False


class Context:
    def __init__(self, io_threads=1):
        self._ctx = _native.AsyncContext(io_threads)
        self._closed = False

    def socket(self, socket_type, socket_class=None, **kwargs):
        native = self._ctx.socket(socket_type)
        cls = socket_class or Socket
        s = object.__new__(cls)
        s._sock = native
        s._context = self
        s._closed = False
        return s

    def term(self):
        self._closed = True

    def destroy(self):
        self.term()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.term()
        return False


__all__ = ["Context", "Socket"]

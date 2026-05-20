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

import asyncio
import json
import pickle

from . import _native  # type: ignore[attr-defined]
from . import error
from . import (
    POLLIN, POLLOUT, SNDHWM, RCVHWM, LINGER, TYPE, LAST_ENDPOINT,
    _TYPE_NAMES, _SOCKOPT_NAMES,
)


class Socket:
    _sock: _native.AsyncSocket
    _context: "Context"
    _closed: bool

    def __init__(self, _sock, _context):
        self._sock = _sock
        self._context = _context
        self._closed = False
        self._last_endpoint = None

    def __getattr__(self, name):
        opt = _SOCKOPT_NAMES.get(name)
        if opt is not None:
            if opt == LAST_ENDPOINT:
                return self._last_endpoint
            return self.getsockopt(opt)
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'"
        )

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        opt = _SOCKOPT_NAMES.get(name)
        if opt is not None:
            self.setsockopt(opt, value)
            return
        object.__setattr__(self, name, value)

    def __repr__(self):
        st = _TYPE_NAMES.get(self.socket_type, str(self.socket_type))
        return f"<pyomq.asyncio.Socket(pyomq.{st}) at {id(self):#x}>"

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

    @property
    def underlying(self):
        return self

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

    async def send_serialized(self, msg, serialize, flags=0, copy=True,
                              **kwargs):
        frames = serialize(msg)
        return await self.send_multipart(frames, flags=flags, copy=copy,
                                         **kwargs)

    async def recv_serialized(self, deserialize, flags=0, copy=True):
        frames = await self.recv_multipart(flags=flags, copy=copy)
        return deserialize(frames)

    # ── Options (sync -- matches pyzmq) ──────────────────────────────

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

    set_string = setsockopt_string
    get_string = getsockopt_string

    def set_hwm(self, value):
        self.setsockopt(SNDHWM, value)
        self.setsockopt(RCVHWM, value)

    def get_hwm(self):
        return self.getsockopt(SNDHWM)

    hwm = property(get_hwm, set_hwm)

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

    async def poll(self, timeout=None, flags=POLLIN):
        p = Poller()
        p.register(self, flags)
        evts = await p.poll(timeout)
        for sock, mask in evts:
            if sock is self:
                return mask
        return 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()
        return False


class Poller:
    def __init__(self):
        self._sockets = {}

    def register(self, socket, flags=POLLIN):
        self._sockets[socket._sock.socket_id()] = (socket, flags)

    def unregister(self, socket):
        self._sockets.pop(socket._sock.socket_id(), None)

    def modify(self, socket, flags):
        k = socket._sock.socket_id()
        if k in self._sockets:
            self._sockets[k] = (socket, flags)

    @property
    def sockets(self):
        return [(s, f) for s, f in self._sockets.values()]

    async def poll(self, timeout=None):
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
        loop = asyncio.get_running_loop()
        ready_ids = await loop.run_in_executor(
            None, _native.wait_any, pollin_socks, t
        )
        return [
            (self._sockets[rid][0], POLLIN)
            for rid in ready_ids
            if rid in self._sockets
        ]


class Context:
    def __init__(self, io_threads=1):
        self._ctx = _native.AsyncContext(io_threads)
        self._closed = False
        self._sockets = set()

    @property
    def closed(self):
        return self._closed

    def socket(self, socket_type, socket_class=None, **kwargs):
        native = self._ctx.socket(socket_type)
        cls = socket_class or Socket
        s = object.__new__(cls)
        s._sock = native
        s._context = self
        s._closed = False
        s._last_endpoint = None
        self._sockets.add(s)
        return s

    def term(self):
        self._closed = True

    def destroy(self, linger=None):
        for s in list(self._sockets):
            if not s.closed:
                if linger is not None:
                    s.setsockopt(LINGER, linger)
        self._sockets.clear()
        self.term()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.term()
        return False


__all__ = ["Context", "Socket", "Poller"]

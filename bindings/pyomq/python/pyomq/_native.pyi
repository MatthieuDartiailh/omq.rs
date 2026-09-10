"""Interface of the PyO3 library.

This file is deliberately maintained by hand because PyO3's generated
stubs are intentionally limited and do not cover the full public
Python/native API contract of pyomq.

"""

import builtins
import types
from collections.abc import Iterable, Sequence
from typing import Any, Self

class ZMQBaseError(Exception): ...

class ZMQError(ZMQBaseError):
    errno: int | None

# Socket type constants (libzmq-compatible)
PAIR: int
PUB: int
SUB: int
REQ: int
REP: int
DEALER: int
ROUTER: int
PULL: int
PUSH: int
XPUB: int
XSUB: int
STREAM: int
SERVER: int
CLIENT: int
RADIO: int
DISH: int
GATHER: int
SCATTER: int
PEER: int
CHANNEL: int

# Socket option constants
AFFINITY: int
IDENTITY: int
SUBSCRIBE: int
UNSUBSCRIBE: int
RCVMORE: int
TYPE: int
LINGER: int
RECONNECT_IVL: int
BACKLOG: int
RECONNECT_IVL_MAX: int
MAXMSGSIZE: int
SNDHWM: int
RCVHWM: int
RCVTIMEO: int
SNDTIMEO: int
ROUTER_MANDATORY: int
TCP_KEEPALIVE: int
TCP_KEEPALIVE_CNT: int
TCP_KEEPALIVE_IDLE: int
TCP_KEEPALIVE_INTVL: int
IMMEDIATE: int
IPV6: int
HEARTBEAT_IVL: int
HEARTBEAT_TTL: int
HEARTBEAT_TIMEOUT: int
HANDSHAKE_IVL: int
CONFLATE: int
CURVE_SERVER: int
CURVE_PUBLICKEY: int
CURVE_SECRETKEY: int
CURVE_SERVERKEY: int
OMQ_ON_MUTE: int
OMQ_ON_MUTE_BLOCK: int
OMQ_ON_MUTE_DROP_NEWEST: int
OMQ_ON_MUTE_DROP_OLDEST: int
OMQ_COMPRESSION_LEVEL: int
OMQ_COMPRESSION_DICT: int
OMQ_COMPRESSION_AUTO_TRAIN: int

# Compatibility constants
NOBLOCK: int
DONTWAIT: int
SNDMORE: int

# In-process connection metadata
class PeerInfo:
    @property
    def public_key(self) -> bytes: ...
    @property
    def identity(self) -> bytes | None: ...
    @property
    def peer_address(self) -> str | None: ...
    @property
    def username(self) -> str | None: ...
    @property
    def password(self) -> str | None: ...

# Module-level helpers

def backend_name() -> str: ...
def version() -> str: ...
def has_feature(name: str) -> bool: ...
def wait_any(
    sockets: Sequence[Socket | AsyncSocket], timeout_ms: int | None = ...
) -> list[int]: ...
def rust_thread_send_via_share_key(
    share_key: int, endpoint: str, payload: bytes
) -> None: ...
def native_proxy(
    frontend: Socket,
    backend: Socket,
    capture: Socket | None = ...,
    control: Socket | None = ...,
) -> None: ...
def curve_keypair() -> tuple[bytes, bytes]: ...
def curve_public(secret: bytes | str) -> bytes: ...

class Context:
    def __init__(self, io_threads: int = 1) -> None: ...
    @staticmethod
    def shadow_async(context: AsyncContext) -> Context: ...
    def share_key(self) -> int: ...
    @staticmethod
    def from_share_key(share_key: int) -> Context: ...
    def socket(self, socket_type: int, /) -> Socket: ...
    def term(self) -> None: ...
    def destroy(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_val: BaseException | None = ...,
        exc_tb: types.TracebackType | None = ...,
    ) -> bool: ...

class AsyncContext:
    def __init__(self, io_threads: int = 1) -> None: ...
    @staticmethod
    def shadow_sync(context: Context) -> AsyncContext: ...
    def share_key(self) -> int: ...
    @staticmethod
    def from_share_key(share_key: int) -> AsyncContext: ...
    def socket(self, socket_type: int, /) -> AsyncSocket: ...
    def term(self) -> None: ...
    def destroy(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_val: BaseException | None = ...,
        exc_tb: types.TracebackType | None = ...,
    ) -> bool: ...

class Frame:
    def __init__(
        self,
        data: builtins.bytes | bytearray | memoryview | object = ...,
        track: bool = False,
        copy: bool | None = ...,
        copy_threshold: int | None = ...,
    ) -> None: ...
    @property
    def bytes(self) -> builtins.bytes: ...
    @property
    def buffer(self) -> Any: ...
    @property
    def more(self) -> bool: ...
    @property
    def routing_id(self) -> int: ...
    @routing_id.setter
    def routing_id(self, routing_id: int) -> None: ...
    @property
    def tracker(self) -> Any: ...
    def __bytes__(self) -> builtins.bytes: ...
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __eq__(self, other: object, /) -> bool: ...
    def __ne__(self, other: object, /) -> bool: ...
    # This is a lie but memoryview does work with the current implementation
    def __buffer__(self, flags: int = 0) -> memoryview: ...

class Monitor:
    def recv(self, timeout_ms: int = -1) -> dict[str, Any]: ...
    def recv_nowait(self) -> dict[str, Any]: ...

class Socket:
    def socket_id(self) -> int: ...
    def bind(self, endpoint: str | bytes) -> str | bytes: ...
    def connect(self, endpoint: str | bytes) -> None: ...
    def unbind(self, endpoint: str | bytes) -> None: ...
    def disconnect(self, endpoint: str | bytes) -> None: ...
    def send(
        self,
        payload: Any,
        flags: int = 0,
        copy: bool = True,
    ) -> None: ...
    def send_multipart(
        self,
        parts: Iterable[Any],
        flags: int = 0,
        copy: bool = True,
    ) -> None: ...
    def recv(self, flags: int = 0) -> bytes: ...
    def recv_frame(self, flags: int = 0) -> Frame: ...
    def recv_multipart(self, flags: int = 0) -> list[bytes]: ...
    def recv_multipart_frames(self, flags: int = 0) -> list[Frame]: ...
    def subscribe(self, prefix: bytes | str) -> None: ...
    def unsubscribe(self, prefix: bytes | str) -> None: ...
    def join(self, group: bytes | str) -> None: ...
    def leave(self, group: bytes | str) -> None: ...
    def connections(self) -> list[dict[str, Any]]: ...
    def connection_info(self, connection_id: int) -> dict[str, Any] | None: ...
    def monitor(self) -> Monitor: ...
    def setsockopt(self, option: int, value: Any) -> None: ...
    def getsockopt(self, option: int) -> Any: ...
    def set_curve_auth(self, auth: Any) -> None: ...
    def set_plain_auth(self, auth: Any) -> None: ...
    def close(self, linger: int | None = None) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_val: BaseException | None = ...,
        exc_tb: types.TracebackType | None = ...,
    ) -> bool: ...

class AsyncSocket:
    def socket_id(self) -> int: ...
    def _try_recv(self) -> bytes | None: ...
    def _try_recv_frame(self) -> Frame | None: ...
    def _try_recv_multipart(self) -> list[bytes] | None: ...
    def _try_recv_multipart_frames(self) -> list[Frame] | None: ...
    def _recv_fd(self) -> int: ...
    def _send_fd(self) -> int: ...
    def _set_wakeup_hooks(
        self,
        recv_async: Any | None = ...,
        recv_event: Any | None = ...,
        send_async: Any | None = ...,
        send_event: Any | None = ...,
    ) -> None: ...
    def _set_wakeup_modes(
        self,
        recv_mode: int | None = ...,
        send_mode: int | None = ...,
    ) -> None: ...
    def _clear_wakeup_modes(
        self,
        recv_mode: int | None = ...,
        send_mode: int | None = ...,
    ) -> None: ...
    def _mark_recv_drain_complete(self) -> None: ...
    def _mark_send_drain_complete(self) -> None: ...
    def bind(self, endpoint: str | bytes) -> str | bytes: ...
    def connect(self, endpoint: str | bytes) -> None: ...
    def unbind(self, endpoint: str | bytes) -> None: ...
    def disconnect(self, endpoint: str | bytes) -> None: ...
    def send(
        self,
        payload: Any,
        flags: int = 0,
        copy: bool = True,
    ) -> None: ...
    def send_multipart(
        self,
        parts: Iterable[Any],
        flags: int = 0,
        copy: bool = True,
    ) -> None: ...
    def recv(self, flags: int = 0) -> bytes: ...
    def recv_frame(self, flags: int = 0) -> Frame: ...
    def recv_multipart(self, flags: int = 0) -> list[bytes]: ...
    def recv_multipart_frames(self, flags: int = 0) -> list[Frame]: ...
    def subscribe(self, prefix: bytes | str) -> None: ...
    def unsubscribe(self, prefix: bytes | str) -> None: ...
    def join(self, group: bytes | str) -> None: ...
    def leave(self, group: bytes | str) -> None: ...
    def connections(self) -> list[dict[str, Any]]: ...
    def connection_info(self, connection_id: int) -> dict[str, Any] | None: ...
    def monitor(self) -> Monitor: ...
    def setsockopt(self, option: int, value: Any) -> None: ...
    def getsockopt(self, option: int) -> Any: ...
    def set_curve_auth(self, auth: Any) -> None: ...
    def set_plain_auth(self, auth: Any) -> None: ...
    def close(self, linger: int | None = None) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_val: BaseException | None = ...,
        exc_tb: types.TracebackType | None = ...,
    ) -> bool: ...
    def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_val: BaseException | None = ...,
        exc_tb: types.TracebackType | None = ...,
    ) -> None: ...

__all__ = [
    "AFFINITY",
    "BACKLOG",
    "CHANNEL",
    "CLIENT",
    "CONFLATE",
    "CURVE_PUBLICKEY",
    "CURVE_SECRETKEY",
    "CURVE_SERVER",
    "CURVE_SERVERKEY",
    "DEALER",
    "DISH",
    "DONTWAIT",
    "GATHER",
    "HANDSHAKE_IVL",
    "HEARTBEAT_IVL",
    "HEARTBEAT_TIMEOUT",
    "HEARTBEAT_TTL",
    "IDENTITY",
    "IMMEDIATE",
    "IPV6",
    "LINGER",
    "MAXMSGSIZE",
    "NOBLOCK",
    "OMQ_COMPRESSION_AUTO_TRAIN",
    "OMQ_COMPRESSION_DICT",
    "OMQ_COMPRESSION_LEVEL",
    "OMQ_ON_MUTE",
    "OMQ_ON_MUTE_BLOCK",
    "OMQ_ON_MUTE_DROP_NEWEST",
    "OMQ_ON_MUTE_DROP_OLDEST",
    "PAIR",
    "PEER",
    "PUB",
    "PULL",
    "PUSH",
    "RADIO",
    "RCVHWM",
    "RCVMORE",
    "RCVTIMEO",
    "RECONNECT_IVL",
    "RECONNECT_IVL_MAX",
    "REP",
    "REQ",
    "ROUTER",
    "ROUTER_MANDATORY",
    "SCATTER",
    "SERVER",
    "SNDHWM",
    "SNDMORE",
    "SNDTIMEO",
    "STREAM",
    "SUB",
    "SUBSCRIBE",
    "TCP_KEEPALIVE",
    "TCP_KEEPALIVE_CNT",
    "TCP_KEEPALIVE_IDLE",
    "TCP_KEEPALIVE_INTVL",
    "TYPE",
    "UNSUBSCRIBE",
    "XPUB",
    "XSUB",
    "AsyncContext",
    "AsyncSocket",
    "Context",
    "Frame",
    "Monitor",
    "PeerInfo",
    "Socket",
    "ZMQBaseError",
    "ZMQError",
    "backend_name",
    "curve_keypair",
    "curve_public",
    "has_feature",
    "native_proxy",
    "rust_thread_send_via_share_key",
    "version",
    "wait_any",
]

# pyomq Architecture

PyO3 binding for `omq-compio`. Drop-in pyzmq API for Python (sync and
async). Single stable-ABI wheel (`abi3-py39`, Python 3.9+) via maturin.
Linux only (io_uring via compio).

## Source layout

```
python/pyomq/
  __init__.py       sync API: Socket, Context, Poller, proxy, select
  asyncio.py        async API: wraps _native.AsyncSocket
  error.py          exception hierarchy (pyzmq-compatible)

src/
  lib.rs            module root: classes, constants, wait_any, proxy,
                    curve_keypair, blake3zmq_keypair, has_feature
  runtime.rs        compio runtime on dedicated thread; socket registry,
                    materialize, compio_future_into_py, wait_any, proxy
  socket.rs         sync Socket + SocketInner + RecvNotify (eventfd) +
                    Monitor (connection event stream)
  socket_async.rs   AsyncSocket: recv via compio_future_into_py,
                    _send_direct, _try_recv, _recv_fd for eventfd
  context.rs        Context / AsyncContext (stateless factories)
  options.rs        setsockopt/getsockopt: Overlay cache, option dispatch
  dispatch.rs       shared bind/connect/subscribe dispatch helpers
  constants.rs      libzmq-compatible socket type + option constants
  conversions.rs    zero-copy PyBytes via PyBytesOwner + Bytes::from_owner
  error.rs          ZMQError with errno (EAGAIN, ETERM, etc.)
  auth.rs           CURVE authenticator: key-list or Python callable
  blake3zmq_auth.rs BLAKE3ZMQ authenticator (same pattern)
```

## Threading model

```
Python threads ──flume──▶ compio thread (single, "pyomq-compio")
                              ├─ socket registry (thread_local HashMap<u64, Rc<Socket>>)
                              ├─ send pump per socket (drain yring → socket)
                              └─ recv pump per socket (socket → yring, signal eventfd)
```

`omq_compio::Socket` holds `Rc`s and is `!Send`. All sockets live on a
single dedicated compio thread. Python wrappers hold only a `u64` id.

Every I/O call from Python posts a closure through an unbounded
`flume::Sender<Job>` channel. The compio thread picks up the job, looks
up the socket in its `thread_local` `HashMap<u64, Rc<InnerSocket>>`
registry, runs the operation, and sends the result back through a
oneshot channel. The Python side blocks on that oneshot with the GIL
released (`py.allow_threads`).

Socket IDs are allocated by `AtomicU64::fetch_add`. They are monotonic
and never recycled.

## Lazy materialization

Sockets are not created on the compio thread at construction time.
`Context.socket()` only allocates a `SocketInner` with an `Overlay`
(option cache). The actual `omq_compio::Socket` is created on the first
I/O call (`bind`, `connect`, `send`, `recv`, etc.) via
`SocketInner::materialize()`.

Materialization:

1. Extract options from the `Overlay` into `omq_compio::Options`.
2. Create yring producer/consumer pairs (capacities from SNDHWM/RCVHWM).
3. Post to compio thread: build the socket, spawn send and recv pump
   tasks, insert into registry.
4. Store `Materialized { id, send_prod, recv_cons, recv_notify }` in
   the `SocketInner`.

This lets Python code do `setsockopt` freely before the socket exists
on the compio thread.

## Queue relay (yring pumps)

Each materialized socket has two pump tasks on the compio thread:

**Send pump.** Drains the `AsyncProducer<Message>` (fed from Python)
into `socket.send()`. Yields every 64 messages (10 µs sleep) to prevent
a single high-volume socket from starving others on the single-threaded
runtime.

**Recv pump.** Drains `socket.recv()` into a `Producer<Message>` (read
by Python). On ring-full, retries with 10 µs sleep (yring backpressure).
After pushing, signals the per-socket `RecvNotify` eventfd and the
global `RECV_READY` flag (used by `wait_any`).

## RecvNotify (eventfd)

`RecvNotify` wraps a Linux `eventfd(EFD_NONBLOCK)` plus an
`AtomicBool parking` flag.

- `notify()`: writes to the eventfd only if `parking` is true. On the
  hot path (consumer not parked), this is a single atomic load with no
  syscall.
- `park_begin()` / `park_end()`: arm/disarm the parking flag.
- `wait_timeout(dur)`: `poll(2)` on the eventfd with a timeout.
- `force_wake()`: unconditional write. Used on socket close to unblock
  any parked recv.
- `dup_fd()`: duplicate the fd for async recv integration.

The parking flag is set before re-checking the consumer. This closes
the race where a notification arrives between the consumer check and
the park.

## Sync send path

```
Socket.send(bytes, flags)
  → build_or_buffer(bytes, flags)
      if SNDMORE: buffer frame, return
      else: assemble Message from buffered frames + this frame
  → send_message(msg)
      prod.push_and_flush(msg)
      if Ok: done (fast path, GIL held)
      if Err (ring full): release GIL, loop:
          sleep 10 µs, retry push_and_flush
          check SNDTIMEO deadline → raise EAGAIN on timeout
```

SNDMORE frames accumulate in a `SendBuffer` (`Vec<Bytes>`). The final
`send` (no SNDMORE flag) flushes all buffered frames plus the final
frame into one multipart `Message`.

## Sync recv path

```
Socket.recv(flags)
  → if rxbuf not empty: pop head frame, return (no lock contention)
  → recv_message()
      lock consumer, try pop (fast path)
      if Some(msg): return first frame, store rest in rxbuf
      else: release GIL, slow path:
          park_begin()
          re-check consumer (closes race)
          loop:
              wait_timeout(100 ms or remaining RCVTIMEO)
              re-check consumer
              if msg: park_end(), return
              check RCVTIMEO deadline → raise EAGAIN
```

Each `recv()` returns one frame. If the message is multipart, remaining
frames go into `rxbuf` and are returned by subsequent `recv()` calls.
`recv_multipart()` returns all frames at once.

## Async send

**Async send is synchronous.** The Python-side `asyncio.py` `Socket.send()`
busy-loops with `time.sleep(0.0001)` (100 µs) on EAGAIN. It is not
truly async. `_send_direct()` pushes to the yring without going through
the compio thread.

This is a known shortcut: send rarely blocks (ring is typically not
full), so the busy-loop is acceptable in practice.

## Async recv

Truly async. `compio_future_into_py()` bridges a Rust future to a
Python `asyncio.Future`:

1. Create `asyncio.Future` via `loop.create_future()`.
2. Post job to compio thread. The builder closure constructs the future
   with `!Send` state (socket access via registry).
3. On completion, acquire GIL and call `loop.call_soon_threadsafe()` to
   set the result on the Python future.

The async recv path differs from sync in one important way: it never
calls `park_end()`. If it did, multiple concurrent async recvs on the
same socket would starve (the first to complete would disarm
notifications). Instead, the recv pump always writes the eventfd when
pushing (one extra syscall per push, but correct under concurrency).

The Python-side `asyncio.py` also has a `_wait_and_recv()` helper that
registers the duplicated eventfd with `loop.add_reader()` for
readiness-based wakeup. Cleanup removes the reader and closes the fd on
both success and `CancelledError`.

## Zero-copy conversions

`PyBytesOwner` holds a `Py<PyBytes>` (preventing GC) and captures the
raw `*const u8` + `len` under the GIL at construction. Because Python
bytes are immutable, the pointer is stable for the object's lifetime.
`Bytes::from_owner(PyBytesOwner)` borrows the buffer without copying.

Other buffer types (`bytearray`, `memoryview`) go through
`copy_from_slice` because their contents can be mutated from Python.

## Proxy

`runtime::proxy()` takes exclusive control of the participating sockets:

1. Stop send/recv pumps on frontend, backend, and optional
   capture/control sockets.
2. Drain any buffered messages from the yring queues.
3. Run `proxy_loop()` with `futures::select!` on `fe.recv()`,
   `be.recv()`, and optional `ctrl.recv()`.
4. Forward messages between frontend and backend. Capture socket
   receives copies of all forwarded messages.
5. Control commands: PAUSE (spin-wait for RESUME), TERMINATE/KILL (exit
   loop).
6. Block the calling Python thread until the loop exits.

## Socket options

`Overlay` is a per-socket option cache that mirrors `omq_proto::Options`
plus wrapper-only fields (RCVTIMEO, SNDTIMEO, LINGER, HWMs).
`setsockopt` writes to the overlay; `materialize()` converts it to
`omq_compio::Options`.

Post-materialization `setsockopt` for SUBSCRIBE/UNSUBSCRIBE dispatches
to the compio thread (the socket must process it). Most other options
are read-only after materialization.

Some options are accepted as no-ops for pyzmq compatibility: IMMEDIATE,
IPV6, RATE, PROBE_ROUTER. Some raise ENOSYS: AFFINITY, BACKLOG.

## Authentication

CURVE and BLAKE3ZMQ share the same bridge pattern. The Python side sets
an authenticator on the overlay via `setsockopt`:

- `None`: clear authenticator.
- Iterable of keys: build a `HashSet` of accepted keys. CURVE keys are
  Z85-encoded strings; BLAKE3ZMQ keys are raw 32-byte `bytes`.
- Callable: wrap as `Py<PyAny>`. Called with a `PeerInfo` pyclass (has
  `.public_key` attribute). Must return truthy/falsy.

At materialization, `build_authenticator()` converts the enum into an
`omq_proto::Authenticator` closure. Callable authenticators acquire the
GIL when invoked from the compio thread.

## Error mapping

`error.rs` maps `omq_proto::Error` variants to libzmq-compatible errno
codes:

| Rust error        | errno         | Python exception      |
|-------------------|---------------|-----------------------|
| `Closed`          | ETERM (156)   | `ContextTerminated`   |
| `Timeout`         | EAGAIN        | `Again`               |
| `HandshakeFailed` | EPROTO        | `ZMQError`            |
| `Unroutable`      | EHOSTUNREACH  | `ZMQError`            |
| `MessageTooLarge` | EMSGSIZE      | `ZMQError`            |
| `InvalidEndpoint` | EINVAL        | `ZMQError`            |
| `Io(e)`           | `e.raw_os_errno()` or EIO | `ZMQError`   |

The Python exception hierarchy matches pyzmq: `ZMQBaseError` is the
root; `ZMQError` and `ZMQBindError` are siblings under it (not
parent-child). `Again`, `ContextTerminated`, `NotImplementedError` are
subclasses of `ZMQError`.

## Monitor

`Socket.monitor()` returns a `Monitor` object with a
`flume::Receiver<MonitorEvent>` and a `lagged: Arc<AtomicU64>` counter
for dropped events on overflow.

- `recv(timeout_ms)`: blocking receive, returns a dict
  (`{"event": "listening", "endpoint": "..."}`).
- `recv_nowait()`: non-blocking, returns dict or `None`.

## Known limitations

- `Poller` registers POLLIN only; POLLOUT is ignored.
- `send(copy=False)` and `send(track=True)` raise `NotImplementedError`.
- `wait_any` returns socket IDs, not file descriptors.
- Async send is synchronous (busy-loop on EAGAIN).

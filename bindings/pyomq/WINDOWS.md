# Windows Support in pyomq

## Status

✅ **Windows support is now available**.

- **Sync API**: Full support on Windows
- **Async API**: Full support with `SelectorEventLoop` (default on Windows)
- **IPC transport**: Not available on Windows (use TCP for local communication)

## Platform Differences

### Windows-Specific Details

#### Event Loop

- **Default**: `SelectorEventLoop` (Python's selector-based multiplexing)
- **Not used**: `ProactorEventLoop` (Windows IOCP) due to architectural constraints
  - See "Why ProactorEventLoop" section below for technical details

#### Notification Mechanism

- **Unix**: eventfd (efficient kernel notification)
- **Windows**: TCP socket pair (127.0.0.1:random_port)
  - Both sockets created locally on-demand
  - Non-blocking I/O for minimal latency
  - Cleaned up automatically on socket closure

#### Performance

- **Sync sockets**: Identical to Unix (uses yring lock-free queue)
- **Async sockets**: Minor overhead from SelectorEventLoop vs ideal IOCP
  - Notification latency: ~1-2 µs per message (negligible at scale)
  - Single TCP write per batch of messages (not per-message overhead)

### Using pyomq on Windows

```python
import pyomq
import pyomq.asyncio as zmq_async
import asyncio

# Sync API (fully supported)
ctx = zmq.Context()
push = ctx.socket(zmq.PUSH)
push.bind("tcp://127.0.0.1:5555")  # Use TCP, not IPC
push.send(b"hello")

# Async API (fully supported, uses SelectorEventLoop)
async def main():
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.SUB)
    sock.subscribe(b"")
    await sock.connect("tcp://127.0.0.1:5555")
    msg = await sock.recv()
    print(msg)

    # Note: IPC endpoints will raise an error on Windows
    # Use TCP instead

asyncio.run(main())
```

## Technical Implementation

### Notification Abstraction

Cross-platform notification primitive (`src/notification.rs`):

```rust
// Unix: efficient eventfd
#[cfg(unix)]
pub struct Notification {
    fd: i32,  // eventfd file descriptor
}

// Windows: TCP socket pair for cross-IOCP bridge
#[cfg(windows)]
pub struct Notification {
    read_sock: TcpStream,   // read end (exposed to asyncio)
    write_sock: TcpStream,  // write end (used for signaling)
}
```

Both implementations present identical API:

- `notify()` — signal the notification
- `dup_fd()` — expose FD to Python's event loop
- `wait_timeout()` — block until signaled (polling fallback)
- `park_begin()/park_end()` — optimization to avoid unnecessary signals

### Integration with asyncio

Python's `asyncio` loop registers the notification FD:

```python
# In bindings/pyomq/python/pyomq/asyncio.py
fd = socket._recv_fd()  # Get notification FD
loop.add_reader(fd, _on_readable)  # Register with event loop
```

On Windows, this registers the TCP socket's read end. When the recv pump writes to the socket, the event loop wakes up and calls the callback.

## Limitations

### IPC Transport

- **Not available**: Unix domain sockets don't exist on Windows
- **Workaround**: Use `tcp://127.0.0.1:<port>` for local communication
- **Error message**: "IPC endpoint parsing not supported on Windows" (graceful)

### Event Loop Selection

- **Default on Windows**: `SelectorEventLoop`
- **Not available**: `ProactorEventLoop` (Windows native IOCP)
  - See "Why ProactorEventLoop is Impossible" below

## Why ProactorEventLoop is Impossible

**Architectural constraint, not a code limitation.**

### The Problem

Windows IOCP (I/O Completion Ports) require socket registration with a specific IOCP handle. In pyomq's threading model:

1. **Python thread** — calls asyncio event loop (has Python's IOCP)
2. **Tokio background thread** — owns socket handles (has tokio's IOCP)
3. **Socket ownership conflict** — sockets registered with tokio's IOCP, not Python's
4. **Result**: Socket completions go to tokio's IOCP, not Python's

```text
Python IOCP ← [waiting, but sees nothing]
Tokio IOCP ← [socket completions arrive here]
```

### Why Unix epoll Works

On Linux/macOS, multiple epoll/select instances can watch the same FD. Both Python's selector and tokio's mio poll the same FD without conflict. Not true for IOCP.

### Workaround: Notification Socket

The notification socket acts as a **bridge** between IOCPs:

```text
Tokio IOCP ← [receives socket completion]
  ↓ (on_recv)
Notification socket write
  ↓
Python IOCP ← [sees notification socket readable]
  ↓
Python asyncio wakes up
  ↓
Python code drains yring
```

## Testing on Windows

### GitHub Actions CI

The CI pipeline now includes `windows-pyomq` job:

```yaml
runs-on: windows-latest
matrix:
  python-version: ["3.9", "3.13"]
```

Tests:

- Sync socket tests (PUSH/PULL, PUB/SUB, REQ/REP, etc.)
- Async socket tests (with SelectorEventLoop)
- Multipart message handling
- Socket options (HWM, linger, etc.)

### Local Testing

```bash
cd bindings/pyomq
python -m venv .venv
.\.venv\Scripts\pip install maturin pytest pytest-asyncio
.\.venv\Scripts\maturin develop --release
.\.venv\Scripts\pytest -v tests/
```

### Troubleshooting

**"no Python 3.x interpreter found"**

- Ensure Python is in PATH
- Or use explicit venv: `.venv\Scripts\maturin develop --release`

**TCP socket pair errors on startup**

- Rare: indicates 127.0.0.1:random_port is unavailable
- Check for port exhaustion or firewall issues

**Event loop selector errors in tests**

- Normal on Windows: SelectorEventLoop doesn't support all FD types
- pyomq only uses sockets, which are fully supported

## Future Work (Tier 2)

When **omq-compio Windows issues are resolved**:

1. Implement feature flag for compio backend selection
2. Add ProactorEventLoop detection and selection
3. New CI job for Windows compio path
4. Performance comparison (Tier 1 vs Tier 2)

Until then, **SelectorEventLoop is the permanent solution** for Windows, not a temporary workaround.

## References

- **Architecture**: [doc/architecture.md](../../doc/architecture.md)
- **Performance**: [doc/performance.md](../../doc/performance.md)
- **Notification implementation**: [src/notification.rs](src/notification.rs)
- **Socket integration**: [src/socket_async.rs](src/socket_async.rs)

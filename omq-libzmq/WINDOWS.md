# Windows Support for omq-libzmq

This document describes the Windows-specific implementation, limitations, and usage guidance for the `omq-libzmq` C API wrapper.

## Overview

`omq-libzmq` provides a libzmq-compatible C API backed by `omq-tokio`. The library provides comprehensive cross-platform Windows support, enabling identical functionality on Windows MSVC and GNU targets as on Unix systems.

## What Works on Windows

### ✅ Supported Features

- **Transports:**
  - `inproc://` — In-process (zero-copy message passing)
  - `tcp://` — TCP/IP network communication

- **Socket Types:**
  - PUSH/PULL
  - PUB/SUB
  - REQ/REP
  - DEALER/ROUTER
  - XPUB/XSUB
  - RADIO/DISH
  - SCATTER/GATHER

- **Security & Compression:**
  - PLAIN authentication (`plain` feature)
  - CURVE key exchange (`curve` feature)
  - BLAKE3 with ChaCha20 (`blake3zmq` feature)
  - LZ4 compression (`lz4` feature)

- **Messaging:**
  - `zmq_send()` / `zmq_recv()` — Blocking and non-blocking send/receive
  - `zmq_poll()` — Event-driven multiplexing (uses native Windows events)

### ⚠️ Platform-Specific Limitations

- **IPC Transport (`ipc://`):**
  - Unix domain sockets are not available on Windows
  - Returns `ENOTSUP` (POSIX error 95) when attempting to use IPC
  - **Workaround:** Use `tcp://127.0.0.1:port` instead for inter-process communication

- **File Descriptor Polling (`ZMQ_FD` socket option):**
  - Windows uses HANDLE-based I/O instead of file descriptors
  - `zmq_getsockopt(sock, ZMQ_FD, ...)` returns `ENOPROTOOPT`
  - **Workaround:** Use `zmq_poll()` for multiplexing instead

- **IPC Bypass Optimization:**
  - Both Unix and Windows use identical lock-free byte ring buffer for inproc PUSH/PULL
  - Zero-copy message passing on both platforms
  - Cross-platform abstraction encapsulated in `notify.rs`

## Cross-Platform Architecture

### Notification Abstraction (`RecvNotify`)

Both Unix and Windows use a unified `RecvNotify` struct for hot-path signal/drain operations:

**Unix Implementation:**

- Wraps `RawFd` (Linux `eventfd` or Unix pipe pair)
- `signal()`: Atomic write to eventfd/pipe (1-8 bytes)
- `drain()`: Non-blocking read to clear the notification
- Inproc PUSH/PULL: Lock-free SPSC ring buffer (`yring`) for zero-copy delivery

**Windows Implementation:**

- Wraps `HANDLE` (manual-reset Windows event)
- `signal()`: `SetEvent()` Win32 API call
- `drain()`: `ResetEvent()` Win32 API call
- Inproc PUSH/PULL: Same lock-free ring buffer as Unix (cross-platform)

**Key Property:** Both implementations are `Copy` and inline-able, eliminating vtable dispatch on the critical path.

### Polling Mechanisms

The `zmq_poll()` implementation uses a unified code path on both Unix and Windows with platform differences encapsulated in abstractions:

**Polling Strategy (Both Platforms):**

1. **Fast path:** Check for buffered inproc messages (zero syscalls)
   - Uses `has_bypass_data()` helper for cross-platform bypass detection
   - Uses `recv_cons` check for yring consumers
2. **Slow path:** Block on OS events via `PollWaiter` abstraction
   - Calls `prepare_for_wait()` to prepare (platform-specific semantics hidden)
   - Unix: `poll()` syscall on file descriptor set
   - Windows: `WaitForMultipleObjects()` on event handles (tiered batching for >64 sockets)
3. **Final check:** Detect messages that arrived while blocking

**Key Property:** No platform-specific code visible in `poll.rs`; all differences encapsulated in `notify.rs`.

### Inproc Optimization (Both Platforms)

Inproc PUSH/PULL connections use the identical lock-free SPSC byte ring buffer (`yring`) on both Unix and Windows:

- Direct memory access, zero-copy message passing
- Atomic signaling via `RecvNotify::signal()` (abstraction hides platform differences)
- `has_bypass_data()` helper provides cross-platform buffered message detection
- `prepare_for_wait()` method ensures platform-consistent polling semantics

## Usage on Windows

### Building

```bash
# MSVC target (recommended on Windows)
cargo build -p omq-libzmq --lib --target x86_64-pc-windows-msvc

# MinGW target (alternative)
cargo build -p omq-libzmq --lib --target x86_64-pc-windows-gnu
```

### Example: Cross-Platform PUSH/PULL

```c
#include "zmq.h"
#include <stdio.h>
#include <string.h>

int main() {
    void *ctx = zmq_ctx_new();

    // PUSH socket (sender)
    void *push = zmq_socket(ctx, ZMQ_PUSH);
    zmq_bind(push, "tcp://127.0.0.1:5555");  // Windows: use TCP instead of IPC

    // PULL socket (receiver)
    void *pull = zmq_socket(ctx, ZMQ_PULL);
    zmq_connect(pull, "tcp://127.0.0.1:5555");

    // Send message
    char msg[] = "Hello Windows!";
    zmq_send(push, msg, strlen(msg), 0);

    // Receive message
    char buffer[256];
    int sz = zmq_recv(pull, buffer, 255, 0);
    buffer[sz] = '\0';
    printf("Received: %s\n", buffer);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
    return 0;
}
```

### Platform-Agnostic Pattern

For maximum portability across Unix and Windows:

1. **Avoid IPC**, use TCP instead:

   ```c
   // Good: Works on Windows
   zmq_bind(sock, "tcp://127.0.0.1:*");

   // Bad: Fails on Windows with ENOTSUP
   zmq_bind(sock, "ipc:///tmp/zmq");
   ```

2. **Avoid file descriptor operations**:

   ```c
   // Good: Works everywhere
   zmq_poll(items, nitems, timeout);

   // Bad: Fails on Windows with ENOPROTOOPT
   int fd = 0;
   zmq_getsockopt(sock, ZMQ_FD, &fd, ...);
   ```

3. **Use inproc for process-local IPC**:

   ```c
   // Works on all platforms (with cross-process limitation)
   zmq_bind(sock, "inproc://internal");
   zmq_connect(other, "inproc://internal");
   ```

## Error Handling

Windows-specific error codes returned by `zmq_errno()`:

| Error | Meaning | Common Cause |
|-------|---------|--------------|
| `ENOTSUP` (95 POSIX) | Operation not supported | IPC transport on Windows, `ZMQ_FD` option on Windows |
| `EINVAL` (22 POSIX) | Invalid argument | Invalid transport string, bad socket type |
| `ETERM` (Custom: 156) | Context terminated | Socket operation after `zmq_ctx_term()` |
| `EAGAIN` (11 POSIX) | Try again | `ZMQ_RCVTIMEO` expired, no message available |

## Performance Characteristics

**TCP Throughput:**

- Windows vs Unix: Near parity; both use kernel TCP stack
- Bottleneck: Network I/O, not signaling mechanism

**Inproc Throughput:**

- **Identical on both platforms:** Lock-free ring buffer with zero-copy messaging
- Hot path: `RecvNotify::signal()` inlines directly (no function call overhead)
- Expected: Microsecond-scale latency for small messages

**Polling Latency:**

- **Unix:** `poll()` syscall (~1-5µs per 10 sockets)
- **Windows:** `WaitForMultipleObjects` + kernel wait (~2-8µs per 10 sockets)
- **Practical impact:** Sub-microsecond differences for typical socket counts

## Testing on Windows

Run the test suite:

```bash
# Run all omq-libzmq tests
cargo test -p omq-libzmq --lib

# Run specific test
cargo test -p omq-libzmq --lib zmq_poll -- --nocapture

# Build and run examples (inproc + TCP only; no IPC)
cargo run --example bench_recv --release -p omq-libzmq
```

## High-Level Polling: Tiered Batching (>64 Sockets)

The Windows implementation automatically handles polling more than 64 sockets using a **tiered batching** strategy that maintains both performance and correctness:

### The Problem: 64-Handle Limit

`WaitForMultipleObjects()` imposes a maximum of 64 handles per call. For applications polling N > 64 sockets, this requires special handling.

### The Solution: Tiered Batching

Sockets are divided into batches of 64 handles each:

1. **Batch 0 (handles 0-63):** Waits with the **full user timeout**
   - Sleeps until events arrive or timeout expires
   - First pass: honors user's time budget

2. **Batches 1+ (handles 64+):** Poll with **timeout=0** (non-blocking)
   - Wake immediately if events ready
   - Don't consume additional time
   - Fall through to completion

### Example: 200 Sockets

```text
User calls:  zmq_poll(items, 200, 5000ms)

Batch 0 (sockets 0-63):
  → WaitForMultipleObjects(64, handles, FALSE, 5000ms)
  → Blocks up to 5 seconds OR until events arrive

IF events found in Batch 0:
  → Return immediately with results

IF Batch 0 timeout (5000ms elapsed):
  → Check Batches 1 & 2 for any buffered messages
  → Return (total time ~5000ms)

IF events in Batch 0 before timeout:
  → Fall through to Batches 1 & 2
  → Poll each with timeout=0 (immediate check)
  → Return all events found (total time << 5000ms)
```

### Key Properties

| Metric | Guarantee |
|--------|-----------|
| **Timeout Accuracy** | First batch honors timeout; batches 1+ don't add delay |
| **Event Loss** | Zero; non-blocking poll ensures no events missed |
| **Syscall Count** | O(n/64); exactly n/64 `WaitForMultipleObjects` calls for n sockets |
| **Latency (100 sockets)** | ~2-10µs typical (2 syscalls: 1 blocking, 1 non-blocking) |

### Buffering Detection (Both Platforms)

When poll() is invoked, the inproc implementation checks for **buffered messages** in the lock-free ring buffer (`yring::Consumer`) BEFORE calling any OS wait function:

```c
// Fast path: check ring buffers first (before syscall)
int ready = check_immediate(items, nitems);
if (ready > 0 || timeout == 0) {
    return ready;  // Fast return, no syscall
}

// Slow path: wait for events
PollWaiter waiter = PollWaiter::new(items, nitems);
waiter.wait(timeout_ms, items);

// Final check: ensure no buffered data was missed
return check_immediate(items, nitems);
```

This is implemented identically on Unix and Windows, ensuring consistent behavior across platforms.

### Implementation Details

**notify.rs** defines the platform-specific polling abstraction:

- **`RecvNotify`:** Atomic signal/drain for inproc messages (portable)
  - Unix: `eventfd` (Linux) or pipe pair (other Unix)
  - Windows: manual-reset `HANDLE`

- **`PollWaiter`:** Platform-specific poll multiplexer
  - Unix: Single `poll()` call on `eventfd` array
  - Windows: Tiered `WaitForMultipleObjects()` with batches of 64

- **`check_immediate()`:** Detects buffered messages without waiting
  - Checks `recv_cons` yring consumers (both platforms)
  - Checks `bypass_recv` byte ring (Unix only; IPC optimization)

**poll.rs** implements the C API entry point `zmq_poll()`:

1. Validate socket pointers and event flags
2. Call `check_immediate()` to detect buffered messages (fast path)
3. Create platform-specific `PollWaiter`
4. Wait for events (Windows: tiered batching; Unix: single poll)
5. Final `check_immediate()` to ensure completeness

### Testing

Comprehensive test coverage validates tiered batching:

- `poll_65_sockets_boundary` — Crosses batch 0→1 boundary
- `poll_128_sockets_boundary` — Tests 2 batches
- `poll_256_sockets_boundary` — Tests 4 batches
- `poll_128_sockets_fairness` — Verifies all sends detected in single poll
- `poll_128_sockets_timeout` — Ensures timeout honored with many sockets

Run tests:

```bash
cargo test -p omq-libzmq poll -- --nocapture
```

### Future Enhancements

**Platform Limitations (by Design):**

1. **IPC Transport Not Available:**
   - Windows lacks Unix domain sockets
   - Use TCP for inter-process communication instead
   - Future: Named Pipes transport (`namedpipe://`) as Windows-native alternative

2. **No File Descriptor Option:**
   - `ZMQ_FD` socket option returns `ENOPROTOOPT`
   - Windows uses handle-based I/O instead of file descriptors
   - Use `zmq_poll()` for multiplexing instead

**Potential Enhancements:**

1. Named Pipes transport for Windows-native IPC between processes
2. Performance optimization for high-concurrency (>128 sockets)
3. Windows Event Tracing (ETW) integration for diagnostics
4. Windows-specific socket option extensions

## Building Against omq-libzmq on Windows

### Dynamic Library (DLL)

```bash
cargo build -p omq-libzmq --release
# Produces: target/release/omq_zmq.dll
```

### Static Library

```bash
cargo build -p omq-libzmq --release --lib
# Produces: target/release/omq_zmq.lib
```

### Linking in Visual Studio

```c
#pragma comment(lib, "omq_zmq.lib")
#include "zmq.h"
```

### Linking with MinGW

```bash
gcc myprogram.c -L. -lomq_zmq -o myprogram.exe
```

## Windows CI/CD

The CI pipeline (`ci.yml`) includes:

- **MSVC Target:** `x86_64-pc-windows-msvc` (primary recommendation)
- **GNU Target:** `x86_64-pc-windows-gnu` (alternative)
- **Tests:** `cargo test -p omq-libzmq --lib --target <target>`

All commits to `main` trigger Windows builds on both targets.

## Debugging on Windows

### With MSVC Debugger

```bash
# Build with debug info
cargo build -p omq-libzmq

# Attach debugger to devenv
devenv /debugexe target\debug\omq_zmq.dll
```

### With WinDbg

```bash
# Build with symbols
cargo rustc -p omq-libzmq -- -g

# Launch WinDbg
windbg target\debug\omq_zmq.dll
```

### With gdb (MinGW)

```bash
# Build with MinGW target
cargo build -p omq-libzmq --target x86_64-pc-windows-gnu

# Debug
x86_64-w64-mingw32-gdb target/x86_64-pc-windows-gnu/debug/omq_zmq.dll
```

## Contributing Windows-Specific Code

When adding Windows-specific features:

1. Use `#[cfg(windows)]` guards for platform-specific code
2. Provide Unix equivalents where possible
3. Add cross-platform tests (at minimum: inproc + TCP)
4. Document limitations clearly
5. Ensure Windows CI passes before merging

## References

- [Windows API Documentation](https://learn.microsoft.com/en-us/windows/win32/api/)
- [ZMQ Protocol Specification](https://rfc.zeromq.org/)
- [omq-libzmq README](README.md)
- [omq.rs Architecture](../doc/architecture.md)

//! `zmq_poll` -- multiplexed I/O readiness.
//!
//! # Architecture
//!
//! ## Cross-Platform Polling
//!
//! Provides libzmq-compatible `zmq_poll()` for event-driven socket multiplexing:
//! - **Unix:** Uses `poll()` on eventfds (Linux) or pipe pairs (other Unix)
//! - **Windows:** Uses `WaitForMultipleObjects()` with manual-reset events (tiered batching for >64 handles)
//!
//! ## Inproc Message Detection (Key Feature)
//!
//! Inproc (in-process) messages are delivered via lock-free ring buffers (yring consumers),
//! not through the OS notification mechanism. This means `zmq_poll()` must actively check
//! for buffered messages BEFORE and AFTER waiting on OS events:
//!
//! ### Fast Path (`check_immediate`)
//!
//! Before blocking on OS events, scan all sockets for buffered messages:
//! - **`recv_cons`:** Per-socket yring consumers containing inproc messages
//!   - `fast`: Direct SPSC from first inproc peer (zero-copy, no atomics)
//!   - `pump`: Fallback for additional peers (fair queuing)
//!   - Check: `!fast.is_empty() || !pump.is_empty()` (one Acquire load each)
//! - **`bypass_recv`:** Cross-platform optimization for PUSH→PULL inproc byte-ring
//!   - Available on both Unix and Windows
//!   - Check: via `crate::notify::has_bypass_data()` abstraction
//!
//! If messages found, return immediately (zero syscalls).
//! If timeout=0 is specified, also return immediately (poll semantics).
//!
//! ### Slow Path (wait on OS)
//!
//! If no immediate messages and timeout > 0:
//! 1. Create platform-specific `PollWaiter` (Unix: pfds array; Windows: event handles)
//! 2. Call `waiter.prepare_for_wait()` to prepare (platform-specific semantics hidden)
//! 3. Call `PollWaiter::wait()` with user timeout
//!    - **Unix:** Single `poll()` syscall on all fds
//!    - **Windows:** Tiered `WaitForMultipleObjects()` with batching
//!      - Batch 0: Full timeout
//!      - Batches 1+: Non-blocking (timeout=0)
//! 4. Perform FINAL `check_immediate()` to catch buffered messages
//!
//! This ensures:
//! - Buffered inproc messages never missed
//! - OS events always detected
//! - Timeout honored correctly (Windows: first batch only)
//!
//! ## Platform Abstractions
//!
//! Both Unix and Windows use identical poll.rs code with zero platform gates (no `#[cfg(unix)]`/`#[cfg(windows)]`).
//! All platform-specific logic is encapsulated in `notify.rs`:
//! - `has_bypass_data()` — Cross-platform bypass check
//! - `PollWaiter::prepare_for_wait()` — Platform-specific preparation (Unix: drain; Windows: no-op)
//! - `PollWaiter::wait()` — Platform-specific blocking (`poll()` vs `WaitForMultipleObjects()`)
//!
//! See `notify.rs` for platform implementations.

use std::ffi::c_int;
use std::sync::Arc;

use crate::consts;
use crate::socket::OmqSocket;

pub(crate) const ZMQ_POLLIN: libc::c_short = consts::ZMQ_POLLIN as libc::c_short;
pub(crate) const ZMQ_POLLOUT: libc::c_short = consts::ZMQ_POLLOUT as libc::c_short;
#[allow(dead_code)]
pub(crate) const ZMQ_POLLERR: libc::c_short = consts::ZMQ_POLLERR as libc::c_short;

/// `zmq_pollitem_t` layout compatible with libzmq.
#[repr(C)]
#[derive(Debug)]
pub struct ZmqPollItem {
    pub socket: *mut libc::c_void,
    pub fd: libc::c_int,
    pub events: libc::c_short,
    pub revents: libc::c_short,
}

/// Check for immediately-available events without blocking.
///
/// This function scans all poll items for:
/// 1. **Buffered inproc messages** in yring consumers (`recv_cons`, `bypass_recv`)
/// 2. **Leftover multipart frames** from a previous recv (`drain_nonempty` flag)
///
/// # Algorithm
///
/// For each POLLIN item:
/// - Check `recv_cons.fast.is_empty()` — first peer's direct SPSC messages
/// - Check `recv_cons.pump.is_empty()` — additional peers' messages (fair queuing)
/// - Check `has_bypass_data(sock)` — cross-platform IPC optimization (Unix and Windows)
/// - Check `drain_nonempty` — leftover multipart frames needing draining
///
/// For each POLLOUT item:
/// - Mark writable if socket is not blocking
///
/// # Key Insight
///
/// Buffered inproc messages are NOT delivered through OS events (eventfd, HANDLE).
/// Instead, they sit in lock-free yring ring buffers, discovered only by explicit
/// checking. This is why `check_immediate()` must run BEFORE blocking on OS wait,
/// and again AFTER wait returns (to detect messages arrived while poll was blocking).
///
/// # Performance
///
/// Each `is_empty()` check is O(1) with one Acquire load from the ring buffer's atomic
/// tail pointer. Total cost for n sockets: O(n) with minimal atomics.
///
/// # Cross-Platform Implementation
///
/// All checks work identically on Unix and Windows:
/// - `recv_cons` populated and checked on both platforms
/// - `bypass_recv` available and checked on both platforms
/// - `drain_nonempty` checked on both platforms
/// - Platform differences encapsulated in `notify.rs` abstractions (`has_bypass_data()`, etc.)
fn check_immediate(items: &mut [ZmqPollItem]) -> i32 {
    let mut ready = 0i32;
    for item in items.iter_mut() {
        item.revents = 0;
        if item.socket.is_null() {
            continue;
        }
        // SAFETY: socket is non-null (checked above); caller guarantees a valid socket.
        let sock = unsafe { &*(item.socket.cast::<Arc<OmqSocket>>()) };

        if (item.events & ZMQ_POLLIN) != 0 {
            let has_buffered = sock
                .drain_nonempty
                .load(std::sync::atomic::Ordering::Relaxed)
                || sock
                    .recv_cons
                    .get()
                    .as_ref()
                    .is_some_and(|c| !c.fast.is_empty() || !c.pump.is_empty())
                || sock
                    .bypass_recv
                    .get()
                    .as_ref()
                    .is_some_and(|br| !br.is_empty());
            if has_buffered {
                item.revents |= ZMQ_POLLIN;
            }
        }
        if (item.events & ZMQ_POLLOUT) != 0 {
            item.revents |= ZMQ_POLLOUT;
        }
        if item.revents != 0 {
            ready += 1;
        }
    }
    ready
}

/// Accumulate buffered inproc messages to existing revents (without clearing).
/// Called after `waiter.wait()` to detect buffered messages that arrived while blocking.
/// Preserves any revents already set by OS events.
fn accumulate_buffered(items: &mut [ZmqPollItem]) -> i32 {
    let mut ready = 0i32;
    for item in items.iter_mut() {
        if item.socket.is_null() {
            continue;
        }
        // SAFETY: socket is non-null (checked above); caller guarantees a valid socket.
        let sock = unsafe { &*(item.socket.cast::<Arc<OmqSocket>>()) };

        if (item.events & ZMQ_POLLIN) != 0 {
            let drain_nonempty = sock
                .drain_nonempty
                .load(std::sync::atomic::Ordering::Relaxed);

            let cons_ptr = &*sock.recv_cons.get();
            let recv_cons_has_data = cons_ptr
                .as_ref()
                .is_some_and(|c| !c.fast.is_empty() || !c.pump.is_empty());

            let bypass_recv_has_data = crate::notify::has_bypass_data(sock);

            let has_buffered = drain_nonempty || recv_cons_has_data || bypass_recv_has_data;

            if has_buffered {
                if item.revents == 0 {
                    ready += 1;
                }
                item.revents |= ZMQ_POLLIN;
            }
        }
    }
    ready
}

/// Cross-platform C API for `zmq_poll`.
///
/// Multiplexed I/O readiness on one or more sockets or file descriptors.
/// Uses unified code path on all platforms with platform differences encapsulated in abstractions.
///
/// **Implementation:**
/// - Fast path: Check immediately available messages (zero syscalls)
/// - Slow path: Block on OS events via `PollWaiter` abstraction
///   - Unix: `poll()` on event file descriptors (eventfd on Linux, pipes on other Unix)
///   - Windows: `WaitForMultipleObjects()` with tiered batching (supports >64 sockets)
/// - Final check: Detect messages that arrived while blocking
///
/// **Note:** item.fd polling on Unix works; Windows sockets must use item.socket
#[unsafe(no_mangle)]
pub extern "C" fn zmq_poll(
    items: *mut ZmqPollItem,
    nitems: c_int,
    timeout_ms: libc::c_long,
) -> c_int {
    if nitems < 0 {
        return crate::error::fail(libc::EINVAL);
    }
    if items.is_null() && nitems > 0 {
        return crate::error::fail(libc::EFAULT);
    }
    let n = nitems as usize;
    // SAFETY: items is non-null (checked above) with nitems elements.
    let items_slice = unsafe { std::slice::from_raw_parts_mut(items, n) };

    // Fast path: check for immediately available messages
    let ready = check_immediate(items_slice);
    if ready > 0 || timeout_ms == 0 {
        return ready;
    }

    // Create platform-specific poller for this set of items
    let mut waiter = crate::notify::PollWaiter::new(items_slice);

    // If no handles/fds to wait on, just sleep and return
    if waiter.has_no_handles() {
        if timeout_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(timeout_ms as u64));
        }
        return 0;
    }

    // Prepare poller (platform-specific semantics encapsulated in prepare_for_wait):
    // - Unix: drain accumulated eventfd signals
    // - Windows: no-op (manual-reset events don't accumulate)
    waiter.prepare_for_wait();

    // Check again after preparing poller (may have found buffered data)
    let ready = check_immediate(items_slice);
    if ready > 0 {
        return ready;
    }

    // Wait for events
    let _rc = waiter.wait(timeout_ms, items_slice);

    // Accumulate buffered inproc messages to OS events (don't clear existing revents)
    // This ensures we report both OS-detected events and buffered inproc data
    let _buffered = accumulate_buffered(items_slice);

    // Count total items with any revents set (combines OS events + buffered data)
    let mut ready = 0i32;
    for item in items_slice {
        if item.revents != 0 {
            ready += 1;
        }
    }
    ready
}

//! Cross-platform notification handle abstraction for socket signaling.
//!
//! This module provides a unified `NotifyHandle` trait and platform-specific implementations for
//! signaling inproc message arrival and send buffer availability. It bridges the gap between
//! Unix file descriptor semantics and Windows handle-based I/O.
//!
//! ## Architecture
//!
//! ### Notification Primitives
//!
//! **Unix Implementation:**
//! - **Linux:** eventfd (64-bit atomic counter, supports `EV_CLOEXEC`)
//! - **Other Unix:** Pipe pair (`recv_read/recv_write` + `send_read/send_write`)
//! - Operations: atomic write to signal, non-blocking read to drain, poll to wait
//!
//! **Windows Implementation:**
//! - Manual-reset events (HANDLE returned by `CreateEventW`)
//! - Operations: `SetEvent()` to signal, explicit `ResetEvent()` drain before each `WaitForMultipleObjects`
//!
//! ### Inproc Message Delivery (Cross-Platform)
//!
//! Both Unix and Windows use identical infrastructure for inproc message delivery:
//! - Inproc bypass byte rings for PUSH/PULL optimization (cross-platform, `RecvNotify` signaling)
//! - Lock-free yring consumers for message buffering (cross-platform)
//! - `RecvNotify` (platform-specific, Copy, monomorphic) for signal/drain/wait operations
//!
//! **Message flow (all platforms):**
//! 1. **Send side:** PUSH writes frame to yring, calls `RecvNotify::signal()`
//! 2. **Poll side:** Checks `yring::Consumer::is_empty()` before blocking
//! 3. **Recv side:** PULL reads frames from yring
//! 4. **Block side:** Calls `RecvNotify::wait_for_readable()` if no buffered messages
//!
//! The signaling mechanism is platform-specific (eventfd write vs `SetEvent` call), but the
//! message buffering and blocking strategy are identical, ensuring consistent behavior across platforms.
//!
//! ### Polling Mechanisms
//!
//! **Unix:** Single `poll()` syscall on array of file descriptors
//! - O(n) syscalls for n sockets
//! - Kernel updates revents in-place
//!
//! **Windows:** Tiered `WaitForMultipleObjects()` with 64-handle batching
//! - O(n/64) syscalls for n sockets
//! - First batch honors user timeout; subsequent batches poll non-blocking
//! - Zero event loss; final check detects buffered messages
//!
//! ### Abstraction Boundaries
//!
//! - `NotifyHandle` trait: Platform-agnostic signal/drain interface (dynamic dispatch for setup)
//! - `RecvNotify` struct: Platform-specific hot-path operations (Copy, monomorphic, inlineable)
//! - `PollWaiter` struct: Platform-specific polling (static dispatch via `#[cfg(...)]`)
//!
//! Call sites in `poll.rs` and `send_recv.rs` use:
//! - `sock.notify.recv_notifier()` to get `RecvNotify` (no platform-specific code visible)
//! - `RecvNotify::signal()` / `drain()` / `wait_for_readable()` for all blocking/signaling needs
//! - `PollWaiter::new()` (compile-time selection) for multi-socket polling

// Default channel capacity (matches default HWM in socket.rs).
#[allow(dead_code)]
const DEFAULT_HWM: usize = 1000;

/// Platform-agnostic notification handle for signaling recv/send events.
#[allow(dead_code)]
pub(crate) trait NotifyHandle: Send + Sync {
    /// Signal that a message has arrived (recv event).
    fn signal_recv(&self);

    /// Signal that a send slot has been freed (send event).
    fn signal_send(&self);

    /// Close and clean up resources.
    fn close(&self);

    /// Get the raw receive FD for polling (Unix only; returns -1 on Windows).
    fn recv_fd(&self) -> std::os::raw::c_int;

    /// Get the raw send FD for polling (Unix only; returns -1 on Windows).
    fn send_fd(&self) -> std::os::raw::c_int;

    /// Create a platform-specific notifier for hot-path signal/drain operations.
    /// This method encapsulates platform detection and primitive extraction,
    /// allowing call sites to avoid fd/event terminology entirely.
    fn recv_notifier(&self) -> RecvNotify;
}

#[cfg(unix)]
mod unix {
    use super::{DEFAULT_HWM, NotifyHandle};

    /// Platform-specific notification handle for signal/drain operations.
    /// Monomorphic (no vtable), compile-time platform selection via #[cfg(...)].
    /// Used by hot paths (inproc bypass, recv pump signaling).
    #[derive(Clone, Copy)]
    pub(crate) struct RecvNotify {
        fd: std::os::unix::io::RawFd,
    }

    // SAFETY: RecvNotify is Send+Sync because:
    // - Field `fd` is a std::os::unix::io::RawFd (i32), which is just a handle/reference
    // - RawFd itself is Send+Sync (it's an i32, not a resource ownership type)
    // - File descriptors can be safely sent across threads; the kernel maintains state
    // - Ownership model ensures logical single-threaded access (socket owns the fd)
    // - Operations (write, read, poll) are atomic at syscall level
    unsafe impl Send for RecvNotify {}
    unsafe impl Sync for RecvNotify {}

    impl RecvNotify {
        /// Signal that a message has arrived (non-blocking write to eventfd/pipe).
        #[inline]
        pub(crate) fn signal(self) {
            if self.fd < 0 {
                return;
            }
            #[cfg(target_os = "linux")]
            {
                let val: u64 = 1;
                // SAFETY: fd is a valid eventfd; writing 8 bytes atomically increments.
                unsafe {
                    libc::write(self.fd, (&raw const val).cast::<libc::c_void>(), 8);
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                let b: u8 = 1;
                // SAFETY: fd is a valid pipe write end; writing 1 byte signals.
                unsafe {
                    libc::write(self.fd, (&raw const b).cast::<libc::c_void>(), 1);
                }
            }
        }

        /// Drain notification state (non-blocking read to clear eventfd/pipe).
        /// Called when ring becomes empty so `poll()` sees fd as not-readable.
        pub(crate) fn drain(self) {
            if self.fd < 0 {
                return;
            }
            #[cfg(target_os = "linux")]
            {
                let mut buf = 0u64;
                // SAFETY: fd is a valid eventfd; 8-byte read drains the counter.
                unsafe {
                    libc::read(self.fd, (&raw mut buf).cast::<libc::c_void>(), 8);
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                let mut buf = [0u8; 64];
                loop {
                    // SAFETY: fd is a valid pipe read end; draining signal bytes.
                    let n = unsafe {
                        libc::read(self.fd, buf.as_mut_ptr().cast::<libc::c_void>(), buf.len())
                    };
                    if n <= 0 {
                        break;
                    }
                }
            }
        }

        /// Wait until the notification is readable or timeout expires.
        /// Returns true if readable, false on timeout/error.
        /// Used by blocking recv operations to wait for message arrival.
        pub(crate) fn wait_for_readable(self, timeout_ms: std::os::raw::c_int) -> bool {
            if self.fd < 0 {
                return false;
            }
            let mut pfd = libc::pollfd {
                fd: self.fd,
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: pfd is a valid single-element pollfd array.
            let rc = unsafe { libc::poll(&raw mut pfd, 1, timeout_ms) };
            rc > 0
        }
    }

    /// Unix implementation: eventfd on Linux, pipe pairs on other Unix.
    #[expect(dead_code)]
    pub(crate) struct UnixNotifyHandle {
        linux: Option<LinuxEventFd>,
        unix: Option<UnixPipeFd>,
    }

    struct LinuxEventFd {
        recv_fd: std::os::raw::c_int,
        send_fd: std::os::raw::c_int,
    }

    #[allow(dead_code)]
    struct UnixPipeFd {
        recv_read: std::os::raw::c_int,
        recv_write: std::os::raw::c_int,
        send_read: std::os::raw::c_int,
        send_write: std::os::raw::c_int,
    }

    impl UnixNotifyHandle {
        pub(crate) fn new() -> Option<Self> {
            #[cfg(target_os = "linux")]
            {
                Self::new_linux()
            }
            #[cfg(not(target_os = "linux"))]
            {
                Self::new_pipes()
            }
        }

        #[cfg(target_os = "linux")]
        fn new_linux() -> Option<Self> {
            // Non-semaphore: a single read returns the accumulated count
            // and resets to 0. This allows O(1) drain instead of O(N).
            // SAFETY: eventfd returns a valid fd on success, -1 on failure.
            let recv_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
            if recv_fd < 0 {
                return None;
            }
            // SAFETY: same as above.
            let send_fd = unsafe { libc::eventfd(DEFAULT_HWM as u32, libc::EFD_NONBLOCK) };
            if send_fd < 0 {
                // SAFETY: recv_fd is a valid open fd from the successful eventfd call above.
                unsafe { libc::close(recv_fd) };
                return None;
            }
            Some(Self {
                linux: Some(LinuxEventFd { recv_fd, send_fd }),
                unix: None,
            })
        }

        #[cfg(not(target_os = "linux"))]
        fn new_pipes() -> Option<Self> {
            let mut fds = [-1i32; 2];
            // SAFETY: fds is a valid 2-element array; pipe() writes two fds into it on success.
            let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
            if rc != 0 {
                return None;
            }
            // SAFETY: fds[0] and fds[1] are valid fds from the successful pipe() call.
            unsafe {
                libc::fcntl(fds[0], libc::F_SETFL, libc::O_NONBLOCK);
                libc::fcntl(fds[1], libc::F_SETFL, libc::O_NONBLOCK);
            }
            let (recv_read, recv_write) = (fds[0], fds[1]);
            // SAFETY: same as above.
            let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
            if rc != 0 {
                // SAFETY: recv_read and recv_write are valid open fds.
                unsafe {
                    libc::close(recv_read);
                    libc::close(recv_write);
                }
                return None;
            }
            // SAFETY: fds[0] and fds[1] are valid fds from the successful pipe() call.
            unsafe {
                libc::fcntl(fds[0], libc::F_SETFL, libc::O_NONBLOCK);
                libc::fcntl(fds[1], libc::F_SETFL, libc::O_NONBLOCK);
            }
            let (send_read, send_write) = (fds[0], fds[1]);
            Some(Self {
                linux: None,
                unix: Some(UnixPipeFd {
                    recv_read,
                    recv_write,
                    send_read,
                    send_write,
                }),
            })
        }
    }

    impl NotifyHandle for UnixNotifyHandle {
        fn signal_recv(&self) {
            #[cfg(target_os = "linux")]
            if let Some(linux) = &self.linux {
                let val: u64 = 1;
                // SAFETY: fd is a valid eventfd; writing 8 bytes atomically increments the counter.
                unsafe {
                    libc::write(linux.recv_fd, (&raw const val).cast::<libc::c_void>(), 8);
                }
            }
            #[cfg(not(target_os = "linux"))]
            if let Some(unix) = &self.unix {
                let b: u8 = 1;
                // SAFETY: fd is a valid pipe write end; writing 1 byte signals readiness.
                unsafe {
                    libc::write(unix.recv_write, (&raw const b).cast::<libc::c_void>(), 1);
                }
            }
        }

        fn signal_send(&self) {
            #[cfg(target_os = "linux")]
            if let Some(linux) = &self.linux {
                let val: u64 = 1;
                // SAFETY: fd is a valid eventfd; writing 8 bytes atomically decrements the counter.
                unsafe {
                    libc::write(linux.send_fd, (&raw const val).cast::<libc::c_void>(), 8);
                }
            }
            #[cfg(not(target_os = "linux"))]
            if let Some(unix) = &self.unix {
                let b: u8 = 1;
                // SAFETY: fd is a valid pipe write end; writing 1 byte signals readiness.
                unsafe {
                    libc::write(unix.send_write, (&raw const b).cast::<libc::c_void>(), 1);
                }
            }
        }

        fn close(&self) {
            #[cfg(target_os = "linux")]
            if let Some(linux) = &self.linux {
                // SAFETY: recv_fd and send_fd are valid fds opened by new().
                unsafe {
                    libc::close(linux.recv_fd);
                    libc::close(linux.send_fd);
                }
            }
            #[cfg(not(target_os = "linux"))]
            if let Some(unix) = &self.unix {
                // SAFETY: all fds are valid, opened by new().
                unsafe {
                    libc::close(unix.recv_read);
                    libc::close(unix.recv_write);
                    libc::close(unix.send_read);
                    libc::close(unix.send_write);
                }
            }
        }

        fn recv_fd(&self) -> std::os::raw::c_int {
            #[cfg(target_os = "linux")]
            {
                self.linux.as_ref().map(|l| l.recv_fd).unwrap_or(-1)
            }
            #[cfg(not(target_os = "linux"))]
            {
                self.unix.as_ref().map(|u| u.recv_read).unwrap_or(-1)
            }
        }

        fn send_fd(&self) -> std::os::raw::c_int {
            #[cfg(target_os = "linux")]
            {
                self.linux.as_ref().map(|l| l.send_fd).unwrap_or(-1)
            }
            #[cfg(not(target_os = "linux"))]
            {
                self.unix.as_ref().map(|u| u.send_read).unwrap_or(-1)
            }
        }

        fn recv_notifier(&self) -> RecvNotify {
            RecvNotify { fd: self.recv_fd() }
        }
    }

    impl std::fmt::Debug for UnixNotifyHandle {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("UnixNotifyHandle { .. }")
        }
    }

    /// Platform-specific poller for collecting socket fds and waiting for readiness.
    /// Unix: Wraps libc::poll() directly on eventfds/pipes.
    pub(crate) struct PollWaiter {
        pfds: Vec<libc::pollfd>,
        map: Vec<(usize, libc::c_short)>, // (item_idx, zmq_event_type)
    }

    impl PollWaiter {
        /// Collect recv/send fds from poll items into a pollfd array.
        pub(crate) fn new(items: &[crate::poll::ZmqPollItem]) -> Self {
            let mut pfds = Vec::new();
            let mut map = Vec::new();

            for (i, item) in items.iter().enumerate() {
                if !item.socket.is_null() {
                    // SAFETY: socket is non-null (checked above); caller guarantees valid socket.
                    let sock = unsafe {
                        &*(item
                            .socket
                            .cast::<std::sync::Arc<crate::socket::OmqSocket>>())
                    };

                    if (item.events & crate::poll::ZMQ_POLLIN as libc::c_short) != 0 {
                        let fd = sock.notify.recv_fd();
                        if fd >= 0 {
                            pfds.push(libc::pollfd {
                                fd,
                                events: libc::POLLIN,
                                revents: 0,
                            });
                            map.push((i, crate::poll::ZMQ_POLLIN as libc::c_short));
                        }
                    }
                    if (item.events & crate::poll::ZMQ_POLLOUT as libc::c_short) != 0 {
                        let fd = sock.notify.send_fd();
                        if fd >= 0 {
                            pfds.push(libc::pollfd {
                                fd,
                                events: libc::POLLIN,
                                revents: 0,
                            });
                            map.push((i, crate::poll::ZMQ_POLLOUT as libc::c_short));
                        }
                    }
                } else if item.fd >= 0 {
                    let mut events: libc::c_short = 0;
                    if (item.events & crate::poll::ZMQ_POLLIN as libc::c_short) != 0 {
                        events |= libc::POLLIN as libc::c_short;
                    }
                    if (item.events & crate::poll::ZMQ_POLLOUT as libc::c_short) != 0 {
                        events |= libc::POLLOUT as libc::c_short;
                    }
                    if (item.events & crate::poll::ZMQ_POLLERR as libc::c_short) != 0 {
                        events |= libc::POLLERR as libc::c_short;
                    }
                    pfds.push(libc::pollfd {
                        fd: item.fd,
                        events,
                        revents: 0,
                    });
                    map.push((i, 0));
                }
            }

            Self { pfds, map }
        }

        /// Check if this poller has any handles to wait on.
        pub(crate) fn has_no_handles(&self) -> bool {
            self.pfds.is_empty()
        }

        /// Prepare poller for wait: drain all recv notification fds/eventfds before blocking poll.
        /// Called before waiting to clear any accumulated signals.
        pub(crate) fn prepare_for_wait(&mut self) {
            for (_pfd_idx, pfd) in self.pfds.iter_mut().enumerate() {
                if pfd.fd < 0 {
                    continue;
                }
                #[cfg(target_os = "linux")]
                {
                    let mut val = 0u64;
                    // SAFETY: fd is a valid eventfd; 8-byte read drains the counter.
                    unsafe { libc::read(pfd.fd, (&raw mut val).cast(), 8) };
                }
                #[cfg(not(target_os = "linux"))]
                {
                    let mut buf = [0u8; 64];
                    loop {
                        // SAFETY: fd is a valid pipe read end; draining signal bytes.
                        let n = unsafe {
                            libc::read(pfd.fd, buf.as_mut_ptr().cast::<libc::c_void>(), buf.len())
                        };
                        if n <= 0 {
                            break;
                        }
                    }
                }
            }
        }

        /// Wait for events with the given timeout (in milliseconds).
        /// Returns number of items with events.
        pub(crate) fn wait(
            &mut self,
            timeout_ms: libc::c_long,
            items: &mut [crate::poll::ZmqPollItem],
        ) -> std::os::raw::c_int {
            if self.pfds.is_empty() {
                return 0;
            }

            let poll_timeout = if timeout_ms < 0 {
                -1
            } else {
                timeout_ms as std::os::raw::c_int
            };

            // SAFETY: pfds is a valid pollfd array; poll blocks until events or timeout.
            let rc = unsafe {
                libc::poll(
                    self.pfds.as_mut_ptr(),
                    self.pfds.len() as libc::nfds_t,
                    poll_timeout,
                )
            };
            if rc < 0 {
                return crate::error::fail(
                    std::io::Error::last_os_error()
                        .raw_os_error()
                        .unwrap_or(libc::EINTR) as std::os::raw::c_int,
                );
            }
            if rc == 0 {
                return 0;
            }

            // Clear revents before accumulating results
            for item in items.iter_mut() {
                item.revents = 0;
            }

            let mut ready_count = 0i32;
            for (pfd_idx, pfd) in self.pfds.iter().enumerate() {
                if pfd.revents == 0 {
                    continue;
                }
                let (item_idx, zmq_event) = self.map[pfd_idx];

                if zmq_event == 0 {
                    // Raw fd item
                    if (pfd.revents & libc::POLLIN as libc::c_short) != 0 {
                        items[item_idx].revents |= crate::poll::ZMQ_POLLIN as libc::c_short;
                    }
                    if (pfd.revents & libc::POLLOUT as libc::c_short) != 0 {
                        items[item_idx].revents |= crate::poll::ZMQ_POLLOUT as libc::c_short;
                    }
                    if (pfd.revents
                        & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) as libc::c_short)
                        != 0
                    {
                        items[item_idx].revents |= crate::poll::ZMQ_POLLERR as libc::c_short;
                    }
                } else {
                    // ZMQ socket item
                    items[item_idx].revents |= zmq_event;
                }

                if items[item_idx].revents != 0 && ready_count == 0 {
                    ready_count = 1;
                } else if items[item_idx].revents != 0 {
                    ready_count += 1;
                }
            }

            ready_count
        }
    }

    #[allow(unused_imports)]
    pub(super) use UnixNotifyHandle as PlatformNotifyHandle;
}

#[cfg(windows)]
pub(crate) mod windows {
    use super::NotifyHandle;
    use std::sync::Mutex;
    use windows::Win32::Foundation::{CloseHandle, HANDLE};
    use windows::Win32::System::Threading::{CreateEventW, ResetEvent, SetEvent};

    /// Platform-specific notification handle for signal/drain operations.
    /// Monomorphic (no vtable), compile-time platform selection via #[cfg(...)].
    /// Used by hot paths (inproc bypass, recv pump signaling).
    #[derive(Clone, Copy)]
    pub(crate) struct RecvNotify {
        event: HANDLE,
    }

    // SAFETY: RecvNotify is Send+Sync because:
    // - Field `event` is a windows::Win32::Foundation::HANDLE (opaque kernel object reference)
    // - HANDLE is not a resource ownership type; it's just an identifier/reference to kernel state
    // - Multiple threads can safely hold the same HANDLE; Windows kernel ensures synchronization
    // - SetEvent() and ResetEvent() are internally atomic operations
    // - The actual Windows kernel event object is thread-safe by design
    unsafe impl Send for RecvNotify {}
    unsafe impl Sync for RecvNotify {}

    impl RecvNotify {
        /// Signal by setting the event (non-blocking, wake any waiters).
        #[inline]
        pub(crate) fn signal(self) {
            unsafe {
                let _ = SetEvent(self.event);
            }
        }

        /// Drain by resetting the event (clear for next signal).
        pub(crate) fn drain(self) {
            unsafe {
                let _ = ResetEvent(self.event);
            }
        }

        /// Wait until the notification is signaled or timeout expires.
        /// Returns true if signaled, false on timeout/error.
        /// Used by blocking recv operations to wait for message arrival.
        pub(crate) fn wait_for_readable(self, timeout_ms: std::os::raw::c_int) -> bool {
            use windows::Win32::Foundation::WAIT_OBJECT_0;
            use windows::Win32::System::Threading::WaitForSingleObject;

            let wait_ms = if timeout_ms < 0 {
                u32::MAX
            } else {
                timeout_ms as u32
            };
            // SAFETY: self.event is a valid HANDLE created by CreateEventW.
            let rc = unsafe { WaitForSingleObject(self.event, wait_ms) };
            rc == WAIT_OBJECT_0
        }
    }

    /// Windows implementation using manual-reset events.
    ///
    /// Creates two manual-reset events for recv and send notifications,
    /// leveraging `CreateEventW` / `SetEvent` / `CloseHandle` Win32 API.
    pub(crate) struct WindowsNotifyHandle {
        recv_event: Mutex<Option<HANDLE>>,
        send_event: Mutex<Option<HANDLE>>,
    }

    // SAFETY: WindowsNotifyHandle is Send+Sync because:
    // - Contains `Mutex<Option<HANDLE>>` which is Send+Sync when T is Send+Sync
    // - RecvNotify's HANDLE is Send+Sync (kernel object reference, not owned resource)
    // - Mutex ensures thread-safe interior mutability
    // - Multiple threads can safely access the underlying HANDLE through Mutex
    unsafe impl Send for WindowsNotifyHandle {}
    unsafe impl Sync for WindowsNotifyHandle {}

    impl std::fmt::Debug for WindowsNotifyHandle {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("WindowsNotifyHandle")
                .field("recv_event", &"<HANDLE>")
                .field("send_event", &"<HANDLE>")
                .finish()
        }
    }

    impl WindowsNotifyHandle {
        pub(crate) fn new() -> Self {
            // Create two manual-reset events (initially non-signaled)
            // Manual-reset: explicitly reset by prepare_for_wait() before each wait
            let recv_event = unsafe {
                CreateEventW(
                    None,  // default security attributes
                    true,  // bManualReset = true (manual-reset)
                    false, // bInitialState = false (initially non-signaled)
                    None,  // lpName = NULL (unnamed)
                )
            };

            let send_event = unsafe {
                CreateEventW(
                    None, true,  // manual-reset
                    false, // initially non-signaled
                    None,
                )
            };

            Self {
                recv_event: Mutex::new(recv_event.ok()),
                send_event: Mutex::new(send_event.ok()),
            }
        }
    }

    impl Drop for WindowsNotifyHandle {
        fn drop(&mut self) {
            // Clean up event handles
            if let Ok(mut recv) = self.recv_event.lock()
                && let Some(handle) = recv.take()
            {
                unsafe {
                    let _ = CloseHandle(handle);
                }
            }
            if let Ok(mut send) = self.send_event.lock()
                && let Some(handle) = send.take()
            {
                unsafe {
                    let _ = CloseHandle(handle);
                }
            }
        }
    }

    impl NotifyHandle for WindowsNotifyHandle {
        fn signal_recv(&self) {
            if let Ok(recv) = self.recv_event.lock()
                && let Some(handle) = *recv
            {
                unsafe {
                    let _ = SetEvent(handle);
                }
            }
        }

        fn signal_send(&self) {
            if let Ok(send) = self.send_event.lock()
                && let Some(handle) = *send
            {
                unsafe {
                    let _ = SetEvent(handle);
                }
            }
        }

        fn close(&self) {
            // Trigger Drop to clean up resources
            // (drop will be called automatically when Arc goes away)
        }

        fn recv_fd(&self) -> std::os::raw::c_int {
            -1 // Not applicable on Windows
        }

        fn send_fd(&self) -> std::os::raw::c_int {
            -1 // Not applicable on Windows
        }

        fn recv_notifier(&self) -> RecvNotify {
            let event = if let Ok(recv) = self.recv_event.lock() {
                recv.unwrap_or_default()
            } else {
                // Fallback to default if lock fails
                HANDLE::default()
            };
            RecvNotify { event }
        }
    }

    /// Public accessor to get the recv event `HANDLE` (used by `poll.rs` `WaitForMultipleObjects`).
    pub(crate) fn get_recv_event(handle: &WindowsNotifyHandle) -> Option<HANDLE> {
        handle.recv_event.lock().ok().and_then(|g| *g)
    }

    /// Public accessor to get the send event `HANDLE` (used by `poll.rs` `WaitForMultipleObjects`).
    pub(crate) fn get_send_event(handle: &WindowsNotifyHandle) -> Option<HANDLE> {
        handle.send_event.lock().ok().and_then(|g| *g)
    }

    /// Platform-specific poller for collecting socket event handles and waiting for readiness.
    ///
    /// # Tiered Batching Strategy (>64 Handles)
    ///
    /// `WaitForMultipleObjects()` imposes a 64-handle limit per call. For applications with
    /// N > 64 sockets, this implementation automatically partitions handles into batches:
    ///
    /// - **Batch 0** (handles 0-63): Wait with **full user timeout**
    ///   - Blocks until events arrive OR timeout expires
    ///   - Honors user's time budget
    ///
    /// - **Batches 1+** (handles 64+): Poll with **timeout=0** (non-blocking)
    ///   - Wake immediately if events ready
    ///   - Don't consume additional time
    ///   - Ensure no event loss
    ///
    /// # Example: 200 Sockets
    ///
    /// For `zmq_poll(items, 200, 5000ms)`:
    /// 1. Batch 0 (handles 0-63): `WaitForMultipleObjects(..., 5000ms)` → blocks OR finds events
    /// 2. Batch 1 (handles 64-127): `WaitForMultipleObjects(..., 0ms)` → non-blocking check
    /// 3. Batch 2 (handles 128-191): `WaitForMultipleObjects(..., 0ms)` → non-blocking check
    /// 4. Batch 3 (handles 192-199): `WaitForMultipleObjects(..., 0ms)` → non-blocking check
    ///
    /// Total time: ≈ min(5000ms, time to first event in any batch)
    ///
    /// # Invariants
    ///
    /// - **Zero event loss:** Non-blocking polls ensure all signaled events are captured
    /// - **Timeout accuracy:** First batch honors timeout; subsequent don't add delay
    /// - **Scalability:** O(n/64) syscalls for n sockets
    /// - **Buffering detection:** Before and after wait, check `yring::Consumer::is_empty()`
    ///   to detect buffered messages that don't require signaling
    ///
    /// # Handle Metadata
    ///
    /// The `handle_map` tracks (`item_idx`, `zmq_event_type`, `is_send`) for each handle,
    /// enabling efficient revents assignment after wait completes.
    pub(crate) struct PollWaiter {
        /// Batches of handles, each batch <= 64 handles (WFMO limit)
        batches: Vec<Vec<HANDLE>>,
        /// Metadata: (`item_idx`, `zmq_event_type`, `is_send`)
        /// Tracks which item index and event type each handle corresponds to
        handle_map: Vec<(usize, libc::c_short, bool)>,
    }

    impl PollWaiter {
        const BATCH_SIZE: usize = 64; // Windows WFMO limit

        /// Collect recv/send event HANDLEs from poll items into batches.
        pub(crate) fn new(items: &[crate::poll::ZmqPollItem]) -> Self {
            let mut batches: Vec<Vec<HANDLE>> = vec![Vec::new()];
            let mut handle_map = Vec::new();

            for (idx, item) in items.iter().enumerate() {
                if item.socket.is_null() {
                    continue;
                }

                // Extract socket and get its notification events.
                // SAFETY: socket is non-null (checked above); caller guarantees valid socket.
                let sock = unsafe {
                    &*(item
                        .socket
                        .cast::<std::sync::Arc<crate::socket::OmqSocket>>())
                };

                // Check for recv event (POLLIN)
                if (item.events & crate::poll::ZMQ_POLLIN as libc::c_short) != 0
                    && let Some(handle) = get_recv_event(sock.notify.as_ref())
                    && !handle.is_invalid()
                {
                    // Add to current batch; if full, create new batch
                    if batches.last().unwrap().len() >= Self::BATCH_SIZE {
                        batches.push(Vec::new());
                    }
                    batches.last_mut().unwrap().push(handle);
                    handle_map.push((idx, crate::poll::ZMQ_POLLIN as libc::c_short, false));
                }

                // Check for send event (POLLOUT)
                if (item.events & crate::poll::ZMQ_POLLOUT as libc::c_short) != 0
                    && let Some(handle) = get_send_event(sock.notify.as_ref())
                    && !handle.is_invalid()
                {
                    if batches.last().unwrap().len() >= Self::BATCH_SIZE {
                        batches.push(Vec::new());
                    }
                    batches.last_mut().unwrap().push(handle);
                    handle_map.push((idx, crate::poll::ZMQ_POLLOUT as libc::c_short, true));
                }
            }

            // Remove any empty batches
            batches.retain(|b| !b.is_empty());

            Self {
                batches,
                handle_map,
            }
        }

        /// Check if this poller has any handles to wait on.
        pub(crate) fn has_no_handles(&self) -> bool {
            self.batches.is_empty()
        }

        /// Get the total number of handles being tracked.
        #[expect(dead_code)]
        pub(crate) fn handle_count(&self) -> usize {
            self.handle_map.len()
        }

        /// Prepare poller for wait: drain all manual-reset events before waiting.
        /// Mirrors Unix path which drains accumulated signals from eventfds.
        /// By resetting all events before WFMO, we ensure we only wake on NEW signal events,
        /// not stale signaling state from the previous poll.
        pub(crate) fn prepare_for_wait(&mut self) {
            use windows::Win32::System::Threading::ResetEvent;
            for batch in &self.batches {
                for handle in batch {
                    unsafe {
                        let _ = ResetEvent(*handle);
                    }
                }
            }
        }

        /// Wait for events with tiered batching.
        /// Batch 0 waits with full timeout; batches 1+ poll non-blocking.
        /// Updates revents in items and returns count of ready items.
        pub(crate) fn wait(
            &mut self,
            timeout_ms: libc::c_long,
            items: &mut [crate::poll::ZmqPollItem],
        ) -> std::os::raw::c_int {
            use windows::Win32::Foundation::WAIT_OBJECT_0;
            use windows::Win32::System::Threading::WaitForMultipleObjects;

            if self.batches.is_empty() {
                return 0;
            }

            // Track (item_idx, event_bits) to preserve which event type was signaled
            let mut ready_events: std::collections::HashMap<usize, libc::c_short> =
                std::collections::HashMap::new();

            // Process each batch, first with full timeout, rest non-blocking
            for (batch_idx, batch) in self.batches.iter().enumerate() {
                if batch.is_empty() {
                    continue;
                }

                let wait_timeout = if batch_idx == 0 {
                    // First batch: use full timeout
                    if timeout_ms < 0 {
                        u32::MAX // INFINITE
                    } else {
                        timeout_ms as u32
                    }
                } else {
                    // Subsequent batches: non-blocking poll
                    0
                };

                // Wait for any handle in this batch to signal
                // SAFETY: We've verified batch is non-empty above
                let wait_result = unsafe { WaitForMultipleObjects(batch, false, wait_timeout) };

                // If result indicates an object was signaled
                let wait_result_u32 = wait_result.0;
                if wait_result_u32 < WAIT_OBJECT_0.0 + batch.len() as u32 {
                    let signaled_idx = (wait_result_u32 - WAIT_OBJECT_0.0) as usize;
                    if let Some((item_idx, event_type, _is_send)) = self.handle_map.get(
                        self.batches[..batch_idx]
                            .iter()
                            .map(std::vec::Vec::len)
                            .sum::<usize>()
                            + signaled_idx,
                    ) {
                        // Track which event was signaled (POLLIN or POLLOUT)
                        ready_events
                            .entry(*item_idx)
                            .and_modify(|bits| *bits |= event_type)
                            .or_insert(*event_type);
                    }
                }
            }

            // Update revents for ready items
            let mut ready_count = 0i32;
            for (idx, item) in items.iter_mut().enumerate() {
                item.revents = 0;
                if let Some(event_bits) = ready_events.get(&idx) {
                    item.revents = *event_bits;
                    ready_count += 1;
                }
            }

            ready_count
        }
    }

    #[allow(unused_imports)]
    pub(super) use WindowsNotifyHandle as PlatformNotifyHandle;
}

/// Create a platform-specific notification handle.
#[allow(clippy::unnecessary_wraps)] // Unix variant returns Option, Windows doesn't; keep uniform API
pub(crate) fn create_notify() -> Option<std::sync::Arc<PlatformNotifyHandle>> {
    #[cfg(unix)]
    {
        unix::UnixNotifyHandle::new().map(|h| std::sync::Arc::new(h))
    }
    #[cfg(windows)]
    {
        Some(std::sync::Arc::new(windows::WindowsNotifyHandle::new()))
    }
}

// Re-export RecvNotify at crate level for cross-module access
#[cfg(unix)]
pub(crate) use unix::RecvNotify;

#[cfg(windows)]
pub(crate) use windows::RecvNotify;

// Re-export PollWaiter at crate level for polling abstraction
#[cfg(unix)]
pub(crate) use unix::PollWaiter;

#[cfg(windows)]
#[allow(unused_imports)]
pub(crate) use windows::PollWaiter;

// Export platform-specific handle types for static dispatch
/// Platform-specific notification handle (Unix or Windows).
/// Use for compile-time static dispatch instead of dynamic trait objects.
#[cfg(unix)]
pub(crate) use unix::UnixNotifyHandle as PlatformNotifyHandle;

#[cfg(windows)]
pub(crate) use windows::WindowsNotifyHandle as PlatformNotifyHandle;

/// Check if a socket has buffered inproc bypass data available.
/// Returns true if `bypass_recv` exists and has data, false otherwise.
/// Works cross-platform: on Unix checks byte ring, on Windows also checks byte ring
/// (bypass infrastructure is identical on both platforms).
pub(crate) fn has_bypass_data(sock: &crate::socket::OmqSocket) -> bool {
    // SAFETY: zmq contract guarantees single-threaded access per socket.
    let bypass_ptr = &*sock.bypass_recv.get();
    bypass_ptr.as_ref().is_some_and(|br| !br.is_empty())
}

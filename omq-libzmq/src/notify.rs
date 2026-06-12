//! Cross-platform notification handle abstraction for socket signaling.
//!
//! Abstracts platform-specific mechanisms:
//! - **Unix:** eventfd (Linux) or pipe pairs (other Unix)
//! - **Windows:** Manual-reset events (Phase 2+)

// Default channel capacity (matches default HWM in socket.rs).
#[expect(dead_code)]
const DEFAULT_HWM: usize = 1000;

/// Platform-agnostic notification handle for signaling recv/send events.
#[expect(dead_code)]
pub(crate) trait NotifyHandle: Send + Sync {
    /// Signal that a message has arrived (recv event).
    fn signal_recv(&self);

    /// Signal that a send slot has been freed (send event).
    fn signal_send(&self);

    /// Close and clean up resources.
    fn close(&self);

    /// Get the raw receive FD for polling (Unix only; returns -1 on Windows in Phase 1).
    fn recv_fd(&self) -> std::os::raw::c_int;

    /// Get the raw send FD for polling (Unix only; returns -1 on Windows in Phase 1).
    fn send_fd(&self) -> std::os::raw::c_int;
}

#[cfg(unix)]
mod unix {
    use super::*;

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
    }

    impl std::fmt::Debug for UnixNotifyHandle {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("UnixNotifyHandle { .. }")
        }
    }

    pub(super) use UnixNotifyHandle as PlatformNotifyHandle;
}

#[cfg(windows)]
pub(crate) mod windows {
    use std::sync::Mutex;
    use windows::Win32::Foundation::{CloseHandle, HANDLE};
    use windows::Win32::System::Threading::{CreateEventW, SetEvent};

    /// Windows implementation using manual-reset events.
    ///
    /// Creates two manual-reset events for recv and send notifications,
    /// leveraging `CreateEventW` / `SetEvent` / `CloseHandle` Win32 API.
    pub(crate) struct WindowsNotifyHandle {
        recv_event: Mutex<Option<HANDLE>>,
        send_event: Mutex<Option<HANDLE>>,
    }

    // SAFETY: HANDLE is a unique Windows kernel object reference.
    // It is safe to Send and Sync across threads; the Windows kernel handles
    // synchronization internally.
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

    use super::NotifyHandle;

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
    }

    /// Public accessor to get the recv event `HANDLE` (Phase 3+: used by `poll.rs` `WaitForMultipleObjects`).
    pub(crate) fn get_recv_event(handle: &dyn super::NotifyHandle) -> Option<HANDLE> {
        // Downcast to WindowsNotifyHandle to access internal state
        // This is safe because we control both the trait and implementation
        #[allow(unsafe_code, clippy::cast_ptr_alignment, clippy::ptr_as_ptr)]
        unsafe {
            let ptr =
                std::ptr::from_ref::<dyn super::NotifyHandle>(handle).cast::<WindowsNotifyHandle>();
            if !ptr.is_null()
                && let Ok(recv) = (*ptr).recv_event.lock()
            {
                return *recv;
            }
        }
        None
    }

    /// Public accessor to get the send event `HANDLE` (Phase 3+: used by `poll.rs` `WaitForMultipleObjects`).
    pub(crate) fn get_send_event(handle: &dyn super::NotifyHandle) -> Option<HANDLE> {
        #[allow(unsafe_code, clippy::cast_ptr_alignment, clippy::ptr_as_ptr)]
        unsafe {
            let ptr =
                std::ptr::from_ref::<dyn super::NotifyHandle>(handle).cast::<WindowsNotifyHandle>();
            if !ptr.is_null()
                && let Ok(send) = (*ptr).send_event.lock()
            {
                return *send;
            }
        }
        None
    }
}

/// Create a platform-specific notification handle.
#[allow(clippy::unnecessary_wraps)] // Unix variant returns Option, Windows doesn't; keep uniform API
pub(crate) fn create_notify() -> Option<std::sync::Arc<dyn NotifyHandle>> {
    #[cfg(unix)]
    {
        unix::UnixNotifyHandle::new()
            .map(|h| std::sync::Arc::new(h) as std::sync::Arc<dyn NotifyHandle>)
    }
    #[cfg(windows)]
    {
        Some(std::sync::Arc::new(windows::WindowsNotifyHandle::new())
            as std::sync::Arc<dyn NotifyHandle>)
    }
}

/// Signal a raw file descriptor (Unix only, used by inproc bypass).
/// This is a low-level utility for signaling a recv FD created by a NotifyHandle.
#[cfg(unix)]
#[expect(dead_code)]
pub(crate) fn signal_raw_recv_fd(fd: std::os::raw::c_int) {
    if fd < 0 {
        return;
    }
    #[cfg(target_os = "linux")]
    {
        let val: u64 = 1;
        // SAFETY: fd is expected to be a valid eventfd; writing 8 bytes atomically increments.
        unsafe {
            libc::write(fd, (&raw const val).cast::<libc::c_void>(), 8);
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let b: u8 = 1;
        // SAFETY: fd is expected to be a valid pipe write end; writing 1 byte signals.
        unsafe {
            libc::write(fd, (&raw const b).cast::<libc::c_void>(), 1);
        }
    }
}

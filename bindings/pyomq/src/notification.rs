//! Cross-platform notification primitive for async event wakeup.
//!
//! Unix: Uses eventfd for efficient signaling.
//! Windows: Uses a TCP socket pair to bridge between the tokio runtime's IOCP
//! and Python's event loop IOCP/selector.
//!
//! Both implementations present the same API to consumers. The notification
//! mechanism is only used for waking the Python asyncio event loop when new
//! messages arrive; it does not carry message data.

use std::sync::atomic::{AtomicBool, Ordering};

/// Platform-specific notification implementation.
#[cfg(unix)]
mod platform {
    use std::io;

    pub struct Notification {
        fd: i32,
    }

    impl Notification {
        pub fn new() -> io::Result<Self> {
            let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(Self { fd })
        }

        pub fn fd(&self) -> i32 {
            self.fd
        }

        pub fn write_signal(&self) -> io::Result<()> {
            let val: u64 = 1;
            loop {
                let res =
                    unsafe { libc::write(self.fd, &val as *const u64 as *const libc::c_void, 8) };
                if res >= 0 {
                    return Ok(());
                }
                let err = io::Error::last_os_error();
                match err.raw_os_error() {
                    Some(libc::EINTR) => continue,
                    _ => return Err(err),
                }
            }
        }

        pub fn read_ack(&self) -> io::Result<()> {
            let mut val: u64 = 0;
            unsafe {
                libc::read(self.fd, &mut val as *mut u64 as *mut libc::c_void, 8);
            }
            Ok(())
        }

        pub fn poll_wait(&self, timeout_ms: i32) -> io::Result<bool> {
            let mut pfd = libc::pollfd {
                fd: self.fd,
                events: libc::POLLIN,
                revents: 0,
            };
            let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
            if ret < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(ret > 0)
        }

        pub fn dup(&self) -> io::Result<i32> {
            use std::os::fd::FromRawFd;
            let fd = unsafe { libc::dup(self.fd) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(fd)
        }
    }

    impl Drop for Notification {
        fn drop(&mut self) {
            unsafe { libc::close(self.fd) };
        }
    }
}

/// Windows implementation: TCP socket pair for cross-IOCP signaling.
#[cfg(windows)]
mod platform {
    use std::io::{self, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::windows::io::AsRawSocket;

    pub struct Notification {
        /// Read end of the socket pair (registered with asyncio event loop).
        read_sock: TcpStream,
        /// Write end of the socket pair (used for signaling).
        write_sock: TcpStream,
    }

    impl Notification {
        pub fn new() -> io::Result<Self> {
            // Create a TCP listener on 127.0.0.1:0 (OS chooses port).
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let addr = listener.local_addr()?;

            // Connect to it from the same machine.
            let write_sock = TcpStream::connect(addr)?;
            let (read_sock, _) = listener.accept()?;

            // Set both sockets non-blocking for efficient signaling.
            read_sock.set_nonblocking(true)?;
            write_sock.set_nonblocking(true)?;

            Ok(Self {
                read_sock,
                write_sock,
            })
        }

        pub fn fd(&self) -> i32 {
            // Convert Windows SOCKET to i32 for Python compatibility.
            // Python's asyncio on Windows uses socket FDs via winsock APIs.
            self.read_sock.as_raw_socket() as i32
        }

        pub fn write_signal(&self) -> io::Result<()> {
            let buf = [1u8; 1];
            match (&self.write_sock).write_all(&buf) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // Non-blocking socket, send buffer full is OK (signal lost, but that's OK)
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }

        pub fn read_ack(&self) -> io::Result<()> {
            let mut buf = [0u8; 1];
            match (&self.read_sock).read_exact(&mut buf) {
                Ok(()) => Ok(()),
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::UnexpectedEof =>
                {
                    // Non-blocking socket, no data available or EOF is OK
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }

        pub fn poll_wait(&self, timeout_ms: i32) -> io::Result<bool> {
            use std::io::Read;
            use std::time::Duration;

            // Set socket read timeout based on parameter
            let timeout = if timeout_ms < 0 {
                Duration::new(3600, 0) // 1 hour for infinite wait
            } else {
                Duration::from_millis(timeout_ms as u64)
            };
            self.read_sock.set_read_timeout(Some(timeout))?;

            // Try to read one byte; this blocks until data available or timeout
            let mut buf = [0u8; 1];
            let result = (&self.read_sock).read_exact(&mut buf).is_ok();

            // Reset to non-blocking
            self.read_sock
                .set_read_timeout(Some(Duration::from_millis(0)))?;
            Ok(result)
        }

        pub fn dup(&self) -> io::Result<i32> {
            // On Windows, we can't directly dup socket FDs like Unix.
            // Return the same FD — it's safe to share for read purposes.
            Ok(self.read_sock.as_raw_socket() as i32)
        }
    }
}

pub use platform::Notification;

/// Notification wrapper with parking logic for the recv path.
///
/// Avoids excessive writes to the notification FD by only signaling when
/// the consumer explicitly sets the `parking` flag (i.e., when it's about
/// to wait for new messages).
pub(crate) struct RecvNotify {
    notification: Notification,
    parking: AtomicBool,
}

unsafe impl Send for RecvNotify {}
unsafe impl Sync for RecvNotify {}

impl RecvNotify {
    pub fn new() -> Self {
        let notification = Notification::new().expect("failed to create notification");
        Self {
            notification,
            parking: AtomicBool::new(false),
        }
    }

    /// Signal the notification if parking is active.
    /// Used on the hot path after pushing a message to the yring.
    pub fn notify(&self) {
        if self.parking.load(Ordering::Acquire) {
            let _ = self.notification.write_signal();
        }
    }

    /// Force a signal regardless of parking state.
    /// Used for explicit wakeups (e.g., during socket closure).
    pub fn force_wake(&self) {
        let _ = self.notification.write_signal();
    }

    /// Mark that we're about to wait for messages. Only then will `notify()` signal.
    pub fn park_begin(&self) {
        self.parking.store(true, Ordering::Release);
    }

    /// Mark that we're no longer waiting.
    pub fn park_end(&self) {
        self.parking.store(false, Ordering::Relaxed);
    }

    /// Wait for a signal with a timeout. Returns true if signaled, false if timeout.
    pub fn wait_timeout(&self, timeout: std::time::Duration) -> bool {
        let ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        match self.notification.poll_wait(ms) {
            Ok(signaled) => {
                if signaled {
                    let _ = self.notification.read_ack();
                }
                signaled
            }
            Err(_) => false,
        }
    }

    /// Return an FD suitable for exposure to Python asyncio event loop.
    /// On Unix, this is a duplicate of the eventfd. On Windows, it's the
    /// TCP socket read FD (cannot be duplicated, so we return the same FD).
    /// The caller is responsible for closing the returned FD.
    pub fn dup_fd(&self) -> std::io::Result<i32> {
        self.notification.dup()
    }

    /// Get the raw FD for the notification (for direct read/write or polling).
    pub fn fd(&self) -> i32 {
        self.notification.fd()
    }

    /// Permanently arm the notification so signals are always sent.
    /// Used when the FD is exposed to an external event loop (asyncio, etc).
    pub fn arm_persistent(&self) {
        self.parking.store(true, Ordering::Release);
    }
}

//! `zmq_poll` -- multiplexed I/O readiness.
//!
//! Cross-platform polling:
//! - **Unix:** Uses `poll()` on eventfds (Linux) or pipe pairs (other Unix)
//! - **Windows:** Uses `WaitForMultipleObjects` with manual-reset events
//!
//! **Windows limitation:** `WaitForMultipleObjects` supports max 64 handles;
//! if polling > 64 sockets, implement batching in future phases.

use std::ffi::c_int;
use std::sync::Arc;

use crate::consts;
use crate::socket::OmqSocket;

const ZMQ_POLLIN: libc::c_short = consts::ZMQ_POLLIN as libc::c_short;
const ZMQ_POLLOUT: libc::c_short = consts::ZMQ_POLLOUT as libc::c_short;
#[allow(dead_code)]
const ZMQ_POLLERR: libc::c_short = consts::ZMQ_POLLERR as libc::c_short;

/// `zmq_pollitem_t` layout compatible with libzmq.
#[repr(C)]
#[derive(Debug)]
pub struct ZmqPollItem {
    pub socket: *mut libc::c_void,
    pub fd: libc::c_int,
    pub events: libc::c_short,
    pub revents: libc::c_short,
}

#[cfg(unix)]
mod unix_impl {
    use super::{ZmqPollItem, c_int};
    use crate::notify::NotifyHandle;

    pub(super) fn build_pollfds(
        items: &[ZmqPollItem],
    ) -> (Vec<libc::pollfd>, Vec<(usize, libc::c_short)>) {
        let mut pfds = Vec::new();
        let mut map = Vec::new();

        for (i, item) in items.iter().enumerate() {
            if !item.socket.is_null() {
                // SAFETY: socket is non-null (checked above); caller guarantees a valid socket.
                let sock = unsafe { &*(item.socket.cast::<Arc<OmqSocket>>()) };

                if (item.events & ZMQ_POLLIN) != 0 {
                    let fd = sock.notify.recv_fd();
                    if fd >= 0 {
                        pfds.push(libc::pollfd {
                            fd,
                            events: libc::POLLIN,
                            revents: 0,
                        });
                        map.push((i, ZMQ_POLLIN));
                    }
                }
                if (item.events & ZMQ_POLLOUT) != 0 {
                    let fd = sock.notify.send_fd();
                    if fd >= 0 {
                        pfds.push(libc::pollfd {
                            fd,
                            events: libc::POLLIN,
                            revents: 0,
                        });
                        map.push((i, ZMQ_POLLOUT));
                    }
                }
            } else if item.fd >= 0 {
                let mut events: libc::c_short = 0;
                if (item.events & ZMQ_POLLIN) != 0 {
                    events |= libc::POLLIN;
                }
                if (item.events & ZMQ_POLLOUT) != 0 {
                    events |= libc::POLLOUT;
                }
                if (item.events & ZMQ_POLLERR) != 0 {
                    events |= libc::POLLERR;
                }
                pfds.push(libc::pollfd {
                    fd: item.fd,
                    events,
                    revents: 0,
                });
                map.push((i, 0));
            }
        }

        (pfds, map)
    }

    pub(super) fn drain_eventfds(items: &[ZmqPollItem]) {
        for item in items {
            if item.socket.is_null() {
                continue;
            }
            // SAFETY: socket is non-null (checked above); caller guarantees a valid socket.
            let sock = unsafe { &*(item.socket.cast::<Arc<OmqSocket>>()) };

            if (item.events & ZMQ_POLLIN) != 0 {
                let fd = sock.notify.recv_fd();
                if fd >= 0 {
                    #[cfg(target_os = "linux")]
                    {
                        let mut val = 0u64;
                        // SAFETY: fd is a valid eventfd; 8-byte read drains the counter.
                        unsafe { libc::read(fd, (&raw mut val).cast(), 8) };
                    }
                    #[cfg(not(target_os = "linux"))]
                    {
                        let mut buf = [0u8; 64];
                        loop {
                            // SAFETY: fd is a valid pipe read end; draining buffered signal bytes.
                            let n = unsafe {
                                libc::read(fd, buf.as_mut_ptr().cast::<libc::c_void>(), buf.len())
                            };
                            if n <= 0 {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    pub(super) fn poll_impl(
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

        let ready = check_immediate(items_slice);
        if ready > 0 || timeout_ms == 0 {
            return ready;
        }

        let (mut pfds, map) = build_pollfds(items_slice);
        if pfds.is_empty() {
            if timeout_ms > 0 {
                std::thread::sleep(std::time::Duration::from_millis(timeout_ms as u64));
            }
            return 0;
        }

        drain_eventfds(items_slice);
        let ready = check_immediate(items_slice);
        if ready > 0 {
            return ready;
        }

        let poll_timeout = if timeout_ms < 0 {
            -1
        } else {
            timeout_ms as c_int
        };
        // SAFETY: pfds is a valid pollfd array; poll blocks until events or timeout.
        let rc = unsafe { libc::poll(pfds.as_mut_ptr(), pfds.len() as libc::nfds_t, poll_timeout) };
        if rc < 0 {
            return crate::error::fail(
                std::io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EINTR) as libc::c_int,
            );
        }
        if rc == 0 {
            return 0;
        }

        for item in items_slice.iter_mut() {
            item.revents = 0;
        }

        for (pfd_idx, pfd) in pfds.iter().enumerate() {
            if pfd.revents == 0 {
                continue;
            }
            let (item_idx, zmq_event) = map[pfd_idx];

            if zmq_event == 0 {
                if (pfd.revents & libc::POLLIN) != 0 {
                    items_slice[item_idx].revents |= ZMQ_POLLIN;
                }
                if (pfd.revents & libc::POLLOUT) != 0 {
                    items_slice[item_idx].revents |= ZMQ_POLLOUT;
                }
                if (pfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0 {
                    items_slice[item_idx].revents |= ZMQ_POLLERR;
                }
            } else {
                items_slice[item_idx].revents |= zmq_event;
            }
        }

        let mut ready_count = 0i32;
        for item in items_slice.iter() {
            if item.revents != 0 {
                ready_count += 1;
            }
        }
        ready_count
    }
}

#[cfg(windows)]
mod windows_impl {
    use super::{ZmqPollItem, c_int, check_immediate};
    use windows::Win32::System::Threading::WaitForMultipleObjects;

    /// Windows implementation using `WaitForMultipleObjects`.
    ///
    /// Collects recv/send event HANDLEs from sockets and waits for any to signal.
    /// Supports up to 64 HANDLEs (Windows API limit). Beyond 64 sockets,
    /// polling falls back to timeout + `check_immediate`.
    pub(super) fn poll_impl(
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

        let ready = check_immediate(items_slice);
        if ready > 0 || timeout_ms == 0 {
            return ready;
        }

        // Collect notification HANDLEs from sockets.
        let mut handles = Vec::new();
        let mut handle_to_socket: Vec<usize> = Vec::new();

        for (idx, item) in items_slice.iter().enumerate() {
            if item.socket.is_null() {
                continue;
            }

            // Extract socket and get its notification handle.
            // SAFETY: socket is non-null (checked above); caller guarantees valid socket.
            let sock = unsafe {
                &*(item
                    .socket
                    .cast::<std::sync::Arc<crate::socket::OmqSocket>>())
            };

            // Attempt to extract recv HANDLE (if events request recv notification).
            if (item.events & super::ZMQ_POLLIN) != 0
                && let Some(handle) = crate::notify::windows::get_recv_event(sock.notify.as_ref())
                && !handle.is_invalid()
            {
                handles.push(handle);
                handle_to_socket.push(idx);
            }

            // Attempt to extract send HANDLE (if events request send notification).
            if (item.events & super::ZMQ_POLLOUT) != 0
                && let Some(handle) = crate::notify::windows::get_send_event(sock.notify.as_ref())
                && !handle.is_invalid()
            {
                handles.push(handle);
                handle_to_socket.push(idx);
            }

            // Windows limit: max 64 handles per WaitForMultipleObjects call.
            if handles.len() >= 64 {
                break;
            }
        }

        // If no handles to wait on, just apply timeout and check again.
        if handles.is_empty() {
            if timeout_ms > 0 {
                std::thread::sleep(std::time::Duration::from_millis(timeout_ms as u64));
            }
            return check_immediate(items_slice);
        }

        // Convert timeout: zmq uses -1 for infinite.
        let wait_timeout = if timeout_ms < 0 {
            u32::MAX // INFINITE
        } else {
            timeout_ms as u32
        };

        // Wait for any handle to signal.
        let _wait_result = unsafe { WaitForMultipleObjects(&handles, false, wait_timeout) };

        // After wait, always check immediate buffer again.
        check_immediate(items_slice)
    }
}

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
                .load(std::sync::atomic::Ordering::Relaxed);
            #[cfg(unix)]
            let has_buffered = has_buffered
                // SAFETY: zmq contract guarantees single-threaded access per socket.
                || unsafe { &*sock.recv_cons.get() }
                    .as_ref()
                    .is_some_and(|c| !c.fast.is_empty() || !c.pump.is_empty())
                    // SAFETY: zmq contract guarantees single-threaded access per socket.
                    || unsafe { &*sock.bypass_recv.get() }
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

/// Cross-platform C API for `zmq_poll`.
///
/// Multiplexed I/O readiness on one or more sockets or file descriptors.
///
/// **Platform-specific notes:**
/// - **Unix:** Uses `poll()` on event file descriptors (eventfd on Linux, pipes on other Unix)
/// - **Windows:** Uses `WaitForMultipleObjects` with manual-reset events
///   - **Limitation:** Maximum 64 handles per call (`WaitForMultipleObjects` constraint)
///   - **Note:** item.fd polling is not supported on Windows; use item.socket only
#[unsafe(no_mangle)]
pub extern "C" fn zmq_poll(
    items: *mut ZmqPollItem,
    nitems: c_int,
    timeout_ms: libc::c_long,
) -> c_int {
    #[cfg(unix)]
    {
        unix_impl::poll_impl(items, nitems, timeout_ms)
    }
    #[cfg(windows)]
    {
        windows_impl::poll_impl(items, nitems, timeout_ms)
    }
}

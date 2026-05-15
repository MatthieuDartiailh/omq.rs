//! `zmq_poll` -- multiplexed I/O readiness via epoll/poll on eventfds.

use std::ffi::c_int;
use std::sync::Arc;

use crate::socket::OmqSocket;

const ZMQ_POLLIN: libc::c_short = 1;
const ZMQ_POLLOUT: libc::c_short = 2;
const ZMQ_POLLERR: libc::c_short = 4;

/// `zmq_pollitem_t` layout compatible with libzmq.
#[repr(C)]
#[derive(Debug)]
pub struct ZmqPollItem {
    pub socket: *mut libc::c_void,
    pub fd: libc::c_int,
    pub events: libc::c_short,
    pub revents: libc::c_short,
}

/// Collect the raw fd to poll for each item's requested event direction.
/// Returns a Vec of `libc::pollfd` entries and a parallel mapping back to
/// the item index + event mask that produced each entry.
fn build_pollfds(items: &[ZmqPollItem]) -> (Vec<libc::pollfd>, Vec<(usize, libc::c_short)>) {
    let mut pfds = Vec::new();
    let mut map = Vec::new();

    for (i, item) in items.iter().enumerate() {
        if !item.socket.is_null() {
            let sock = unsafe { &*(item.socket.cast::<Arc<OmqSocket>>()) };

            if (item.events & ZMQ_POLLIN) != 0 {
                #[cfg(target_os = "linux")]
                let fd = sock.notify.recv_fd;
                #[cfg(not(target_os = "linux"))]
                let fd = sock.notify.recv_read;

                pfds.push(libc::pollfd {
                    fd,
                    events: libc::POLLIN,
                    revents: 0,
                });
                map.push((i, ZMQ_POLLIN));
            }
            if (item.events & ZMQ_POLLOUT) != 0 {
                #[cfg(target_os = "linux")]
                let fd = sock.notify.send_fd;
                #[cfg(not(target_os = "linux"))]
                let fd = sock.notify.send_read;

                pfds.push(libc::pollfd {
                    fd,
                    events: libc::POLLIN, // eventfd readable = has credits
                    revents: 0,
                });
                map.push((i, ZMQ_POLLOUT));
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
            map.push((i, 0)); // 0 = raw fd, map revents directly
        }
    }

    (pfds, map)
}

/// Check zmq sockets for immediately available data (`recv_drain` or `recv_rx`)
/// without blocking. This catches frames already buffered in userspace that
/// the eventfd doesn't reflect (e.g. remaining multipart frames).
fn check_immediate(items: &mut [ZmqPollItem]) -> i32 {
    let mut ready = 0i32;
    for item in items.iter_mut() {
        item.revents = 0;
        if item.socket.is_null() {
            continue;
        }
        let sock = unsafe { &*(item.socket.cast::<Arc<OmqSocket>>()) };

        if (item.events & ZMQ_POLLIN) != 0 {
            let has_buffered =
                !sock.recv_drain.lock().unwrap().is_empty() || !sock.recv_rx.is_empty();
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

/// Drain all pending eventfd counters for zmq socket items so that
/// `libc::poll` only wakes on messages that arrive after this point.
fn drain_eventfds(items: &[ZmqPollItem]) {
    for item in items {
        if item.socket.is_null() {
            continue;
        }
        let sock = unsafe { &*(item.socket.cast::<Arc<OmqSocket>>()) };

        // Drain recv eventfd: one read resets counter to 0.
        if (item.events & ZMQ_POLLIN) != 0 {
            #[cfg(target_os = "linux")]
            let fd = sock.notify.recv_fd;
            #[cfg(not(target_os = "linux"))]
            let fd = sock.notify.recv_read;

            let mut val: u64 = 0;
            unsafe { libc::read(fd, (&raw mut val).cast(), 8) };
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_poll(
    items: *mut ZmqPollItem,
    nitems: c_int,
    timeout_ms: libc::c_long,
) -> c_int {
    if items.is_null() && nitems > 0 {
        return crate::error::fail(libc::EFAULT);
    }
    let n = nitems as usize;
    let items_slice = unsafe { std::slice::from_raw_parts_mut(items, n) };

    // Fast path: check userspace buffers first (multipart drain, channel).
    let ready = check_immediate(items_slice);
    if ready > 0 || timeout_ms == 0 {
        return ready;
    }

    // Build pollfd array from eventfds / raw fds.
    let (mut pfds, map) = build_pollfds(items_slice);
    if pfds.is_empty() {
        if timeout_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(timeout_ms as u64));
        }
        return 0;
    }

    // Drain stale eventfd counters before blocking. zmq_send/zmq_recv
    // skip the eventfd for performance; stale signals accumulate. A
    // single 8-byte read on an EFD_SEMAPHORE fd drains one count; we
    // drain all counts so libc::poll only wakes on genuinely new data.
    drain_eventfds(items_slice);

    let poll_timeout = if timeout_ms < 0 {
        -1
    } else {
        timeout_ms as c_int
    };
    let rc = unsafe { libc::poll(pfds.as_mut_ptr(), pfds.len() as libc::nfds_t, poll_timeout) };
    if rc < 0 {
        return crate::error::fail(unsafe { *libc::__errno_location() });
    }
    if rc == 0 {
        return 0;
    }

    // Map poll results back to zmq items.
    for item in items_slice.iter_mut() {
        item.revents = 0;
    }

    for (pfd_idx, pfd) in pfds.iter().enumerate() {
        if pfd.revents == 0 {
            continue;
        }
        let (item_idx, zmq_event) = map[pfd_idx];

        if zmq_event == 0 {
            // Raw fd: translate poll revents to ZMQ revents.
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
            // zmq socket eventfd became readable -> set the corresponding event.
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

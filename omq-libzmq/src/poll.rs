//! `zmq_poll` -- multiplexed I/O readiness via poll on eventfds.

use std::ffi::c_int;
use std::sync::Arc;

use crate::consts;
use crate::socket::OmqSocket;

const ZMQ_POLLIN: libc::c_short = consts::ZMQ_POLLIN as libc::c_short;
const ZMQ_POLLOUT: libc::c_short = consts::ZMQ_POLLOUT as libc::c_short;
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

fn build_pollfds(items: &[ZmqPollItem]) -> (Vec<libc::pollfd>, Vec<(usize, libc::c_short)>) {
    let mut pfds = Vec::new();
    let mut map = Vec::new();

    for (i, item) in items.iter().enumerate() {
        if !item.socket.is_null() {
            // SAFETY: socket is non-null (checked above); caller guarantees a valid socket.
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
                    events: libc::POLLIN,
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
            map.push((i, 0));
        }
    }

    (pfds, map)
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
                .load(std::sync::atomic::Ordering::Relaxed)
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

fn drain_eventfds(items: &[ZmqPollItem]) {
    for item in items {
        if item.socket.is_null() {
            continue;
        }
        // SAFETY: socket is non-null (checked above); caller guarantees a valid socket.
        let sock = unsafe { &*(item.socket.cast::<Arc<OmqSocket>>()) };

        if (item.events & ZMQ_POLLIN) != 0 {
            #[cfg(target_os = "linux")]
            {
                let fd = sock.notify.recv_fd;
                let mut val = 0u64;
                // SAFETY: fd is a valid eventfd; 8-byte read drains the counter.
                unsafe { libc::read(fd, (&raw mut val).cast(), 8) };
            }
            #[cfg(not(target_os = "linux"))]
            {
                let fd = sock.notify.recv_read;
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

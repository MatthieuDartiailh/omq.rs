//! `zmq_proxy` / `zmq_proxy_steerable`.

use std::ffi::c_void;
use std::sync::Arc;

use crate::poll::{ZmqPollItem, zmq_poll};
use crate::send_recv::{zmq_recv, zmq_send};
use crate::socket::OmqSocket;

const ZMQ_POLLIN: libc::c_short = 1;
const ZMQ_SNDMORE: i32 = 2;
const ZMQ_RCVMORE: i32 = 13;
const ZMQ_DONTWAIT: i32 = 1;

#[allow(clippy::large_stack_arrays)]
#[allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]
fn forward(from: *mut c_void, to: *mut c_void, capture: *mut c_void) -> libc::c_int {
    let mut buf = [0u8; 65536];
    loop {
        let rc = zmq_recv(from, buf.as_mut_ptr().cast(), buf.len(), 0);
        if rc < 0 {
            return -1;
        }
        let len = rc as usize;
        let more = getsockopt_rcvmore(from);
        let flags = if more { ZMQ_SNDMORE } else { 0 };

        if !capture.is_null() {
            zmq_send(capture, buf[..len].as_ptr().cast(), len, flags);
        }

        let rc = zmq_send(to, buf[..len].as_ptr().cast(), len, flags);
        if rc < 0 {
            return -1;
        }
        if !more {
            break;
        }
    }
    0
}

#[allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]
fn getsockopt_rcvmore(sock: *mut c_void) -> bool {
    let mut v: i32 = 0;
    let mut sz = std::mem::size_of::<i32>();
    crate::opts::zmq_getsockopt(sock, ZMQ_RCVMORE, (&mut v as *mut i32).cast(), &mut sz);
    v != 0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_proxy(
    frontend: *mut c_void,
    backend: *mut c_void,
    capture: *mut c_void,
) -> libc::c_int {
    zmq_proxy_steerable(frontend, backend, capture, std::ptr::null_mut())
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_proxy_steerable(
    frontend: *mut c_void,
    backend: *mut c_void,
    capture: *mut c_void,
    control: *mut c_void,
) -> libc::c_int {
    if frontend.is_null() || backend.is_null() {
        return crate::error::fail(libc::EFAULT);
    }

    let has_control = !control.is_null();
    let npoll = if has_control { 3 } else { 2 };

    loop {
        let mut poll_items = vec![
            ZmqPollItem {
                socket: frontend,
                fd: -1,
                events: ZMQ_POLLIN,
                revents: 0,
            },
            ZmqPollItem {
                socket: backend,
                fd: -1,
                events: ZMQ_POLLIN,
                revents: 0,
            },
        ];
        if has_control {
            poll_items.push(ZmqPollItem {
                socket: control,
                fd: -1,
                events: ZMQ_POLLIN,
                revents: 0,
            });
        }

        let rc = zmq_poll(poll_items.as_mut_ptr(), npoll, 100);
        if rc < 0 {
            return -1;
        }

        if has_control && (poll_items[2].revents & ZMQ_POLLIN) != 0 {
            let mut cmd = [0u8; 64];
            let rc = zmq_recv(control, cmd.as_mut_ptr().cast(), cmd.len(), ZMQ_DONTWAIT);
            if rc > 0 {
                let msg = std::str::from_utf8(&cmd[..rc as usize]).unwrap_or("");
                if msg == "TERMINATE" || msg == "KILL" {
                    return 0;
                }
                if msg == "PAUSE" {
                    loop {
                        let mut pause_items = [ZmqPollItem {
                            socket: control,
                            fd: -1,
                            events: ZMQ_POLLIN,
                            revents: 0,
                        }];
                        zmq_poll(pause_items.as_mut_ptr(), 1, 100);
                        if (pause_items[0].revents & ZMQ_POLLIN) != 0 {
                            let rc =
                                zmq_recv(control, cmd.as_mut_ptr().cast(), cmd.len(), ZMQ_DONTWAIT);
                            if rc > 0 {
                                let m = std::str::from_utf8(&cmd[..rc as usize]).unwrap_or("");
                                if m == "RESUME" {
                                    break;
                                }
                                if m == "TERMINATE" || m == "KILL" {
                                    return 0;
                                }
                            }
                        }
                    }
                }
            }
        }

        if (poll_items[0].revents & ZMQ_POLLIN) != 0 && forward(frontend, backend, capture) < 0 {
            return -1;
        }
        if (poll_items[1].revents & ZMQ_POLLIN) != 0 && forward(backend, frontend, capture) < 0 {
            return -1;
        }

        let fe_sock = unsafe { &*(frontend.cast::<Arc<OmqSocket>>()) };
        if fe_sock
            .ctx
            .terminated
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return crate::error::fail(crate::error::ETERM);
        }
    }
}

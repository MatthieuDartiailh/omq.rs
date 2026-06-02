//! `zmq_proxy` / `zmq_proxy_steerable`.

use std::ffi::c_void;
use std::sync::Arc;

use crate::consts;
use crate::msg::{
    OmqMsgRepr, zmq_msg_close, zmq_msg_init, zmq_msg_more, zmq_msg_recv, zmq_msg_send,
};
use crate::poll::{ZmqPollItem, zmq_poll};
use crate::send_recv::zmq_recv;
use crate::socket::OmqSocket;

const ZMQ_POLLIN: libc::c_short = consts::ZMQ_POLLIN as libc::c_short;
const ZMQ_SNDMORE: i32 = consts::ZMQ_SNDMORE;
const ZMQ_DONTWAIT: i32 = consts::ZMQ_DONTWAIT;

fn forward(from: *mut c_void, to: *mut c_void, capture: *mut c_void) -> libc::c_int {
    let mut msg = std::mem::MaybeUninit::<OmqMsgRepr>::uninit();
    loop {
        zmq_msg_init(msg.as_mut_ptr());
        let rc = zmq_msg_recv(msg.as_mut_ptr(), from, 0);
        if rc < 0 {
            zmq_msg_close(msg.as_mut_ptr());
            return -1;
        }
        let more = zmq_msg_more(msg.as_ptr()) != 0;
        let flags = if more { ZMQ_SNDMORE } else { 0 };

        if !capture.is_null() {
            let mut copy = std::mem::MaybeUninit::<OmqMsgRepr>::uninit();
            zmq_msg_init(copy.as_mut_ptr());
            crate::msg::zmq_msg_copy(copy.as_mut_ptr(), msg.as_ptr());
            // zmq_msg_send closes the msg on success; close on failure.
            if zmq_msg_send(copy.as_mut_ptr(), capture, flags) < 0 {
                zmq_msg_close(copy.as_mut_ptr());
            }
        }

        // zmq_msg_send closes the msg on success.
        let rc = zmq_msg_send(msg.as_mut_ptr(), to, flags);
        if rc < 0 {
            zmq_msg_close(msg.as_mut_ptr());
            return -1;
        }
        if !more {
            break;
        }
    }
    0
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

    loop {
        for item in &mut poll_items {
            item.revents = 0;
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

        // SAFETY: frontend is non-null (checked at function entry).
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

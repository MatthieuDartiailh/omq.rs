//! `zmq_proxy` tests, including large (>64 KB) message forwarding.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr, clippy::similar_names)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_msg_close, zmq_msg_data,
    zmq_msg_init, zmq_msg_recv, zmq_msg_send, zmq_msg_size, zmq_proxy_steerable, zmq_recv,
    zmq_send, zmq_setsockopt, zmq_socket,
};

const ZMQ_PAIR: i32 = 0;
const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;

fn set_timeo(sock: *mut c_void, opt: i32, ms: i32) {
    zmq_setsockopt(sock, opt, (&ms as *const i32).cast(), size_of::<i32>());
}

#[repr(C, align(8))]
struct ZmqMsg([u8; 64]);

impl ZmqMsg {
    fn new() -> Self {
        let mut m = Self([0u8; 64]);
        zmq_msg_init(m.0.as_mut_ptr().cast());
        m
    }
}

struct ProxyArgs {
    fe: *mut c_void,
    be: *mut c_void,
    ctrl: *mut c_void,
}

// SAFETY: ZMQ sockets are thread-safe when used by one thread at a time.
unsafe impl Send for ProxyArgs {}

/// Proxy forwards a small message (well under 64 KB).
#[test]
fn proxy_small_message() {
    let ctx = zmq_ctx_new();
    let fe = zmq_socket(ctx, ZMQ_PULL);
    let be = zmq_socket(ctx, ZMQ_PUSH);
    let src = zmq_socket(ctx, ZMQ_PUSH);
    let dst = zmq_socket(ctx, ZMQ_PULL);
    let ctrl_a = zmq_socket(ctx, ZMQ_PAIR);
    let ctrl_b = zmq_socket(ctx, ZMQ_PAIR);

    let addr_fe = CString::new("inproc://proxy-fe-small").unwrap();
    let addr_be = CString::new("inproc://proxy-be-small").unwrap();
    let addr_ctrl = CString::new("inproc://proxy-ctrl-small").unwrap();

    zmq_bind(fe, addr_fe.as_ptr());
    zmq_bind(be, addr_be.as_ptr());
    zmq_bind(ctrl_a, addr_ctrl.as_ptr());
    zmq_connect(src, addr_fe.as_ptr());
    zmq_connect(dst, addr_be.as_ptr());
    zmq_connect(ctrl_b, addr_ctrl.as_ptr());
    std::thread::sleep(Duration::from_millis(50));

    set_timeo(dst, ZMQ_RCVTIMEO, 5000);
    set_timeo(src, ZMQ_SNDTIMEO, 5000);

    let args = ProxyArgs {
        fe,
        be,
        ctrl: ctrl_a,
    };
    let proxy = std::thread::spawn(move || {
        let a = args;
        zmq_proxy_steerable(a.fe, a.be, std::ptr::null_mut(), a.ctrl)
    });

    let payload = b"small proxy test";
    zmq_send(src, payload.as_ptr().cast(), payload.len(), 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(dst, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc as usize, payload.len());
    assert_eq!(&buf[..payload.len()], payload);

    zmq_send(ctrl_b, b"TERMINATE".as_ptr().cast(), 9, 0);
    proxy.join().ok();

    zmq_close(src);
    zmq_close(dst);
    zmq_close(ctrl_b);
    zmq_close(fe);
    zmq_close(be);
    zmq_close(ctrl_a);
    zmq_ctx_term(ctx);
}

/// Proxy correctly forwards messages larger than 64 KB.
///
/// Before the fix, `forward()` used a 64 KB stack buffer with `zmq_recv`,
/// which returned the full frame length even when truncated. Indexing
/// `buf[..len]` with `len > 65536` panicked on the bounds check.
#[test]
fn proxy_large_message() {
    let ctx = zmq_ctx_new();
    let fe = zmq_socket(ctx, ZMQ_PULL);
    let be = zmq_socket(ctx, ZMQ_PUSH);
    let src = zmq_socket(ctx, ZMQ_PUSH);
    let dst = zmq_socket(ctx, ZMQ_PULL);
    let ctrl_a = zmq_socket(ctx, ZMQ_PAIR);
    let ctrl_b = zmq_socket(ctx, ZMQ_PAIR);

    let port_fe = helpers::free_port();
    let port_be = helpers::free_port();
    let addr_fe = CString::new(format!("tcp://127.0.0.1:{port_fe}")).unwrap();
    let addr_be = CString::new(format!("tcp://127.0.0.1:{port_be}")).unwrap();
    let addr_ctrl = CString::new("inproc://proxy-ctrl-large").unwrap();

    zmq_bind(fe, addr_fe.as_ptr());
    zmq_bind(be, addr_be.as_ptr());
    zmq_bind(ctrl_a, addr_ctrl.as_ptr());
    zmq_connect(src, addr_fe.as_ptr());
    zmq_connect(dst, addr_be.as_ptr());
    zmq_connect(ctrl_b, addr_ctrl.as_ptr());
    std::thread::sleep(Duration::from_millis(200));

    set_timeo(dst, ZMQ_RCVTIMEO, 10000);
    set_timeo(src, ZMQ_SNDTIMEO, 10000);

    let args = ProxyArgs {
        fe,
        be,
        ctrl: ctrl_a,
    };
    let proxy = std::thread::spawn(move || {
        let a = args;
        zmq_proxy_steerable(a.fe, a.be, std::ptr::null_mut(), a.ctrl)
    });

    // 128 KB payload: well above the old 64 KB stack buffer limit.
    let size = 128 * 1024;
    let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();

    let mut msg = ZmqMsg([0u8; 64]);
    omq_zmq::zmq_msg_init_size(msg.0.as_mut_ptr().cast(), size);
    let data = zmq_msg_data(msg.0.as_mut_ptr().cast());
    unsafe { std::ptr::copy_nonoverlapping(payload.as_ptr(), data.cast::<u8>(), size) };
    let rc = zmq_msg_send(msg.0.as_mut_ptr().cast(), src, 0);
    assert_eq!(rc as usize, size);

    let mut recv_msg = ZmqMsg::new();
    let rc = zmq_msg_recv(recv_msg.0.as_mut_ptr().cast(), dst, 0);
    assert_eq!(rc as usize, size);
    assert_eq!(zmq_msg_size(recv_msg.0.as_ptr().cast()), size);
    let got = unsafe {
        std::slice::from_raw_parts(
            zmq_msg_data(recv_msg.0.as_mut_ptr().cast()).cast::<u8>(),
            size,
        )
    };
    assert_eq!(got, &payload[..]);

    zmq_msg_close(recv_msg.0.as_mut_ptr().cast());
    zmq_send(ctrl_b, b"TERMINATE".as_ptr().cast(), 9, 0);
    proxy.join().ok();

    zmq_close(src);
    zmq_close(dst);
    zmq_close(ctrl_b);
    zmq_close(fe);
    zmq_close(be);
    zmq_close(ctrl_a);
    zmq_ctx_term(ctx);
}

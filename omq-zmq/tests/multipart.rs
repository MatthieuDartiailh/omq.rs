//! Multipart message tests using `zmq_msg_*` API.
//! Tests `SNDMORE` accumulation, `RCVMORE` drain, `zmq_msg_more` flag.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_getsockopt, zmq_msg_close,
    zmq_msg_copy, zmq_msg_data, zmq_msg_init, zmq_msg_init_buffer, zmq_msg_init_data,
    zmq_msg_init_size, zmq_msg_more, zmq_msg_move, zmq_msg_recv, zmq_msg_send, zmq_msg_size,
    zmq_recv, zmq_send, zmq_setsockopt, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_SNDMORE: i32 = 2;
const ZMQ_RCVMORE: i32 = 13;
const ZMQ_RCVTIMEO: i32 = 27;

// 64-byte opaque zmq_msg_t, aligned to pointer size (OmqMsgRepr has *mut u8 fields).
#[repr(C, align(8))]
struct ZmqMsg([u8; 64]);

impl ZmqMsg {
    fn new() -> Self {
        let mut m = Self([0u8; 64]);
        zmq_msg_init(m.0.as_mut_ptr().cast());
        m
    }
}

fn set_timeo(sock: *mut c_void, opt: i32, ms: i32) {
    zmq_setsockopt(sock, opt, (&ms as *const i32).cast(), size_of::<i32>());
}

fn rcvmore(sock: *mut c_void) -> bool {
    let mut v: i32 = 0;
    let mut sz = size_of::<i32>();
    zmq_getsockopt(sock, ZMQ_RCVMORE, (&mut v as *mut i32).cast(), &mut sz);
    v != 0
}

/// `zmq_msg_init` / close lifecycle
#[test]
fn msg_init_close() {
    let mut m = ZmqMsg::new();
    assert_eq!(zmq_msg_size(m.0.as_ptr().cast()), 0);
    assert_eq!(zmq_msg_more(m.0.as_ptr().cast()), 0);
    assert_eq!(zmq_msg_close(m.0.as_mut_ptr().cast()), 0);
}

/// `zmq_msg_init_size` allocates writable memory
#[test]
fn msg_init_size() {
    let mut m = ZmqMsg([0u8; 64]);
    assert_eq!(zmq_msg_init_size(m.0.as_mut_ptr().cast(), 16), 0);
    assert_eq!(zmq_msg_size(m.0.as_ptr().cast()), 16);

    let data = zmq_msg_data(m.0.as_mut_ptr().cast());
    assert!(!data.is_null());
    // Write through the pointer.
    unsafe { std::ptr::write_bytes(data.cast::<u8>(), 0xAB, 16) };

    zmq_msg_close(m.0.as_mut_ptr().cast());
}

/// `zmq_msg_init_buffer` copies the buffer
#[test]
fn msg_init_buffer() {
    let payload = b"hello buffer";
    let mut m = ZmqMsg([0u8; 64]);
    assert_eq!(
        zmq_msg_init_buffer(
            m.0.as_mut_ptr().cast(),
            payload.as_ptr().cast(),
            payload.len()
        ),
        0
    );
    assert_eq!(zmq_msg_size(m.0.as_ptr().cast()), payload.len());

    let data = zmq_msg_data(m.0.as_mut_ptr().cast());
    let slice = unsafe { std::slice::from_raw_parts(data.cast::<u8>(), payload.len()) };
    assert_eq!(slice, payload);
    zmq_msg_close(m.0.as_mut_ptr().cast());
}

/// `zmq_msg_init_data` with `free_fn`
#[test]
fn msg_init_data_with_free_fn() {
    use std::sync::atomic::{AtomicBool, Ordering};
    static FREED: AtomicBool = AtomicBool::new(false);

    unsafe extern "C" fn my_free(_data: *mut c_void, _hint: *mut c_void) {
        FREED.store(true, Ordering::SeqCst);
    }

    // Allocate a heap buffer to pass.
    let buf = unsafe { libc::malloc(8) };
    assert!(!buf.is_null());
    unsafe { std::ptr::write_bytes(buf.cast::<u8>(), 0x55, 8) };

    let mut m = ZmqMsg([0u8; 64]);
    assert_eq!(
        zmq_msg_init_data(
            m.0.as_mut_ptr().cast(),
            buf,
            8,
            Some(my_free),
            std::ptr::null_mut(),
        ),
        0
    );
    assert_eq!(zmq_msg_size(m.0.as_ptr().cast()), 8);

    zmq_msg_close(m.0.as_mut_ptr().cast());
    assert!(FREED.load(Ordering::SeqCst), "free_fn was not called");
}

/// `zmq_msg_move` transfers ownership; src becomes empty
#[test]
fn msg_move() {
    let mut src = ZmqMsg([0u8; 64]);
    zmq_msg_init_buffer(src.0.as_mut_ptr().cast(), b"move-me".as_ptr().cast(), 7);

    let mut dst = ZmqMsg::new();
    assert_eq!(
        zmq_msg_move(dst.0.as_mut_ptr().cast(), src.0.as_mut_ptr().cast()),
        0
    );

    assert_eq!(zmq_msg_size(dst.0.as_ptr().cast()), 7);
    assert_eq!(zmq_msg_size(src.0.as_ptr().cast()), 0);

    zmq_msg_close(dst.0.as_mut_ptr().cast());
    zmq_msg_close(src.0.as_mut_ptr().cast());
}

/// `zmq_msg_copy` makes an independent copy
#[test]
fn msg_copy() {
    let mut src = ZmqMsg([0u8; 64]);
    zmq_msg_init_buffer(src.0.as_mut_ptr().cast(), b"copy-me".as_ptr().cast(), 7);

    let mut dst = ZmqMsg::new();
    assert_eq!(
        zmq_msg_copy(dst.0.as_mut_ptr().cast(), src.0.as_ptr().cast()),
        0
    );

    assert_eq!(zmq_msg_size(dst.0.as_ptr().cast()), 7);
    assert_eq!(zmq_msg_size(src.0.as_ptr().cast()), 7);

    zmq_msg_close(dst.0.as_mut_ptr().cast());
    zmq_msg_close(src.0.as_mut_ptr().cast());
}

/// `zmq_msg_send` / `zmq_msg_recv` roundtrip
#[test]
fn msg_send_recv_roundtrip() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-msg-rtt").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(pull, ZMQ_RCVTIMEO, 1000);

    let payload = b"msg api roundtrip";
    let mut out_m = ZmqMsg([0u8; 64]);
    zmq_msg_init_buffer(
        out_m.0.as_mut_ptr().cast(),
        payload.as_ptr().cast(),
        payload.len(),
    );
    let rc = zmq_msg_send(out_m.0.as_mut_ptr().cast(), push, 0);
    assert_eq!(rc as usize, payload.len());

    let mut in_m = ZmqMsg::new();
    let rc = zmq_msg_recv(in_m.0.as_mut_ptr().cast(), pull, 0);
    assert_eq!(rc as usize, payload.len());
    assert_eq!(zmq_msg_more(in_m.0.as_ptr().cast()), 0);

    let data = zmq_msg_data(in_m.0.as_mut_ptr().cast());
    let got = unsafe { std::slice::from_raw_parts(data.cast::<u8>(), rc as usize) };
    assert_eq!(got, payload);

    zmq_msg_close(in_m.0.as_mut_ptr().cast());
    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

/// Multipart with `zmq_msg_more` flag set correctly
#[test]
fn msg_more_flag_in_recv() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-msg-more").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(pull, ZMQ_RCVTIMEO, 1000);

    // Send 3-part message.
    zmq_send(push, b"A".as_ptr().cast(), 1, ZMQ_SNDMORE);
    zmq_send(push, b"B".as_ptr().cast(), 1, ZMQ_SNDMORE);
    zmq_send(push, b"C".as_ptr().cast(), 1, 0);

    let mut m = ZmqMsg::new();

    // Frame 1: more=1
    let rc = zmq_msg_recv(m.0.as_mut_ptr().cast(), pull, 0);
    assert_eq!(rc, 1);
    assert_eq!(
        zmq_msg_more(m.0.as_ptr().cast()),
        1,
        "frame 1 should have more=1"
    );
    assert!(rcvmore(pull), "RCVMORE after frame 1");

    // Frame 2: more=1
    let rc = zmq_msg_recv(m.0.as_mut_ptr().cast(), pull, 0);
    assert_eq!(rc, 1);
    assert_eq!(
        zmq_msg_more(m.0.as_ptr().cast()),
        1,
        "frame 2 should have more=1"
    );

    // Frame 3: more=0
    let rc = zmq_msg_recv(m.0.as_mut_ptr().cast(), pull, 0);
    assert_eq!(rc, 1);
    assert_eq!(
        zmq_msg_more(m.0.as_ptr().cast()),
        0,
        "frame 3 should have more=0"
    );
    assert!(!rcvmore(pull), "RCVMORE should be clear after last frame");

    zmq_msg_close(m.0.as_mut_ptr().cast());
    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

/// Mixed `zmq_msg_send` and `zmq_recv`
#[test]
fn msg_send_then_raw_recv() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-msg-mixed").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(pull, ZMQ_RCVTIMEO, 1000);

    let mut m = ZmqMsg([0u8; 64]);
    zmq_msg_init_buffer(m.0.as_mut_ptr().cast(), b"raw recv".as_ptr().cast(), 8);
    zmq_msg_send(m.0.as_mut_ptr().cast(), push, 0);

    let mut buf = [0u8; 32];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 8);
    assert_eq!(&buf[..8], b"raw recv");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

/// Mixed `zmq_send` and `zmq_msg_recv`
#[test]
fn raw_send_then_msg_recv() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-msg-mixed2").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(pull, ZMQ_RCVTIMEO, 1000);

    zmq_send(push, b"msg recv".as_ptr().cast(), 8, 0);

    let mut m = ZmqMsg::new();
    let rc = zmq_msg_recv(m.0.as_mut_ptr().cast(), pull, 0);
    assert_eq!(rc, 8);

    let data = zmq_msg_data(m.0.as_mut_ptr().cast());
    let got = unsafe { std::slice::from_raw_parts(data.cast::<u8>(), 8) };
    assert_eq!(got, b"msg recv");

    zmq_msg_close(m.0.as_mut_ptr().cast());
    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

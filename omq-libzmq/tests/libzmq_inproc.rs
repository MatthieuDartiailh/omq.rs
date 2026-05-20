//! Port of `libzmq/tests/test_inproc.cpp` (subset)
//! Inproc transport: zero-copy in-process messaging.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_recv, zmq_send,
    zmq_setsockopt, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_PAIR: i32 = 0;
const ZMQ_PUB: i32 = 1;
const ZMQ_SUB: i32 = 2;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SUBSCRIBE: i32 = 6;
const ZMQ_SNDMORE: i32 = 2;

fn set_timeo(sock: *mut c_void, ms: i32) {
    zmq_setsockopt(
        sock,
        ZMQ_RCVTIMEO,
        (&ms as *const i32).cast(),
        size_of::<i32>(),
    );
}

#[test]
fn inproc_push_pull() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-inproc-pp").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(pull, 1000);

    for i in 0u8..10 {
        let msg = [i; 8];
        zmq_send(push, msg.as_ptr().cast(), 8, 0);
    }

    let mut buf = [0u8; 64];
    for i in 0u8..10 {
        let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert_eq!(rc, 8, "recv {i}");
        assert!(buf[..8].iter().all(|&b| b == i), "content mismatch at {i}");
    }

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn inproc_pair_bidirectional() {
    let ctx = zmq_ctx_new();
    let a = zmq_socket(ctx, ZMQ_PAIR);
    let b = zmq_socket(ctx, ZMQ_PAIR);

    let addr = CString::new("inproc://test-inproc-pair").unwrap();
    zmq_bind(a, addr.as_ptr());
    zmq_connect(b, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(a, 1000);
    set_timeo(b, 1000);

    zmq_send(a, b"from-a".as_ptr().cast(), 6, 0);
    zmq_send(b, b"from-b".as_ptr().cast(), 6, 0);

    let mut buf = [0u8; 32];
    let rc = zmq_recv(b, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 6);
    assert_eq!(&buf[..6], b"from-a");

    let rc = zmq_recv(a, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 6);
    assert_eq!(&buf[..6], b"from-b");

    zmq_close(a);
    zmq_close(b);
    zmq_ctx_term(ctx);
}

#[test]
fn inproc_multipart() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-inproc-mp").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(pull, 1000);

    zmq_send(push, b"part1".as_ptr().cast(), 5, ZMQ_SNDMORE);
    zmq_send(push, b"part2".as_ptr().cast(), 5, ZMQ_SNDMORE);
    zmq_send(push, b"part3".as_ptr().cast(), 5, 0);

    let mut buf = [0u8; 32];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"part1");

    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"part2");

    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"part3");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn inproc_pub_sub() {
    let ctx = zmq_ctx_new();
    let pub_ = zmq_socket(ctx, ZMQ_PUB);
    let sub = zmq_socket(ctx, ZMQ_SUB);

    let addr = CString::new("inproc://test-inproc-pubsub").unwrap();
    zmq_bind(pub_, addr.as_ptr());

    zmq_setsockopt(sub, ZMQ_SUBSCRIBE, b"".as_ptr().cast(), 0);
    zmq_connect(sub, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(50));
    set_timeo(sub, 1000);

    zmq_send(pub_, b"inproc msg".as_ptr().cast(), 10, 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(sub, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 10);
    assert_eq!(&buf[..10], b"inproc msg");

    zmq_close(sub);
    zmq_close(pub_);
    zmq_ctx_term(ctx);
}

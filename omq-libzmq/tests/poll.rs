//! `zmq_poll` smoke tests.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_poll, zmq_recv, zmq_send,
    zmq_setsockopt, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
#[allow(dead_code)]
const ZMQ_PAIR: i32 = 0;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_POLLIN: i16 = 1;
const ZMQ_POLLOUT: i16 = 2;

#[repr(C)]
struct PollItem {
    socket: *mut c_void,
    fd: i32,
    events: i16,
    revents: i16,
}

fn set_timeo(sock: *mut c_void, ms: i32) {
    zmq_setsockopt(
        sock,
        ZMQ_RCVTIMEO,
        (&ms as *const i32).cast(),
        size_of::<i32>(),
    );
    zmq_setsockopt(
        sock,
        ZMQ_SNDTIMEO,
        (&ms as *const i32).cast(),
        size_of::<i32>(),
    );
}

#[test]
fn poll_timeout_no_events() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let addr = CString::new("inproc://test-poll-timeout").unwrap();
    zmq_bind(pull, addr.as_ptr());

    let mut items = [PollItem {
        socket: pull,
        fd: -1,
        events: ZMQ_POLLIN,
        revents: 0,
    }];

    let rc = zmq_poll(items.as_mut_ptr().cast(), 1, 10);
    assert_eq!(rc, 0, "expected 0 ready items on timeout");
    assert_eq!(items[0].revents, 0);

    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn poll_detects_readable() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-poll-readable").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(push, 1000);
    set_timeo(pull, 1000);

    zmq_send(push, b"msg".as_ptr().cast(), 3, 0);
    std::thread::sleep(Duration::from_millis(20));

    let mut items = [PollItem {
        socket: pull,
        fd: -1,
        events: ZMQ_POLLIN,
        revents: 0,
    }];

    let rc = zmq_poll(items.as_mut_ptr().cast(), 1, 1000);
    assert_eq!(rc, 1);
    assert_ne!(items[0].revents & ZMQ_POLLIN, 0);

    let mut buf = [0u8; 32];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 3);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn poll_multiple_sockets() {
    let ctx = zmq_ctx_new();
    let push1 = zmq_socket(ctx, ZMQ_PUSH);
    let pull1 = zmq_socket(ctx, ZMQ_PULL);
    let push2 = zmq_socket(ctx, ZMQ_PUSH);
    let pull2 = zmq_socket(ctx, ZMQ_PULL);

    let addr1 = CString::new("inproc://poll-multi-1").unwrap();
    let addr2 = CString::new("inproc://poll-multi-2").unwrap();
    zmq_bind(pull1, addr1.as_ptr());
    zmq_connect(push1, addr1.as_ptr());
    zmq_bind(pull2, addr2.as_ptr());
    zmq_connect(push2, addr2.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(push1, 1000);
    set_timeo(push2, 1000);

    zmq_send(push2, b"two".as_ptr().cast(), 3, 0);
    std::thread::sleep(Duration::from_millis(20));

    let mut items = [
        PollItem {
            socket: pull1,
            fd: -1,
            events: ZMQ_POLLIN,
            revents: 0,
        },
        PollItem {
            socket: pull2,
            fd: -1,
            events: ZMQ_POLLIN,
            revents: 0,
        },
    ];

    let rc = zmq_poll(items.as_mut_ptr().cast(), 2, 1000);
    assert!(rc >= 1);
    assert_eq!(
        items[0].revents & ZMQ_POLLIN,
        0,
        "pull1 should not be readable"
    );
    assert_ne!(items[1].revents & ZMQ_POLLIN, 0, "pull2 should be readable");

    zmq_close(push1);
    zmq_close(pull1);
    zmq_close(push2);
    zmq_close(pull2);
    zmq_ctx_term(ctx);
}

#[test]
fn poll_pollout_on_empty_socket() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://poll-pollout").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    let mut items = [PollItem {
        socket: push,
        fd: -1,
        events: ZMQ_POLLOUT,
        revents: 0,
    }];

    let rc = zmq_poll(items.as_mut_ptr().cast(), 1, 100);
    assert_eq!(rc, 1);
    assert_ne!(items[0].revents & ZMQ_POLLOUT, 0);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

//! Context lifecycle tests.

use omq_zmq::{
    zmq_close, zmq_ctx_get, zmq_ctx_new, zmq_ctx_set, zmq_ctx_shutdown, zmq_ctx_term, zmq_socket,
};
use std::ffi::c_void;

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_IO_THREADS: i32 = 1;
const ZMQ_MAX_SOCKETS: i32 = 2;
const ZMQ_MAX_MSGSZ: i32 = 5;

#[test]
fn ctx_new_term() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn ctx_destroy_alias() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    assert_eq!(omq_zmq::zmq_ctx_destroy(ctx), 0);
}

#[test]
fn ctx_shutdown_then_term() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_shutdown(ctx), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn ctx_null_term_returns_error() {
    let rc = zmq_ctx_term(std::ptr::null_mut::<c_void>());
    assert_eq!(rc, -1);
    assert_ne!(omq_zmq::zmq_errno(), 0);
}

#[test]
fn ctx_get_io_threads_default() {
    let ctx = zmq_ctx_new();
    let n = zmq_ctx_get(ctx, ZMQ_IO_THREADS);
    assert_eq!(n, 1);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_get_max_sockets_default() {
    let ctx = zmq_ctx_new();
    let n = zmq_ctx_get(ctx, ZMQ_MAX_SOCKETS);
    assert_eq!(n, 1023);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_set_max_sockets() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_set(ctx, ZMQ_MAX_SOCKETS, 512), 0);
    assert_eq!(zmq_ctx_get(ctx, ZMQ_MAX_SOCKETS), 512);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_set_max_msgsz() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_set(ctx, ZMQ_MAX_MSGSZ, 65536), 0);
    assert_eq!(zmq_ctx_get(ctx, ZMQ_MAX_MSGSZ), 65536);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_term_waits_for_socket_close() {
    use std::sync::{Arc, Barrier};

    let ctx = zmq_ctx_new();
    let sock = zmq_socket(ctx, ZMQ_PUSH);
    assert!(!sock.is_null());

    // Close the socket on a separate thread, then term the context.
    let ctx_copy = ctx as usize; // send across threads as usize
    let sock_copy = sock as usize;
    let barrier = Arc::new(Barrier::new(2));
    let b2 = barrier.clone();

    let t = std::thread::spawn(move || {
        b2.wait(); // sync: main thread is in ctx_term waiting
        // Give main thread time to enter the wait
        std::thread::sleep(std::time::Duration::from_millis(20));
        zmq_close(sock_copy as *mut c_void);
    });

    barrier.wait();
    // ctx_term blocks until the socket count reaches 0 (i.e. zmq_close is called)
    let rc = zmq_ctx_term(ctx_copy as *mut c_void);
    assert_eq!(rc, 0);
    t.join().unwrap();
}

#[test]
fn ctx_multiple_sockets_closed_before_term() {
    let ctx = zmq_ctx_new();
    let s1 = zmq_socket(ctx, ZMQ_PUSH);
    let s2 = zmq_socket(ctx, ZMQ_PULL);
    assert!(!s1.is_null());
    assert!(!s2.is_null());
    zmq_close(s1);
    zmq_close(s2);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

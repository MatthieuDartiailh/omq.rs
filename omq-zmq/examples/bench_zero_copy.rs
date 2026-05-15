//! Measures the recv→forward path: zero-copy (arc steal) vs copy-on-send.
//!
//! Both paths do identical socket work: send → recv → fwd-send → fwd-recv.
//! They differ only in whether the forwarding send copies the Bytes:
//!   - zero-copy: `zmq_msg_recv` → `zmq_msg_send`  (`KIND_BYTES` arc stolen, 0 copies in fwd)
//!   - copy:      `zmq_msg_recv` → `zmq_send(ptr)`  (1 copy in fwd-send via `zmq_send`)
//!
//! Run: `cargo run --example bench_zero_copy --release -p omq-zmq`

use std::ffi::CString;
use std::time::Instant;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_msg_close, zmq_msg_data,
    zmq_msg_init, zmq_msg_recv, zmq_msg_send, zmq_msg_size, zmq_recv, zmq_send, zmq_setsockopt,
    zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_RCVTIMEO: i32 = 27;

#[repr(C, align(8))]
struct Msg([u8; 64]);

impl Msg {
    fn new() -> Self {
        let mut m = Self([0u8; 64]);
        zmq_msg_init(m.0.as_mut_ptr().cast());
        m
    }
}

fn set_rcvtimeo(sock: *mut libc::c_void, ms: i32) {
    zmq_setsockopt(
        sock,
        ZMQ_RCVTIMEO,
        (&raw const ms).cast(),
        std::mem::size_of::<i32>(),
    );
}

fn bench(label: &str, iters: usize, mut f: impl FnMut()) {
    for _ in 0..iters / 10 {
        f();
    }
    let t = Instant::now();
    for _ in 0..iters {
        f();
    }
    let elapsed = t.elapsed();
    let ns_per_iter = elapsed.as_nanos() as f64 / iters as f64;
    println!("{label:50}  {ns_per_iter:8.1} ns/iter");
}

fn sockets(
    ctx: *mut libc::c_void,
    addr1: &str,
    addr2: &str,
) -> (
    *mut libc::c_void,
    *mut libc::c_void,
    *mut libc::c_void,
    *mut libc::c_void,
) {
    let src_push = zmq_socket(ctx, ZMQ_PUSH);
    let src_pull = zmq_socket(ctx, ZMQ_PULL);
    let dst_push = zmq_socket(ctx, ZMQ_PUSH);
    let dst_pull = zmq_socket(ctx, ZMQ_PULL);

    let a1 = CString::new(addr1).unwrap();
    let a2 = CString::new(addr2).unwrap();
    zmq_bind(src_pull, a1.as_ptr());
    zmq_connect(src_push, a1.as_ptr());
    zmq_bind(dst_pull, a2.as_ptr());
    zmq_connect(dst_push, a2.as_ptr());
    std::thread::sleep(std::time::Duration::from_millis(20));
    set_rcvtimeo(src_pull, 5000);
    set_rcvtimeo(dst_pull, 5000);
    (src_push, src_pull, dst_push, dst_pull)
}

fn main() {
    println!("{:<50}  {:>14}", "benchmark", "latency");
    println!("{}", "-".repeat(70));

    for &msg_size in &[64usize, 256, 1024, 16 * 1024, 256 * 1024] {
        let payload: Vec<u8> = (0..msg_size).map(|i| i as u8).collect();
        let iters = if msg_size <= 1024 { 20_000 } else { 2_000 };
        let mut recv_buf = vec![0u8; msg_size + 16];

        // --- zero-copy: zmq_msg_recv → zmq_msg_send (arc steal, 0 copies in fwd) ---
        {
            let ctx = zmq_ctx_new();
            let (src_push, src_pull, dst_push, dst_pull) =
                sockets(ctx, "inproc://bench-zc-a", "inproc://bench-zc-b");

            bench(
                &format!("zmq_msg_recv→zmq_msg_send zero-copy sz={msg_size:>7}"),
                iters,
                || {
                    zmq_send(src_push, payload.as_ptr().cast(), payload.len(), 0);
                    let mut msg = Msg::new();
                    zmq_msg_recv(msg.0.as_mut_ptr().cast(), src_pull, 0);
                    zmq_msg_send(msg.0.as_mut_ptr().cast(), dst_push, 0);
                    let mut out = Msg::new();
                    zmq_msg_recv(out.0.as_mut_ptr().cast(), dst_pull, 0);
                    zmq_msg_close(out.0.as_mut_ptr().cast());
                },
            );

            zmq_close(src_push);
            zmq_close(src_pull);
            zmq_close(dst_push);
            zmq_close(dst_pull);
            zmq_ctx_term(ctx);
        }

        // --- copy: zmq_msg_recv → zmq_send(data_ptr) (1 copy in fwd-send) ---
        {
            let ctx = zmq_ctx_new();
            let (src_push, src_pull, dst_push, dst_pull) =
                sockets(ctx, "inproc://bench-cp-a", "inproc://bench-cp-b");

            bench(
                &format!("zmq_msg_recv→zmq_send(ptr)  copy      sz={msg_size:>7}"),
                iters,
                || {
                    zmq_send(src_push, payload.as_ptr().cast(), payload.len(), 0);
                    let mut msg = Msg::new();
                    zmq_msg_recv(msg.0.as_mut_ptr().cast(), src_pull, 0);
                    let data = zmq_msg_data(msg.0.as_mut_ptr().cast());
                    let size = zmq_msg_size(msg.0.as_ptr().cast());
                    zmq_send(dst_push, data, size, 0);
                    zmq_msg_close(msg.0.as_mut_ptr().cast());
                    zmq_recv(dst_pull, recv_buf.as_mut_ptr().cast(), recv_buf.len(), 0);
                },
            );

            zmq_close(src_push);
            zmq_close(src_pull);
            zmq_close(dst_push);
            zmq_close(dst_pull);
            zmq_ctx_term(ctx);
        }

        println!();
    }
}

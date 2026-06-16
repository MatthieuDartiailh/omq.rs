//! Round-trip latency benchmark: single REQ/REP pair over inproc.
//!
//! This is the primary target of the pump-removal in Part 2. The old path:
//!   C → `send_tx` channel → pump task wakes → `Socket::send`
//! New path:
//!   C → `run_on`/`with_socket` → `Socket::send` directly on io thread
//!
//! Run: `cargo run --example bench_latency --release -p omq-libzmq`
//!
//! Note: On Windows, IPC transport is not supported; only inproc is used.

use std::ffi::CString;
use std::time::Instant;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_recv, zmq_send,
    zmq_setsockopt, zmq_socket,
};

const ZMQ_REQ: i32 = 3;
const ZMQ_REP: i32 = 4;
const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_RCVTIMEO: i32 = 27;

fn set_rcvtimeo(sock: *mut libc::c_void, ms: i32) {
    zmq_setsockopt(
        sock,
        ZMQ_RCVTIMEO,
        (&raw const ms).cast(),
        std::mem::size_of::<i32>(),
    );
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn bench_req_rep_inproc(iters: usize) {
    let ctx = zmq_ctx_new();
    let req = zmq_socket(ctx, ZMQ_REQ);
    let rep = zmq_socket(ctx, ZMQ_REP);

    let addr = CString::new("inproc://bench-rtt").unwrap();
    zmq_bind(rep, addr.as_ptr());
    zmq_connect(req, addr.as_ptr());
    std::thread::sleep(std::time::Duration::from_millis(20));
    set_rcvtimeo(req, 5000);
    set_rcvtimeo(rep, 5000);

    let payload = b"ping";
    let reply = b"pong";
    let mut buf = [0u8; 16];

    // REP thread
    let rep_raw = rep as usize;
    let rep_thread = std::thread::spawn(move || {
        let rep = rep_raw as *mut libc::c_void;
        for _ in 0..iters + iters / 10 {
            let rc = zmq_recv(rep, buf.as_mut_ptr().cast(), buf.len(), 0);
            if rc < 0 {
                break;
            }
            zmq_send(rep, reply.as_ptr().cast(), reply.len(), 0);
        }
    });

    // warmup
    for _ in 0..iters / 10 {
        zmq_send(req, payload.as_ptr().cast(), payload.len(), 0);
        zmq_recv(req, buf.as_mut_ptr().cast(), buf.len(), 0);
    }

    let mut latencies = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        zmq_send(req, payload.as_ptr().cast(), payload.len(), 0);
        zmq_recv(req, buf.as_mut_ptr().cast(), buf.len(), 0);
        latencies.push(t.elapsed().as_nanos() as u64);
    }

    rep_thread.join().unwrap();

    latencies.sort_unstable();
    println!(
        "REQ/REP inproc round-trip  ({iters} iters)  \
         p50={:6}ns  p95={:6}ns  p99={:6}ns  mean={:.0}ns",
        percentile(&latencies, 50.0),
        percentile(&latencies, 95.0),
        percentile(&latencies, 99.0),
        latencies.iter().sum::<u64>() as f64 / iters as f64,
    );

    zmq_close(req);
    zmq_close(rep);
    zmq_ctx_term(ctx);
}

fn bench_push_pull_throughput(msg_size: usize, iters: usize) {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://bench-tput").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(std::time::Duration::from_millis(20));
    set_rcvtimeo(pull, 5000);

    let payload: Vec<u8> = (0..msg_size).map(|i| i as u8).collect();
    let mut recv_buf = vec![0u8; msg_size];

    // warmup
    for _ in 0..iters / 10 {
        zmq_send(push, payload.as_ptr().cast(), payload.len(), 0);
        zmq_recv(pull, recv_buf.as_mut_ptr().cast(), recv_buf.len(), 0);
    }

    let t = Instant::now();
    for _ in 0..iters {
        zmq_send(push, payload.as_ptr().cast(), payload.len(), 0);
        zmq_recv(pull, recv_buf.as_mut_ptr().cast(), recv_buf.len(), 0);
    }
    let elapsed = t.elapsed();
    let ns_per = elapsed.as_nanos() as f64 / iters as f64;
    let gbps = (msg_size as f64 * iters as f64) / elapsed.as_secs_f64() / f64::from(1_u32 << 30);
    println!("PUSH/PULL inproc  sz={msg_size:>7}  {ns_per:8.0}ns/msg  {gbps:5.2} GB/s");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

fn main() {
    println!("--- round-trip latency ---");
    bench_req_rep_inproc(10_000);

    println!();
    println!("--- push/pull throughput ---");
    for &sz in &[64usize, 1024, 16 * 1024, 256 * 1024] {
        bench_push_pull_throughput(sz, if sz <= 1024 { 20_000 } else { 2_000 });
    }
}

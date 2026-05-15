//! Push/pull throughput and req/rep latency bench for omq-zmq.
//!
//! Run: `cargo run --example bench_recv --release -p omq-zmq`

use std::ffi::CString;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_getsockopt, zmq_recv,
    zmq_send, zmq_setsockopt, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_REQ: i32 = 3;
const ZMQ_REP: i32 = 4;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDHWM: i32 = 23;
const ZMQ_RCVHWM: i32 = 24;
const ZMQ_LAST_ENDPOINT: i32 = 32;

static ADDR_CTR: AtomicUsize = AtomicUsize::new(0);

fn set_opt_i32(sock: *mut libc::c_void, opt: i32, val: i32) {
    zmq_setsockopt(
        sock,
        opt,
        (&raw const val).cast(),
        std::mem::size_of::<i32>(),
    );
}

fn unique_inproc() -> CString {
    let n = ADDR_CTR.fetch_add(1, Ordering::Relaxed);
    CString::new(format!("inproc://bench-{n}")).unwrap()
}

fn get_last_endpoint(sock: *mut libc::c_void) -> CString {
    let mut buf = [0u8; 256];
    let mut len = buf.len();
    zmq_getsockopt(sock, ZMQ_LAST_ENDPOINT, buf.as_mut_ptr().cast(), &mut len);
    // len includes the trailing NUL; the string is len-1 bytes.
    let s = std::str::from_utf8(&buf[..len.saturating_sub(1)]).unwrap();
    CString::new(s).unwrap()
}

fn bench_push_pull(transport: &str, msg_size: usize, batch: usize) {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let hwm = (batch as i32) * 2;
    set_opt_i32(push, ZMQ_SNDHWM, hwm);
    set_opt_i32(pull, ZMQ_RCVHWM, hwm);
    set_opt_i32(pull, ZMQ_RCVTIMEO, 10_000);

    let bind_addr = if transport.starts_with("inproc") {
        unique_inproc()
    } else {
        CString::new(transport).unwrap()
    };
    zmq_bind(pull, bind_addr.as_ptr());
    let connect_addr = if transport.starts_with("tcp") {
        get_last_endpoint(pull)
    } else {
        bind_addr
    };
    zmq_connect(push, connect_addr.as_ptr());
    std::thread::sleep(std::time::Duration::from_millis(50));

    let payload: Vec<u8> = vec![0xABu8; msg_size];
    let mut recv_buf = vec![0u8; msg_size + 1];

    let rounds = 7;
    let mut ns_per_msg_samples = Vec::with_capacity(rounds);

    for _round in 0..rounds {
        let push_raw = push as usize;
        let pull_raw = pull as usize;
        let payload_clone = payload.clone();
        let batch_copy = batch;
        let msg_sz = msg_size;

        let t = Instant::now();

        let sender = std::thread::spawn(move || {
            let push = push_raw as *mut libc::c_void;
            for _ in 0..batch_copy {
                zmq_send(push, payload_clone.as_ptr().cast(), payload_clone.len(), 0);
            }
        });

        let pull_local = pull_raw as *mut libc::c_void;
        for _ in 0..batch {
            let rc = zmq_recv(pull_local, recv_buf.as_mut_ptr().cast(), recv_buf.len(), 0);
            assert!(rc >= 0, "recv failed (sz={msg_sz}): {rc}");
        }
        sender.join().unwrap();

        let elapsed = t.elapsed();
        ns_per_msg_samples.push(elapsed.as_nanos() as f64 / batch as f64);
    }

    ns_per_msg_samples.sort_by(f64::total_cmp);
    let median = ns_per_msg_samples[rounds / 2];
    let best = ns_per_msg_samples[0];
    // Convert ns/msg to millions of messages per second.
    let best_mmps = 1_000.0 / best;
    let median_mmps = 1_000.0 / median;
    println!(
        "  sz={msg_size:>7}  best={best_mmps:6.2}  median={median_mmps:6.2}  M msg/s"
    );

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

fn bench_req_rep(transport: &str, msg_size: usize, iters: usize) {
    let ctx = zmq_ctx_new();
    let req = zmq_socket(ctx, ZMQ_REQ);
    let rep = zmq_socket(ctx, ZMQ_REP);

    set_opt_i32(req, ZMQ_RCVTIMEO, 5_000);
    set_opt_i32(rep, ZMQ_RCVTIMEO, 5_000);

    let bind_addr = if transport.starts_with("inproc") {
        unique_inproc()
    } else {
        CString::new(transport).unwrap()
    };
    zmq_bind(rep, bind_addr.as_ptr());
    let connect_addr = if transport.starts_with("tcp") {
        get_last_endpoint(rep)
    } else {
        bind_addr
    };
    zmq_connect(req, connect_addr.as_ptr());
    std::thread::sleep(std::time::Duration::from_millis(50));

    let payload: Vec<u8> = vec![0xCDu8; msg_size];
    let mut recv_buf = vec![0u8; msg_size + 1];

    let rounds = 7;
    let mut ns_per_rt = Vec::with_capacity(rounds);

    for _round in 0..rounds {
        let rep_raw = rep as usize;
        let rep_sz = msg_size;
        // +10 for warmup
        let total = iters + 10;
        let responder = std::thread::spawn(move || {
            let rep = rep_raw as *mut libc::c_void;
            let mut buf = vec![0u8; rep_sz + 1];
            for _ in 0..total {
                let n = zmq_recv(rep, buf.as_mut_ptr().cast(), buf.len(), 0);
                if n < 0 {
                    break;
                }
                zmq_send(rep, buf.as_ptr().cast(), n as usize, 0);
            }
        });

        // Warmup
        for _ in 0..10 {
            zmq_send(req, payload.as_ptr().cast(), payload.len(), 0);
            zmq_recv(req, recv_buf.as_mut_ptr().cast(), recv_buf.len(), 0);
        }

        let t = Instant::now();
        for _ in 0..iters {
            zmq_send(req, payload.as_ptr().cast(), payload.len(), 0);
            zmq_recv(req, recv_buf.as_mut_ptr().cast(), recv_buf.len(), 0);
        }
        let elapsed = t.elapsed();
        ns_per_rt.push(elapsed.as_nanos() as f64 / iters as f64);

        responder.join().unwrap();
    }

    ns_per_rt.sort_by(f64::total_cmp);
    let median = ns_per_rt[rounds / 2];
    let best = ns_per_rt[0];
    // Convert ns/rt to thousands of round-trips per second.
    let best_krt = 1_000_000.0 / best;
    let median_krt = 1_000_000.0 / median;
    println!(
        "  sz={msg_size:>7}  best={best_krt:7.1}  median={median_krt:7.1}  k rt/s"
    );

    zmq_close(req);
    zmq_close(rep);
    zmq_ctx_term(ctx);
}

fn main() {
    let batch: usize = std::env::var("BENCH_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);
    let rt_iters: usize = std::env::var("BENCH_RT_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);

    let ipc_addr = format!("ipc:///tmp/omq-bench-{}.sock", std::process::id());
    let sizes = [8, 64, 256, 1024, 16384];

    println!("=== omq-zmq push/pull throughput ({batch} msgs/round) ===");
    println!("--- inproc ---");
    for &sz in &sizes {
        bench_push_pull("inproc://x", sz, batch);
    }
    println!("--- ipc ---");
    for &sz in &sizes {
        bench_push_pull(&ipc_addr, sz, batch);
    }
    println!("--- tcp ---");
    for &sz in &sizes {
        bench_push_pull("tcp://127.0.0.1:*", sz, batch);
    }

    println!("\n=== omq-zmq req/rep latency ({rt_iters} round-trips/round) ===");
    println!("--- inproc ---");
    for &sz in &sizes {
        bench_req_rep("inproc://x", sz, rt_iters);
    }
    println!("--- ipc ---");
    for &sz in &sizes {
        bench_req_rep(&ipc_addr, sz, rt_iters);
    }
    println!("--- tcp ---");
    for &sz in &sizes {
        bench_req_rep("tcp://127.0.0.1:*", sz, rt_iters);
    }
}

//! Two-process throughput peer for zeromq (zmq.rs).
//!
//! Usage:
//!   zmqrs_bench_peer push <addr> <msg_size_bytes>
//!   zmqrs_bench_peer pull <addr> <msg_size_bytes> <duration_secs>
//!
//! <addr>: a port number (→ tcp://127.0.0.1:<port>) or a full ZMQ address
//!         (e.g. ipc:///tmp/bench.sock or tcp://127.0.0.1:15655).
//!
//! Push: binds, sends <msg_size> byte messages forever.
//! Pull: connects, warms up for 500 ms, then counts messages for <duration>
//!       seconds and prints one line to stdout:
//!         <count> <elapsed_secs> <msg_size>

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use zeromq::{PullSocket, PushSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

fn resolve_addr(s: &str) -> String {
    if s.chars().all(|c| c.is_ascii_digit()) {
        format!("tcp://127.0.0.1:{s}")
    } else {
        s.to_owned()
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("push") => {
            let addr = resolve_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_push(&addr, size).await;
        }
        Some("pull") => {
            let addr = resolve_addr(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull(&addr, size, Duration::from_secs_f64(duration)).await;
        }
        _ => {
            eprintln!("usage: zmqrs_bench_peer push <addr> <size>");
            eprintln!("       zmqrs_bench_peer pull <addr> <size> <duration_secs>");
            eprintln!("<addr>: port number or full ZMQ address (tcp:// ipc://)");
            std::process::exit(1);
        }
    }
}

async fn run_push(addr: &str, size: usize) {
    let mut socket = PushSocket::new();
    socket.bind(addr).await.expect("push bind");
    let payload = Bytes::from(vec![b'x'; size]);
    loop {
        if socket.send(ZmqMessage::from(payload.clone())).await.is_err() {
            tokio::task::yield_now().await;
        }
    }
}

async fn run_pull(addr: &str, size: usize, duration: Duration) {
    let mut socket = PullSocket::new();
    socket.connect(addr).await.expect("pull connect");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // zeromq 0.6's PullSocket::recv stalls within a few thousand messages when
    // wrapped in tokio::time::timeout per call, even when the timeout never
    // fires. Spawn a recv task that runs to completion and time the window
    // outside it instead of cancelling recv mid-flight.
    let count = Arc::new(AtomicU64::new(0));
    let count_recv = count.clone();
    let recv_handle = tokio::spawn(async move {
        loop {
            if socket.recv().await.is_err() {
                break;
            }
            count_recv.fetch_add(1, Ordering::Relaxed);
        }
    });

    let t0 = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();
    let final_count = count.load(Ordering::Relaxed);
    recv_handle.abort();

    println!("{final_count} {elapsed:.6} {size}");
    // zeromq spawns background tokio tasks that don't shut down cleanly on
    // socket drop; without this the runtime blocks in sigsuspend indefinitely,
    // keeping the pipe open and stalling the caller's command substitution.
    std::process::exit(0);
}

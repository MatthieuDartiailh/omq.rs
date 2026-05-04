//! Two-process TCP throughput peer for zeromq (zmq.rs).
//!
//! Usage:
//!   zmqrs_bench_peer push <port> <msg_size_bytes>
//!   zmqrs_bench_peer pull <port> <msg_size_bytes> <duration_secs>
//!
//! Push: binds tcp://127.0.0.1:<port>, sends <msg_size> byte messages forever.
//! Pull: connects, warms up for 500 ms, then counts messages for <duration>
//!       seconds and prints one line to stdout:
//!         <count> <elapsed_secs> <msg_size>

use std::time::{Duration, Instant};

use bytes::Bytes;
use zeromq::{PullSocket, PushSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("push") => {
            let port: u16 = args[2].parse().expect("port");
            let size: usize = args[3].parse().expect("msg_size");
            run_push(port, size).await;
        }
        Some("pull") => {
            let port: u16 = args[2].parse().expect("port");
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull(port, size, Duration::from_secs_f64(duration)).await;
        }
        _ => {
            eprintln!("usage: zmqrs_bench_peer push <port> <size>");
            eprintln!("       zmqrs_bench_peer pull <port> <size> <duration_secs>");
            std::process::exit(1);
        }
    }
}

async fn run_push(port: u16, size: usize) {
    let mut socket = PushSocket::new();
    socket
        .bind(&format!("tcp://127.0.0.1:{port}"))
        .await
        .expect("push bind");
    let payload = Bytes::from(vec![b'x'; size]);
    loop {
        // send_round_robin returns ReturnToSender when no peers are connected;
        // yield instead of breaking so the push stays alive until killed.
        if socket.send(ZmqMessage::from(payload.clone())).await.is_err() {
            tokio::task::yield_now().await;
        }
    }
}

async fn run_pull(port: u16, size: usize, duration: Duration) {
    let mut socket = PullSocket::new();
    socket
        .connect(&format!("tcp://127.0.0.1:{port}"))
        .await
        .expect("pull connect");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, socket.recv()).await {
            Ok(Ok(_)) => count += 1,
            _ => break,
        }
    }

    let elapsed = t0.elapsed().as_secs_f64();
    println!("{count} {elapsed:.6} {size}");
    // zeromq spawns background tokio tasks that don't shut down cleanly on
    // socket drop; without this the runtime blocks in sigsuspend indefinitely,
    // keeping the pipe open and stalling the caller's command substitution.
    std::process::exit(0);
}

//! Two-process throughput peer using the omq-zeromq compat layer.
//!
//! Same protocol as `zmqrs_bench_peer`: uses the zeromq crate API surface.

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

extern "C" fn exit_on_signal(_sig: libc::c_int) {
    unsafe { libc::_exit(0) };
}

#[tokio::main]
async fn main() {
    unsafe {
        libc::signal(
            libc::SIGTERM,
            exit_on_signal as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGINT,
            exit_on_signal as *const () as libc::sighandler_t,
        );
    }
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
            eprintln!("usage: bench_peer_zeromq push <addr> <size>");
            eprintln!("       bench_peer_zeromq pull <addr> <size> <duration_secs>");
            eprintln!("<addr>: port number or full endpoint (tcp:// ipc://)");
            std::process::exit(1);
        }
    }
}

async fn run_push(addr: &str, size: usize) {
    let mut socket = PushSocket::new();
    socket.bind(addr).await.expect("push bind");
    let payload = Bytes::from(vec![b'x'; size]);
    loop {
        socket
            .send(ZmqMessage::from(payload.clone()))
            .await
            .unwrap();
    }
}

async fn run_pull(addr: &str, size: usize, duration: Duration) {
    let mut socket = PullSocket::new();
    socket.connect(addr).await.expect("pull connect");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        let _msg = socket.recv().await.unwrap();
        count += 1;
        if Instant::now() >= deadline {
            break;
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    println!("{count} {elapsed:.6} {size}");
}

//! Two-process TCP throughput peer for omq-tokio.
//!
//! Usage:
//!   `bench_peer` push \<port\> \<`msg_size_bytes`\>
//!   `bench_peer` pull \<port\> \<`msg_size_bytes`\> \<`duration_secs`\>
//!
//! Push: binds <tcp://127.0.0.1>:\<port\>, sends \<`msg_size`\> byte messages forever.
//! Pull: connects, warms up for 500 ms, then counts messages for \<duration\>
//!       seconds and prints one line to stdout:
//!         \<count\> \<`elapsed_secs`\> \<`msg_size`\>

use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};
use std::net::Ipv4Addr;

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

extern "C" fn exit_on_signal(_sig: libc::c_int) {
    unsafe { libc::_exit(0) };
}

#[tokio::main]
async fn main() {
    unsafe {
        libc::signal(libc::SIGTERM, exit_on_signal as *const () as libc::sighandler_t);
        libc::signal(libc::SIGINT, exit_on_signal as *const () as libc::sighandler_t);
    }
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
            eprintln!("usage: bench_peer push <port> <size>");
            eprintln!("       bench_peer pull <port> <size> <duration_secs>");
            std::process::exit(1);
        }
    }
}

async fn run_push(port: u16, size: usize) {
    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(tcp_ep(port)).await.expect("push bind");
    let payload = Bytes::from(vec![b'x'; size]);
    loop {
        push.send(Message::single(payload.clone())).await.unwrap();
    }
}

async fn run_pull(port: u16, size: usize, duration: Duration) {
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.connect(tcp_ep(port)).await.expect("pull connect");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        pull.recv().await.unwrap();
        count += 1;
        while pull.try_recv().is_ok() {
            count += 1;
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    println!("{count} {elapsed:.6} {size}");
}

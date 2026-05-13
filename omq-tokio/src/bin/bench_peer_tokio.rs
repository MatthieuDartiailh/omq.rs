//! Two-process throughput peer for omq-tokio.
//!
//! Usage:
//!   `bench_peer_tokio` push \<addr\> \<`msg_size_bytes`\>
//!   `bench_peer_tokio` pull \<addr\> \<`msg_size_bytes`\> \<`duration_secs`\>
//!
//! \<addr\>: a port number (`tcp://127.0.0.1:<port>`) or a full endpoint
//!   (e.g. `ipc:///tmp/bench.sock`).
//!
//! Push: binds, sends \<`msg_size`\> byte messages forever.
//! Pull: connects, warms up for 500 ms, then counts messages for \<duration\>
//!       seconds and prints one line to stdout:
//!         \<count\> \<`elapsed_secs`\> \<`msg_size`\>

use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};
use std::net::Ipv4Addr;

fn parse_ep(s: &str) -> Endpoint {
    if let Ok(port) = s.parse::<u16>() {
        Endpoint::Tcp {
            host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
            port,
        }
    } else {
        s.parse()
            .expect("valid endpoint (port number or ipc:// path)")
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
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_push(ep, size).await;
        }
        Some("pull") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull(ep, size, Duration::from_secs_f64(duration)).await;
        }
        Some("inproc") => {
            let name = args[2].clone();
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_inproc(name, size, Duration::from_secs_f64(duration)).await;
        }
        _ => {
            eprintln!("usage: bench_peer_tokio push <addr> <size>");
            eprintln!("       bench_peer_tokio pull <addr> <size> <duration_secs>");
            eprintln!("       bench_peer_tokio inproc <name> <size> <duration_secs>");
            eprintln!("<addr>: port number or full endpoint (tcp:// ipc://)");
            std::process::exit(1);
        }
    }
}

fn bench_options(msg_size: usize) -> Options {
    let mut o = Options::default();
    if msg_size >= 2 * 1024 * 1024 {
        let buf = msg_size * 2;
        o = o.recv_buffer_size(buf).send_buffer_size(buf);
    }
    o
}

async fn run_push(ep: Endpoint, size: usize) {
    let push = Socket::new(SocketType::Push, bench_options(size));
    push.bind(ep).await.expect("push bind");
    let payload = Bytes::from(vec![b'x'; size]);
    loop {
        push.send(Message::single(payload.clone())).await.unwrap();
    }
}

async fn run_inproc(name: String, size: usize, duration: Duration) {
    let ep = Endpoint::Inproc { name };
    let push = Socket::new(SocketType::Push, bench_options(size));
    push.bind(ep.clone()).await.expect("push bind");
    let pull = Socket::new(SocketType::Pull, bench_options(size));
    pull.connect(ep).await.expect("pull connect");

    let payload = Bytes::from(vec![b'x'; size]);
    tokio::spawn(async move {
        loop {
            if push.send(Message::single(payload.clone())).await.is_err() {
                break;
            }
        }
    });

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

async fn run_pull(ep: Endpoint, size: usize, duration: Duration) {
    let pull = Socket::new(SocketType::Pull, bench_options(size));
    pull.connect(ep).await.expect("pull connect");

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

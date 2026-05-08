//! Two-process throughput peer for omq-compio.
//!
//! Usage:
//!   `bench_peer` push \<endpoint\> \<`msg_size_bytes`\>
//!   `bench_peer` pull \<endpoint\> \<`msg_size_bytes`\> \<`duration_secs`\>
//!
//! Endpoint: a port number (`tcp://127.0.0.1:<port>`) or a path (`/tmp/foo.sock`).
//!
//! Push: binds, sends \<`msg_size`\> byte messages forever.
//! Pull: connects, warms up for 500 ms, then counts messages for \<duration\>
//!       seconds and prints one line to stdout:
//!         \<count\> \<`elapsed_secs`\> \<`msg_size`\>

use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType, build_default_runtime};
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

fn main() {
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
    let rt = build_default_runtime().expect("compio runtime");
    rt.block_on(async {
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
            _ => {
                eprintln!("usage: bench_peer push <endpoint> <size>");
                eprintln!("       bench_peer pull <endpoint> <size> <duration_secs>");
                std::process::exit(1);
            }
        }
    });
}

async fn run_push(ep: Endpoint, size: usize) {
    let push = Socket::new(SocketType::Push, bench_options(size));
    push.bind(ep).await.expect("push bind");
    let payload = Bytes::from(vec![b'x'; size]);
    loop {
        push.send(Message::single(payload.clone())).await.unwrap();
    }
}

fn bench_options(msg_size: usize) -> Options {
    let mut o = Options::default();
    if std::env::var_os("OMQ_NO_LARGE_MSG").is_some() {
        o = o.disable_large_message_path();
    }
    if msg_size >= 2 * 1024 * 1024 {
        let buf = msg_size * 2;
        o = o.tcp_recv_buffer_size(buf).tcp_send_buffer_size(buf);
    }
    o
}

async fn run_pull(ep: Endpoint, size: usize, duration: Duration) {
    let pull = Socket::new(SocketType::Pull, bench_options(size));
    pull.connect(ep).await.expect("pull connect");

    compio::time::sleep(Duration::from_millis(500)).await;

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

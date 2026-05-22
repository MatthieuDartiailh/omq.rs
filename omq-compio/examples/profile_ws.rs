//! perf-friendly WS PUSH/PULL driver for `perf record`.
//!
//! Tweak via env:
//!   `OMQ_PROFILE_SIZE`   bytes per message (default 8)
//!   `OMQ_PROFILE_SECS`   wall-clock seconds (default 5)
//!
//! Build & profile:
//!   cargo build --release --features ws --example profile_ws -p omq-compio
//!   samply record target/release/examples/profile_ws

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

fn loopback_port() -> u16 {
    let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

#[compio::main]
async fn main() {
    let size: usize = std::env::var("OMQ_PROFILE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let secs: u64 = std::env::var("OMQ_PROFILE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let dur = Duration::from_secs(secs);

    let port = loopback_port();
    let ep = Endpoint::Ws {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
        path: String::new(),
    };

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    let payload = Bytes::from(vec![b'x'; size]);

    // Let the WS handshake complete.
    compio::time::sleep(Duration::from_millis(200)).await;

    // Warmup: interleave send/recv to avoid blocking on single-thread runtime.
    let warmup_h = compio::runtime::spawn({
        let push = push.clone();
        let payload = payload.clone();
        async move {
            for _ in 0..2_000 {
                push.send(Message::single(payload.clone())).await.unwrap();
            }
        }
    });
    for _ in 0..2_000 {
        pull.recv().await.unwrap();
    }
    warmup_h.await.unwrap();

    let start = Instant::now();
    let mut recv: u64 = 0;
    let send_h = compio::runtime::spawn({
        let push = push.clone();
        let payload = payload.clone();
        async move {
            let mut n = 0u64;
            while start.elapsed() < dur {
                for _ in 0..256 {
                    push.send(Message::single(payload.clone())).await.unwrap();
                    n += 1;
                }
            }
            n
        }
    });
    while start.elapsed() < dur {
        for _ in 0..256 {
            pull.recv().await.unwrap();
            recv += 1;
        }
    }
    let sent = send_h.await.unwrap();
    let elapsed = start.elapsed();
    eprintln!(
        "profile_ws: size={size}B secs={:.2} sent={sent} recv={recv} rate={:.0} msg/s",
        elapsed.as_secs_f64(),
        recv as f64 / elapsed.as_secs_f64()
    );
}

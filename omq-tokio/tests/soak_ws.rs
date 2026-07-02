#![cfg(all(feature = "soak", feature = "ws"))]
//! Soak: WebSocket sustained throughput + bind-side restart storm.
//!
//! Exercises the HTTP upgrade handshake, WS framing, and reconnection
//! over `ws://` under sustained load. Two sub-tests:
//!
//! 1. Sustained throughput: PUSH/PULL over WS for `soak_duration`.
//! 2. Restart storm: bind-side closes and rebinds repeatedly while
//!    the connect-side reconnects and resumes sending.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use omq_tokio::options::ReconnectPolicy;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn ws_ep(port: u16) -> Endpoint {
    format!("ws://127.0.0.1:{port}/").parse().unwrap()
}

fn get_port(ep: &Endpoint) -> u16 {
    match ep {
        Endpoint::Ws { port, .. } => *port,
        other => panic!("expected Ws, got {other:?}"),
    }
}

fn fast_reconnect() -> Options {
    Options {
        reconnect: ReconnectPolicy::Fixed(Duration::from_millis(10)),
        ..soak_common::soak_options()
    }
}

async fn rebind_ws(port: u16) -> Option<Socket> {
    for _ in 0..40 {
        let s = Socket::new(SocketType::Pull, soak_common::soak_options());
        if s.bind(ws_ep(port)).await.is_ok() {
            return Some(s);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    None
}

#[test]
fn soak_ws_throughput() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let rt = soak_common::tokio_runtime();
    rt.block_on(async {
        let pull = Socket::new(SocketType::Pull, soak_common::soak_options());
        let ep = pull.bind(ws_ep(0)).await.unwrap();
        let port = get_port(&ep);

        let push = Socket::new(SocketType::Push, soak_common::soak_options().send_hwm(1024));
        push.connect(ws_ep(port)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut sent: u64 = 0;
        let mut recvd: u64 = 0;
        let start = Instant::now();
        let mut last_log = start;

        while start.elapsed() < duration {
            for _ in 0..100 {
                if let Ok(Ok(())) = tokio::time::timeout(
                    Duration::from_millis(1),
                    push.send(Message::single(format!("ws-{sent}"))),
                )
                .await
                {
                    sent += 1;
                }
            }

            while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(1), pull.recv()).await
            {
                recvd += 1;
            }

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[ws_throughput] {:.0}s, sent {sent}, recvd {recvd}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        push.close().await.unwrap();
        pull.close().await.unwrap();

        eprintln!(
            "[ws_throughput] done: sent {sent}, recvd {recvd} in {:.1}s",
            start.elapsed().as_secs_f64(),
        );
        assert!(recvd > 0, "no messages received");
    });

    let report = monitor.stop();
    report.assert_no_leak("ws_throughput");
}

#[test]
fn soak_ws_reconnect_storm() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let rt = soak_common::tokio_runtime();
    rt.block_on(async {
        // Probe for a port.
        let probe = Socket::new(SocketType::Pull, soak_common::soak_options());
        let ep = probe.bind(ws_ep(0)).await.unwrap();
        let port = get_port(&ep);
        probe.close().await.unwrap();

        let push = Socket::new(SocketType::Push, fast_reconnect().send_hwm(16));
        push.connect(ws_ep(port)).await.unwrap();

        let start = Instant::now();
        let mut cycles: u64 = 0;
        let mut delivered: u64 = 0;
        let mut last_log = start;

        while start.elapsed() < duration {
            let Some(pull) = rebind_ws(port).await else {
                eprintln!("[ws_reconnect_storm] rebind failed at cycle {cycles}");
                continue;
            };

            let tag = format!("ws-r-{cycles}");
            if !matches!(
                tokio::time::timeout(Duration::from_secs(5), push.send(Message::single(tag))).await,
                Ok(Ok(())),
            ) {
                pull.close().await.unwrap();
                cycles += 1;
                continue;
            }

            if matches!(
                tokio::time::timeout(Duration::from_secs(5), pull.recv()).await,
                Ok(Ok(_)),
            ) {
                delivered += 1;
            }

            pull.close().await.unwrap();
            cycles += 1;

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[ws_reconnect_storm] {:.0}s, cycles {cycles}, delivered {delivered}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        push.close().await.unwrap();

        let pct = if cycles > 0 {
            delivered as f64 / cycles as f64 * 100.0
        } else {
            100.0
        };
        eprintln!(
            "[ws_reconnect_storm] done: {delivered}/{cycles} ({pct:.1}%) in {:.1}s",
            start.elapsed().as_secs_f64(),
        );
        assert!(pct >= 70.0, "delivery rate too low: {pct:.1}%");
    });

    let report = monitor.stop();
    report.assert_no_leak("ws_reconnect_storm");
}

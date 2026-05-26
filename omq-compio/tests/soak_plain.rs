#![cfg(all(feature = "soak", feature = "plain"))]
//! Soak: PLAIN mechanism sustained.
//!
//! PUSH/PULL over TCP with PLAIN username/password authentication.
//! Sends small messages continuously for the full soak duration.
//! Asserts no memory or FD leaks from repeated PLAIN handshakes.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::join;

use omq_compio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

fn accept_alice(peer: &omq_compio::MechanismPeerInfo) -> bool {
    peer.username.as_deref() == Some("alice") && peer.password.as_deref() == Some("secret")
}

#[test]
fn soak_plain() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let sent = Arc::new(AtomicU64::new(0));
    let recvd = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    {
        let rt = compio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            let pull = Socket::new(
                SocketType::Pull,
                Options::default().plain_server(accept_alice),
            );
            let mut mon = pull.monitor();
            pull.bind(soak_common::tcp_ep(0)).await.unwrap();
            let ev = compio::time::timeout(Duration::from_millis(500), mon.recv())
                .await
                .unwrap()
                .unwrap();
            let port = match ev {
                MonitorEvent::Listening {
                    endpoint: Endpoint::Tcp { port, .. },
                } => port,
                other => panic!("expected Tcp Listening, got {other:?}"),
            };

            let push = Socket::new(
                SocketType::Push,
                Options::default()
                    .plain_client("alice", "secret")
                    .linger(Duration::from_secs(5)),
            );
            push.connect(soak_common::tcp_ep(port)).await.unwrap();
            compio::time::sleep(Duration::from_millis(100)).await;

            let send_sent = sent.clone();
            let send_stop = stop.clone();
            let send_fut = async {
                while !send_stop.load(Ordering::Relaxed) {
                    if let Ok(Ok(())) = compio::time::timeout(
                        Duration::from_secs(2),
                        push.send(Message::single("p")),
                    )
                    .await
                    {
                        send_sent.fetch_add(1, Ordering::Relaxed);
                    }
                }
            };

            let recv_recvd = recvd.clone();
            let recv_stop = stop.clone();
            let recv_fut = async {
                while !recv_stop.load(Ordering::Relaxed) {
                    if let Ok(Ok(_)) =
                        compio::time::timeout(Duration::from_secs(2), pull.recv()).await
                    {
                        recv_recvd.fetch_add(1, Ordering::Relaxed);
                    }
                }
            };

            let timer_stop = stop.clone();
            let timer_sent = sent.clone();
            let timer_recvd = recvd.clone();
            let timer_fut = async {
                let start = Instant::now();
                let mut last_log = start;

                while start.elapsed() < duration {
                    compio::time::sleep(Duration::from_secs(1)).await;

                    if last_log.elapsed() >= Duration::from_secs(30) {
                        let s = timer_sent.load(Ordering::Relaxed);
                        let r = timer_recvd.load(Ordering::Relaxed);
                        eprintln!(
                            "[plain] {:.0}s, sent {s}, recvd {r}",
                            start.elapsed().as_secs_f64(),
                        );
                        last_log = Instant::now();
                    }
                }
                timer_stop.store(true, Ordering::Relaxed);
            };

            join!(send_fut, recv_fut, timer_fut);

            let s = sent.load(Ordering::Relaxed);
            let r = recvd.load(Ordering::Relaxed);
            eprintln!(
                "[plain] done: sent {s}, recvd {r} in {:.1}s",
                duration.as_secs_f64(),
            );

            push.close().await.unwrap();
            pull.close().await.unwrap();
        });
    }

    let report = monitor.stop();
    report.assert_no_leak("plain");
}

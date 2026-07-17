#![cfg(all(feature = "soak", feature = "plain"))]

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use omq_tokio::{Message, Socket, SocketType};

#[test]
fn soak_plain_sustained() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let sent = Arc::new(AtomicU64::new(0));
    let recvd = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let pull = Socket::new(
            SocketType::Pull,
            soak_common::soak_options().plain_server(|peer| {
                peer.username.as_deref() == Some("alice")
                    && peer.password.as_deref() == Some("secret")
            }),
        );
        let ep = pull.bind(soak_common::tcp_ep(0)).await.unwrap();

        let push = Socket::new(
            SocketType::Push,
            soak_common::soak_options()
                .plain_client("alice", "secret")
                .linger(Duration::from_secs(5)),
        );
        push.connect(ep).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let send_sent = sent.clone();
        let send_stop = stop.clone();
        let push_clone = push.clone();
        let send_task = tokio::spawn(async move {
            let mut idx: u64 = 0;
            while !send_stop.load(Ordering::Relaxed) {
                if let Ok(Ok(())) = tokio::time::timeout(
                    Duration::from_secs(2),
                    push_clone.send(Message::single(format!("p{idx}"))),
                )
                .await
                {
                    send_sent.fetch_add(1, Ordering::Relaxed);
                }
                idx += 1;
            }
        });

        let recv_recvd = recvd.clone();
        let recv_stop = stop.clone();
        let pull_clone = pull.clone();
        let recv_task = tokio::spawn(async move {
            while !recv_stop.load(Ordering::Relaxed) {
                if let Ok(Ok(_)) =
                    tokio::time::timeout(Duration::from_secs(2), pull_clone.recv()).await
                {
                    recv_recvd.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        let timer_stop = stop.clone();
        let timer_sent = sent.clone();
        let timer_recvd = recvd.clone();
        let start = Instant::now();
        let mut last_log = start;

        while start.elapsed() < duration {
            tokio::time::sleep(Duration::from_secs(1)).await;

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

        let _ = send_task.await;
        let _ = recv_task.await;

        let s = sent.load(Ordering::Relaxed);
        let r = recvd.load(Ordering::Relaxed);
        eprintln!(
            "[plain] done: sent {s}, recvd {r} in {:.1}s",
            duration.as_secs_f64(),
        );

        push.close().await.unwrap();
        pull.close().await.unwrap();
    });

    let report = monitor.stop();
    report.assert_no_leak("plain");
}

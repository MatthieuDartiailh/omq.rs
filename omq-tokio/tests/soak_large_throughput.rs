#![cfg(feature = "soak")]

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use omq_tokio::{Message, Options, Socket, SocketType};

const MSG_SIZE: usize = 1024 * 1024;

#[test]
fn soak_large_message_throughput() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let sent = Arc::new(AtomicU64::new(0));
    let recvd = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let port = soak_common::loopback_port();
        let ep = soak_common::tcp_ep(port);

        let pull = Socket::new(SocketType::Pull, Options::default().recv_hwm(4));
        pull.bind(ep.clone()).await.unwrap();

        let push = Socket::new(SocketType::Push, Options::default().send_hwm(4));
        push.connect(ep).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let payload: Vec<u8> = (0..MSG_SIZE).map(|i| (i & 0xFF) as u8).collect();

        let send_sent = sent.clone();
        let send_stop = stop.clone();
        let push_clone = push.clone();
        let send_task = tokio::spawn(async move {
            while !send_stop.load(Ordering::Relaxed) {
                if let Ok(Ok(())) = tokio::time::timeout(
                    Duration::from_secs(2),
                    push_clone.send(Message::single(payload.clone())),
                )
                .await
                {
                    send_sent.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        let recv_recvd = recvd.clone();
        let recv_stop = stop.clone();
        let pull_clone = pull.clone();
        let recv_task = tokio::spawn(async move {
            while !recv_stop.load(Ordering::Relaxed) {
                if let Ok(Ok(m)) =
                    tokio::time::timeout(Duration::from_secs(2), pull_clone.recv()).await
                {
                    assert_eq!(
                        m.part_bytes(0).unwrap().len(),
                        MSG_SIZE,
                        "payload size mismatch"
                    );
                    recv_recvd.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        let start = Instant::now();
        let mut last_log = start;
        let mut tracker = soak_common::ThroughputTracker::new(Duration::from_secs(10));

        while start.elapsed() < duration {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let s = sent.load(Ordering::Relaxed);
            let r = recvd.load(Ordering::Relaxed);
            tracker.record(r);

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[large_throughput] {:.0}s, sent {s}, recvd {r}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }
        stop.store(true, Ordering::Relaxed);
        tracker.assert_stable("large_throughput");

        let _ = send_task.await;
        let _ = recv_task.await;

        let s = sent.load(Ordering::Relaxed);
        let r = recvd.load(Ordering::Relaxed);
        eprintln!(
            "[large_throughput] done: sent {s}, recvd {r} in {:.1}s ({:.1} MiB/s)",
            duration.as_secs_f64(),
            r as f64 * MSG_SIZE as f64 / duration.as_secs_f64() / 1_048_576.0,
        );

        push.close().await.unwrap();
        pull.close().await.unwrap();
    });

    let report = monitor.stop();
    report.assert_no_leak("large_throughput");
}

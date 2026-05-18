#![cfg(feature = "soak")]

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use omq_tokio::{Message, Options, Socket, SocketType};

#[test]
fn soak_inproc_cross_thread() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let ep = soak_common::inproc_ep("soak-xthread");
    let sent = Arc::new(AtomicU64::new(0));
    let recvd = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let pull = Socket::new(SocketType::Pull, Options::default());
        pull.bind(ep.clone()).await.unwrap();

        let push = Socket::new(SocketType::Push, Options::default());
        push.connect(ep).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let recv_recvd = recvd.clone();
        let recv_stop = stop.clone();
        let pull_clone = pull.clone();
        let recv_task = tokio::spawn(async move {
            while !recv_stop.load(Ordering::Relaxed) {
                if let Ok(Ok(_)) =
                    tokio::time::timeout(Duration::from_millis(100), pull_clone.recv()).await
                {
                    recv_recvd.fetch_add(1, Ordering::Relaxed);
                }
            }
            // Drain remaining.
            while let Ok(Ok(_)) =
                tokio::time::timeout(Duration::from_millis(100), pull_clone.recv()).await
            {
                recv_recvd.fetch_add(1, Ordering::Relaxed);
            }
        });

        let push_sent = sent.clone();
        let push_recvd = recvd.clone();
        let start = Instant::now();
        let mut last_log = start;

        while start.elapsed() < duration {
            if let Ok(Ok(())) =
                tokio::time::timeout(Duration::from_millis(100), push.send(Message::single("x")))
                    .await
            {
                push_sent.fetch_add(1, Ordering::Relaxed);
            }

            if last_log.elapsed() >= Duration::from_secs(30) {
                let s = push_sent.load(Ordering::Relaxed);
                let r = push_recvd.load(Ordering::Relaxed);
                eprintln!(
                    "[inproc_xthread] {:.0}s, sent {s}, recvd {r}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        push.close().await.unwrap();

        // Give recv task time to drain, then stop.
        tokio::time::sleep(Duration::from_secs(1)).await;
        stop.store(true, Ordering::Relaxed);
        let _ = recv_task.await;
    });

    let s = sent.load(Ordering::Relaxed);
    let r = recvd.load(Ordering::Relaxed);
    eprintln!(
        "[inproc_xthread] done: sent {s}, recvd {r} in {:.1}s",
        duration.as_secs_f64(),
    );

    assert!(r > 0, "no messages received");
    let loss_pct = if s > 0 {
        (s - r) as f64 / s as f64 * 100.0
    } else {
        0.0
    };
    assert!(
        loss_pct < 1.0,
        "message loss too high: {loss_pct:.2}% ({s} sent, {r} received)"
    );

    let report = monitor.stop();
    report.assert_no_leak("inproc_xthread");
}

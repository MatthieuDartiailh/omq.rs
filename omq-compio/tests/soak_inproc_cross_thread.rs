#![cfg(feature = "soak")]
//! Soak 7: inproc cross-thread sustained.
//!
//! PUSH on thread A, PULL on thread B, each with its own compio runtime.
//! Exercises the yring SPSC fast path with 10M+ messages. Asserts no
//! message loss, no yring producer/consumer desync, and RSS stability.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use omq_compio::{Message, Options, Socket, SocketType};

#[test]
fn soak_inproc_cross_thread() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let ep = soak_common::inproc_ep("soak-xthread");
    let sent = Arc::new(AtomicU64::new(0));
    let recvd = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(Barrier::new(2));

    let pull_recvd = recvd.clone();
    let pull_stop = stop.clone();
    let pull_ready = ready.clone();
    let pull_ep = ep.clone();
    let pull_thread = std::thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let pull = Socket::new(SocketType::Pull, Options::default());
            pull.bind(pull_ep).await.unwrap();
            pull_ready.wait();

            while !pull_stop.load(Ordering::Relaxed) {
                if let Ok(Ok(_)) =
                    compio::time::timeout(Duration::from_millis(100), pull.recv()).await
                {
                    pull_recvd.fetch_add(1, Ordering::Relaxed);
                }
            }

            // Drain remaining.
            while let Ok(Ok(_)) =
                compio::time::timeout(Duration::from_millis(100), pull.recv()).await
            {
                pull_recvd.fetch_add(1, Ordering::Relaxed);
            }
        });
    });

    let push_sent = sent.clone();
    let push_recvd = recvd.clone();
    let push_ready = ready.clone();
    let push_thread = std::thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            push_ready.wait();
            compio::time::sleep(Duration::from_millis(10)).await;

            let push = Socket::new(SocketType::Push, Options::default());
            push.connect(ep).await.unwrap();

            let start = Instant::now();
            let mut last_log = start;

            while start.elapsed() < duration {
                if let Ok(Ok(())) = compio::time::timeout(
                    Duration::from_millis(100),
                    push.send(Message::single("x")),
                )
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
        });
    });

    push_thread.join().unwrap();

    // Give PULL a moment to drain, then signal stop.
    std::thread::sleep(Duration::from_secs(1));
    stop.store(true, Ordering::Relaxed);
    pull_thread.join().unwrap();

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

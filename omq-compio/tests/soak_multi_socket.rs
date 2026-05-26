#![cfg(feature = "soak")]
//! Soak 6: multi-socket per process.
//!
//! 50+ sockets in one process, mix of PUSH/PULL (TCP), PUB/SUB (inproc),
//! and REQ/REP (TCP). Each pair exchanges messages at moderate rate.
//! Asserts RSS scales with socket count (not time), no cross-socket
//! interference, and `close()` frees all resources.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use omq_compio::{Message, Options, Socket, SocketType};

struct SocketPair {
    sender: Socket,
    receiver: Socket,
    kind: &'static str,
}

async fn create_pairs() -> Vec<SocketPair> {
    let mut pairs = Vec::new();

    for i in 0..20 {
        let pull = Socket::new(SocketType::Pull, Options::default().recv_hwm(16));
        let ep = pull.bind(soak_common::tcp_ep(0)).await.unwrap();
        let push = Socket::new(SocketType::Push, Options::default().send_hwm(16));
        push.connect(ep).await.unwrap();
        pairs.push(SocketPair {
            sender: push,
            receiver: pull,
            kind: "push/pull-tcp",
        });
        if i == 0 {
            compio::time::sleep(Duration::from_millis(30)).await;
        }
    }

    for i in 0..20 {
        let ep = soak_common::inproc_ep(&format!("soak-multi-ps-{i}"));
        let pub_ = Socket::new(SocketType::Pub, Options::default());
        pub_.bind(ep.clone()).await.unwrap();
        let sub = Socket::new(SocketType::Sub, Options::default().recv_hwm(16));
        sub.connect(ep).await.unwrap();
        sub.subscribe("").await.unwrap();
        pairs.push(SocketPair {
            sender: pub_,
            receiver: sub,
            kind: "pub/sub-inproc",
        });
    }

    for _ in 0..10 {
        let rep = Socket::new(SocketType::Rep, Options::default());
        let ep = rep.bind(soak_common::tcp_ep(0)).await.unwrap();
        let req = Socket::new(SocketType::Req, Options::default());
        req.connect(ep).await.unwrap();
        pairs.push(SocketPair {
            sender: req,
            receiver: rep,
            kind: "req/rep-tcp",
        });
    }

    pairs
}

#[test]
fn soak_multi_socket() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();
    let mut tracker = soak_common::ThroughputTracker::new(Duration::from_secs(10));

    {
        let rt = compio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            let pairs = create_pairs().await;
            eprintln!("[multi_socket] created {} socket pairs", pairs.len());
            compio::time::sleep(Duration::from_millis(100)).await;

            let start = Instant::now();
            let mut total_exchanged: u64 = 0;
            let mut last_log = start;

            while start.elapsed() < duration {
                // Multiple messages per pair per iteration to increase throughput.
                for _ in 0..10 {
                    for pair in &pairs {
                        let sent = compio::time::timeout(
                            Duration::from_millis(1),
                            pair.sender.send(Message::single("multi")),
                        )
                        .await;

                        if sent.is_err() || sent.unwrap().is_err() {
                            continue;
                        }

                        if pair.kind != "req/rep-tcp" {
                            continue;
                        }

                        if let Ok(Ok(_)) =
                            compio::time::timeout(Duration::from_millis(5), pair.receiver.recv())
                                .await
                        {
                            total_exchanged += 1;
                            let _ = compio::time::timeout(
                                Duration::from_millis(5),
                                pair.receiver.send(Message::single("reply")),
                            )
                            .await;
                            let _ =
                                compio::time::timeout(Duration::from_millis(5), pair.sender.recv())
                                    .await;
                        }
                    }
                }

                // Drain non-REQ/REP receivers without blocking.
                for pair in &pairs {
                    if pair.kind != "req/rep-tcp" {
                        while pair.receiver.try_recv().is_ok() {
                            total_exchanged += 1;
                        }
                    }
                }

                tracker.record(total_exchanged);

                if last_log.elapsed() >= Duration::from_secs(30) {
                    eprintln!(
                        "[multi_socket] {:.0}s, exchanged {total_exchanged}",
                        start.elapsed().as_secs_f64(),
                    );
                    last_log = Instant::now();
                }
            }

            for pair in pairs {
                pair.sender.close().await.unwrap();
                pair.receiver.close().await.unwrap();
            }

            eprintln!(
                "[multi_socket] done: {total_exchanged} messages across 50 pairs in {:.1}s",
                start.elapsed().as_secs_f64(),
            );
        });
    }

    let report = monitor.stop();
    report.assert_no_leak("multi_socket");
    tracker.assert_stable("multi_socket");
}

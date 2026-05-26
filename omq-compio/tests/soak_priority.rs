#![cfg(all(feature = "soak", feature = "priority"))]
//! Soak: priority tiers sustained.
//!
//! PUSH connected to 3 inproc PULLs at priorities 1, 3, 5. Sends
//! continuously for the full soak duration while draining all PULLs.
//! Asserts no memory or FD leaks from priority routing.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::num::NonZeroU8;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use omq_compio::{ConnectOpts, Endpoint, Message, Options, Socket, SocketType};

#[test]
fn soak_priority() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let delivered = Arc::new(AtomicU64::new(0));
    let sent = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    {
        let rt = compio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            let ep1 = Endpoint::Inproc {
                name: "soak-prio-1".into(),
            };
            let ep3 = Endpoint::Inproc {
                name: "soak-prio-3".into(),
            };
            let ep5 = Endpoint::Inproc {
                name: "soak-prio-5".into(),
            };

            let pull1 = Socket::new(SocketType::Pull, Options::default());
            let pull3 = Socket::new(SocketType::Pull, Options::default());
            let pull5 = Socket::new(SocketType::Pull, Options::default());
            pull1.bind(ep1.clone()).await.unwrap();
            pull3.bind(ep3.clone()).await.unwrap();
            pull5.bind(ep5.clone()).await.unwrap();

            let push = Socket::new(SocketType::Push, Options::default());
            push.connect_with(
                ep1,
                ConnectOpts {
                    priority: NonZeroU8::new(1).unwrap(),
                },
            )
            .await
            .unwrap();
            push.connect_with(
                ep3,
                ConnectOpts {
                    priority: NonZeroU8::new(3).unwrap(),
                },
            )
            .await
            .unwrap();
            push.connect_with(
                ep5,
                ConnectOpts {
                    priority: NonZeroU8::new(5).unwrap(),
                },
            )
            .await
            .unwrap();

            let start = Instant::now();
            let mut last_log = start;

            while start.elapsed() < duration {
                // Send a batch.
                for _ in 0..100 {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    if let Ok(Ok(())) = compio::time::timeout(
                        Duration::from_millis(50),
                        push.send(Message::single("x")),
                    )
                    .await
                    {
                        sent.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Drain all PULLs.
                for pull in [&pull1, &pull3, &pull5] {
                    while pull.try_recv().is_ok() {
                        delivered.fetch_add(1, Ordering::Relaxed);
                    }
                }

                if last_log.elapsed() >= Duration::from_secs(30) {
                    let s = sent.load(Ordering::Relaxed);
                    let d = delivered.load(Ordering::Relaxed);
                    eprintln!(
                        "[priority] {:.0}s, sent {s}, delivered {d}",
                        start.elapsed().as_secs_f64(),
                    );
                    last_log = Instant::now();
                }
            }

            // Final drain.
            for pull in [&pull1, &pull3, &pull5] {
                while pull.try_recv().is_ok() {
                    delivered.fetch_add(1, Ordering::Relaxed);
                }
            }

            let s = sent.load(Ordering::Relaxed);
            let d = delivered.load(Ordering::Relaxed);
            eprintln!(
                "[priority] done: sent {s}, delivered {d} in {:.1}s",
                duration.as_secs_f64(),
            );

            push.close().await.unwrap();
            pull1.close().await.unwrap();
            pull3.close().await.unwrap();
            pull5.close().await.unwrap();
        });
    }

    let report = monitor.stop();
    report.assert_no_leak("priority");
}

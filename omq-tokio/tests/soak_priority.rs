#![cfg(all(feature = "soak", feature = "priority"))]

mod soak_common;

use std::num::NonZeroU8;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use omq_tokio::{ConnectOpts, Message, MonitorEvent, Options, Socket, SocketType};

#[test]
#[allow(clippy::too_many_lines)]
fn soak_priority_delivery() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let delivered = Arc::new(AtomicU64::new(0));

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let pull_1 = Socket::new(SocketType::Pull, Options::default());
        let pull_3 = Socket::new(SocketType::Pull, Options::default());
        let pull_5 = Socket::new(SocketType::Pull, Options::default());
        pull_1
            .bind(soak_common::inproc_ep("soak-prio-1"))
            .await
            .unwrap();
        pull_3
            .bind(soak_common::inproc_ep("soak-prio-3"))
            .await
            .unwrap();
        pull_5
            .bind(soak_common::inproc_ep("soak-prio-5"))
            .await
            .unwrap();

        let push = Socket::new(SocketType::Push, Options::default());
        let mut mon = push.monitor();
        push.connect_with(
            soak_common::inproc_ep("soak-prio-1"),
            ConnectOpts {
                priority: NonZeroU8::new(1).unwrap(),
            },
        )
        .await
        .unwrap();
        push.connect_with(
            soak_common::inproc_ep("soak-prio-3"),
            ConnectOpts {
                priority: NonZeroU8::new(3).unwrap(),
            },
        )
        .await
        .unwrap();
        push.connect_with(
            soak_common::inproc_ep("soak-prio-5"),
            ConnectOpts {
                priority: NonZeroU8::new(5).unwrap(),
            },
        )
        .await
        .unwrap();

        // Wait for all 3 handshakes before sending.
        let mut handshakes = 0;
        while handshakes < 3 {
            let ev = tokio::time::timeout(Duration::from_secs(2), mon.recv())
                .await
                .expect("monitor timeout waiting for handshake")
                .expect("monitor closed");
            if matches!(ev, MonitorEvent::HandshakeSucceeded { .. }) {
                handshakes += 1;
            }
        }

        let start = Instant::now();
        let mut last_log = start;
        let mut sent: u64 = 0;

        while start.elapsed() < duration {
            // Send a batch.
            for _ in 0..100 {
                if let Ok(Ok(())) = tokio::time::timeout(
                    Duration::from_millis(100),
                    push.send(Message::single("x")),
                )
                .await
                {
                    sent += 1;
                }
            }

            // Drain all PULLs.
            for pull in [&pull_1, &pull_3, &pull_5] {
                while let Ok(Ok(_)) =
                    tokio::time::timeout(Duration::from_millis(10), pull.recv()).await
                {
                    delivered.fetch_add(1, Ordering::Relaxed);
                }
            }

            if last_log.elapsed() >= Duration::from_secs(30) {
                let d = delivered.load(Ordering::Relaxed);
                eprintln!(
                    "[priority] {:.0}s, sent {sent}, delivered {d}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        // Final drain.
        for pull in [&pull_1, &pull_3, &pull_5] {
            while let Ok(Ok(_)) =
                tokio::time::timeout(Duration::from_millis(100), pull.recv()).await
            {
                delivered.fetch_add(1, Ordering::Relaxed);
            }
        }

        let d = delivered.load(Ordering::Relaxed);
        eprintln!(
            "[priority] done: sent {sent}, delivered {d} in {:.1}s",
            duration.as_secs_f64(),
        );

        push.close().await.unwrap();
        pull_1.close().await.unwrap();
        pull_3.close().await.unwrap();
        pull_5.close().await.unwrap();
    });

    let report = monitor.stop();
    report.assert_no_leak("priority");
}

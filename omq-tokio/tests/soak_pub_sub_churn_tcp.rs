#![cfg(feature = "soak")]
//! Soak: PUB/SUB churn over TCP.
//!
//! Same pattern as `soak_pub_sub_churn` (inproc) but over TCP,
//! exercising the full ZMTP codec, wire-slot encoding, per-subscriber
//! HWM, and subscription filter propagation over the network. SUB peers
//! join and leave with different prefix subscriptions while PUB sends
//! continuously.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::RngExt;
use rand::rngs::StdRng;

use omq_tokio::{Message, Options, ReconnectPolicy, Socket, SocketType};

const TOPICS: &[&str] = &["fast.", "slow.", "all.", "rare."];

fn no_reconnect() -> Options {
    Options {
        reconnect: ReconnectPolicy::Disabled,
        ..soak_common::soak_options()
    }
}

#[test]
fn soak_pub_sub_churn_tcp() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let publisher = Socket::new(SocketType::Pub, soak_common::soak_options());
        let ep = publisher.bind(soak_common::tcp_ep(0)).await.unwrap();

        let mut rng = rand::make_rng::<StdRng>();
        let mut subs: Vec<Socket> = Vec::new();
        let mut pub_count: u64 = 0;
        let start = Instant::now();
        let mut last_churn = start;
        let mut last_log = start;

        while start.elapsed() < duration {
            for _ in 0..1_000 {
                let topic = TOPICS[pub_count as usize % TOPICS.len()];
                let msg = format!("{topic}{pub_count}");
                if let Ok(Ok(())) = tokio::time::timeout(
                    Duration::from_millis(1),
                    publisher.send(Message::single(msg)),
                )
                .await
                {
                    pub_count += 1;
                }
            }

            for sub in &subs {
                while sub.try_recv().is_ok() {}
            }

            if last_churn.elapsed() >= Duration::from_millis(500) {
                last_churn = Instant::now();

                if !subs.is_empty() && rng.random_bool(0.5) {
                    let idx = rng.random_range(0..subs.len());
                    let sub = subs.swap_remove(idx);
                    sub.close().await.unwrap();
                }

                if subs.len() < 10 {
                    let sub = Socket::new(SocketType::Sub, no_reconnect().recv_hwm(32));
                    sub.connect(ep.clone()).await.unwrap();
                    let prefix = TOPICS[rng.random_range(0..TOPICS.len())];
                    sub.subscribe(Bytes::from(prefix.to_string()))
                        .await
                        .unwrap();
                    subs.push(sub);
                }
            }

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[pub_sub_churn_tcp] {:.0}s, pub_count {pub_count}, subs {}",
                    start.elapsed().as_secs_f64(),
                    subs.len(),
                );
                last_log = Instant::now();
            }
        }

        for sub in subs {
            sub.close().await.unwrap();
        }
        publisher.close().await.unwrap();

        eprintln!(
            "[pub_sub_churn_tcp] done: {pub_count} published in {:.1}s",
            start.elapsed().as_secs_f64(),
        );
    });

    let report = monitor.stop();
    report.assert_no_leak("pub_sub_churn_tcp");
}

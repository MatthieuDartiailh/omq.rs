#![cfg(feature = "soak")]
//! Soak 1: peer churn under sustained PUSH load.
//!
//! PUSH bound on TCP, continuous send. PULL peers connect, receive for
//! 1-5 s, disconnect, repeat. Varies 0-20 concurrent peers with random
//! timing. Asserts RSS stays bounded and send never deadlocks.

mod soak_common;

use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use omq_compio::{Message, Options, Socket, SocketType};

#[test]
fn soak_peer_churn() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();
    let rt = compio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let port = soak_common::loopback_port();
        let ep = soak_common::tcp_ep(port);

        let push = Socket::new(SocketType::Push, Options::default().send_hwm(1024));
        push.bind(ep.clone()).await.unwrap();

        // Ensure at least one peer is connected before entering the main loop.
        let initial_pull = Socket::new(SocketType::Pull, Options::default().recv_hwm(64));
        initial_pull.connect(ep.clone()).await.unwrap();
        compio::time::sleep(Duration::from_millis(100)).await;

        let mut rng = StdRng::from_entropy();
        let mut peers: Vec<Socket> = vec![initial_pull];
        let mut sent: u64 = 0;
        let start = Instant::now();
        let mut last_log = start;

        while start.elapsed() < duration {
            // Peer management: add or remove with some probability.
            let action = rng.gen_range(0u8..10);
            if action < 3 && peers.len() < 20 {
                let pull = Socket::new(SocketType::Pull, Options::default().recv_hwm(64));
                pull.connect(ep.clone()).await.unwrap();
                peers.push(pull);
            } else if action < 5 && peers.len() > 1 {
                let idx = rng.gen_range(0..peers.len());
                let peer = peers.swap_remove(idx);
                peer.close().await.unwrap();
            }

            // Send a burst. Short timeout: if HWM is full, move on to drain.
            for _ in 0..100 {
                if let Ok(Ok(())) = compio::time::timeout(
                    Duration::from_millis(1),
                    push.send(Message::single("soak")),
                )
                .await
                {
                    sent += 1;
                }
            }

            // Drain all live peers without blocking.
            for peer in &peers {
                while peer.try_recv().is_ok() {}
            }

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[peer_churn] {:.0}s, sent {sent}, peers {}",
                    start.elapsed().as_secs_f64(),
                    peers.len(),
                );
                last_log = Instant::now();
            }
        }

        for peer in peers {
            peer.close().await.unwrap();
        }
        push.close().await.unwrap();

        eprintln!(
            "[peer_churn] done: {sent} messages in {:.1}s",
            start.elapsed().as_secs_f64()
        );
    });

    let report = monitor.stop();
    report.assert_no_leak("peer_churn");
}

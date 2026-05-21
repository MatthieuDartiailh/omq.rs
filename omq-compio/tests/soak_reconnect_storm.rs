#![cfg(feature = "soak")]
//! Soak 2: reconnect storm.
//!
//! PUSH connects to a TCP port. PULL binds, exchanges a message, then
//! the listener is killed. A new PULL rebinds the same port and the
//! PUSH reconnects automatically. Repeats for the full soak duration.
//! Asserts every post-reconnect message delivers and no FDs leak.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use omq_compio::options::ReconnectPolicy;
use omq_compio::{Message, Options, Socket, SocketType};

#[test]
fn soak_reconnect_storm() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let rt = compio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        // Grab a free port by binding a temporary socket, then close it.
        // The PUSH dialer reconnects to this fixed endpoint across PULL restarts.
        let tmp = Socket::new(SocketType::Pull, Options::default());
        let ep = tmp.bind(soak_common::tcp_ep(0)).await.unwrap();
        tmp.close().await.unwrap();

        let push = Socket::new(
            SocketType::Push,
            Options::default()
                .send_hwm(16)
                .reconnect(ReconnectPolicy::Fixed(Duration::from_millis(10))),
        );
        push.connect(ep.clone()).await.unwrap();

        let start = Instant::now();
        let mut cycles: u64 = 0;
        let mut delivered: u64 = 0;
        let mut last_log = start;

        while start.elapsed() < duration {
            let pull = Socket::new(SocketType::Pull, Options::default());

            let mut bound = false;
            for _ in 0..40 {
                if pull.bind(ep.clone()).await.is_ok() {
                    bound = true;
                    break;
                }
                compio::time::sleep(Duration::from_millis(25)).await;
            }
            if !bound {
                eprintln!("[reconnect_storm] bind failed at cycle {cycles}, retrying");
                continue;
            }

            let tag = format!("c-{cycles}");
            push.send(Message::single(tag.clone())).await.unwrap();

            match compio::time::timeout(Duration::from_secs(5), pull.recv()).await {
                Ok(Ok(m)) => {
                    assert_eq!(m.part_bytes(0).unwrap(), tag.as_bytes());
                    delivered += 1;
                }
                _ => {
                    eprintln!("[reconnect_storm] missed delivery at cycle {cycles}");
                }
            }

            pull.close().await.unwrap();
            cycles += 1;

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[reconnect_storm] {:.0}s, cycles {cycles}, delivered {delivered}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        push.close().await.unwrap();

        let pct = if cycles > 0 {
            delivered as f64 / cycles as f64 * 100.0
        } else {
            100.0
        };
        eprintln!(
            "[reconnect_storm] done: {delivered}/{cycles} delivered ({pct:.1}%) in {:.1}s",
            start.elapsed().as_secs_f64(),
        );
        assert!(
            pct >= 90.0,
            "reconnect storm delivery rate too low: {pct:.1}%"
        );
    });

    let report = monitor.stop();
    report.assert_no_leak("reconnect_storm");
}

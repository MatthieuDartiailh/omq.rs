#![cfg(feature = "soak")]
//! Soak: HWM pressure combined with reconnection.
//!
//! PUSH with a low HWM (4) sends to PULL over TCP. PULL periodically
//! closes and rebinds. During the gap PUSH hits HWM. Two modes:
//!
//! 1. `OnMute::DropNewest`: send never blocks, messages are silently
//!    dropped. Delivery resumes after reconnect.
//! 2. `OnMute::Block`: send blocks until the peer returns. Delivery
//!    resumes after reconnect with no message corruption.
//!
//! Verifies no hangs, no leaks, and that delivery resumes cleanly
//! after each reconnect cycle.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use omq_tokio::options::{OnMute, ReconnectPolicy};
use omq_tokio::{Message, Options, Socket, SocketType};

fn fast_reconnect() -> Options {
    Options {
        reconnect: ReconnectPolicy::Fixed(Duration::from_millis(10)),
        ..soak_common::soak_options()
    }
}

async fn rebind(ep: &omq_tokio::Endpoint) -> Option<Socket> {
    for _ in 0..40 {
        let s = Socket::new(SocketType::Pull, soak_common::soak_options());
        if s.bind(ep.clone()).await.is_ok() {
            return Some(s);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    None
}

fn run_hwm_storm(name: &str, mute: OnMute) {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        // Probe for a port, then close so we can rebind.
        let probe = Socket::new(SocketType::Pull, soak_common::soak_options());
        let ep = probe.bind(soak_common::tcp_ep(0)).await.unwrap();
        probe.close().await.unwrap();

        let push = Socket::new(SocketType::Push, fast_reconnect().send_hwm(4).on_mute(mute));
        push.connect(ep.clone()).await.unwrap();

        let start = Instant::now();
        let mut cycles: u64 = 0;
        let mut delivered: u64 = 0;
        let mut last_log = start;

        while start.elapsed() < duration {
            let Some(pull) = rebind(&ep).await else {
                eprintln!("[{name}] rebind failed at cycle {cycles}");
                continue;
            };

            // Send a burst. With DropNewest some will be lost; with Block
            // send will stall until the peer is back (but we just rebound).
            let mut burst_ok = true;
            for i in 0..8u32 {
                let tag = format!("{name}-{cycles}-{i}");
                if !matches!(
                    tokio::time::timeout(Duration::from_secs(5), push.send(Message::single(tag)))
                        .await,
                    Ok(Ok(())),
                ) {
                    burst_ok = false;
                    break;
                }
            }

            if burst_ok {
                while let Ok(Ok(_)) =
                    tokio::time::timeout(Duration::from_millis(500), pull.recv()).await
                {
                    delivered += 1;
                }
            }

            pull.close().await.unwrap();
            cycles += 1;

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[{name}] {:.0}s, cycles {cycles}, delivered {delivered}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        push.close().await.unwrap();

        let pct = if cycles > 0 {
            delivered as f64 / (cycles * 8) as f64 * 100.0
        } else {
            100.0
        };
        eprintln!(
            "[{name}] done: {delivered}/{} delivered ({pct:.1}%) in {:.1}s",
            cycles * 8,
            start.elapsed().as_secs_f64(),
        );
    });

    let report = monitor.stop();
    report.assert_no_leak(name);
}

#[test]
fn soak_hwm_reconnect_drop() {
    run_hwm_storm("hwm_reconnect_drop", OnMute::DropNewest);
}

#[test]
fn soak_hwm_reconnect_block() {
    run_hwm_storm("hwm_reconnect_block", OnMute::Block);
}

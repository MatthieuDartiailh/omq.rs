#![cfg(feature = "soak")]
//! Soak: sustained IPC PUB/SUB fan-out with recv timeouts.
//!
//! Under sustained load, multishot recv buffer pools can exhaust (ENOBUFS)
//! and if recv timeouts cancel operations mid-flight, connections could
//! spuriously break. This exercises the fixed code paths:
//! - ENOBUFS in `pull_and_feed` / `accumulate_large_recv` → one-shot fallback
//! - `flush_codec_output` cancel-safety (`encoded_queue`, not direct write)
//! - `flush_encoded_queue` written==0 data preservation
//! - multishot stream `None` during accumulation → one-shot fallback

mod soak_common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use bytes::Bytes;
use omq_compio::{Message, OnMute, Options, Socket, SocketType, build_default_runtime};

fn block_on_and_drain<F: std::future::Future>(rt: &compio::runtime::Runtime, fut: F) -> F::Output {
    let out = rt.block_on(fut);
    rt.enter(|| while rt.run() {});
    out
}

const PEERS: usize = 3;
const MSG_SIZE: usize = 131_072;

#[test]
#[expect(clippy::too_many_lines)]
fn soak_ipc_fanout_no_message_loss() {
    let duration = soak_common::soak_duration();

    let ep: omq_compio::Endpoint =
        omq_compio::Endpoint::Ipc(omq_compio::endpoint::IpcPath::Abstract(format!(
            "omq-soak-fanout-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        )));

    let recv_count = Arc::new(AtomicUsize::new(0));
    let sent_count = Arc::new(AtomicUsize::new(0));
    let counting = Arc::new(AtomicBool::new(false));
    let sending_done = Arc::new(AtomicBool::new(false));
    let subs_ready: Arc<Vec<AtomicBool>> =
        Arc::new((0..PEERS).map(|_| AtomicBool::new(false)).collect());
    let bind_barrier = Arc::new(Barrier::new(PEERS + 1));
    let (drain_tx, drain_rx) = flume::bounded::<()>(PEERS);

    let sub_threads: Vec<_> = (0..PEERS)
        .map(|i| {
            let ep = ep.clone();
            let recv_count = recv_count.clone();
            let sent_count = sent_count.clone();
            let counting = counting.clone();
            let sending_done = sending_done.clone();
            let drain_tx = drain_tx.clone();
            let subs_ready = subs_ready.clone();
            let bind_barrier = bind_barrier.clone();
            std::thread::spawn(move || {
                let rt = build_default_runtime().expect("sub runtime");
                block_on_and_drain(&rt, async move {
                    bind_barrier.wait();
                    let s = Socket::new(SocketType::Sub, Options::default());
                    s.connect(ep).await.expect("connect SUB");
                    s.subscribe(Bytes::new()).await.expect("subscribe");

                    loop {
                        match compio::time::timeout(Duration::from_millis(20), s.recv()).await {
                            Ok(Ok(_)) => {
                                subs_ready[i].store(true, Ordering::Relaxed);
                                if counting.load(Ordering::Acquire) {
                                    recv_count.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            _ => {
                                if sending_done.load(Ordering::Acquire) {
                                    break;
                                }
                            }
                        }
                    }

                    // Drain messages still in the pipeline.
                    let expected = sent_count.load(Ordering::Acquire) * PEERS;
                    let deadline = std::time::Instant::now() + Duration::from_secs(5);
                    while std::time::Instant::now() < deadline {
                        match compio::time::timeout(Duration::from_secs(1), s.recv()).await {
                            Ok(Ok(_)) => {
                                recv_count.fetch_add(1, Ordering::Relaxed);
                            }
                            _ => {
                                if recv_count.load(Ordering::Relaxed) >= expected {
                                    break;
                                }
                            }
                        }
                    }

                    let _ = drain_tx.send(());
                });
            })
        })
        .collect();
    drop(drain_tx);

    let pub_thread = {
        let recv_count = recv_count.clone();
        let sent_count = sent_count.clone();
        let counting = counting.clone();
        let sending_done = sending_done.clone();
        let subs_ready = subs_ready.clone();
        std::thread::spawn(move || {
            let rt = build_default_runtime().expect("pub runtime");
            block_on_and_drain(&rt, async move {
                let pub_ = Socket::new(SocketType::Pub, Options::default().on_mute(OnMute::Block));
                pub_.bind(ep).await.expect("bind PUB");
                bind_barrier.wait();

                let payload = Bytes::from(vec![0xABu8; MSG_SIZE]);

                // Warmup: send until all SUBs have received at least one
                // message, confirming subscriptions are registered.
                loop {
                    let _ = pub_.send(Message::single(payload.clone())).await;
                    if subs_ready.iter().all(|r| r.load(Ordering::Relaxed)) {
                        break;
                    }
                    compio::time::sleep(Duration::from_micros(50)).await;
                }

                // Let warmup messages drain before counting begins.
                compio::time::sleep(Duration::from_millis(100)).await;
                counting.store(true, Ordering::Release);

                let start = std::time::Instant::now();
                let mut sent: usize = 0;
                let mut last_log = start;
                while start.elapsed() < duration {
                    pub_.send(Message::single(payload.clone())).await.unwrap();
                    sent += 1;
                    if last_log.elapsed() >= Duration::from_secs(30) {
                        let r = recv_count.load(Ordering::Relaxed);
                        eprintln!(
                            "[ipc_fanout] {:.0}s, sent {sent}, recvd {r}",
                            start.elapsed().as_secs_f64(),
                        );
                        last_log = std::time::Instant::now();
                    }
                }

                sent_count.store(sent, Ordering::Release);
                sending_done.store(true, Ordering::Release);

                // Wait for all SUBs to finish draining. This is async
                // so the PUB's connection drivers keep flushing.
                for _ in 0..PEERS {
                    let _ = drain_rx.recv_async().await;
                }

                pub_.close().await.unwrap();
            });
        })
    };

    pub_thread.join().expect("pub thread panicked");
    for t in sub_threads {
        t.join().expect("sub thread panicked");
    }

    let s = sent_count.load(Ordering::Relaxed);
    let r = recv_count.load(Ordering::Relaxed);
    let expected = s * PEERS;
    eprintln!("[ipc_fanout] done: sent {s}, recvd {r}, expected {expected}");
    assert_eq!(r, expected, "message loss detected");
}

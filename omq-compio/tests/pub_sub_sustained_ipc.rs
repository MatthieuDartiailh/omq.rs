//! Regression test for sustained IPC PUB/SUB fan-out with recv timeouts.
//!
//! Under sustained load, multishot recv buffer pools can exhaust (ENOBUFS)
//! and if recv timeouts cancel operations mid-flight, connections could
//! spuriously break. This test exercises the fixed code paths:
//! - ENOBUFS in `pull_and_feed` → fallback to one-shot (not `signal_eof`)
//! - ENOBUFS in `accumulate_large_recv` → fallback to one-shot
//! - `flush_codec_output` cancel-safety (`encoded_queue`, not direct write)
//! - `flush_encoded_queue` written==0 data preservation
//! - multishot stream `None` during accumulation → fallback to one-shot

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use bytes::Bytes;
use omq_compio::{Message, OnMute, Options, Socket, SocketType, build_default_runtime};

fn block_on_and_drain<F: std::future::Future>(rt: &compio::runtime::Runtime, fut: F) -> F::Output {
    let out = rt.block_on(fut);
    while rt.run() {}
    out
}

/// Sustained IPC PUB → 3 SUBs fan-out with aggressive recv timeouts.
/// The SUBs use a 20ms recv timeout which triggers frequent io_uring
/// read cancellations. Under load this provokes ENOBUFS on the multishot
/// recv path. Without the fixes, the connection breaks and messages are
/// lost, causing this test to hang.
#[test]
fn sustained_ipc_fanout_no_message_loss() {
    const PEERS: usize = 3;
    const MSG_SIZE: usize = 131_072;
    const TOTAL_MESSAGES: usize = 50;

    let ep: omq_compio::Endpoint =
        omq_compio::Endpoint::Ipc(omq_compio::endpoint::IpcPath::Abstract(format!(
            "omq-test-sustained-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        )));

    let recv_count = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let subs_ready: Arc<Vec<AtomicBool>> =
        Arc::new((0..PEERS).map(|_| AtomicBool::new(false)).collect());
    let bind_barrier = Arc::new(Barrier::new(PEERS + 1));

    let sub_threads: Vec<_> = (0..PEERS)
        .map(|i| {
            let ep = ep.clone();
            let recv_count = recv_count.clone();
            let subs_ready = subs_ready.clone();
            let stop = stop.clone();
            let bind_barrier = bind_barrier.clone();
            std::thread::spawn(move || {
                let rt = build_default_runtime().expect("sub runtime");
                block_on_and_drain(&rt, async move {
                    bind_barrier.wait();
                    let s = Socket::new(SocketType::Sub, Options::default());
                    s.connect(ep).await.expect("connect SUB");
                    s.subscribe(Bytes::new()).await.expect("subscribe");
                    while !stop.load(Ordering::Relaxed) {
                        if let Ok(Ok(_)) =
                            compio::time::timeout(Duration::from_millis(20), s.recv()).await
                        {
                            subs_ready[i].store(true, Ordering::Relaxed);
                            recv_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            })
        })
        .collect();

    let pub_thread = {
        let recv_count = recv_count.clone();
        let subs_ready = subs_ready.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let rt = build_default_runtime().expect("pub runtime");
            block_on_and_drain(&rt, async move {
                let pub_ = Socket::new(SocketType::Pub, Options::default().on_mute(OnMute::Block));
                pub_.bind(ep).await.expect("bind PUB");
                bind_barrier.wait();

                let payload = Bytes::from(vec![0xABu8; MSG_SIZE]);

                // Wait for all subs to connect and subscribe.
                loop {
                    let _ = pub_.send(Message::single(payload.clone())).await;
                    if subs_ready.iter().all(|r| r.load(Ordering::Relaxed)) {
                        break;
                    }
                    compio::time::sleep(Duration::from_micros(50)).await;
                }

                // Send TOTAL_MESSAGES and verify all are received.
                let before = recv_count.load(Ordering::Relaxed);
                let target = before + TOTAL_MESSAGES * PEERS;
                for _ in 0..TOTAL_MESSAGES {
                    pub_.send(Message::single(payload.clone())).await.unwrap();
                }

                // Wait for all receives with a generous timeout.
                let deadline = std::time::Instant::now() + Duration::from_secs(30);
                while recv_count.load(Ordering::Relaxed) < target {
                    if std::time::Instant::now() > deadline {
                        let got = recv_count.load(Ordering::Relaxed);
                        stop.store(true, Ordering::Relaxed);
                        panic!(
                            "message loss: expected {target} receives, got {got} \
                             (deficit {})",
                            target - got
                        );
                    }
                    compio::time::sleep(Duration::from_micros(100)).await;
                }
                stop.store(true, Ordering::Relaxed);
            });
        })
    };

    pub_thread.join().expect("pub thread panicked");
    for t in sub_threads {
        t.join().expect("sub thread panicked");
    }
}

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
    rt.enter(|| while rt.run() {});
    out
}

/// Sustained IPC PUB → 3 SUBs fan-out with aggressive recv timeouts.
/// The SUBs use a 20ms recv timeout which triggers frequent io_uring
/// read cancellations. Under load this provokes ENOBUFS on the multishot
/// recv path. Without the fixes, the connection breaks and messages are
/// lost, causing this test to hang.
#[test]
#[expect(clippy::too_many_lines)]
fn sustained_ipc_fanout_no_message_loss() {
    const PEERS: usize = 3;
    const MSG_SIZE: usize = 131_072;
    const TOTAL_MESSAGES: usize = 50;
    const WARMUP_SEQ: u64 = u64::MAX;

    fn payload(seq: u64) -> Bytes {
        let mut buf = vec![0xABu8; MSG_SIZE];
        buf[..8].copy_from_slice(&seq.to_le_bytes());
        Bytes::from(buf)
    }

    fn payload_seq(msg: &Message) -> u64 {
        let bytes = msg.part_bytes(0).expect("single-part payload");
        let seq = bytes
            .get(..8)
            .expect("payload carries sequence number")
            .try_into()
            .expect("sequence number is 8 bytes");
        u64::from_le_bytes(seq)
    }

    let ep: omq_compio::Endpoint =
        omq_compio::Endpoint::Ipc(omq_compio::endpoint::IpcPath::Abstract(format!(
            "omq-test-sustained-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        )));

    let received_by_sub: Arc<Vec<AtomicUsize>> =
        Arc::new((0..PEERS).map(|_| AtomicUsize::new(0)).collect());
    let sending_done = Arc::new(AtomicBool::new(false));
    let subs_ready: Arc<Vec<AtomicBool>> =
        Arc::new((0..PEERS).map(|_| AtomicBool::new(false)).collect());
    let bind_barrier = Arc::new(Barrier::new(PEERS + 1));

    let sub_threads: Vec<_> = (0..PEERS)
        .map(|i| {
            let ep = ep.clone();
            let received_by_sub = received_by_sub.clone();
            let subs_ready = subs_ready.clone();
            let sending_done = sending_done.clone();
            let bind_barrier = bind_barrier.clone();
            std::thread::spawn(move || {
                let rt = build_default_runtime().expect("sub runtime");
                block_on_and_drain(&rt, async move {
                    bind_barrier.wait();
                    let s = Socket::new(SocketType::Sub, Options::default());
                    s.connect(ep).await.expect("connect SUB");
                    s.subscribe(Bytes::new()).await.expect("subscribe");
                    let mut next = 0usize;
                    let mut drain_deadline = None;
                    while next < TOTAL_MESSAGES {
                        match compio::time::timeout(Duration::from_millis(20), s.recv()).await {
                            Ok(Ok(msg)) => {
                                let seq = payload_seq(&msg);
                                if seq == WARMUP_SEQ {
                                    subs_ready[i].store(true, Ordering::Relaxed);
                                    continue;
                                }
                                assert_eq!(
                                    seq as usize, next,
                                    "sub {i} received out-of-order or duplicate payload"
                                );
                                next += 1;
                                received_by_sub[i].store(next, Ordering::Relaxed);
                            }
                            _ if sending_done.load(Ordering::Acquire) => {
                                let deadline = drain_deadline.get_or_insert_with(|| {
                                    std::time::Instant::now() + Duration::from_secs(30)
                                });
                                assert!(
                                    std::time::Instant::now() <= *deadline,
                                    "sub {i} timed out after {next}/{TOTAL_MESSAGES} payloads"
                                );
                            }
                            _ => {}
                        }
                    }
                });
            })
        })
        .collect();

    let pub_thread = {
        let received_by_sub = received_by_sub.clone();
        let subs_ready = subs_ready.clone();
        let sending_done = sending_done.clone();
        std::thread::spawn(move || {
            let rt = build_default_runtime().expect("pub runtime");
            block_on_and_drain(&rt, async move {
                let pub_ = Socket::new(SocketType::Pub, Options::default().on_mute(OnMute::Block));
                pub_.bind(ep).await.expect("bind PUB");
                bind_barrier.wait();

                let warmup = payload(WARMUP_SEQ);

                // Wait for all subs to connect and subscribe.
                loop {
                    let _ = pub_.send(Message::single(warmup.clone())).await;
                    if subs_ready.iter().all(|r| r.load(Ordering::Relaxed)) {
                        break;
                    }
                    compio::time::sleep(Duration::from_micros(50)).await;
                }

                for seq in 0..TOTAL_MESSAGES {
                    pub_.send(Message::single(payload(seq as u64)))
                        .await
                        .unwrap();
                }
                sending_done.store(true, Ordering::Release);

                let deadline = std::time::Instant::now() + Duration::from_secs(30);
                while received_by_sub
                    .iter()
                    .any(|count| count.load(Ordering::Relaxed) < TOTAL_MESSAGES)
                {
                    if std::time::Instant::now() > deadline {
                        let got: Vec<_> = received_by_sub
                            .iter()
                            .map(|count| count.load(Ordering::Relaxed))
                            .collect();
                        panic!("message loss: expected {TOTAL_MESSAGES} per sub, got {got:?}");
                    }
                    compio::time::sleep(Duration::from_micros(100)).await;
                }
            });
        })
    };

    pub_thread.join().expect("pub thread panicked");
    for t in sub_threads {
        t.join().expect("sub thread panicked");
    }
}

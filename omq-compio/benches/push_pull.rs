//! PUSH/PULL sustained pipeline throughput. Mirrors
//! `omq-tokio/benches/push_pull.rs`.
//!
//! Multi-runtime bench: PULL runs on its own thread/runtime, all `PUSH`es
//! share another. This exercises a typical 2-core deployment shape
//! (each side pinned to its own core via per-thread runtimes) instead
//! of single-runtime cooperative scheduling.
//!
//! Measurement: PUSH side runs prime + warmup + N rounds; per-round
//! wait completes once PULL has drained the round's K messages. Min
//! wall time across rounds.

#[path = "common/mod.rs"]
mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use bytes::Bytes;
use omq_compio::{Message, Options, Socket, SocketType, build_default_runtime};

const PATTERN: &str = "push_pull";
const PEER_COUNTS: &[usize] = &[1, 3, 8];

fn main() {
    common::print_header("PUSH/PULL");
    let peer_counts = common::peers_override();
    let peer_counts = peer_counts.as_deref().unwrap_or(PEER_COUNTS);

    let mut seq = 0usize;
    for transport in common::transports() {
        for &peers in peer_counts {
            common::print_subheader(&transport, peers);
            for &size in &common::sizes() {
                seq += 1;
                let label = format!("{transport}/{peers}peer/{size}B");
                let cell = run_cell_threaded(&transport, peers, size, seq)
                    .unwrap_or_else(|e| panic!("{label} panicked: {e:?}"));
                common::print_cell(size, cell);
                common::append_jsonl(PATTERN, &transport, peers, size, cell);
            }
            println!();
        }
    }
}

fn run_cell_threaded(
    transport: &str,
    peers: usize,
    size: usize,
    seq: usize,
) -> Result<common::Cell, Box<dyn std::any::Any + Send>> {
    let ep = common::endpoint(transport, seq);
    let pull_count = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(Barrier::new(2));

    let pull_thread = {
        let ep = ep.clone();
        let pull_count = pull_count.clone();
        let stop = stop.clone();
        let ready = ready.clone();
        std::thread::spawn(move || {
            build_default_runtime()
                .expect("pull runtime")
                .block_on(async move {
                    let pull = Socket::new(SocketType::Pull, Options::default());
                    pull.bind(ep).await.expect("bind PULL");
                    ready.wait();
                    while !stop.load(Ordering::Relaxed) {
                        if let Ok(Ok(_)) =
                            compio::time::timeout(Duration::from_millis(20), pull.recv()).await
                        {
                            pull_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
        })
    };

    let push_thread = {
        let ep = ep.clone();
        let pull_count = pull_count.clone();
        let stop = stop.clone();
        let ready = ready.clone();
        std::thread::spawn(move || {
            build_default_runtime()
                .expect("push runtime")
                .block_on(async move {
                    ready.wait();
                    let mut pushes: Vec<Socket> = Vec::with_capacity(peers);
                    for _ in 0..peers {
                        let p = Socket::new(SocketType::Push, Options::default());
                        p.connect(ep.clone()).await.expect("connect PUSH");
                        pushes.push(p);
                    }
                    let refs: Vec<&Socket> = pushes.iter().collect();
                    common::wait_connected(&refs).await;
                    let pushes = Arc::new(pushes);

                    let payload = Bytes::from(vec![b'x'; size]);

                    let burst = |k: usize| {
                        let pushes = pushes.clone();
                        let payload = payload.clone();
                        let pull_count = pull_count.clone();
                        async move {
                            let per = (k / pushes.len()).max(1);
                            let target = pull_count.load(Ordering::Relaxed) + per * pushes.len();
                            let mut handles = Vec::with_capacity(pushes.len());
                            for i in 0..pushes.len() {
                                let p = pushes.clone();
                                let payload = payload.clone();
                                handles.push(compio::runtime::spawn(async move {
                                    for _ in 0..per {
                                        p[i].send(Message::single(payload.clone())).await.unwrap();
                                    }
                                }));
                            }
                            for h in handles {
                                let _ = h.await;
                            }
                            while pull_count.load(Ordering::Relaxed) < target {
                                compio::time::sleep(Duration::from_micros(50)).await;
                            }
                        }
                    };

                    let cell = common::measure_min_of(size, pushes.len(), burst).await;
                    stop.store(true, Ordering::Relaxed);
                    cell
                })
        })
    };

    let cell = push_thread.join()?;
    pull_thread.join()?;
    Ok(cell)
}

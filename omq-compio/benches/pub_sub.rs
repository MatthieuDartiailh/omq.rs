//! PUB/SUB fan-out throughput. PUB sends N, each SUB receives all N.
//!
//! Two topologies:
//! - **Inproc**: single-runtime cooperative scheduling.
//! - **Wire**: multi-runtime — each SUB on its own thread/runtime,
//!   PUB on another.

#[path = "common/mod.rs"]
mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::time::Duration;

use bytes::Bytes;
use omq_compio::{Message, OnMute, Options, ProactorBuilderExt, Socket, SocketType};

const PATTERN: &str = "pub_sub";
const PEER_COUNTS: &[usize] = &[3];

fn build_runtime(peers: usize) -> std::io::Result<compio::runtime::Runtime> {
    let count = u16::try_from(64_usize.max(peers * 4)).unwrap_or(u16::MAX);
    let len = common::bench_buffer_len();
    let mut p = compio::driver::ProactorBuilder::new();
    p.with_omq_buffer_pool_sized(std::num::NonZero::new(count).expect("nonzero"), len);
    compio::runtime::RuntimeBuilder::new()
        .with_proactor(p)
        .build()
}

fn main() {
    common::print_header("PUB/SUB");
    let peer_counts = common::peers_override();
    let peer_counts = peer_counts.as_deref().unwrap_or(PEER_COUNTS);

    let mut seq = 0usize;
    for transport in common::transports() {
        for &peers in peer_counts {
            common::print_subheader(&transport, peers);
            for &size in &common::sizes() {
                seq += 1;
                let label = format!("{transport}/{peers}peer/{size}B");
                let cell = if transport == "inproc" {
                    run_cell_single(&transport, peers, size, seq)
                } else {
                    run_cell_with_watchdog(&transport, peers, size, seq, &label)
                };
                common::print_cell(size, cell);
                common::append_jsonl(PATTERN, &transport, peers, size, cell);
            }
            println!();
        }
    }
}

// ── single-runtime (inproc) ──────────────────────────────────────────

#[allow(clippy::arc_with_non_send_sync)] // compio is single-threaded; Arc for spawn sharing
fn run_cell_single(transport: &str, peers: usize, size: usize, seq: usize) -> common::Cell {
    let rt = build_runtime(peers).expect("single runtime");
    common::block_on_and_drain(rt, async {
        let ep = common::endpoint(transport, seq);
        let pub_ = Socket::new(SocketType::Pub, Options::default().on_mute(OnMute::Block));
        pub_.bind(ep.clone()).await.expect("bind PUB");

        let mut subs: Vec<Socket> = Vec::with_capacity(peers);
        for _ in 0..peers {
            let s = Socket::new(SocketType::Sub, Options::default());
            s.connect(ep.clone()).await.expect("connect SUB");
            s.subscribe(Bytes::new()).await.expect("subscribe");
            subs.push(s);
        }
        {
            let refs: Vec<&Socket> = subs.iter().collect();
            common::wait_subscribed(&pub_, &refs).await;
        }

        let payload = common::payload(size);
        let pub_ = Arc::new(pub_);
        let subs = Arc::new(subs);

        let burst = |k: usize| {
            let pub_ = pub_.clone();
            let subs = subs.clone();
            let payload = payload.clone();
            async move {
                let send_handle = {
                    let pub_ = pub_.clone();
                    let payload = payload.clone();
                    compio::runtime::spawn(async move {
                        for _ in 0..k {
                            pub_.send(Message::single(payload.clone())).await.unwrap();
                        }
                    })
                };
                let mut recv_handles = Vec::with_capacity(subs.len());
                for i in 0..subs.len() {
                    let s = subs.clone();
                    recv_handles.push(compio::runtime::spawn(async move {
                        for _ in 0..k {
                            s[i].recv().await.unwrap();
                        }
                    }));
                }
                for h in recv_handles {
                    let _ = h.await;
                }
                let _ = send_handle.await;
            }
        };

        common::measure_min_of(size, 1, burst).await
    })
}

// ── multi-runtime (wire transports) ──────────────────────────────────

fn run_cell_with_watchdog(
    transport: &str,
    peers: usize,
    size: usize,
    seq: usize,
    label: &str,
) -> common::Cell {
    let (tx, rx) = mpsc::channel();
    let transport = transport.to_string();
    std::thread::spawn(move || {
        let res = run_cell_threaded(&transport, peers, size, seq);
        let _ = tx.send(res);
    });
    let budget = common::run_timeout() + Duration::from_secs(15);
    match rx.recv_timeout(budget) {
        Ok(Ok(cell)) => cell,
        Ok(Err(e)) => panic!("{label} panicked: {e:?}"),
        Err(e) => panic!("BENCH TIMEOUT: {label} exceeded {budget:?}: {e}"),
    }
}

#[allow(clippy::arc_with_non_send_sync)] // compio is single-threaded; Arc for spawn sharing
fn run_cell_threaded(
    transport: &str,
    peers: usize,
    size: usize,
    seq: usize,
) -> Result<common::Cell, Box<dyn std::any::Any + Send>> {
    let ep = common::endpoint(transport, seq);
    let recv_count = Arc::new(AtomicUsize::new(0));
    let subs_ready: Arc<Vec<AtomicBool>> =
        Arc::new((0..peers).map(|_| AtomicBool::new(false)).collect());
    let stop = Arc::new(AtomicBool::new(false));
    let bind_barrier = Arc::new(Barrier::new(peers + 1));

    let sub_threads: Vec<_> = (0..peers)
        .map(|i| {
            let ep = ep.clone();
            let recv_count = recv_count.clone();
            let subs_ready = subs_ready.clone();
            let stop = stop.clone();
            let bind_barrier = bind_barrier.clone();
            std::thread::spawn(move || {
                let rt = build_runtime(1).expect("sub runtime");
                common::block_on_and_drain(rt, async move {
                    bind_barrier.wait();
                    let s = Socket::new(SocketType::Sub, Options::default());
                    s.connect(ep).await.expect("connect SUB");
                    s.subscribe(Bytes::new()).await.expect("subscribe");
                    while !stop.load(Ordering::Relaxed) {
                        if let Ok(Ok(_)) =
                            compio::time::timeout(Duration::from_millis(200), s.recv()).await
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
            let rt = build_runtime(1).expect("pub runtime");
            common::block_on_and_drain(rt, async move {
                let pub_ = Socket::new(SocketType::Pub, Options::default().on_mute(OnMute::Block));
                pub_.bind(ep).await.expect("bind PUB");
                bind_barrier.wait();

                let payload = common::payload(size);

                loop {
                    let _ = pub_.send(Message::single(payload.clone())).await;
                    if subs_ready.iter().all(|r| r.load(Ordering::Relaxed)) {
                        break;
                    }
                    compio::time::sleep(Duration::from_micros(50)).await;
                }

                let pub_ = Arc::new(pub_);

                let burst = |k: usize| {
                    let pub_ = pub_.clone();
                    let payload = payload.clone();
                    let recv_count = recv_count.clone();
                    async move {
                        let target = recv_count.load(Ordering::Relaxed) + k * peers;
                        for _ in 0..k {
                            pub_.send(Message::single(payload.clone())).await.unwrap();
                        }
                        while recv_count.load(Ordering::Relaxed) < target {
                            compio::time::sleep(Duration::from_micros(50)).await;
                        }
                    }
                };

                let cell = common::measure_min_of(size, 1, burst).await;
                stop.store(true, Ordering::Relaxed);
                cell
            })
        })
    };

    let cell = pub_thread.join()?;
    for t in sub_threads {
        t.join()?;
    }
    Ok(cell)
}

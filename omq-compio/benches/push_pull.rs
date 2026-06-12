//! PUSH/PULL sustained pipeline throughput. Mirrors
//! `omq-tokio/benches/push_pull.rs`.
//!
//! Three topologies:
//! - **inproc**: single-runtime cooperative scheduling (IO-bound
//!   workloads where both ends share a thread).
//! - **inproc-mt**: multi-runtime inproc — PULL on its own
//!   thread/runtime, `PUSH`es on another (CPU-bound workloads).
//! - **Wire** (TCP, IPC, lz4+tcp): multi-runtime, same
//!   shape as inproc-mt but over kernel sockets.

#[path = "common/mod.rs"]
mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use omq_compio::{
    Message, MonitorEvent, MonitorStream, Options, ProactorBuilderExt, Socket, SocketType,
};

/// Build a per-cell runtime with a configurable `BUF_RING` pool.
/// `OMQ_BENCH_POOL=count,len_bytes` overrides the default 64 × 64 KiB.
fn build_runtime() -> std::io::Result<compio::runtime::Runtime> {
    use compio::driver::ProactorBuilder;
    use compio::runtime::RuntimeBuilder;
    let (count, len) = std::env::var("OMQ_BENCH_POOL")
        .ok()
        .and_then(|s| {
            let mut it = s.split(',');
            let c: u16 = it.next()?.parse().ok()?;
            let l: usize = it.next()?.parse().ok()?;
            Some((c, l))
        })
        .unwrap_or((64, bench_buffer_len()));
    let mut p = ProactorBuilder::new();
    p.with_omq_buffer_pool_sized(std::num::NonZero::new(count).expect("nonzero"), len);
    RuntimeBuilder::new().with_proactor(p).build()
}

fn bench_buffer_len() -> usize {
    common::bench_buffer_len()
}

const PATTERN: &str = "push_pull";
const PEER_COUNTS: &[usize] = &[1, 8];

fn main() {
    eprintln!("push_pull pid={}", std::process::id());
    common::print_header("PUSH/PULL");
    let peer_counts = common::peers_override();
    let peer_counts = peer_counts.as_deref().unwrap_or(PEER_COUNTS);

    let mut seq = 0usize;
    let transports = common::all_transports();
    for transport in transports {
        for &peers in peer_counts {
            common::print_subheader(&transport, peers);
            for &size in &common::sizes() {
                seq += 1;
                let label = format!("{transport}/{peers}peer/{size}B");
                let cell = if transport == "inproc" {
                    run_cell_single(&transport, peers, size, seq)
                } else if transport == "inproc-mt" {
                    run_cell_threaded("inproc", peers, size, seq)
                        .unwrap_or_else(|e| panic!("{label} panicked: {e:?}"))
                } else {
                    run_cell_threaded(&transport, peers, size, seq)
                        .unwrap_or_else(|e| panic!("{label} panicked: {e:?}"))
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
    let rt = build_runtime().expect("single runtime");
    common::block_on_and_drain(rt, async {
        let ep = common::endpoint(transport, seq);
        let pull = Socket::new(SocketType::Pull, Options::default());
        pull.bind(ep.clone()).await.expect("bind PULL");

        let mut pushes: Vec<Socket> = Vec::with_capacity(peers);
        for _ in 0..peers {
            let p = Socket::new(SocketType::Push, Options::default());
            p.connect(ep.clone()).await.expect("connect PUSH");
            pushes.push(p);
        }
        let refs: Vec<&Socket> = pushes.iter().collect();
        common::wait_connected(&refs).await;

        let payload = common::payload(size);
        let pull = Arc::new(pull);
        let pushes = Arc::new(pushes);

        let burst = |k: usize| {
            let pull = pull.clone();
            let pushes = pushes.clone();
            let payload = payload.clone();
            async move {
                let per = (k / pushes.len()).max(1);
                let mut handles = Vec::with_capacity(pushes.len());
                for i in 0..pushes.len() {
                    let p = pushes.clone();
                    let payload = payload.clone();
                    handles.push(compio::runtime::spawn(async move {
                        let msg = Message::from_slice(&payload);
                        for _ in 0..per {
                            p[i].send(msg.clone()).await.unwrap();
                        }
                    }));
                }
                for _ in 0..(per * pushes.len()) {
                    pull.recv().await.unwrap();
                }
                for h in handles {
                    let _ = h.await;
                }
            }
        };

        common::measure_min_of(size, pushes.len(), burst).await
    })
}

// ── multi-runtime (wire transports) ──────────────────────────────────

#[allow(clippy::arc_with_non_send_sync, clippy::too_many_lines)]
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
            let rt = build_runtime().expect("pull runtime");
            common::block_on_and_drain(rt, async move {
                let pull = Socket::new(SocketType::Pull, Options::default());
                pull.bind(ep).await.expect("bind PULL");
                ready.wait();
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(Ok(_)) =
                        compio::time::timeout(Duration::from_millis(20), pull.recv()).await
                    {
                        pull_count.fetch_add(1, Ordering::Relaxed);
                        let mut drained = 0u64;
                        while pull.try_recv().is_ok() {
                            drained += 1;
                        }
                        pull_count.fetch_add(drained as usize, Ordering::Relaxed);
                    }
                }
                drop(pull);
            });
        })
    };

    let push_thread = {
        let ep = ep.clone();
        let pull_count = pull_count.clone();
        let stop = stop.clone();
        let ready = ready.clone();
        std::thread::spawn(move || {
            let rt = build_runtime().expect("push runtime");
            common::block_on_and_drain(rt, async move {
                ready.wait();
                let mut pushes: Vec<Socket> = Vec::with_capacity(peers);
                let mut monitors: Vec<MonitorStream> = Vec::with_capacity(peers);
                for _ in 0..peers {
                    let p = Socket::new(SocketType::Push, Options::default());
                    monitors.push(p.monitor());
                    p.connect(ep.clone()).await.expect("connect PUSH");
                    pushes.push(p);
                }
                let refs: Vec<&Socket> = pushes.iter().collect();
                wait_connected_with_monitors(&refs, &mut monitors).await;

                let pushes = Arc::new(pushes);

                let payload = common::payload(size);

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
                                let msg = Message::from_slice(&payload);
                                for _ in 0..per {
                                    p[i].send(msg.clone()).await.unwrap();
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
                if let Ok(pushes) = Arc::try_unwrap(pushes) {
                    drop(pushes);
                }
                cell
            })
        })
    };

    let cell = push_thread.join()?;
    pull_thread.join()?;
    Ok(cell)
}

async fn wait_connected_with_monitors(socks: &[&Socket], monitors: &mut [MonitorStream]) {
    use std::time::Instant;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let mut pending = 0usize;
        for s in socks {
            let conns = s.connections().await.unwrap_or_default();
            if !conns.iter().any(|c| c.peer_info.is_some()) {
                pending += 1;
            }
        }
        if pending == 0 {
            return;
        }
        if Instant::now() > deadline {
            let mut detail = Vec::with_capacity(socks.len());
            for (i, m) in monitors.iter_mut().enumerate() {
                let mut counts = std::collections::HashMap::<&'static str, usize>::new();
                while let Ok(ev) = m.try_recv() {
                    let k = match ev {
                        MonitorEvent::Listening { .. } => "listening",
                        MonitorEvent::Accepted { .. } => "accepted",
                        MonitorEvent::Connected { .. } => "connected",
                        MonitorEvent::ConnectDelayed { .. } => "connect_delayed",
                        MonitorEvent::HandshakeFailed { .. } => "handshake_failed",
                        MonitorEvent::HandshakeSucceeded { .. } => "handshake_ok",
                        MonitorEvent::Disconnected { .. } => "disconnected",
                        MonitorEvent::Closed => "closed",
                        MonitorEvent::PeerCommand { .. } => "peer_command",
                        _ => "unknown",
                    };
                    *counts.entry(k).or_default() += 1;
                }
                let conns = socks[i].connections().await.unwrap_or_default();
                let mut kv: Vec<String> = counts
                    .into_iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect();
                kv.sort();
                detail.push(format!(
                    "#{i}: slots={} info_some={} events=[{}]",
                    conns.len(),
                    conns.iter().filter(|c| c.peer_info.is_some()).count(),
                    kv.join(",")
                ));
            }
            eprintln!(
                "STUCK: {pending}/{} PUSH peer(s) never reached \
                 peer_info=Some within 15s. {}",
                socks.len(),
                detail.join(" | ")
            );
            eprintln!(
                "PID={} — attach gdb now. Sleeping 300s before abort.",
                std::process::id()
            );
            let park = std::env::var("OMQ_BENCH_PARK_SECS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(300);
            compio::time::sleep(Duration::from_secs(park)).await;
            panic!("aborting after park; see STUCK message above");
        }
        compio::time::sleep(Duration::from_millis(5)).await;
    }
}

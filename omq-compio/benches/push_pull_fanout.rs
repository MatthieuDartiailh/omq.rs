//! 1 PUSH → N PULL fan-out throughput.
//!
//! Exercises the send path with multiple receivers contending.
//! Complement to `push_pull.rs` which measures fan-in (N PUSH → 1 PULL).
//!
//! Inproc is omitted: single-runtime can't drive 1 sender + N receivers
//! concurrently, and cross-thread inproc has an intermittent wakeup race
//! in compio (compio-rs/compio#911).

#[path = "common/mod.rs"]
mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use omq_compio::{Message, MonitorStream, Options, ProactorBuilderExt, Socket, SocketType};

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
        .unwrap_or((64, 64 * 1024));
    let mut p = ProactorBuilder::new();
    p.with_omq_buffer_pool_sized(std::num::NonZero::new(count).expect("nonzero"), len);
    RuntimeBuilder::new().with_proactor(p).build()
}

const PATTERN: &str = "push_pull_fanout";
const PEER_COUNTS: &[usize] = &[1, 8];

fn main() {
    eprintln!("push_pull_fanout pid={}", std::process::id());
    common::print_header("PUSH/PULL fan-out");
    let peer_counts = common::peers_override();
    let peer_counts = peer_counts.as_deref().unwrap_or(PEER_COUNTS);

    let mut seq = 0usize;
    let transports = common::all_transports();
    for transport in transports.iter().filter(|t| *t != "inproc") {
        for &peers in peer_counts {
            common::print_subheader(transport, peers);
            for &size in &common::sizes() {
                seq += 1;
                let label = format!("{transport}/{peers}peer/{size}B");
                let cell = run_cell_threaded(transport, peers, size, seq)
                    .unwrap_or_else(|e| panic!("{label} panicked: {e:?}"));
                common::print_cell(size, cell);
                common::append_jsonl(PATTERN, transport, peers, size, cell);
            }
            println!();
        }
    }
}

#[allow(clippy::arc_with_non_send_sync, clippy::too_many_lines)]
fn run_cell_threaded(
    transport: &str,
    peers: usize,
    size: usize,
    seq: usize,
) -> Result<common::Cell, Box<dyn std::any::Any + Send>> {
    let ep = common::endpoint(transport, seq);
    let stop = Arc::new(AtomicBool::new(false));
    // +1 for the send thread, +peers for each recv thread.
    let ready = Arc::new(Barrier::new(1 + peers));

    let recv_threads: Vec<_> = (0..peers)
        .map(|_| {
            let ep = ep.clone();
            let stop = stop.clone();
            let ready = ready.clone();
            std::thread::spawn(move || {
                let rt = build_runtime().expect("recv runtime");
                common::block_on_and_drain(rt, async move {
                    ready.wait();
                    let pull = Socket::new(SocketType::Pull, Options::default());
                    pull.connect(ep).await.expect("connect PULL");
                    while !stop.load(Ordering::Relaxed) {
                        if let Ok(Ok(_)) =
                            compio::time::timeout(Duration::from_millis(20), pull.recv()).await
                        {
                            while pull.try_recv().is_ok() {}
                        }
                    }
                    drop(pull);
                });
            })
        })
        .collect();

    let send_thread = {
        let ep = ep.clone();
        let stop = stop.clone();
        let ready = ready.clone();
        std::thread::spawn(move || {
            let rt = build_runtime().expect("send runtime");
            common::block_on_and_drain(rt, async move {
                let push = Socket::new(SocketType::Push, Options::default());
                let mut monitor = push.monitor();
                push.bind(ep).await.expect("bind PUSH");
                ready.wait();

                wait_all_connected(&push, &mut monitor, peers).await;

                let payload = common::payload(size);

                let burst = |k: usize| {
                    let push = &push;
                    let payload = payload.clone();
                    async move {
                        let total = (k / peers) * peers;
                        for _ in 0..total {
                            push.send(Message::single(payload.clone())).await.unwrap();
                        }
                    }
                };

                let cell = common::measure_min_of(size, peers, burst).await;
                stop.store(true, Ordering::Relaxed);
                drop(push);
                cell
            })
        })
    };

    let cell = send_thread.join()?;
    for t in recv_threads {
        t.join()?;
    }
    Ok(cell)
}

async fn wait_all_connected(push: &Socket, monitor: &mut MonitorStream, peers: usize) {
    use std::time::Instant;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let conns = push.connections().await.unwrap_or_default();
        if conns.iter().filter(|c| c.peer_info.is_some()).count() >= peers {
            return;
        }
        if Instant::now() > deadline {
            let mut events = Vec::new();
            while let Ok(ev) = monitor.try_recv() {
                events.push(format!("{ev:?}"));
            }
            let conns = push.connections().await.unwrap_or_default();
            panic!(
                "bench: {}/{peers} peer(s) never reached peer_info=Some within 15s. \
                 slots={} info_some={} events={:?}",
                conns.iter().filter(|c| c.peer_info.is_some()).count(),
                conns.len(),
                conns.iter().filter(|c| c.peer_info.is_some()).count(),
                events,
            );
        }
        compio::time::sleep(Duration::from_millis(5)).await;
    }
}

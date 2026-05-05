//! PUB/SUB fan-out throughput. PUB sends N, each SUB receives all N.
//!
//! Multi-runtime bench: each SUB runs on its own thread/runtime, PUB runs on
//! another. The bind barrier ensures PUB is bound before SUBs connect; the
//! probe loop then serves as subscription confirmation (equivalent to
//! `wait_subscribed` but cross-runtime).

#[path = "common/mod.rs"]
mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use bytes::Bytes;
use omq_compio::{Message, OnMute, Options, Socket, SocketType, build_default_runtime};

const PATTERN: &str = "pub_sub";
const PEER_COUNTS: &[usize] = &[3];

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
    let recv_count = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    // Barrier: PUB binds first, then SUBs connect. Prevents inproc races.
    let bind_barrier = Arc::new(Barrier::new(peers + 1));

    let sub_threads: Vec<_> = (0..peers)
        .map(|_| {
            let ep = ep.clone();
            let recv_count = recv_count.clone();
            let stop = stop.clone();
            let bind_barrier = bind_barrier.clone();
            std::thread::spawn(move || {
                build_default_runtime()
                    .expect("sub runtime")
                    .block_on(async move {
                        bind_barrier.wait();
                        let s = Socket::new(SocketType::Sub, Options::default());
                        s.connect(ep).await.expect("connect SUB");
                        s.subscribe(Bytes::new()).await.expect("subscribe");
                        while !stop.load(Ordering::Relaxed) {
                            if let Ok(Ok(_)) =
                                compio::time::timeout(Duration::from_millis(20), s.recv()).await
                            {
                                recv_count.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    });
            })
        })
        .collect();

    let pub_thread = {
        let recv_count = recv_count.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            build_default_runtime()
                .expect("pub runtime")
                .block_on(async move {
                    let pub_ =
                        Socket::new(SocketType::Pub, Options::default().on_mute(OnMute::Block));
                    pub_.bind(ep).await.expect("bind PUB");
                    bind_barrier.wait();

                    let payload = Bytes::from(vec![b'x'; size]);

                    // Probe until subscriptions are active. PUB sends probes; SUBs
                    // receive them once their subscription message has propagated.
                    // Each published message reaches all N SUBs, so recv_count grows
                    // by `peers` per delivered message. One full delivery suffices.
                    loop {
                        let _ = pub_.send(Message::single(payload.clone())).await;
                        if recv_count.load(Ordering::Relaxed) >= peers {
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

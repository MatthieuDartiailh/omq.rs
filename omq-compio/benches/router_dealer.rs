//! ROUTER/DEALER throughput: DEALERs send, ROUTER receives.
//!
//! Multi-runtime bench: ROUTER runs on its own thread/runtime, all DEALERs
//! share another. Mirrors `push_pull.rs`.

#[path = "common/mod.rs"]
mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use bytes::Bytes;
use omq_compio::{Message, Options, Socket, SocketType, build_default_runtime};

const PATTERN: &str = "router_dealer";
const PEER_COUNTS: &[usize] = &[3];

fn main() {
    common::print_header("ROUTER/DEALER");
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
    let ready = Arc::new(Barrier::new(2));

    let router_thread = {
        let ep = ep.clone();
        let recv_count = recv_count.clone();
        let stop = stop.clone();
        let ready = ready.clone();
        std::thread::spawn(move || {
            build_default_runtime()
                .expect("router runtime")
                .block_on(async move {
                    let router = Socket::new(SocketType::Router, Options::default());
                    router.bind(ep).await.expect("bind ROUTER");
                    ready.wait();
                    while !stop.load(Ordering::Relaxed) {
                        if let Ok(Ok(_)) =
                            compio::time::timeout(Duration::from_millis(20), router.recv()).await
                        {
                            recv_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
        })
    };

    let dealer_thread = {
        let recv_count = recv_count.clone();
        let stop = stop.clone();
        let ready = ready.clone();
        std::thread::spawn(move || {
            build_default_runtime()
                .expect("dealer runtime")
                .block_on(async move {
                    ready.wait();
                    let mut dealers: Vec<Socket> = Vec::with_capacity(peers);
                    for i in 0..peers {
                        let id: Bytes = format!("d{i}").into();
                        let d =
                            Socket::new(SocketType::Dealer, Options::default().identity(id));
                        d.connect(ep.clone()).await.expect("connect DEALER");
                        dealers.push(d);
                    }
                    let refs: Vec<&Socket> = dealers.iter().collect();
                    common::wait_connected(&refs).await;
                    let dealers = Arc::new(dealers);

                    let payload = Bytes::from(vec![b'x'; size]);

                    let burst = |k: usize| {
                        let dealers = dealers.clone();
                        let payload = payload.clone();
                        let recv_count = recv_count.clone();
                        async move {
                            let per = (k / dealers.len()).max(1);
                            let target =
                                recv_count.load(Ordering::Relaxed) + per * dealers.len();
                            let mut handles = Vec::with_capacity(dealers.len());
                            for i in 0..dealers.len() {
                                let d = dealers.clone();
                                let payload = payload.clone();
                                handles.push(compio::runtime::spawn(async move {
                                    for _ in 0..per {
                                        d[i].send(Message::single(payload.clone()))
                                            .await
                                            .unwrap();
                                    }
                                }));
                            }
                            for h in handles {
                                let _ = h.await;
                            }
                            while recv_count.load(Ordering::Relaxed) < target {
                                compio::time::sleep(Duration::from_micros(50)).await;
                            }
                        }
                    };

                    let cell =
                        common::measure_min_of(size, dealers.len(), burst).await;
                    stop.store(true, Ordering::Relaxed);
                    cell
                })
        })
    };

    let cell = dealer_thread.join()?;
    router_thread.join()?;
    Ok(cell)
}

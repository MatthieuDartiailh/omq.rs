//! PAIR exclusive 1-to-1 throughput.
//!
//! Multi-runtime bench: receiver runs on its own thread/runtime,
//! sender runs on another. Mirrors `push_pull.rs`.

#[path = "common/mod.rs"]
mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use bytes::Bytes;
use omq_compio::{Message, Options, Socket, SocketType, build_default_runtime};

const PATTERN: &str = "pair";
const PEER_COUNTS: &[usize] = &[1];

fn main() {
    common::print_header("PAIR");
    let peer_counts = common::peers_override();
    let peer_counts = peer_counts.as_deref().unwrap_or(PEER_COUNTS);

    let mut seq = 0usize;
    for transport in common::transports() {
        for &peers in peer_counts {
            common::print_subheader(&transport, peers);
            for &size in &common::sizes() {
                seq += 1;
                let label = format!("{transport}/{peers}peer/{size}B");
                let cell = run_cell_threaded(&transport, size, seq)
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
    size: usize,
    seq: usize,
) -> Result<common::Cell, Box<dyn std::any::Any + Send>> {
    let ep = common::endpoint(transport, seq);
    let recv_count = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(Barrier::new(2));

    let recv_thread = {
        let ep = ep.clone();
        let recv_count = recv_count.clone();
        let stop = stop.clone();
        let ready = ready.clone();
        std::thread::spawn(move || {
            build_default_runtime()
                .expect("recv runtime")
                .block_on(async move {
                    let receiver = Socket::new(SocketType::Pair, Options::default());
                    receiver.bind(ep).await.expect("bind PAIR");
                    ready.wait();
                    while !stop.load(Ordering::Relaxed) {
                        if let Ok(Ok(_)) =
                            compio::time::timeout(Duration::from_millis(20), receiver.recv()).await
                        {
                            recv_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
        })
    };

    let send_thread = {
        let recv_count = recv_count.clone();
        let stop = stop.clone();
        let ready = ready.clone();
        std::thread::spawn(move || {
            build_default_runtime()
                .expect("send runtime")
                .block_on(async move {
                    ready.wait();
                    let sender = Socket::new(SocketType::Pair, Options::default());
                    sender.connect(ep).await.expect("connect PAIR");
                    common::wait_connected(&[&sender]).await;

                    let payload = Bytes::from(vec![b'x'; size]);
                    let sender = Arc::new(sender);

                    let burst = |k: usize| {
                        let sender = sender.clone();
                        let payload = payload.clone();
                        let recv_count = recv_count.clone();
                        async move {
                            let target = recv_count.load(Ordering::Relaxed) + k;
                            for _ in 0..k {
                                sender.send(Message::single(payload.clone())).await.unwrap();
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

    let cell = send_thread.join()?;
    recv_thread.join()?;
    Ok(cell)
}

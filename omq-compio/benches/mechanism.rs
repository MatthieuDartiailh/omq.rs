//! PUSH/PULL over TCP with PLAIN vs CURVE vs BLAKE3ZMQ mechanisms.
//!
//! Measures real end-to-end throughput including handshake, encryption,
//! and decryption overhead. Single peer, loopback TCP.
//!
//! Run:
//!   cargo bench -p omq-compio --bench mechanism --features 'plain curve blake3zmq'

#[path = "common/mod.rs"]
mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use omq_compio::{
    Message, MonitorEvent, MonitorStream, Options, ProactorBuilderExt, Socket, SocketType,
};

const PATTERN: &str = "mechanism";

fn build_runtime() -> std::io::Result<compio::runtime::Runtime> {
    use compio::driver::ProactorBuilder;
    use compio::runtime::RuntimeBuilder;
    let mut p = ProactorBuilder::new();
    p.with_omq_buffer_pool_sized(std::num::NonZero::new(128).expect("nonzero"), 32 * 1024);
    RuntimeBuilder::new().with_proactor(p).build()
}

fn accept_all(_: &omq_compio::MechanismPeerInfo) -> bool {
    true
}

fn main() {
    eprintln!("mechanism pid={}", std::process::id());
    common::print_header("PUSH/PULL mechanism (tcp)");

    let sizes = common::sizes();
    let mut seq = 0usize;

    println!("--- PLAIN (tcp) ---");
    for &size in &sizes {
        seq += 1;
        let cell = run_cell(
            Options::default().plain_server(accept_all),
            Options::default().plain_client("bench", "bench"),
            size,
            seq,
        );
        common::print_cell(size, cell);
        common::append_jsonl(PATTERN, "PLAIN", 1, size, cell);
    }
    println!();

    #[cfg(feature = "curve")]
    {
        use omq_compio::CurveKeypair;
        let server_kp = CurveKeypair::generate();
        let client_kp = CurveKeypair::generate();
        let server_pub = server_kp.public;

        println!("--- CURVE (tcp) ---");
        for &size in &sizes {
            seq += 1;
            let cell = run_cell(
                Options::default().curve_server(server_kp.clone()),
                Options::default().curve_client(client_kp.clone(), server_pub),
                size,
                seq,
            );
            common::print_cell(size, cell);
            common::append_jsonl(PATTERN, "CURVE", 1, size, cell);
        }
        println!();
    }

    #[cfg(feature = "blake3zmq")]
    {
        use omq_compio::Blake3ZmqKeypair;
        let server_kp = Blake3ZmqKeypair::generate();
        let client_kp = Blake3ZmqKeypair::generate();
        let server_pub = server_kp.public;

        println!("--- BLAKE3ZMQ (tcp) ---");
        for &size in &sizes {
            seq += 1;
            let cell = run_cell(
                Options::default().blake3zmq_server(server_kp.clone()),
                Options::default().blake3zmq_client(client_kp.clone(), server_pub),
                size,
                seq,
            );
            common::print_cell(size, cell);
            common::append_jsonl(PATTERN, "BLAKE3ZMQ", 1, size, cell);
        }
        println!();
    }
}

fn run_cell(pull_opts: Options, push_opts: Options, size: usize, seq: usize) -> common::Cell {
    let ep = common::endpoint("tcp", seq);
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
            common::block_on_and_leak(rt, async move {
                let pull = Socket::new(SocketType::Pull, pull_opts);
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
            common::block_on_and_leak(rt, async move {
                ready.wait();
                let push = Socket::new(SocketType::Push, push_opts);
                let mut mon = push.monitor();
                push.connect(ep).await.expect("connect PUSH");
                wait_handshake(&mut mon).await;

                let payload = common::payload(size);
                #[allow(clippy::arc_with_non_send_sync)]
                let push = Arc::new(push);

                let burst = |k: usize| {
                    let push = push.clone();
                    let payload = payload.clone();
                    let pull_count = pull_count.clone();
                    async move {
                        let target = pull_count.load(Ordering::Relaxed) + k;
                        for _ in 0..k {
                            push.send(Message::single(payload.clone())).await.unwrap();
                        }
                        while pull_count.load(Ordering::Relaxed) < target {
                            compio::time::sleep(Duration::from_micros(50)).await;
                        }
                    }
                };

                let cell = common::measure_min_of(size, 1, burst).await;
                stop.store(true, Ordering::Relaxed);
                if let Ok(p) = Arc::try_unwrap(push) {
                    drop(p);
                }
                cell
            })
        })
    };

    let cell = push_thread.join().expect("push thread panicked");
    pull_thread.join().expect("pull thread panicked");
    cell
}

async fn wait_handshake(mon: &mut MonitorStream) {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        match compio::time::timeout(Duration::from_millis(100), mon.recv()).await {
            Ok(Ok(MonitorEvent::HandshakeSucceeded { .. })) => return,
            _ if std::time::Instant::now() > deadline => {
                panic!("bench: handshake never completed within 15s");
            }
            _ => {}
        }
    }
}

#![cfg(all(feature = "soak", feature = "lz4"))]
//! Soak: lz4 compression transport sustained.
//!
//! Same structure as soak_compression (zstd) but exercises the lz4
//! encoder/decoder path. Mixed message sizes, continuous send/recv.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::join;

use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

const SIZES: &[usize] = &[64, 1024, 8 * 1024, 64 * 1024, 256 * 1024];

async fn pull_on_loopback() -> (Socket, Endpoint) {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let mut mon = pull.monitor();
    pull.bind(Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port: 0,
    })
    .await
    .unwrap();
    let ev = compio::time::timeout(Duration::from_millis(500), mon.recv())
        .await
        .unwrap()
        .unwrap();
    let port = match ev {
        MonitorEvent::Listening {
            endpoint: Endpoint::Lz4Tcp { port, .. },
        } => port,
        other => panic!("expected Lz4Tcp Listening, got {other:?}"),
    };
    (
        pull,
        Endpoint::Lz4Tcp {
            host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            port,
        },
    )
}

fn make_payload(idx: u64, size: usize) -> Vec<u8> {
    let seed = (idx & 0xFF) as u8;
    let mut v = vec![seed; size];
    let idx_bytes = idx.to_le_bytes();
    let tag_len = idx_bytes.len().min(size);
    v[..tag_len].copy_from_slice(&idx_bytes[..tag_len]);
    v
}

#[test]
fn soak_compression_lz4_sustained() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let sent = Arc::new(AtomicU64::new(0));
    let recvd = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    {
        let rt = omq_compio::build_default_runtime().expect("runtime");
        rt.block_on(async {
            let (pull, ep) = pull_on_loopback().await;
            let push = Socket::new(
                SocketType::Push,
                Options::default().linger(Duration::from_secs(5)),
            );
            push.connect(ep).await.unwrap();
            compio::time::sleep(Duration::from_millis(100)).await;

            let send_sent = sent.clone();
            let send_stop = stop.clone();
            let send_fut = async {
                let mut idx: u64 = 0;
                while !send_stop.load(Ordering::Relaxed) {
                    let size = SIZES[idx as usize % SIZES.len()];
                    let payload = make_payload(idx, size);
                    if let Ok(Ok(())) = compio::time::timeout(
                        Duration::from_secs(2),
                        push.send(Message::single(payload)),
                    )
                    .await
                    {
                        send_sent.fetch_add(1, Ordering::Relaxed);
                    }
                    idx += 1;
                }
            };

            let recv_recvd = recvd.clone();
            let recv_stop = stop.clone();
            let recv_fut = async {
                while !recv_stop.load(Ordering::Relaxed) {
                    if let Ok(Ok(m)) =
                        compio::time::timeout(Duration::from_secs(2), pull.recv()).await
                    {
                        let part = m.part_bytes(0).unwrap();
                        assert!(
                            SIZES.contains(&part.len()),
                            "unexpected message size: {}",
                            part.len()
                        );
                        recv_recvd.fetch_add(1, Ordering::Relaxed);
                    }
                }
            };

            let timer_stop = stop.clone();
            let timer_sent = sent.clone();
            let timer_recvd = recvd.clone();
            let timer_fut = async {
                let start = Instant::now();
                let mut last_log = start;

                while start.elapsed() < duration {
                    compio::time::sleep(Duration::from_secs(1)).await;

                    if last_log.elapsed() >= Duration::from_secs(30) {
                        let s = timer_sent.load(Ordering::Relaxed);
                        let r = timer_recvd.load(Ordering::Relaxed);
                        eprintln!(
                            "[compression_lz4] {:.0}s, sent {s}, recvd {r}",
                            start.elapsed().as_secs_f64(),
                        );
                        last_log = Instant::now();
                    }
                }
                timer_stop.store(true, Ordering::Relaxed);
            };

            join!(send_fut, recv_fut, timer_fut);

            let s = sent.load(Ordering::Relaxed);
            let r = recvd.load(Ordering::Relaxed);
            eprintln!(
                "[compression_lz4] done: sent {s}, recvd {r} in {:.1}s",
                duration.as_secs_f64(),
            );

            push.close().await.unwrap();
            pull.close().await.unwrap();
        });
    }

    let report = monitor.stop();
    report.assert_no_leak("compression_lz4");
}

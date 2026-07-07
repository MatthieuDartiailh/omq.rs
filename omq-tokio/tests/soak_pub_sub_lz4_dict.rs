#![cfg(all(feature = "soak", feature = "lz4"))]
//! Soak: sharded PUB/SUB over lz4+tcp:// with fan-out dictionaries.
//!
//! Trains the PUB fan-out encoder through sustained traffic, keeps enough
//! decoded SUB peers alive to activate shard workers, and repeatedly adds
//! raw TCP late subscribers. Each raw late subscriber must receive one
//! dictionary shipment before dictionary-compressed payloads.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

const DECODED_SUBS: usize = 4;
const TRAINING_MSGS: u64 = 128;
const RAW_PAYLOAD_CHECKS: usize = 3;

fn lz4_tcp_ep(port: u16) -> Endpoint {
    Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn tcp_from_lz4(ep: &Endpoint) -> Endpoint {
    match ep {
        Endpoint::Lz4Tcp { host, port } => Endpoint::Tcp {
            host: host.clone(),
            port: *port,
        },
        other => panic!("expected lz4+tcp endpoint, got {other:?}"),
    }
}

fn lz4_options() -> Options {
    soak_common::soak_options()
        .compression_auto_train(true)
        .send_hwm(4096)
        .recv_hwm(4096)
}

async fn bind_lz4_pub(publisher: &Socket) -> (Endpoint, omq_tokio::MonitorStream) {
    let mut mon = publisher.monitor();
    publisher.bind(lz4_tcp_ep(0)).await.unwrap();
    loop {
        match tokio::time::timeout(Duration::from_secs(5), mon.recv())
            .await
            .expect("publisher did not report listening")
            .unwrap()
        {
            MonitorEvent::Listening {
                endpoint: Endpoint::Lz4Tcp { port, .. },
            } => return (lz4_tcp_ep(port), mon),
            _ => {}
        }
    }
}

async fn wait_for_subscribes(mon: &mut omq_tokio::MonitorStream, n: usize) {
    let fut = async {
        let mut count = 0;
        while count < n {
            match mon.recv().await {
                Ok(MonitorEvent::SubscribeReceived { .. }) => count += 1,
                Ok(_) => {}
                Err(e) => panic!("monitor closed after {count}/{n} subscribes: {e:?}"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("subscribes did not propagate");
}

fn payload(seq: u64) -> Bytes {
    Bytes::from(format!(
        "{{\"kind\":\"quote\",\"venue\":\"XNAS\",\"symbol\":\"OMQ\",\"seq\":{seq},\"bid\":101.25,\"ask\":101.27,\"depth\":[10125,10126,10127],\"pad\":\"{}\"}}",
        "A".repeat(256)
    ))
}

async fn assert_raw_dict_then_payloads(raw: &Socket) {
    let dict = tokio::time::timeout(Duration::from_secs(5), raw.recv())
        .await
        .expect("raw subscriber did not receive dictionary shipment")
        .unwrap();
    let dict_part = dict.part_bytes(0).unwrap();
    assert!(
        dict_part.starts_with(b"LZ4D"),
        "raw subscriber first message must be LZ4D, got {:?}",
        &dict_part[..dict_part.len().min(4)]
    );

    for idx in 0..RAW_PAYLOAD_CHECKS {
        let msg = tokio::time::timeout(Duration::from_secs(5), raw.recv())
            .await
            .unwrap_or_else(|_| panic!("raw subscriber missed compressed payload {idx}"))
            .unwrap();
        let part = msg.part_bytes(0).unwrap();
        assert!(
            part.starts_with(b"LZ4B"),
            "raw payload {idx} should be dictionary-compressed, got {:?}",
            &part[..part.len().min(4)]
        );
    }
}

#[test]
fn soak_pub_sub_lz4_dict_sharded_fanout() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let sent = Arc::new(AtomicU64::new(0));
    let decoded = Arc::new(AtomicU64::new(0));
    let raw_probes = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let rt = soak_common::tokio_runtime();
    rt.block_on(async {
        let publisher = Socket::new(SocketType::Pub, lz4_options());
        let (ep, mut mon) = bind_lz4_pub(&publisher).await;

        let mut decoded_subs = Vec::with_capacity(DECODED_SUBS);
        for _ in 0..DECODED_SUBS {
            let sub = Socket::new(SocketType::Sub, lz4_options());
            sub.connect(ep.clone()).await.unwrap();
            sub.subscribe(Bytes::new()).await.unwrap();
            decoded_subs.push(sub);
        }
        wait_for_subscribes(&mut mon, DECODED_SUBS).await;

        for seq in 0..TRAINING_MSGS {
            publisher.send(Message::single(payload(seq))).await.unwrap();
            sent.fetch_add(1, Ordering::Relaxed);
        }

        let mut decode_tasks = Vec::with_capacity(DECODED_SUBS);
        for sub in &decoded_subs {
            let sub = sub.clone();
            let decoded = decoded.clone();
            let stop = stop.clone();
            decode_tasks.push(tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(Ok(_msg)) =
                        tokio::time::timeout(Duration::from_secs(2), sub.recv()).await
                    {
                        decoded.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        let send_stop = stop.clone();
        let send_sent = sent.clone();
        let send_pub = publisher.clone();
        let send_task = tokio::spawn(async move {
            let mut seq = TRAINING_MSGS;
            while !send_stop.load(Ordering::Relaxed) {
                if send_pub.send(Message::single(payload(seq))).await.is_ok() {
                    send_sent.fetch_add(1, Ordering::Relaxed);
                }
                seq += 1;
            }
        });

        let start = Instant::now();
        let mut last_probe = Instant::now() - Duration::from_secs(1);
        let mut last_log = start;
        while start.elapsed() < duration {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if last_probe.elapsed() >= Duration::from_millis(500) {
                last_probe = Instant::now();
                let raw = Socket::new(SocketType::Sub, soak_common::soak_options().recv_hwm(64));
                raw.connect(tcp_from_lz4(&ep)).await.unwrap();
                raw.subscribe(Bytes::new()).await.unwrap();
                wait_for_subscribes(&mut mon, 1).await;
                assert_raw_dict_then_payloads(&raw).await;
                raw.close().await.unwrap();
                raw_probes.fetch_add(1, Ordering::Relaxed);
            }

            if last_log.elapsed() >= Duration::from_secs(30) {
                let s = sent.load(Ordering::Relaxed);
                let d = decoded.load(Ordering::Relaxed);
                let r = raw_probes.load(Ordering::Relaxed);
                eprintln!(
                    "[pub_sub_lz4_dict] {:.0}s, sent {s}, decoded {d}, raw_probes {r}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        stop.store(true, Ordering::Relaxed);
        let _ = send_task.await;
        for task in decode_tasks {
            let _ = task.await;
        }

        assert!(
            raw_probes.load(Ordering::Relaxed) > 0,
            "soak must attach at least one raw late subscriber"
        );
        assert!(
            decoded.load(Ordering::Relaxed) > 0,
            "decoded subscribers must receive traffic"
        );

        for sub in decoded_subs {
            sub.close().await.unwrap();
        }
        publisher.close().await.unwrap();
    });

    let report = monitor.stop();
    report.assert_no_leak("pub_sub_lz4_dict");
}

//! PUB / SUB integration tests.

mod test_support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::time::Duration;

use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[compio::test]
async fn pub_sub_simple_prefix_match() {
    let ep = inproc_ep("ps-simple");
    let publisher = Socket::new(SocketType::Pub, Options::default());
    publisher.bind(ep.clone()).await.unwrap();

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(ep).await.unwrap();
    subscriber.subscribe("news.").await.unwrap();

    publisher
        .send(Message::multipart(["news.sports", "ball scores"]))
        .await
        .unwrap();
    publisher
        .send(Message::multipart(["weather", "sunny"]))
        .await
        .unwrap();
    publisher
        .send(Message::multipart(["news.tech", "rust 1.85"]))
        .await
        .unwrap();

    let got1 = compio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    let got2 = compio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got1.part_bytes(0).unwrap(), &b"news.sports"[..]);
    assert_eq!(got1.part_bytes(1).unwrap(), &b"ball scores"[..]);
    assert_eq!(got2.part_bytes(0).unwrap(), &b"news.tech"[..]);

    let third = compio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(third.is_err(), "non-matching message must not be delivered");
}

#[compio::test]
async fn pub_sub_late_subscriber_misses_earlier() {
    let ep = inproc_ep("ps-late");
    let publisher = Socket::new(SocketType::Pub, Options::default());
    publisher.bind(ep.clone()).await.unwrap();

    publisher
        .send(Message::single("pre-subscribe"))
        .await
        .unwrap();

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(ep).await.unwrap();
    subscriber.subscribe("").await.unwrap();

    publisher
        .send(Message::single("post-subscribe"))
        .await
        .unwrap();

    let m = compio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"post-subscribe"[..]);

    let other = compio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(other.is_err());
}

#[compio::test]
async fn pub_sub_subscribe_all_with_empty_prefix() {
    let ep = inproc_ep("ps-all");
    let publisher = Socket::new(SocketType::Pub, Options::default());
    publisher.bind(ep.clone()).await.unwrap();

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(ep).await.unwrap();
    subscriber.subscribe(bytes::Bytes::new()).await.unwrap();

    for t in ["a", "bb", "ccc", "quux"] {
        publisher
            .send(Message::single(t.to_string()))
            .await
            .unwrap();
    }
    for expected in ["a", "bb", "ccc", "quux"] {
        let m = compio::time::timeout(Duration::from_millis(500), subscriber.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), expected.as_bytes());
    }
}

#[compio::test]
async fn pub_sub_unsubscribe() {
    let ep = inproc_ep("ps-unsub");
    let publisher = Socket::new(SocketType::Pub, Options::default());
    publisher.bind(ep.clone()).await.unwrap();

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(ep).await.unwrap();
    subscriber.subscribe("a").await.unwrap();
    subscriber.subscribe("b").await.unwrap();

    publisher.send(Message::single("apple")).await.unwrap();
    publisher.send(Message::single("banana")).await.unwrap();
    let m1 = compio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    let m2 = compio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    let got = [m1.part_bytes(0).unwrap(), m2.part_bytes(0).unwrap()];
    assert!(got.contains(&bytes::Bytes::from_static(b"apple")));
    assert!(got.contains(&bytes::Bytes::from_static(b"banana")));

    subscriber.unsubscribe("b").await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    publisher.send(Message::single("apricot")).await.unwrap();
    publisher.send(Message::single("blueberry")).await.unwrap();
    let m = compio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"apricot"[..]);

    let other = compio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(other.is_err());
}

#[compio::test]
async fn sub_replays_subscriptions_on_new_peer() {
    let ep = inproc_ep("ps-replay");

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.subscribe("x.").await.unwrap();

    let publisher = Socket::new(SocketType::Pub, Options::default());
    publisher.bind(ep.clone()).await.unwrap();
    subscriber.connect(ep).await.unwrap();

    publisher.send(Message::single("x.hello")).await.unwrap();
    publisher.send(Message::single("y.nope")).await.unwrap();

    let m = compio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"x.hello"[..]);
    let other = compio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(other.is_err());
}

/// Regression test for the `bench_peer` TCP PUB/SUB hang: a PUB in a tight
/// send loop on a single-threaded compio runtime must still accept TCP
/// connections and process SUBSCRIBE commands. Without a yield point in
/// `send_pub_filtered` when no subscribers match, the runtime starves and
/// the listener task never runs.
#[test]
fn pub_tcp_tight_send_must_not_starve_listener() {
    let port = Arc::new(AtomicU16::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let port_pub = port.clone();
    let stop_pub = stop.clone();
    let pub_thread = std::thread::spawn(move || {
        let rt = compio::runtime::RuntimeBuilder::new().build().unwrap();
        rt.block_on(async {
            let pub_ = Socket::new(SocketType::Pub, Options::default());
            let bound = pub_.bind(test_support::tcp_loopback(0)).await.unwrap();
            let Endpoint::Tcp { port: p, .. } = bound else {
                panic!("expected TCP endpoint");
            };
            port_pub.store(p, Ordering::Release);
            let payload = vec![b'x'; 64];
            while !stop_pub.load(Ordering::Relaxed) {
                let _ = pub_.send(Message::from_slice(&payload)).await;
            }
        });
    });

    let sub_thread = std::thread::spawn(move || {
        let rt = compio::runtime::RuntimeBuilder::new().build().unwrap();
        rt.block_on(async {
            while port.load(Ordering::Acquire) == 0 {
                compio::time::sleep(Duration::from_millis(10)).await;
            }
            let p = port.load(Ordering::Acquire);
            let sub = Socket::new(SocketType::Sub, Options::default());
            sub.subscribe(bytes::Bytes::new()).await.unwrap();
            sub.connect(test_support::tcp_loopback(p)).await.unwrap();

            compio::time::timeout(Duration::from_secs(5), sub.recv()).await
        })
    });

    let sub_result = sub_thread.join().expect("sub thread panicked");
    stop.store(true, Ordering::Relaxed);
    pub_thread.join().expect("pub thread panicked");
    let msg = sub_result
        .expect("SUB timed out: PUB tight send loop starved the runtime")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap().len(), 64);
}

/// PUB with multiple TCP subscribe-all subscribers exercises the
/// direct-write fan-out path (bypasses flume, encodes once, pushes
/// chunks into each peer's `EncodedQueue` directly).
#[test]
fn pub_tcp_multi_sub_direct_write() {
    let port = Arc::new(AtomicU16::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let port_pub = port.clone();
    let stop_pub = stop.clone();
    let pub_thread = std::thread::spawn(move || {
        let rt = compio::runtime::RuntimeBuilder::new().build().unwrap();
        rt.block_on(async {
            let pub_ = Socket::new(SocketType::Pub, Options::default());
            let bound = pub_.bind(test_support::tcp_loopback(0)).await.unwrap();
            let Endpoint::Tcp { port: p, .. } = bound else {
                panic!("expected TCP endpoint");
            };
            port_pub.store(p, Ordering::Release);
            let mut seq = 0u64;
            while !stop_pub.load(Ordering::Relaxed) {
                let _ = pub_.send(Message::single(seq.to_le_bytes().to_vec())).await;
                seq += 1;
            }
        });
    });

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let port = port.clone();
            std::thread::spawn(move || {
                let rt = compio::runtime::RuntimeBuilder::new().build().unwrap();
                rt.block_on(async {
                    while port.load(Ordering::Acquire) == 0 {
                        compio::time::sleep(Duration::from_millis(5)).await;
                    }
                    let p = port.load(Ordering::Acquire);
                    let sub = Socket::new(SocketType::Sub, Options::default());
                    sub.subscribe(bytes::Bytes::new()).await.unwrap();
                    sub.connect(test_support::tcp_loopback(p)).await.unwrap();

                    let mut received = 0u64;
                    let deadline = std::time::Instant::now() + Duration::from_secs(2);
                    while std::time::Instant::now() < deadline && received < 100 {
                        match compio::time::timeout(Duration::from_secs(5), sub.recv()).await {
                            Ok(Ok(m)) => {
                                assert_eq!(
                                    m.part_bytes(0).unwrap().len(),
                                    8,
                                    "unexpected payload size"
                                );
                                received += 1;
                            }
                            _ => break,
                        }
                    }
                    assert!(
                        received >= 10,
                        "expected at least 10 messages, got {received}"
                    );
                    received
                })
            })
        })
        .collect();

    let results: Vec<_> = handles
        .into_iter()
        .map(std::thread::JoinHandle::join)
        .collect();
    stop.store(true, Ordering::Relaxed);
    pub_thread.join().expect("pub thread panicked");
    let counts: Vec<u64> = results
        .into_iter()
        .map(|r| r.expect("sub thread panicked"))
        .collect();
    assert!(
        counts.iter().all(|&c| c >= 10),
        "all subs must receive messages: {counts:?}"
    );
}

/// PUB over lz4+tcp with multiple subscribe-all subscribers. The
/// direct-write fan-out path must fall back to regular SendMessage
/// for transform peers (pre-encoded ZMTP frames lack the compression
/// sentinel). This test catches the bug where SendEncoded bypassed
/// the transform.
#[cfg(feature = "lz4")]
#[test]
fn pub_lz4_tcp_multi_sub_correctness() {
    use omq_compio::endpoint::Host;

    let port = Arc::new(AtomicU16::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let port_pub = port.clone();
    let stop_pub = stop.clone();
    let pub_thread = std::thread::spawn(move || {
        let rt = compio::runtime::RuntimeBuilder::new().build().unwrap();
        rt.block_on(async {
            let pub_ = Socket::new(SocketType::Pub, Options::default());
            let bound = pub_
                .bind(Endpoint::Lz4Tcp {
                    host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
                    port: 0,
                })
                .await
                .unwrap();
            let Endpoint::Lz4Tcp { port: p, .. } = bound else {
                panic!("expected Lz4Tcp endpoint");
            };
            port_pub.store(p, Ordering::Release);
            let mut seq = 0u32;
            while !stop_pub.load(Ordering::Relaxed) {
                let payload = format!("msg-{seq}");
                let _ = pub_.send(Message::single(payload)).await;
                seq += 1;
            }
        });
    });

    let handles: Vec<_> = (0..3)
        .map(|_| {
            let port = port.clone();
            std::thread::spawn(move || {
                let rt = compio::runtime::RuntimeBuilder::new().build().unwrap();
                rt.block_on(async {
                    while port.load(Ordering::Acquire) == 0 {
                        compio::time::sleep(Duration::from_millis(5)).await;
                    }
                    let p = port.load(Ordering::Acquire);
                    let sub = Socket::new(SocketType::Sub, Options::default());
                    sub.subscribe(bytes::Bytes::new()).await.unwrap();
                    sub.connect(Endpoint::Lz4Tcp {
                        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
                        port: p,
                    })
                    .await
                    .unwrap();

                    let mut received = 0u64;
                    let deadline = std::time::Instant::now() + Duration::from_secs(3);
                    while std::time::Instant::now() < deadline && received < 50 {
                        match compio::time::timeout(Duration::from_secs(5), sub.recv()).await {
                            Ok(Ok(m)) => {
                                let body = m.part_bytes(0).unwrap();
                                let s = std::str::from_utf8(&body)
                                    .expect("payload must be valid UTF-8");
                                assert!(s.starts_with("msg-"), "unexpected payload: {s:?}");
                                received += 1;
                            }
                            _ => break,
                        }
                    }
                    assert!(
                        received >= 5,
                        "expected at least 5 lz4 messages, got {received}"
                    );
                    received
                })
            })
        })
        .collect();

    let results: Vec<_> = handles
        .into_iter()
        .map(std::thread::JoinHandle::join)
        .collect();
    stop.store(true, Ordering::Relaxed);
    pub_thread.join().expect("pub thread panicked");
    let counts: Vec<u64> = results
        .into_iter()
        .map(|r| r.expect("sub thread panicked"))
        .collect();
    assert!(
        counts.iter().all(|&c| c >= 5),
        "all lz4 subs must receive correct messages: {counts:?}"
    );
}

/// PUB with subscriber churn: subscribers connect, receive messages,
/// disconnect, and new subscribers connect. The `pub_direct_io_cache`
/// must be invalidated on peer remove and rebuilt when new peers
/// subscribe. Catches stale-cache bugs in `recompute_pub_all_match_all`.
#[compio::test]
async fn pub_direct_write_survives_subscriber_churn() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let mut mon = pub_.monitor();
    pub_.bind(test_support::tcp_loopback(0)).await.unwrap();
    let port = loop {
        match mon.recv().await {
            Ok(omq_compio::MonitorEvent::Listening {
                endpoint: Endpoint::Tcp { port, .. },
            }) => break port,
            Ok(_) => {}
            other => panic!("expected Listening, got {other:?}"),
        }
    };

    for round in 0..3u32 {
        let sub1 = Socket::new(SocketType::Sub, Options::default());
        sub1.subscribe(bytes::Bytes::new()).await.unwrap();
        sub1.connect(test_support::tcp_loopback(port))
            .await
            .unwrap();

        let sub2 = Socket::new(SocketType::Sub, Options::default());
        sub2.subscribe(bytes::Bytes::new()).await.unwrap();
        sub2.connect(test_support::tcp_loopback(port))
            .await
            .unwrap();

        compio::time::sleep(Duration::from_millis(100)).await;

        let tag = format!("round-{round}");
        pub_.send(Message::single(tag.clone())).await.unwrap();

        let m1 = compio::time::timeout(Duration::from_secs(2), sub1.recv())
            .await
            .expect("sub1 timed out")
            .unwrap();
        assert_eq!(m1.part_bytes(0).unwrap(), tag.as_bytes());

        let m2 = compio::time::timeout(Duration::from_secs(2), sub2.recv())
            .await
            .expect("sub2 timed out")
            .unwrap();
        assert_eq!(m2.part_bytes(0).unwrap(), tag.as_bytes());

        drop(sub1);
        drop(sub2);
        compio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// PUB tight-send loop with active subscribers on single-threaded
/// compio. The direct-write path is sync (no .await), so the sender
/// must still yield often enough for the driver to flush and for
/// new connections to be accepted. This catches starvation regressions.
#[test]
fn pub_direct_write_tight_loop_does_not_starve_runtime() {
    let port = Arc::new(AtomicU16::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let port_pub = port.clone();
    let stop_pub = stop.clone();
    let pub_thread = std::thread::spawn(move || {
        let rt = compio::runtime::RuntimeBuilder::new().build().unwrap();
        rt.block_on(async {
            let pub_ = Socket::new(SocketType::Pub, Options::default());
            let bound = pub_.bind(test_support::tcp_loopback(0)).await.unwrap();
            let Endpoint::Tcp { port: p, .. } = bound else {
                panic!("expected TCP endpoint");
            };
            port_pub.store(p, Ordering::Release);
            while !stop_pub.load(Ordering::Relaxed) {
                let _ = pub_.send(Message::single("tick")).await;
            }
        });
    });

    let done = stop.clone();
    let sub_thread = std::thread::spawn(move || {
        let rt = compio::runtime::RuntimeBuilder::new().build().unwrap();
        rt.block_on(async {
            while port.load(Ordering::Acquire) == 0 {
                compio::time::sleep(Duration::from_millis(5)).await;
            }
            let p = port.load(Ordering::Acquire);

            let sub = Socket::new(SocketType::Sub, Options::default());
            sub.subscribe(bytes::Bytes::new()).await.unwrap();
            sub.connect(test_support::tcp_loopback(p)).await.unwrap();

            let first = compio::time::timeout(Duration::from_secs(10), sub.recv()).await;
            assert!(first.is_ok(), "first recv timed out: runtime starved");

            // Second subscriber connects while PUB is in tight direct-write loop.
            let sub2 = Socket::new(SocketType::Sub, Options::default());
            sub2.subscribe(bytes::Bytes::new()).await.unwrap();
            sub2.connect(test_support::tcp_loopback(p)).await.unwrap();

            let second = compio::time::timeout(Duration::from_secs(10), sub2.recv()).await;
            assert!(
                second.is_ok(),
                "second sub timed out: PUB accept loop starved during direct-write"
            );

            done.store(true, Ordering::Relaxed);
        });
    });

    sub_thread.join().expect("sub thread panicked");
    stop.store(true, Ordering::Relaxed);
    pub_thread.join().expect("pub thread panicked");
}

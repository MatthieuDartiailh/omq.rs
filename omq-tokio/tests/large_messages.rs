//! Large messages over NULL mechanism TCP.
//!
//! Exercises multi-chunk Payload / scatter-gather framing at payload sizes
//! that span many TCP segments. Encryption masks framing bugs in CURVE/BLAKE3
//! suites; this file tests plain framing directly.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn loopback_port() -> u16 {
    let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

async fn push_pull_large(size_bytes: usize) {
    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp_ep(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let payload: Vec<u8> = (0..size_bytes).map(|i| (i & 0xFF) as u8).collect();
    push.send(Message::single(payload.clone())).await.unwrap();

    let m = tokio::time::timeout(Duration::from_secs(10), pull.recv())
        .await
        .expect("large message recv timed out")
        .unwrap();
    let got = m.parts()[0].as_bytes();
    assert_eq!(
        got.len(),
        size_bytes,
        "payload length mismatch at {size_bytes} B"
    );
    assert_eq!(
        &*got,
        &payload[..],
        "payload data corrupted at {size_bytes} B"
    );
}

#[tokio::test]
async fn large_message_64kib() {
    push_pull_large(64 * 1024).await;
}

#[tokio::test]
async fn large_message_256kib() {
    push_pull_large(256 * 1024).await;
}

#[tokio::test]
async fn large_message_1mib() {
    push_pull_large(1024 * 1024).await;
}

#[tokio::test]
async fn large_multipart_over_tcp() {
    let part_size = 256 * 1024;
    let port = loopback_port();
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(tcp_ep(port)).await.unwrap();
    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(tcp_ep(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let part_a: Vec<u8> = vec![0xAA; part_size];
    let part_b: Vec<u8> = vec![0xBB; part_size];

    req.send(Message::multipart([part_a, part_b]))
        .await
        .unwrap();

    let m = tokio::time::timeout(Duration::from_secs(10), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 2, "expected 2-part message");
    assert_eq!(m.parts()[0].as_bytes().len(), part_size);
    assert_eq!(*m.parts()[0].as_bytes().first().unwrap(), 0xAA);
    assert_eq!(m.parts()[1].as_bytes().len(), part_size);
    assert_eq!(*m.parts()[1].as_bytes().first().unwrap(), 0xBB);
}

#[tokio::test]
async fn huge_messages_xxhash() {
    use xxhash_rust::xxh3::xxh3_128;

    const SIZES: [usize; 3] = [100 * 1024 * 1024, 200 * 1024 * 1024, 500 * 1024 * 1024];

    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp_ep(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut hashes = [0u128; 3];
    for (i, &size) in SIZES.iter().enumerate() {
        let payload: Vec<u8> = (0u64..size as u64)
            .map(|j| {
                j.wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407)
                    .to_be_bytes()[0]
            })
            .collect();
        hashes[i] = xxh3_128(&payload);
        push.send(Message::single(payload)).await.unwrap();
    }

    for (i, expected) in hashes.iter().enumerate() {
        let m = tokio::time::timeout(Duration::from_mins(2), pull.recv())
            .await
            .unwrap_or_else(|_| panic!("recv timed out for message {i}"))
            .unwrap();
        let got = m.parts()[0].as_bytes();
        assert_eq!(got.len(), SIZES[i], "length mismatch on message {i}");
        assert_eq!(
            xxh3_128(&got),
            *expected,
            "xxh3-128 mismatch on message {i} — payload corrupted in transit"
        );
    }
}

#[tokio::test]
async fn large_message_back_to_back() {
    // Two large messages sent without waiting; verifies framing continuity.
    let size = 128 * 1024;
    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp_ep(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let p1: Vec<u8> = vec![0x11; size];
    let p2: Vec<u8> = vec![0x22; size];
    push.send(Message::single(p1.clone())).await.unwrap();
    push.send(Message::single(p2.clone())).await.unwrap();

    let m1 = tokio::time::timeout(Duration::from_secs(5), pull.recv())
        .await
        .unwrap()
        .unwrap();
    let m2 = tokio::time::timeout(Duration::from_secs(5), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&*m1.parts()[0].as_bytes(), &p1[..]);
    assert_eq!(&*m2.parts()[0].as_bytes(), &p2[..]);
}

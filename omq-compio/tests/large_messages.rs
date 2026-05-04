//! Large messages over NULL mechanism TCP.
//!
//! Exercises multi-chunk Payload / scatter-gather framing at payload sizes
//! that span many TCP segments.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

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
    compio::time::sleep(Duration::from_millis(50)).await;

    let payload: Vec<u8> = (0..size_bytes).map(|i| (i & 0xFF) as u8).collect();
    push.send(Message::single(payload.clone())).await.unwrap();

    let m = compio::time::timeout(Duration::from_secs(10), pull.recv())
        .await
        .expect("large message recv timed out")
        .unwrap();
    let got = m.parts()[0].as_bytes();
    assert_eq!(got.len(), size_bytes, "payload length mismatch at {size_bytes} B");
    assert_eq!(&*got, &payload[..], "payload data corrupted at {size_bytes} B");
}

#[compio::test]
async fn large_message_64kib() {
    push_pull_large(64 * 1024).await;
}

#[compio::test]
async fn large_message_256kib() {
    push_pull_large(256 * 1024).await;
}

#[compio::test]
async fn large_message_1mib() {
    push_pull_large(1024 * 1024).await;
}

#[compio::test]
async fn large_multipart_over_tcp() {
    let part_size = 256 * 1024;
    let port = loopback_port();
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(tcp_ep(port)).await.unwrap();
    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(tcp_ep(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    let part_a: Vec<u8> = vec![0xAA; part_size];
    let part_b: Vec<u8> = vec![0xBB; part_size];

    req.send(Message::multipart([part_a, part_b]))
        .await
        .unwrap();

    let m = compio::time::timeout(Duration::from_secs(10), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 2, "expected 2-part message");
    assert_eq!(m.parts()[0].as_bytes().len(), part_size);
    assert_eq!(*m.parts()[0].as_bytes().first().unwrap(), 0xAA);
    assert_eq!(m.parts()[1].as_bytes().len(), part_size);
    assert_eq!(*m.parts()[1].as_bytes().first().unwrap(), 0xBB);
}

#[compio::test]
async fn large_message_back_to_back() {
    let size = 128 * 1024;
    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp_ep(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    let p1: Vec<u8> = vec![0x11; size];
    let p2: Vec<u8> = vec![0x22; size];
    push.send(Message::single(p1.clone())).await.unwrap();
    push.send(Message::single(p2.clone())).await.unwrap();

    let m1 = compio::time::timeout(Duration::from_secs(5), pull.recv())
        .await
        .unwrap()
        .unwrap();
    let m2 = compio::time::timeout(Duration::from_secs(5), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&*m1.parts()[0].as_bytes(), &p1[..]);
    assert_eq!(&*m2.parts()[0].as_bytes(), &p2[..]);
}

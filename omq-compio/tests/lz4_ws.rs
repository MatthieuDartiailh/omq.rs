#![cfg(all(feature = "lz4", feature = "ws"))]

use std::time::Duration;

use bytes::Bytes;
use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

async fn pull_on_loopback() -> (Socket, Endpoint) {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let mut mon = pull.monitor();
    pull.bind(Endpoint::Lz4Ws {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port: 0,
        path: "/".to_string(),
    })
    .await
    .unwrap();
    let ev = compio::time::timeout(Duration::from_millis(500), mon.recv())
        .await
        .unwrap()
        .unwrap();
    let port = match ev {
        MonitorEvent::Listening {
            endpoint: Endpoint::Lz4Ws { port, .. },
        } => port,
        other => panic!("expected Lz4Ws Listening, got {other:?}"),
    };
    (
        pull,
        Endpoint::Lz4Ws {
            host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            port,
            path: "/".to_string(),
        },
    )
}

#[compio::test]
async fn lz4_ws_small_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single("hello over lz4+ws"))
        .await
        .unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"hello over lz4+ws"[..]);
}

#[compio::test]
async fn lz4_ws_large_compressible_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    let payload = Bytes::from(vec![b'A'; 16 * 1024]);
    push.send(Message::single(payload.clone())).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&m.part_bytes(0).unwrap()[..], &payload[..]);
}

#[compio::test]
async fn lz4_ws_multipart_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::multipart(["a", "bb", "ccc"]))
        .await
        .unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 3);
    assert_eq!(m.part_bytes(0).unwrap(), &b"a"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"bb"[..]);
    assert_eq!(m.part_bytes(2).unwrap(), &b"ccc"[..]);
}

#![cfg(feature = "lz4")]

//! Pub/sub over `lz4+tcp://` — used to verify the README example.

use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

async fn pub_on_loopback() -> (Socket, Endpoint) {
    let publisher = Socket::new(SocketType::Pub, Options::default());
    let mut mon = publisher.monitor();
    publisher
        .bind(Endpoint::Lz4Tcp {
            host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            port: 0,
        })
        .await
        .unwrap();
    let ev = tokio::time::timeout(Duration::from_millis(500), mon.recv())
        .await
        .unwrap()
        .unwrap();
    let port = match ev {
        MonitorEvent::Listening {
            endpoint: Endpoint::Lz4Tcp { port, .. },
        } => port,
        other => panic!("expected Lz4Tcp Listening, got {other:?}"),
    };
    let connect_ep = Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    };
    (publisher, connect_ep)
}

#[tokio::test]
async fn pub_sub_prefix_filter() {
    let (publisher, ep) = pub_on_loopback().await;

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(ep).await.unwrap();
    subscriber.subscribe("news.").await.unwrap();

    // Let the SUBSCRIBE command travel from SUB -> PUB over the wire.
    tokio::time::sleep(Duration::from_millis(50)).await;

    publisher
        .send(Message::multipart(["news.sports", "ball scores"]))
        .await
        .unwrap();
    publisher
        .send(Message::multipart(["weather", "sunny"]))
        .await
        .unwrap();
    publisher
        .send(Message::multipart(["news.tech", "rust 2.0"]))
        .await
        .unwrap();

    let m1 = tokio::time::timeout(Duration::from_secs(1), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m1.parts()[0].as_bytes(), &b"news.sports"[..]);
    assert_eq!(m1.parts()[1].as_bytes(), &b"ball scores"[..]);

    let m2 = tokio::time::timeout(Duration::from_secs(1), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m2.parts()[0].as_bytes(), &b"news.tech"[..]);
    assert_eq!(m2.parts()[1].as_bytes(), &b"rust 2.0"[..]);

    // "weather" must never arrive.
    let nothing = tokio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(
        nothing.is_err(),
        "non-matching message must not be delivered"
    );
}

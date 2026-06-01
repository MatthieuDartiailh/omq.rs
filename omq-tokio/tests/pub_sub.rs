//! PUB / SUB integration tests.

use std::time::Duration;

use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn pub_sub_simple_prefix_match() {
    let ep = inproc_ep("ps-simple");
    let publisher = Socket::new(SocketType::Pub, Options::default());
    publisher.bind(ep.clone()).await.unwrap();

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(ep).await.unwrap();
    subscriber.subscribe("news.").await.unwrap();

    // Matches: prefix "news."
    publisher
        .send(Message::multipart(["news.sports", "ball scores"]))
        .await
        .unwrap();
    // Doesn't match.
    publisher
        .send(Message::multipart(["weather", "sunny"]))
        .await
        .unwrap();
    // Matches.
    publisher
        .send(Message::multipart(["news.tech", "rust 1.85"]))
        .await
        .unwrap();

    let got1 = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    let got2 = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got1.part_bytes(0).unwrap(), &b"news.sports"[..]);
    assert_eq!(got1.part_bytes(1).unwrap(), &b"ball scores"[..]);
    assert_eq!(got2.part_bytes(0).unwrap(), &b"news.tech"[..]);

    // No third message -- 'weather' was filtered.
    let third = tokio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(third.is_err(), "non-matching message must not be delivered");
}

#[tokio::test]
async fn pub_sub_late_subscriber_misses_earlier() {
    // Classic ZMQ late-joiner semantic: messages published before the
    // subscriber's SUBSCRIBE reaches the PUB are lost.
    let ep = inproc_ep("ps-late");
    let publisher = Socket::new(SocketType::Pub, Options::default());
    publisher.bind(ep.clone()).await.unwrap();

    // Send before any subscriber exists.
    publisher
        .send(Message::single("pre-subscribe"))
        .await
        .unwrap();

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(ep).await.unwrap();
    subscriber.subscribe("").await.unwrap(); // match all

    publisher
        .send(Message::single("post-subscribe"))
        .await
        .unwrap();

    let m = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"post-subscribe"[..]);

    // The pre-subscribe message must NOT arrive.
    let other = tokio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(other.is_err());
}

#[tokio::test]
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
        let m = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), expected.as_bytes());
    }
}

#[tokio::test]
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
    // Drain both.
    let m1 = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    let m2 = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    let got = [m1.part_bytes(0).unwrap(), m2.part_bytes(0).unwrap()];
    assert!(got.contains(&bytes::Bytes::from_static(b"apple")));
    assert!(got.contains(&bytes::Bytes::from_static(b"banana")));

    subscriber.unsubscribe("b").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    publisher.send(Message::single("apricot")).await.unwrap();
    publisher.send(Message::single("blueberry")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"apricot"[..]);

    // blueberry filtered out.
    let other = tokio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(other.is_err());
}

#[tokio::test]
async fn sub_replays_subscriptions_on_new_peer() {
    // Subscribe BEFORE connecting to any PUB. Then connect. SUBSCRIBE must
    // be replayed to the new peer as part of its HandshakeSucceeded hook.
    let ep = inproc_ep("ps-replay");

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.subscribe("x.").await.unwrap();

    let publisher = Socket::new(SocketType::Pub, Options::default());
    publisher.bind(ep.clone()).await.unwrap();
    subscriber.connect(ep).await.unwrap();

    publisher.send(Message::single("x.hello")).await.unwrap();
    publisher.send(Message::single("y.nope")).await.unwrap();

    let m = tokio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"x.hello"[..]);
    let other = tokio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(other.is_err());
}

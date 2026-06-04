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

#![cfg(feature = "lz4")]

//! Pub/sub over `lz4+tcp://`.

mod test_support;

use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

fn lz4_loopback() -> Endpoint {
    Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port: 0,
    }
}

async fn bind_lz4_loopback(sock: &Socket) -> (Endpoint, omq_tokio::MonitorStream) {
    let mut mon = sock.monitor();
    sock.bind(lz4_loopback()).await.unwrap();
    let port = loop {
        if let MonitorEvent::Listening {
            endpoint: Endpoint::Lz4Tcp { port, .. },
        } = mon.recv().await.unwrap()
        {
            break port;
        }
    };
    let ep = Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    };
    (ep, mon)
}

async fn wait_for_subscribes(mon: &mut omq_tokio::MonitorStream, n: usize) {
    let fut = async {
        let mut count = 0;
        while count < n {
            match mon.recv().await {
                Ok(MonitorEvent::SubscribeReceived { .. }) => count += 1,
                Ok(_) => {}
                Err(e) => {
                    panic!("monitor closed after {count}/{n} subscribes: {e:?}")
                }
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("subscribes did not propagate within 5s");
}

#[tokio::test]
async fn pub_sub_prefix_filter() {
    let publisher = Socket::new(SocketType::Pub, Options::default());
    let (ep, mut mon) = bind_lz4_loopback(&publisher).await;

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(ep).await.unwrap();
    subscriber.subscribe("news.").await.unwrap();

    wait_for_subscribes(&mut mon, 1).await;

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
    assert_eq!(m1.part_bytes(0).unwrap(), &b"news.sports"[..]);
    assert_eq!(m1.part_bytes(1).unwrap(), &b"ball scores"[..]);

    let m2 = tokio::time::timeout(Duration::from_secs(1), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m2.part_bytes(0).unwrap(), &b"news.tech"[..]);
    assert_eq!(m2.part_bytes(1).unwrap(), &b"rust 2.0"[..]);

    let nothing = tokio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(
        nothing.is_err(),
        "non-matching message must not be delivered"
    );
}

/// Fan-out to multiple subscribers: exercises the multi-target
/// `dispatch_to_targets` path that encodes once and pushes
/// pre-encoded compressed bytes to all peers.
#[tokio::test]
async fn pub_sub_lz4_fan_out() {
    const N_SUBS: usize = 4;
    const N_MSGS: usize = 200;

    let publisher = Socket::new(SocketType::Pub, Options::default());
    let (ep, mut mon) = bind_lz4_loopback(&publisher).await;

    let mut subs = Vec::with_capacity(N_SUBS);
    for _ in 0..N_SUBS {
        let s = Socket::new(SocketType::Sub, Options::default());
        s.connect(ep.clone()).await.unwrap();
        s.subscribe(bytes::Bytes::new()).await.unwrap();
        subs.push(s);
    }
    wait_for_subscribes(&mut mon, N_SUBS).await;

    for i in 0..N_MSGS {
        let body = format!("msg-{i:04}");
        publisher
            .send(Message::single(bytes::Bytes::from(body)))
            .await
            .unwrap();
    }

    for (si, sub) in subs.iter().enumerate() {
        for i in 0..N_MSGS {
            let m = tokio::time::timeout(Duration::from_secs(5), sub.recv())
                .await
                .unwrap_or_else(|_| panic!("sub {si} timed out at msg {i}"))
                .unwrap();
            let expected = format!("msg-{i:04}");
            assert_eq!(
                m.part_bytes(0).unwrap(),
                expected.as_bytes(),
                "sub {si} msg {i}"
            );
        }
    }
}

/// Fan-out with a compression dictionary configured. The fan-out
/// encoder currently falls back to dictless compression (dict
/// shipment requires the per-connection driver path). Verifies
/// messages still arrive correctly.
#[tokio::test]
async fn pub_sub_lz4_fan_out_with_dict() {
    use omq_proto::proto::transform::lz4::DictTrainer;

    const N_SUBS: usize = 4;
    const N_MSGS: usize = 50;

    let mut trainer = DictTrainer::new(2048);
    for i in 0..100 {
        let s = format!(r#"{{"seq":{i},"msg":"hello world","tag":"bench"}}"#);
        trainer.add_sample(s.as_bytes());
    }
    let dict = bytes::Bytes::from(trainer.train());
    let opts = Options::default().compression_dict(dict);

    let publisher = Socket::new(SocketType::Pub, opts.clone());
    let (ep, mut mon) = bind_lz4_loopback(&publisher).await;

    let mut subs = Vec::with_capacity(N_SUBS);
    for _ in 0..N_SUBS {
        let s = Socket::new(SocketType::Sub, opts.clone());
        s.connect(ep.clone()).await.unwrap();
        s.subscribe(bytes::Bytes::new()).await.unwrap();
        subs.push(s);
    }
    wait_for_subscribes(&mut mon, N_SUBS).await;

    for i in 0..N_MSGS {
        let body = format!(r#"{{"seq":{i},"msg":"hello world","tag":"bench"}}"#);
        publisher
            .send(Message::single(bytes::Bytes::from(body)))
            .await
            .unwrap();
    }

    for (si, sub) in subs.iter().enumerate() {
        for i in 0..N_MSGS {
            let m = tokio::time::timeout(Duration::from_secs(5), sub.recv())
                .await
                .unwrap_or_else(|_| panic!("sub {si} timed out at msg {i}"))
                .unwrap();
            let expected = format!(r#"{{"seq":{i},"msg":"hello world","tag":"bench"}}"#);
            assert_eq!(
                m.part_bytes(0).unwrap(),
                expected.as_bytes(),
                "sub {si} msg {i}"
            );
        }
    }
}

#![cfg(feature = "zstd")]

//! `zstd+tcp://` integration test for omq-compio.

use std::time::Duration;

use bytes::Bytes;
use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};
use omq_proto::proto::transform::train_zdict;

/// Train a small ZDICT-format dict from 200 copies of `seed`. Used by
/// the static-dict tests so the bytes pass `with_send_dict`'s ZDICT
/// magic check (added when zstd dict shipment was made wire-compatible
/// with omq-zstd Ruby).
fn make_test_dict(seed: &[u8]) -> Bytes {
    let samples: Vec<&[u8]> = (0..200).map(|_| seed).collect();
    train_zdict(&samples, 8 * 1024).expect("train_zdict")
}

async fn pull_on_loopback() -> (Socket, Endpoint) {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let mut mon = pull.monitor();
    pull.bind(Endpoint::ZstdTcp {
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
            endpoint: Endpoint::ZstdTcp { port, .. },
        } => port,
        other => panic!("expected ZstdTcp Listening, got {other:?}"),
    };
    (
        pull,
        Endpoint::ZstdTcp {
            host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            port,
        },
    )
}

#[compio::test]
async fn zstd_small_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single("hello over zstd")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"hello over zstd"[..]);
}

#[compio::test]
async fn zstd_large_compressible_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    let payload = Bytes::from(vec![b'A'; 16 * 1024]);
    push.send(Message::single(payload.clone())).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&m.part_bytes(0).unwrap()[..], &payload[..]);
}

#[compio::test]
async fn zstd_multipart_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::multipart(["a", "bb", "ccc"]))
        .await
        .unwrap();
    let m = compio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 3);
    assert_eq!(m.part_bytes(0).unwrap(), &b"a"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"bb"[..]);
    assert_eq!(m.part_bytes(2).unwrap(), &b"ccc"[..]);
}

/// Regression: `close()` used to cancel the dialer supervisor task (and
/// therefore the driver) before the linger drain completed, causing zero
/// messages to be delivered. Verify that all sends queued before `close()`
/// arrive on the peer.
#[compio::test]
async fn zstd_linger_drains_before_close() {
    const N: usize = 100;
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(
        SocketType::Push,
        Options::default().linger(Duration::from_secs(2)),
    );
    push.connect(ep).await.unwrap();

    let payload = Bytes::from(vec![b'Z'; 512]);
    for _ in 0..N {
        push.send(Message::single(payload.clone())).await.unwrap();
    }
    // close() must wait for the driver to flush all N messages before returning.
    push.close().await.unwrap();

    let mut got = 0usize;
    while let Ok(Ok(_)) = compio::time::timeout(Duration::from_millis(200), pull.recv()).await {
        got += 1;
    }
    assert_eq!(got, N, "linger did not drain all messages: got {got}/{N}");
}

#[compio::test]
async fn static_dict_survives_reconnect() {
    let dict = make_test_dict(b"omq-shared-dict-prefix-payload\n");
    let push_opts = || {
        Options::default()
            .compression_dict(dict.clone())
            .linger(Duration::from_secs(2))
    };
    let (pull, ep) = pull_on_loopback().await;
    let payload = vec![b'z'; 100]; // > 64 B with-dict threshold

    for _ in 0..2 {
        let push = Socket::new(SocketType::Push, push_opts());
        push.connect(ep.clone()).await.unwrap();
        for _ in 0..5 {
            push.send(Message::single(payload.clone())).await.unwrap();
        }
        push.close().await.unwrap();
    }

    for _ in 0..10 {
        let m = compio::time::timeout(Duration::from_secs(3), pull.recv())
            .await
            .expect("timed out")
            .expect("recv error");
        assert_eq!(m.part_bytes(0).unwrap().to_vec(), payload);
    }
}

#[compio::test]
async fn auto_train_survives_reconnect() {
    let (pull, ep) = pull_on_loopback().await;

    let make_payload = |i: usize| -> Vec<u8> {
        let prefix = format!("{i:05}|");
        let mut v = prefix.into_bytes();
        v.extend(
            b"omq-zstd-auto-train-reconnect-test-payload-"
                .iter()
                .cycle()
                .take(1000 - v.len()),
        );
        v
    };

    const FIRST: usize = 120;
    {
        let push = Socket::new(
            SocketType::Push,
            Options::default().linger(Duration::from_secs(4)),
        );
        push.connect(ep.clone()).await.unwrap();
        for i in 0..FIRST {
            push.send(Message::single(make_payload(i))).await.unwrap();
        }
        push.close().await.unwrap();
    }

    const SECOND: usize = 20;
    {
        let push = Socket::new(
            SocketType::Push,
            Options::default().linger(Duration::from_secs(2)),
        );
        push.connect(ep.clone()).await.unwrap();
        for i in 0..SECOND {
            push.send(Message::single(make_payload(i))).await.unwrap();
        }
        push.close().await.unwrap();
    }

    let mut got = 0;
    while let Ok(Ok(_)) = compio::time::timeout(Duration::from_millis(500), pull.recv()).await {
        got += 1;
        if got == FIRST + SECOND {
            break;
        }
    }
    assert_eq!(got, FIRST + SECOND);
}

#[compio::test]
async fn zstd_auto_train_end_to_end() {
    // Pull side leaves auto-train off; push side opts in. Once the
    // training threshold fires (1000 messages or 100 KiB), the next
    // outbound message ships a trained dictionary as a single-part
    // ZMTP message before the regular encoded payload - the pull
    // side decodes it transparently and continues delivering
    // plaintext to recv().
    let (pull, ep) = pull_on_loopback().await;
    // Default Options has auto-train enabled. Bump linger so close()
    // drains the post-training dict shipment + the last few sends
    // before the runtime tears down.
    let push = Socket::new(
        SocketType::Push,
        Options::default().linger(Duration::from_secs(1)),
    );
    push.connect(ep).await.unwrap();

    let sample = br#"{"event":"login","user":"alice","ip":"10.0.0.1","ok":true}"#;
    for _ in 0..1500 {
        push.send(Message::single(sample.as_slice())).await.unwrap();
    }
    push.close().await.unwrap();

    let mut got = 0usize;
    while let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(200), pull.recv()).await {
        assert_eq!(m.part_bytes(0).unwrap(), &sample[..]);
        got += 1;
    }
    assert!(
        got >= 1000,
        "auto-train flow lost too many messages: got {got}"
    );
}

#![cfg(feature = "zstd")]

//! End-to-end integration of the `zstd+tcp://` transport scheme.

use std::time::Duration;

use bytes::Bytes;
use omq_proto::proto::transform::train_zdict;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};
use rand::Rng;

/// Train a small ZDICT-format dict from 200 copies of `seed`. Used by
/// the static-dict tests so the bytes pass `with_send_dict`'s ZDICT
/// magic check (added when zstd dict shipment was made wire-compatible
/// with omq-zstd Ruby).
fn make_test_dict(seed: &[u8]) -> Bytes {
    let samples: Vec<&[u8]> = (0..200).map(|_| seed).collect();
    train_zdict(&samples, 8 * 1024).expect("train_zdict")
}

async fn wait_for_handshake(sock: &Socket) {
    let mut mon = sock.monitor();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match mon.recv().await {
                Ok(MonitorEvent::HandshakeSucceeded { .. }) => return,
                Ok(_) => {}
                Err(e) => panic!("monitor closed before handshake: {e:?}"),
            }
        }
    })
    .await
    .expect("handshake did not arrive within 5s");
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
    let ev = tokio::time::timeout(Duration::from_millis(500), mon.recv())
        .await
        .unwrap()
        .unwrap();
    let port = match ev {
        MonitorEvent::Listening {
            endpoint: Endpoint::ZstdTcp { port, .. },
        } => port,
        other => panic!("unexpected {other:?}"),
    };
    let connect_ep = Endpoint::ZstdTcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    };
    (pull, connect_ep)
}

#[tokio::test]
async fn small_plaintext_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single("hello")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"hello"[..]);
}

#[tokio::test]
async fn large_compressible_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    let plain = vec![b'Z'; 16 * 1024];
    push.send(Message::single(plain.clone())).await.unwrap();

    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().to_vec(), plain);
}

#[tokio::test]
async fn multipart_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    let big = vec![b'q'; 4096];
    let msg = Message::multipart::<_, Bytes>([
        Bytes::from_static(b"hdr"),
        Bytes::from(big.clone()),
        Bytes::from_static(b"tail"),
    ]);
    push.send(msg).await.unwrap();

    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 3);
    assert_eq!(m.part_bytes(0).unwrap(), &b"hdr"[..]);
    assert_eq!(m.part_bytes(1).unwrap().to_vec(), big);
    assert_eq!(m.part_bytes(2).unwrap(), &b"tail"[..]);
}

#[tokio::test]
async fn dict_roundtrip_small_payload() {
    let dict = make_test_dict(b"omq-omq-omq-omq-omq-omq-omq-omq-shared-prefix\n");

    let opts = || Options::default().compression_dict(dict.clone());
    let pull = Socket::new(SocketType::Pull, opts());
    let mut mon = pull.monitor();
    pull.bind(Endpoint::ZstdTcp {
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
            endpoint: Endpoint::ZstdTcp { port, .. },
        } => port,
        other => panic!("unexpected {other:?}"),
    };

    let push = Socket::new(SocketType::Push, opts());
    push.connect(Endpoint::ZstdTcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    })
    .await
    .unwrap();

    let plain = b"omq-".repeat(20); // 80 bytes - above with-dict threshold (64).
    for _ in 0..3 {
        push.send(Message::single(plain.clone())).await.unwrap();
        let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap().to_vec(), plain);
    }
}

#[tokio::test]
async fn empty_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single(Bytes::new())).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(m.part_bytes(0).unwrap().is_empty());
}

#[tokio::test]
async fn single_byte_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single(Bytes::from_static(&[0x42])))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &[0x42][..]);
}

#[tokio::test]
async fn incompressible_data_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    let mut random = vec![0u8; 8192];
    rand::rng().fill_bytes(&mut random);
    push.send(Message::single(random.clone())).await.unwrap();

    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().to_vec(), random);
}

#[tokio::test]
async fn req_rep_over_zstd() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let mut mon = rep.monitor();
    rep.bind(Endpoint::ZstdTcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port: 0,
    })
    .await
    .unwrap();
    let port = match tokio::time::timeout(Duration::from_millis(500), mon.recv())
        .await
        .unwrap()
        .unwrap()
    {
        MonitorEvent::Listening {
            endpoint: Endpoint::ZstdTcp { port, .. },
        } => port,
        other => panic!("unexpected {other:?}"),
    };
    let ep = Endpoint::ZstdTcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    };

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();

    req.send(Message::single("question")).await.unwrap();
    let q = tokio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q.part_bytes(0).unwrap(), &b"question"[..]);

    rep.send(Message::single("answer")).await.unwrap();
    let a = tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.part_bytes(0).unwrap(), &b"answer"[..]);
}

#[tokio::test]
async fn many_messages_in_a_row() {
    const N: usize = 200;
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    wait_for_handshake(&pull).await;

    for i in 0..N {
        push.send(Message::single(format!("m-{i}"))).await.unwrap();
    }
    for i in 0..N {
        let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), format!("m-{i}").as_bytes());
    }
}

// Reconnect: decoder fresh per connection, re-accepts dict shipment without
// "shipped twice" error. Push 1 ships dict → 5 msgs; push 2 ships same
// dict (fresh encoder) → 5 msgs. All 10 must arrive.
#[tokio::test]
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
        let m = tokio::time::timeout(Duration::from_secs(3), pull.recv())
            .await
            .expect("timed out waiting for message")
            .expect("recv error");
        assert_eq!(m.part_bytes(0).unwrap().to_vec(), payload);
    }
}

// Reconnect after auto-train fires. First connection accumulates > 100 KiB of
// samples, training fires, dict shipped, compressed messages follow. Second
// connection starts with a fresh encoder (auto-train re-starts from zero);
// all messages from both connections must be received correctly.
#[tokio::test]
async fn auto_train_survives_reconnect() {
    const FIRST: usize = 120; // > 103 training trigger point
    const SECOND: usize = 20;

    let (pull, ep) = pull_on_loopback().await;

    // 1 000-byte messages: < TRAIN_MAX_SAMPLE_LEN (1 024), so collected as
    // training samples. Byte limit (100 KiB) fires at message 103.
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

    // First connection: triggers training, then sends a few dict-compressed msgs.
    {
        let push = Socket::new(
            SocketType::Push,
            Options::default().linger(Duration::from_secs(4)),
        );
        push.connect(ep.clone()).await.unwrap();
        wait_for_handshake(&pull).await;
        for i in 0..FIRST {
            push.send(Message::single(make_payload(i))).await.unwrap();
        }
        push.close().await.unwrap();
    }

    // Second connection: fresh encoder, auto-train starts from scratch.
    // 20 msgs = 20 KiB, well below training threshold; sent as compressed
    // without dict (1 000 B > 512 B MIN_COMPRESS_NO_DICT).
    {
        let push = Socket::new(
            SocketType::Push,
            Options::default().linger(Duration::from_secs(2)),
        );
        push.connect(ep.clone()).await.unwrap();
        wait_for_handshake(&pull).await;
        for i in 0..SECOND {
            push.send(Message::single(make_payload(i))).await.unwrap();
        }
        push.close().await.unwrap();
    }

    let mut got = 0;
    while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(5), pull.recv()).await {
        got += 1;
        if got == FIRST + SECOND {
            break;
        }
    }
    assert_eq!(got, FIRST + SECOND);
}

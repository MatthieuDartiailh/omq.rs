//! Verify SPSC inproc fast paths recover after peer churn.

use std::time::Duration;

use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn inproc(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn send_ring_recovers_after_disconnect_reconnect() {
    let ep = inproc("spsc-recovery-reconnect");
    let pull = Socket::new(SocketType::Pull, Options::default());
    let push = Socket::new(SocketType::Push, Options::default());

    pull.bind(ep.clone()).await.unwrap();
    push.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(Message::from_slice(b"first")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap().as_ref(), b"first");

    push.disconnect(ep.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(Message::from_slice(b"second")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap().as_ref(), b"second");

    push.close().await.unwrap();
    pull.close().await.unwrap();
}

#[tokio::test]
async fn send_ring_reenabled_after_second_peer_leaves() {
    let ep1 = inproc("spsc-recovery-multi1");
    let ep2 = inproc("spsc-recovery-multi2");

    let pull1 = Socket::new(SocketType::Pull, Options::default());
    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let push = Socket::new(SocketType::Push, Options::default());

    pull1.bind(ep1.clone()).await.unwrap();
    pull2.bind(ep2.clone()).await.unwrap();
    push.connect(ep1.clone()).await.unwrap();
    push.connect(ep2.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // With 2 peers, messages round-robin.
    for _ in 0..4 {
        push.send(Message::from_slice(b"rr")).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drain both receivers.
    while pull1.try_recv().is_ok() {}
    while pull2.try_recv().is_ok() {}

    // Disconnect second peer.
    push.disconnect(ep2.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Now only pull1 remains. Fast path should re-enable.
    push.send(Message::from_slice(b"solo")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), pull1.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap().as_ref(), b"solo");

    push.close().await.unwrap();
    pull1.close().await.unwrap();
    pull2.close().await.unwrap();
}

#[tokio::test]
async fn consumers_cleaned_on_disconnect() {
    let ep = inproc("spsc-consumers-cleanup");

    let pull = Socket::new(SocketType::Pull, Options::default());
    let push = Socket::new(SocketType::Push, Options::default());

    pull.bind(ep.clone()).await.unwrap();
    push.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(Message::from_slice(b"a")).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();

    // Disconnect and reconnect multiple times. If consumers aren't
    // cleaned up, the Vec would grow unboundedly.
    for i in 0..5 {
        push.disconnect(ep.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        push.connect(ep.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;

        let payload = format!("iter-{i}");
        push.send(Message::from_slice(payload.as_bytes()))
            .await
            .unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_bytes());
    }

    push.close().await.unwrap();
    pull.close().await.unwrap();
}

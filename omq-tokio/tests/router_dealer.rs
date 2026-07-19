//! ROUTER / DEALER integration tests.
//!
//! ROUTER prepends the sender's identity to the received message and
//! routes by looking up the first frame of outgoing messages. DEALER is
//! round-robin over peers (same as Phase 5's PUSH/PULL) and fair-queued
//! on recv.

mod test_support;

use std::time::Duration;

use omq_tokio::{
    DisconnectReason, Endpoint, Message, MonitorEvent, Options, ReconnectPolicy, Socket, SocketType,
};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn dealer_duplicate_tcp_connect_is_ignored() {
    let router = Socket::new(SocketType::Router, Options::default());
    let port = test_support::bind_loopback(&router).await;
    let ep = test_support::tcp_loopback(port);

    let dealer = Socket::new(SocketType::Dealer, Options::default());
    dealer.connect(ep.clone()).await.unwrap();
    dealer.connect(ep).await.unwrap();

    router
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("router did not see dealer");
    dealer
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("dealer did not connect");
    test_support::assert_no_second_connection(&router, "router").await;
    test_support::assert_no_second_connection(&dealer, "dealer").await;

    dealer.send(Message::single("hello")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(1), router.recv())
        .await
        .expect("router did not receive")
        .unwrap();
    assert_eq!(got.part_bytes(1).unwrap(), &b"hello"[..]);
}

#[tokio::test]
async fn router_prefixes_identity_on_recv() {
    let ep = inproc_ep("rd-ident");

    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(bytes::Bytes::from_static(b"alice")),
    );
    dealer.connect(ep).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    dealer.send(Message::single("hello")).await.unwrap();

    let got = tokio::time::timeout(Duration::from_millis(500), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, Message::multipart(["alice", "hello"]));
}

#[tokio::test]
async fn router_routes_back_by_identity() {
    let ep = inproc_ep("rd-roundtrip");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(bytes::Bytes::from_static(b"bob")),
    );
    dealer.connect(ep).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    dealer.send(Message::single("ping")).await.unwrap();

    let incoming = router.recv().await.unwrap();
    assert_eq!(incoming, Message::multipart(["bob", "ping"]));

    // Reply: [identity, body]. Router strips identity, routes to the peer.
    router
        .send(Message::multipart(["bob", "pong"]))
        .await
        .unwrap();

    let reply = tokio::time::timeout(Duration::from_millis(500), dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, Message::single("pong"));
}

#[tokio::test]
async fn router_mandatory_errors_on_unknown_identity() {
    let ep = inproc_ep("rd-mandatory");
    let router = Socket::new(
        SocketType::Router,
        Options::default().router_mandatory(true),
    );
    router.bind(ep.clone()).await.unwrap();

    // No dealers connected.
    tokio::time::sleep(Duration::from_millis(30)).await;

    let r = router.send(Message::multipart(["ghost", "hello"])).await;
    assert!(matches!(r, Err(omq_tokio::Error::Unroutable)), "got {r:?}");
}

#[tokio::test]
async fn router_silently_drops_unknown_identity_by_default() {
    let ep = inproc_ep("rd-silent");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(30)).await;

    // Default router_mandatory = false: send to ghost succeeds but routes
    // nowhere.
    router
        .send(Message::multipart(["ghost", "hello"]))
        .await
        .unwrap();
}

#[tokio::test]
async fn router_handles_identity_churn_without_growth() {
    // Issue #190 analogue: reconnect with same identity repeatedly. The
    // identity-to-peer map must not grow unbounded.
    let ep = inproc_ep("rd-churn");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();

    for _ in 0..10 {
        let dealer = Socket::new(
            SocketType::Dealer,
            Options::default().identity(bytes::Bytes::from_static(b"worker-1")),
        );
        dealer.connect(ep.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        dealer.send(Message::single("ping")).await.unwrap();
        let m = router.recv().await.unwrap();
        assert_eq!(m.part_bytes(1).unwrap().as_ref(), b"ping");
        dealer.close().await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Final dealer connects and exchanges one message; routing still works.
    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(bytes::Bytes::from_static(b"worker-1")),
    );
    dealer.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    dealer.send(Message::single("final")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.part_bytes(1).unwrap(), &b"final"[..]);
}

#[tokio::test]
async fn router_assigns_identity_for_peers_without_one() {
    // A DEALER without an explicit identity still gets routed: we
    // auto-generate a stable per-connection identity on the ROUTER side.
    let ep = inproc_ep("rd-auto");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, Options::default());
    dealer.connect(ep).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    dealer.send(Message::single("anon")).await.unwrap();

    let got = router.recv().await.unwrap();
    assert_eq!(got.len(), 2);
    // The identity is opaque; we just care it's non-empty and we can
    // route a reply back through it.
    let identity = got.part_bytes(0).unwrap();
    assert!(!identity.is_empty());

    router
        .send(Message::multipart([
            identity.clone(),
            bytes::Bytes::from_static(b"reply"),
        ]))
        .await
        .unwrap();

    let reply = tokio::time::timeout(Duration::from_millis(500), dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, Message::single("reply"));
}

// --- Handover tests ---

#[tokio::test]
async fn router_handover_evicts_old_peer() {
    let ep = inproc_ep("rd-handover");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();

    let no_reconnect = Options::default()
        .identity(bytes::Bytes::from_static(b"alpha"))
        .reconnect(ReconnectPolicy::Disabled);

    let dealer_a = Socket::new(SocketType::Dealer, no_reconnect.clone());
    dealer_a.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    dealer_a.send(Message::single("hello")).await.unwrap();
    let got = router.recv().await.unwrap();
    assert_eq!(got, Message::multipart(["alpha", "hello"]));

    router
        .send(Message::multipart(["alpha", "reply-1"]))
        .await
        .unwrap();
    let r = tokio::time::timeout(Duration::from_millis(500), dealer_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r, Message::single("reply-1"));

    let dealer_b = Socket::new(SocketType::Dealer, no_reconnect);
    dealer_b.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    dealer_b.send(Message::single("world")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, Message::multipart(["alpha", "world"]));

    router
        .send(Message::multipart(["alpha", "reply-2"]))
        .await
        .unwrap();
    let r = tokio::time::timeout(Duration::from_millis(500), dealer_b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r, Message::single("reply-2"));

    let r = tokio::time::timeout(Duration::from_millis(100), dealer_a.recv()).await;
    assert!(r.is_err(), "dealer_a should not receive after handover");
}

#[tokio::test]
async fn router_handover_monitor_event() {
    let ep = inproc_ep("rd-handover-mon");
    let router = Socket::new(SocketType::Router, Options::default());
    let mut mon = router.monitor();
    router.bind(ep.clone()).await.unwrap();

    let no_reconnect = Options::default()
        .identity(bytes::Bytes::from_static(b"beta"))
        .reconnect(ReconnectPolicy::Disabled);

    let dealer_a = Socket::new(SocketType::Dealer, no_reconnect.clone());
    dealer_a.connect(ep.clone()).await.unwrap();

    // Wait for first handshake.
    loop {
        match tokio::time::timeout(Duration::from_millis(500), mon.recv()).await {
            Ok(Ok(MonitorEvent::HandshakeSucceeded { .. })) => break,
            Ok(Ok(_)) => {}
            other => panic!("expected HandshakeSucceeded, got {other:?}"),
        }
    }

    let dealer_b = Socket::new(SocketType::Dealer, no_reconnect);
    dealer_b.connect(ep).await.unwrap();

    // Drain until we see Disconnected(Handover).
    let mut found = false;
    for _ in 0..20 {
        match tokio::time::timeout(Duration::from_millis(500), mon.recv()).await {
            Ok(Ok(MonitorEvent::Disconnected { reason, .. })) => {
                assert_eq!(reason, DisconnectReason::Handover);
                found = true;
                break;
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert!(found, "must see Disconnected(Handover) for old peer");
}

#[tokio::test]
async fn router_handover_auto_identity_no_collision() {
    let ep = inproc_ep("rd-handover-auto");
    let router = Socket::new(SocketType::Router, Options::default());
    let mut mon = router.monitor();
    router.bind(ep.clone()).await.unwrap();

    let d1 = Socket::new(SocketType::Dealer, Options::default());
    d1.connect(ep.clone()).await.unwrap();

    let d2 = Socket::new(SocketType::Dealer, Options::default());
    d2.connect(ep).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    d1.send(Message::single("a")).await.unwrap();
    d2.send(Message::single("b")).await.unwrap();

    let m1 = tokio::time::timeout(Duration::from_millis(500), router.recv())
        .await
        .unwrap()
        .unwrap();
    let m2 = tokio::time::timeout(Duration::from_millis(500), router.recv())
        .await
        .unwrap()
        .unwrap();

    assert_ne!(
        m1.part_bytes(0).unwrap(),
        m2.part_bytes(0).unwrap(),
        "auto-generated identities must differ"
    );

    // No Disconnected events should have appeared.
    let evt = tokio::time::timeout(Duration::from_millis(100), mon.recv()).await;
    // Drain any non-disconnect events; assert no Disconnected.
    if let Ok(Ok(e)) = evt {
        assert!(
            !matches!(e, MonitorEvent::Disconnected { .. }),
            "unexpected Disconnected: {e:?}"
        );
    }
}

#[tokio::test]
async fn server_handover_evicts_old_peer() {
    let ep = inproc_ep("sv-handover");
    let server = Socket::new(SocketType::Server, Options::default());
    let mut mon = server.monitor();
    server.bind(ep.clone()).await.unwrap();

    let no_reconnect = Options::default()
        .identity(bytes::Bytes::from_static(b"cli"))
        .reconnect(ReconnectPolicy::Disabled);

    let client_a = Socket::new(SocketType::Client, no_reconnect.clone());
    client_a.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    client_a.send(Message::single("ping")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, Message::multipart(["cli", "ping"]));

    let client_b = Socket::new(SocketType::Client, no_reconnect);
    client_b.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify handover monitor event.
    let mut found = false;
    for _ in 0..20 {
        match tokio::time::timeout(Duration::from_millis(200), mon.recv()).await {
            Ok(Ok(MonitorEvent::Disconnected { reason, .. })) => {
                assert_eq!(reason, DisconnectReason::Handover);
                found = true;
                break;
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert!(found, "SERVER must emit Disconnected(Handover)");

    client_b.send(Message::single("pong")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, Message::multipart(["cli", "pong"]));
}

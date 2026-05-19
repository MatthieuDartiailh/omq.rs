//! ROUTER / DEALER identity routing.
//!
//! - DEALER → ROUTER: ROUTER prepends DEALER's identity on recv;
//!   ROUTER addresses replies by that identity.
//! - DEALER outbound is round-robin (PUSH/REQ-style), works without
//!   identity tracking on its end.
//! - `router_mandatory`: send to unknown identity returns Unroutable.

use std::net::Ipv4Addr;
use std::time::Duration;

use bytes::Bytes;
use omq_compio::endpoint::Host;
use omq_compio::{
    DisconnectReason, Endpoint, Error, Message, MonitorEvent, Options, ReconnectPolicy, Socket,
    SocketType,
};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn opts_with_identity(id: &str) -> Options {
    Options {
        identity: Bytes::from(id.to_string().into_bytes()),
        ..Default::default()
    }
}

fn opts_no_reconnect(id: &str) -> Options {
    Options {
        identity: Bytes::from(id.to_string().into_bytes()),
        reconnect: ReconnectPolicy::Disabled,
        ..Default::default()
    }
}

#[compio::test]
async fn router_addresses_dealer_by_identity() {
    let router = Socket::new(SocketType::Router, Options::default());
    let ep = router.bind(tcp_ep(0)).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, opts_with_identity("dealer-1"));
    dealer.connect(ep).await.unwrap();

    // DEALER sends a one-frame body. ROUTER receives [identity, body].
    dealer.send(Message::single("hello")).await.unwrap();
    let r = compio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .expect("router recv timeout")
        .unwrap();
    assert_eq!(r.len(), 2);
    assert_eq!(r.part_bytes(0).unwrap(), &b"dealer-1"[..]);
    assert_eq!(r.part_bytes(1).unwrap(), &b"hello"[..]);

    // ROUTER replies by prefixing the dealer identity.
    let reply = Message::multipart([
        Bytes::from_static(b"dealer-1"),
        Bytes::from_static(b"world"),
    ]);
    router.send(reply).await.unwrap();

    let d = compio::time::timeout(Duration::from_secs(2), dealer.recv())
        .await
        .expect("dealer recv timeout")
        .unwrap();
    assert_eq!(d.part_bytes(0).unwrap(), &b"world"[..]);
}

#[compio::test]
async fn router_mandatory_errors_on_unknown_identity() {
    let opts = Options {
        router_mandatory: true,
        ..Default::default()
    };
    let router = Socket::new(SocketType::Router, opts);
    let ep = router.bind(tcp_ep(0)).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, opts_with_identity("dealer-known"));
    dealer.connect(ep).await.unwrap();

    // Round-trip a message so the router learns dealer-known's slot
    // and we know the connection is up.
    dealer.send(Message::single("ping")).await.unwrap();
    let _ = compio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .expect("router recv timeout")
        .unwrap();

    // Send to an identity nobody owns.
    let bad = Message::multipart([Bytes::from_static(b"ghost"), Bytes::from_static(b"oops")]);
    let err = router.send(bad).await.err().unwrap();
    assert!(matches!(err, Error::Unroutable), "got {err:?}");
}

#[compio::test]
async fn router_drops_unknown_identity_by_default() {
    let router = Socket::new(SocketType::Router, Options::default());
    let ep = router.bind(tcp_ep(0)).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, opts_with_identity("d"));
    dealer.connect(ep).await.unwrap();

    dealer.send(Message::single("ping")).await.unwrap();
    let _ = compio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .expect("router recv timeout")
        .unwrap();

    let bad = Message::multipart([Bytes::from_static(b"nope"), Bytes::from_static(b"oops")]);
    // Default is silent drop (libzmq matches).
    router.send(bad).await.unwrap();
}

#[compio::test]
async fn router_assigns_identity_for_peers_without_one() {
    let router = Socket::new(SocketType::Router, Options::default());
    let ep = router.bind(tcp_ep(0)).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, Options::default());
    dealer.connect(ep).await.unwrap();

    compio::time::sleep(Duration::from_millis(50)).await;

    dealer.send(Message::single("anon")).await.unwrap();

    let got = compio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .expect("router recv timeout")
        .unwrap();
    assert_eq!(got.len(), 2);
    let identity = got.part_bytes(0).unwrap();
    assert!(
        !identity.is_empty(),
        "auto-generated identity must be non-empty"
    );

    router
        .send(Message::multipart([identity, Bytes::from_static(b"reply")]))
        .await
        .unwrap();

    let reply = compio::time::timeout(Duration::from_millis(500), dealer.recv())
        .await
        .expect("dealer recv timeout")
        .unwrap();
    assert_eq!(reply.part_bytes(0).unwrap(), &b"reply"[..]);
}

// --- Handover tests ---

#[compio::test]
async fn router_handover_evicts_old_peer() {
    let router = Socket::new(SocketType::Router, Options::default());
    let ep = router.bind(tcp_ep(0)).await.unwrap();

    let dealer_a = Socket::new(SocketType::Dealer, opts_no_reconnect("alpha"));
    dealer_a.connect(ep.clone()).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    dealer_a.send(Message::single("hello")).await.unwrap();
    let got = compio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .expect("router recv timeout")
        .unwrap();
    assert_eq!(got.part_bytes(0).unwrap(), &b"alpha"[..]);
    assert_eq!(got.part_bytes(1).unwrap(), &b"hello"[..]);

    router
        .send(Message::multipart([
            Bytes::from_static(b"alpha"),
            Bytes::from_static(b"reply-1"),
        ]))
        .await
        .unwrap();
    let r = compio::time::timeout(Duration::from_millis(500), dealer_a.recv())
        .await
        .expect("dealer_a recv timeout")
        .unwrap();
    assert_eq!(r.part_bytes(0).unwrap(), &b"reply-1"[..]);

    let dealer_b = Socket::new(SocketType::Dealer, opts_no_reconnect("alpha"));
    dealer_b.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    dealer_b.send(Message::single("world")).await.unwrap();
    let got = compio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .expect("router recv timeout")
        .unwrap();
    assert_eq!(got.part_bytes(0).unwrap(), &b"alpha"[..]);
    assert_eq!(got.part_bytes(1).unwrap(), &b"world"[..]);

    router
        .send(Message::multipart([
            Bytes::from_static(b"alpha"),
            Bytes::from_static(b"reply-2"),
        ]))
        .await
        .unwrap();
    let r = compio::time::timeout(Duration::from_millis(500), dealer_b.recv())
        .await
        .expect("dealer_b recv timeout")
        .unwrap();
    assert_eq!(r.part_bytes(0).unwrap(), &b"reply-2"[..]);
}

#[compio::test]
async fn router_handover_monitor_event() {
    let router = Socket::new(SocketType::Router, Options::default());
    let mut mon = router.monitor();
    router.bind(tcp_ep(0)).await.unwrap();
    let port = match compio::time::timeout(Duration::from_millis(500), mon.recv())
        .await
        .expect("Listening timeout")
        .unwrap()
    {
        MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("expected Listening, got {other:?}"),
    };

    let dealer_a = Socket::new(SocketType::Dealer, opts_no_reconnect("beta"));
    dealer_a.connect(tcp_ep(port)).await.unwrap();

    loop {
        match compio::time::timeout(Duration::from_millis(500), mon.recv()).await {
            Ok(Ok(MonitorEvent::HandshakeSucceeded { .. })) => break,
            Ok(Ok(_)) => {}
            other => panic!("expected HandshakeSucceeded, got {other:?}"),
        }
    }

    let dealer_b = Socket::new(SocketType::Dealer, opts_no_reconnect("beta"));
    dealer_b.connect(tcp_ep(port)).await.unwrap();

    let mut found = false;
    for _ in 0..20 {
        match compio::time::timeout(Duration::from_millis(500), mon.recv()).await {
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

#[compio::test]
async fn router_handover_auto_identity_no_collision() {
    let router = Socket::new(SocketType::Router, Options::default());
    let mut mon = router.monitor();
    let ep = router.bind(tcp_ep(0)).await.unwrap();

    let d1 = Socket::new(SocketType::Dealer, Options::default());
    d1.connect(ep.clone()).await.unwrap();

    let d2 = Socket::new(SocketType::Dealer, Options::default());
    d2.connect(ep).await.unwrap();

    compio::time::sleep(Duration::from_millis(100)).await;

    d1.send(Message::single("a")).await.unwrap();
    d2.send(Message::single("b")).await.unwrap();

    let m1 = compio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .expect("recv timeout")
        .unwrap();
    let m2 = compio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .expect("recv timeout")
        .unwrap();

    assert_ne!(
        m1.part_bytes(0).unwrap(),
        m2.part_bytes(0).unwrap(),
        "auto-generated identities must differ"
    );

    // Drain monitor — no Disconnected events expected.
    for _ in 0..10 {
        match compio::time::timeout(Duration::from_millis(50), mon.recv()).await {
            Ok(Ok(e)) => assert!(
                !matches!(e, MonitorEvent::Disconnected { .. }),
                "unexpected Disconnected: {e:?}"
            ),
            _ => break,
        }
    }
}

#[compio::test]
async fn server_handover_evicts_old_peer() {
    let server = Socket::new(SocketType::Server, Options::default());
    let mut mon = server.monitor();
    server.bind(tcp_ep(0)).await.unwrap();
    let port = match compio::time::timeout(Duration::from_millis(500), mon.recv())
        .await
        .expect("Listening timeout")
        .unwrap()
    {
        MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("expected Listening, got {other:?}"),
    };

    let client_a = Socket::new(SocketType::Client, opts_no_reconnect("cli"));
    client_a.connect(tcp_ep(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    client_a.send(Message::single("ping")).await.unwrap();
    let got = compio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .expect("server recv timeout")
        .unwrap();
    assert_eq!(got.part_bytes(0).unwrap(), &b"cli"[..]);
    assert_eq!(got.part_bytes(1).unwrap(), &b"ping"[..]);

    let client_b = Socket::new(SocketType::Client, opts_no_reconnect("cli"));
    client_b.connect(tcp_ep(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    let mut found = false;
    for _ in 0..20 {
        match compio::time::timeout(Duration::from_millis(200), mon.recv()).await {
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
    let got = compio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .expect("server recv timeout")
        .unwrap();
    assert_eq!(got.part_bytes(0).unwrap(), &b"cli"[..]);
    assert_eq!(got.part_bytes(1).unwrap(), &b"pong"[..]);
}

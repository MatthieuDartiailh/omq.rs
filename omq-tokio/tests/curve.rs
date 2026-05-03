//! CURVE end-to-end integration tests: handshake + per-frame encryption
//! between two omq.rs sockets.

#![cfg(feature = "curve")]

use std::time::Duration;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use omq_tokio::{CurveKeypair, Endpoint, IpcPath, Message, Options, Socket, SocketType};
use omq_tokio::endpoint::Host;

fn temp_ipc(name: &str) -> Endpoint {
    let mut dir = std::env::temp_dir();
    dir.push(format!("omq-curve-{name}-{}.sock", std::process::id()));
    Endpoint::Ipc(IpcPath::Filesystem(dir))
}

fn loopback_port() -> u16 {
    use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
    let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn tcp_ep(port: u16) -> Endpoint {
    use std::net::{IpAddr, Ipv4Addr};
    Endpoint::Tcp {
        host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port,
    }
}

#[tokio::test]
async fn curve_push_pull_roundtrip_over_ipc() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let ep = temp_ipc("push-pull");

    let server = Socket::new(SocketType::Pull, Options::default().curve_server(server_kp));
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().curve_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    client
        .send(Message::single("hello over curve"))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"hello over curve"[..]);
}

#[tokio::test]
async fn curve_multipart_roundtrip() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let ep = temp_ipc("multipart");

    let pair_a = Socket::new(SocketType::Pair, Options::default().curve_server(server_kp));
    pair_a.bind(ep.clone()).await.unwrap();

    let pair_b = Socket::new(
        SocketType::Pair,
        Options::default().curve_client(client_kp, server_pub),
    );
    pair_b.connect(ep).await.unwrap();

    pair_b
        .send(Message::multipart(["a", "bb", "ccc"]))
        .await
        .unwrap();

    let m = tokio::time::timeout(Duration::from_secs(1), pair_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 3);
    assert_eq!(m.parts()[0].coalesce(), &b"a"[..]);
    assert_eq!(m.parts()[1].coalesce(), &b"bb"[..]);
    assert_eq!(m.parts()[2].coalesce(), &b"ccc"[..]);
}

#[tokio::test]
async fn curve_wrong_server_key_fails_handshake() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    // Client expects a different server long-term key than what the
    // server actually has -- handshake should fail.
    let wrong_pub = CurveKeypair::generate().public;

    let ep = temp_ipc("wrong-key");

    let server = Socket::new(SocketType::Pull, Options::default().curve_server(server_kp));
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().curve_client(client_kp, wrong_pub),
    );
    client.connect(ep).await.unwrap();

    // Give the doomed handshake a moment.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // PUSH send blocks waiting for a routable peer that will never
    // arrive (handshake failed); bound it.
    let _ = tokio::time::timeout(
        Duration::from_millis(50),
        client.send(Message::single("ghost")),
    )
    .await;
    let r = tokio::time::timeout(Duration::from_millis(200), server.recv()).await;
    assert!(r.is_err(), "wrong server key must prevent delivery");
}

#[tokio::test]
async fn curve_emits_handshake_succeeded_with_curve_mechanism() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let ep = temp_ipc("monitor");
    let server = Socket::new(SocketType::Pair, Options::default().curve_server(server_kp));
    let mut mon = server.monitor();
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Pair,
        Options::default().curve_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    let mut saw_handshake = false;
    for _ in 0..6 {
        match tokio::time::timeout(Duration::from_millis(500), mon.recv()).await {
            Ok(Ok(omq_tokio::MonitorEvent::HandshakeSucceeded { peer, .. })) => {
                assert_eq!(peer.zmtp_version, (3, 1));
                saw_handshake = true;
                break;
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert!(saw_handshake, "CURVE handshake must complete");
}

#[tokio::test]
async fn curve_authenticator_admits_known_client() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;
    let allowed = client_kp.public.0;

    let ep = temp_ipc("auth-allow");

    let saw_callback = Arc::new(AtomicBool::new(false));
    let saw_callback_cb = saw_callback.clone();

    let server = Socket::new(
        SocketType::Pull,
        Options::default()
            .curve_server(server_kp)
            .authenticator(move |peer| {
                saw_callback_cb.store(true, Ordering::SeqCst);
                peer.public_key == allowed
            }),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().curve_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    client.send(Message::single("authed")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"authed"[..]);
    assert!(
        saw_callback.load(Ordering::SeqCst),
        "authenticator must run"
    );
}

#[tokio::test]
async fn curve_authenticator_rejects_unknown_client() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let ep = temp_ipc("auth-deny");

    let server = Socket::new(
        SocketType::Pull,
        Options::default()
            .curve_server(server_kp)
            .authenticator(|_peer| false),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().curve_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // PUSH send blocks indefinitely without a routable peer; bound it.
    let _ = tokio::time::timeout(
        Duration::from_millis(50),
        client.send(Message::single("denied")),
    )
    .await;
    let r = tokio::time::timeout(Duration::from_millis(200), server.recv()).await;
    assert!(r.is_err(), "rejected client must not deliver any frame");
}

// =====================================================================
// Strategy-bucket coverage: every send strategy must route through a
// CURVE-encrypted connection without surprises. PUSH/PULL covers the
// round-robin bucket above; here: REQ/REP, DEALER/ROUTER (identity),
// PUB/SUB (fan-out subscription-filtered).
// =====================================================================

#[tokio::test]
async fn curve_req_rep() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;
    let ep = temp_ipc("req-rep");

    let rep = Socket::new(SocketType::Rep, Options::default().curve_server(server_kp));
    rep.bind(ep.clone()).await.unwrap();
    let req = Socket::new(
        SocketType::Req,
        Options::default().curve_client(client_kp, server_pub),
    );
    req.connect(ep).await.unwrap();

    req.send(Message::single("q")).await.unwrap();
    let q = tokio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q.parts()[0].coalesce(), &b"q"[..]);
    rep.send(Message::single("a")).await.unwrap();
    let a = tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.parts()[0].coalesce(), &b"a"[..]);
}

#[tokio::test]
async fn curve_dealer_router() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;
    let ep = temp_ipc("dealer-router");

    let router = Socket::new(
        SocketType::Router,
        Options::default().curve_server(server_kp),
    );
    router.bind(ep.clone()).await.unwrap();
    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default()
            .identity(bytes::Bytes::from_static(b"d1"))
            .curve_client(client_kp, server_pub),
    );
    dealer.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    dealer.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"d1"[..]);
    assert_eq!(m.parts()[1].coalesce(), &b"hi"[..]);
}

#[tokio::test]
async fn curve_pub_sub() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;
    let ep = temp_ipc("pub-sub");

    let p = Socket::new(SocketType::Pub, Options::default().curve_server(server_kp));
    p.bind(ep.clone()).await.unwrap();
    let s = Socket::new(
        SocketType::Sub,
        Options::default().curve_client(client_kp, server_pub),
    );
    s.subscribe("").await.unwrap();
    s.connect(ep).await.unwrap();

    for _ in 0..30 {
        let _ = p.send(Message::single("hello")).await;
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(50), s.recv()).await {
            assert_eq!(m.parts()[0].coalesce(), &b"hello"[..]);
            return;
        }
    }
    panic!("SUB never received over CURVE");
}

#[tokio::test]
async fn curve_reconnects_after_server_restart() {
    // Client holds the server's public key. After the server restarts with
    // the same keypair, the client must re-handshake successfully and
    // resume message delivery.
    use omq_tokio::options::ReconnectPolicy;

    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let port = loopback_port();

    let server1 = Socket::new(
        SocketType::Pull,
        Options::default().curve_server(server_kp.clone()),
    );
    server1.bind(tcp_ep(port)).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(50)),
            ..Options::default().curve_client(client_kp, server_pub)
        },
    );
    client.connect(tcp_ep(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    client.send(Message::single("before")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server1.recv())
        .await
        .expect("first recv timed out")
        .unwrap();
    assert_eq!(&*m.parts()[0].coalesce(), b"before");

    // Server restarts with same keypair.
    server1.close().await.unwrap();

    let server2 = Socket::new(
        SocketType::Pull,
        Options::default().curve_server(server_kp),
    );
    let mut bound = false;
    for _ in 0..20 {
        if server2.bind(tcp_ep(port)).await.is_ok() {
            bound = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "server2 failed to bind after server1 closed");

    client.send(Message::single("after")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(3), server2.recv())
        .await
        .expect("second recv timed out — CURVE reconnect failed")
        .unwrap();
    assert_eq!(&*m.parts()[0].coalesce(), b"after");
}

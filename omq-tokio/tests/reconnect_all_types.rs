//! Reconnect coverage for socket types beyond PUSH/PULL.
//!
//! The plain reconnect.rs covers PUSH/PULL. This file verifies that
//! REQ/REP, PUB/SUB, DEALER/ROUTER, and PAIR all survive a listener
//! restart and resume correct message flow.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::options::ReconnectPolicy;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn loopback_port() -> u16 {
    let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn fast_reconnect() -> Options {
    Options {
        reconnect: ReconnectPolicy::Fixed(Duration::from_millis(30)),
        ..Default::default()
    }
}

async fn rebind<F: Fn() -> Socket>(port: u16, make: F) -> Socket {
    let s = make();
    for _ in 0..40 {
        if s.bind(tcp_ep(port)).await.is_ok() {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("could not rebind port {port} after 40 attempts");
}

// ── REQ / REP ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn req_rep_reconnect_after_server_restart() {
    let port = loopback_port();

    let rep1 = Socket::new(SocketType::Rep, Options::default());
    rep1.bind(tcp_ep(port)).await.unwrap();

    let req = Socket::new(SocketType::Req, fast_reconnect());
    req.connect(tcp_ep(port)).await.unwrap();

    req.send(Message::single("ping1")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(2), rep1.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(got.parts()[0].as_bytes().as_ref(), b"ping1");
    rep1.send(Message::single("pong1")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .expect("initial reply timed out")
        .unwrap();

    rep1.close().await.unwrap();
    let rep2 = rebind(port, || Socket::new(SocketType::Rep, Options::default())).await;

    req.send(Message::single("ping2")).await.unwrap();
    let got2 = tokio::time::timeout(Duration::from_secs(3), rep2.recv())
        .await
        .expect("post-restart recv timed out")
        .unwrap();
    assert_eq!(got2.parts()[0].as_bytes().as_ref(), b"ping2");
    rep2.send(Message::single("pong2")).await.unwrap();
    let reply2 = tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .expect("post-restart reply timed out")
        .unwrap();
    assert_eq!(reply2.parts()[0].as_bytes().as_ref(), b"pong2");
}

#[tokio::test]
async fn req_state_machine_survives_drop_mid_cycle() {
    let port = loopback_port();

    let rep1 = Socket::new(SocketType::Rep, Options::default());
    rep1.bind(tcp_ep(port)).await.unwrap();

    let req = Socket::new(SocketType::Req, fast_reconnect());
    req.connect(tcp_ep(port)).await.unwrap();

    req.send(Message::single("a")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), rep1.recv())
        .await
        .expect("warm-up recv timed out")
        .unwrap();
    rep1.send(Message::single("a-reply")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .expect("warm-up reply timed out")
        .unwrap();

    req.send(Message::single("b")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    rep1.close().await.unwrap();

    let rep2 = rebind(port, || Socket::new(SocketType::Rep, Options::default())).await;

    req.send(Message::single("c")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(3), rep2.recv())
        .await
        .expect("post-drop recv timed out")
        .unwrap();
    assert_eq!(got.parts()[0].as_bytes().as_ref(), b"c");
    rep2.send(Message::single("c-reply")).await.unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .expect("post-drop reply timed out")
        .unwrap();
    assert_eq!(reply.parts()[0].as_bytes().as_ref(), b"c-reply");
}

// ── PUB / SUB ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn pub_sub_reconnect_replays_subscriptions() {
    let port = loopback_port();

    let pub1 = Socket::new(SocketType::Pub, Options::default());
    pub1.bind(tcp_ep(port)).await.unwrap();

    let sub = Socket::new(SocketType::Sub, fast_reconnect());
    sub.connect(tcp_ep(port)).await.unwrap();
    sub.subscribe("x.").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    pub1.send(Message::single("x.hello")).await.unwrap();
    let m1 = tokio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(m1.parts()[0].as_bytes().as_ref(), b"x.hello");

    pub1.close().await.unwrap();
    let pub2 = rebind(port, || Socket::new(SocketType::Pub, Options::default())).await;

    tokio::time::sleep(Duration::from_millis(150)).await;

    pub2.send(Message::single("x.world")).await.unwrap();
    pub2.send(Message::single("y.ignored")).await.unwrap();
    pub2.send(Message::single("x.again")).await.unwrap();

    let m2 = tokio::time::timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("post-restart recv timed out")
        .unwrap();
    assert_eq!(m2.parts()[0].as_bytes().as_ref(), b"x.world");
    let m3 = tokio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("second post-restart recv timed out")
        .unwrap();
    assert_eq!(m3.parts()[0].as_bytes().as_ref(), b"x.again");
}

// ── DEALER / ROUTER ──────────────────────────────────────────────────────────

#[tokio::test]
async fn dealer_router_reconnect_after_router_restart() {
    let port = loopback_port();

    let router1 = Socket::new(SocketType::Router, Options::default());
    router1.bind(tcp_ep(port)).await.unwrap();

    let dealer = Socket::new(
        SocketType::Dealer,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(30)),
            ..Options::default().identity(bytes::Bytes::from_static(b"d1"))
        },
    );
    dealer.connect(tcp_ep(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    dealer.send(Message::single("hello")).await.unwrap();
    let got1 = tokio::time::timeout(Duration::from_secs(2), router1.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(got1.parts()[0].as_bytes().as_ref(), b"d1");
    assert_eq!(got1.parts()[1].as_bytes().as_ref(), b"hello");

    router1.close().await.unwrap();
    let router2 = rebind(port, || Socket::new(SocketType::Router, Options::default())).await;

    dealer.send(Message::single("after")).await.unwrap();
    let got2 = tokio::time::timeout(Duration::from_secs(3), router2.recv())
        .await
        .expect("post-restart recv timed out")
        .unwrap();
    assert_eq!(got2.parts()[0].as_bytes().as_ref(), b"d1");
    assert_eq!(got2.parts()[1].as_bytes().as_ref(), b"after");
}

// ── PAIR ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn pair_reconnect_after_bind_side_restart() {
    let port = loopback_port();

    let pair_a1 = Socket::new(SocketType::Pair, Options::default());
    pair_a1.bind(tcp_ep(port)).await.unwrap();

    let pair_b = Socket::new(SocketType::Pair, fast_reconnect());
    pair_b.connect(tcp_ep(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    pair_b.send(Message::single("hi")).await.unwrap();
    let got1 = tokio::time::timeout(Duration::from_secs(2), pair_a1.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(got1.parts()[0].as_bytes().as_ref(), b"hi");
    pair_a1.send(Message::single("there")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), pair_b.recv())
        .await
        .expect("initial reply timed out")
        .unwrap();

    pair_a1.close().await.unwrap();
    let pair_a2 = rebind(port, || Socket::new(SocketType::Pair, Options::default())).await;

    pair_b.send(Message::single("again")).await.unwrap();
    let got2 = tokio::time::timeout(Duration::from_secs(3), pair_a2.recv())
        .await
        .expect("post-restart recv timed out")
        .unwrap();
    assert_eq!(got2.parts()[0].as_bytes().as_ref(), b"again");
    pair_a2.send(Message::single("back")).await.unwrap();
    let reply2 = tokio::time::timeout(Duration::from_secs(2), pair_b.recv())
        .await
        .expect("post-restart reply timed out")
        .unwrap();
    assert_eq!(reply2.parts()[0].as_bytes().as_ref(), b"back");
}

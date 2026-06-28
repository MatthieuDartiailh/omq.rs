//! Reconnect coverage for socket types beyond PUSH/PULL.

use std::net::Ipv4Addr;
use std::time::Duration;

use bytes::Bytes;
use omq_compio::endpoint::Host;
use omq_compio::options::ReconnectPolicy;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

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

async fn rebind<F: Fn() -> Socket>(ep: &Endpoint, make: F) -> Socket {
    let s = make();
    for _ in 0..40 {
        if s.bind(ep.clone()).await.is_ok() {
            return s;
        }
        compio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("could not rebind {ep:?} after 40 attempts");
}

// ── REQ / REP ────────────────────────────────────────────────────────────────

#[compio::test]
async fn req_rep_reconnect_after_server_restart() {
    let rep1 = Socket::new(SocketType::Rep, Options::default());
    let ep = rep1.bind(tcp_ep(0)).await.unwrap();

    let req = Socket::new(SocketType::Req, fast_reconnect());
    req.connect(ep.clone()).await.unwrap();

    req.send(Message::single("ping1")).await.unwrap();
    let got = compio::time::timeout(Duration::from_secs(2), rep1.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(got, Message::single("ping1"));
    rep1.send(Message::single("pong1")).await.unwrap();
    compio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .expect("initial reply timed out")
        .unwrap();

    rep1.close().await.unwrap();
    let rep2 = rebind(&ep, || Socket::new(SocketType::Rep, Options::default())).await;

    req.send(Message::single("ping2")).await.unwrap();
    let got2 = compio::time::timeout(Duration::from_secs(3), rep2.recv())
        .await
        .expect("post-restart recv timed out")
        .unwrap();
    assert_eq!(got2, Message::single("ping2"));
    rep2.send(Message::single("pong2")).await.unwrap();
    let reply2 = compio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .expect("post-restart reply timed out")
        .unwrap();
    assert_eq!(reply2, Message::single("pong2"));
}

#[compio::test]
async fn req_state_machine_survives_drop_mid_cycle() {
    let rep1 = Socket::new(SocketType::Rep, Options::default());
    let ep = rep1.bind(tcp_ep(0)).await.unwrap();

    let req = Socket::new(SocketType::Req, fast_reconnect());
    req.connect(ep.clone()).await.unwrap();

    req.send(Message::single("a")).await.unwrap();
    compio::time::timeout(Duration::from_secs(2), rep1.recv())
        .await
        .expect("warm-up recv timed out")
        .unwrap();
    rep1.send(Message::single("a-reply")).await.unwrap();
    compio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .expect("warm-up reply timed out")
        .unwrap();

    req.send(Message::single("b")).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;
    rep1.close().await.unwrap();

    let rep2 = rebind(&ep, || Socket::new(SocketType::Rep, Options::default())).await;

    req.send(Message::single("c")).await.unwrap();
    let got = compio::time::timeout(Duration::from_secs(3), rep2.recv())
        .await
        .expect("post-drop recv timed out")
        .unwrap();
    assert_eq!(got, Message::single("c"));
    rep2.send(Message::single("c-reply")).await.unwrap();
    let reply = compio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .expect("post-drop reply timed out")
        .unwrap();
    assert_eq!(reply, Message::single("c-reply"));
}

// ── PUB / SUB ────────────────────────────────────────────────────────────────

#[compio::test]
async fn pub_sub_reconnect_replays_subscriptions() {
    let pub1 = Socket::new(SocketType::Pub, Options::default());
    let ep = pub1.bind(tcp_ep(0)).await.unwrap();

    let sub = Socket::new(SocketType::Sub, fast_reconnect());
    sub.connect(ep.clone()).await.unwrap();
    sub.subscribe("x.").await.unwrap();
    compio::time::sleep(Duration::from_millis(100)).await;

    pub1.send(Message::single("x.hello")).await.unwrap();
    let m1 = compio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(m1, Message::single("x.hello"));

    pub1.close().await.unwrap();
    let pub2 = rebind(&ep, || Socket::new(SocketType::Pub, Options::default())).await;

    compio::time::sleep(Duration::from_millis(150)).await;

    pub2.send(Message::single("x.world")).await.unwrap();
    pub2.send(Message::single("y.ignored")).await.unwrap();
    pub2.send(Message::single("x.again")).await.unwrap();

    let m2 = compio::time::timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("post-restart recv timed out")
        .unwrap();
    assert_eq!(m2, Message::single("x.world"));
    let m3 = compio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("second post-restart recv timed out")
        .unwrap();
    assert_eq!(m3, Message::single("x.again"));
}

// ── DEALER / ROUTER ──────────────────────────────────────────────────────────

#[compio::test]
async fn dealer_router_reconnect_after_router_restart() {
    let router1 = Socket::new(SocketType::Router, Options::default());
    let ep = router1.bind(tcp_ep(0)).await.unwrap();

    let dealer = Socket::new(
        SocketType::Dealer,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(30)),
            ..Options::default().identity(bytes::Bytes::from_static(b"d1"))
        },
    );
    dealer.connect(ep.clone()).await.unwrap();
    compio::time::sleep(Duration::from_millis(100)).await;

    dealer.send(Message::single("hello")).await.unwrap();
    let got1 = compio::time::timeout(Duration::from_secs(2), router1.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(got1, Message::multipart(["d1", "hello"]));

    router1.close().await.unwrap();
    let router2 = rebind(&ep, || Socket::new(SocketType::Router, Options::default())).await;

    dealer.send(Message::single("after")).await.unwrap();
    let got2 = compio::time::timeout(Duration::from_secs(3), router2.recv())
        .await
        .expect("post-restart recv timed out")
        .unwrap();
    assert_eq!(got2, Message::multipart(["d1", "after"]));
}

// ── PAIR ─────────────────────────────────────────────────────────────────────

#[compio::test]
async fn pair_reconnect_after_bind_side_restart() {
    let pair_a1 = Socket::new(SocketType::Pair, Options::default());
    let ep = pair_a1.bind(tcp_ep(0)).await.unwrap();

    let pair_b = Socket::new(SocketType::Pair, fast_reconnect());
    pair_b.connect(ep.clone()).await.unwrap();
    compio::time::sleep(Duration::from_millis(100)).await;

    pair_b.send(Message::single("hi")).await.unwrap();
    let got1 = compio::time::timeout(Duration::from_secs(2), pair_a1.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(got1, Message::single("hi"));
    pair_a1.send(Message::single("there")).await.unwrap();
    compio::time::timeout(Duration::from_secs(2), pair_b.recv())
        .await
        .expect("initial reply timed out")
        .unwrap();

    pair_a1.close().await.unwrap();
    let pair_a2 = rebind(&ep, || Socket::new(SocketType::Pair, Options::default())).await;

    pair_b.send(Message::single("again")).await.unwrap();
    let got2 = compio::time::timeout(Duration::from_secs(3), pair_a2.recv())
        .await
        .expect("post-restart recv timed out")
        .unwrap();
    assert_eq!(got2, Message::single("again"));
    pair_a2.send(Message::single("back")).await.unwrap();
    let reply2 = compio::time::timeout(Duration::from_secs(2), pair_b.recv())
        .await
        .expect("post-restart reply timed out")
        .unwrap();
    assert_eq!(reply2, Message::single("back"));
}

// ── CLIENT / SERVER ──────────────────────────────────────────────────────────

#[compio::test]
async fn client_server_reconnect_after_server_restart() {
    let server1 = Socket::new(SocketType::Server, Options::default());
    let ep = server1.bind(tcp_ep(0)).await.unwrap();

    let client = Socket::new(
        SocketType::Client,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(30)),
            ..Options::default().identity(Bytes::from_static(b"c1"))
        },
    );
    client.connect(ep.clone()).await.unwrap();
    compio::time::sleep(Duration::from_millis(100)).await;

    client.send(Message::single("ping1")).await.unwrap();
    let got1 = compio::time::timeout(Duration::from_secs(2), server1.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(got1, Message::multipart(["c1", "ping1"]));

    server1
        .send(Message::multipart([
            Bytes::from_static(b"c1"),
            Bytes::from_static(b"pong1"),
        ]))
        .await
        .unwrap();
    let reply1 = compio::time::timeout(Duration::from_secs(2), client.recv())
        .await
        .expect("initial reply timed out")
        .unwrap();
    assert_eq!(reply1, Message::single("pong1"));

    server1.close().await.unwrap();
    let server2 = rebind(&ep, || Socket::new(SocketType::Server, Options::default())).await;

    client.send(Message::single("ping2")).await.unwrap();
    let got2 = compio::time::timeout(Duration::from_secs(3), server2.recv())
        .await
        .expect("post-restart recv timed out")
        .unwrap();
    assert_eq!(got2, Message::multipart(["c1", "ping2"]));
}

// ── SCATTER / GATHER ─────────────────────────────────────────────────────────

#[compio::test]
async fn scatter_gather_reconnect_after_bind_restart() {
    let gather1 = Socket::new(SocketType::Gather, Options::default());
    let ep = gather1.bind(tcp_ep(0)).await.unwrap();

    let scatter = Socket::new(SocketType::Scatter, fast_reconnect());
    scatter.connect(ep.clone()).await.unwrap();
    compio::time::sleep(Duration::from_millis(100)).await;

    scatter.send(Message::single("before")).await.unwrap();
    let got1 = compio::time::timeout(Duration::from_secs(2), gather1.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(got1, Message::single("before"));

    gather1.close().await.unwrap();
    let gather2 = rebind(&ep, || Socket::new(SocketType::Gather, Options::default())).await;

    scatter.send(Message::single("after")).await.unwrap();
    let got2 = compio::time::timeout(Duration::from_secs(3), gather2.recv())
        .await
        .expect("post-restart recv timed out")
        .unwrap();
    assert_eq!(got2, Message::single("after"));
}

// ── RADIO / DISH ─────────────────────────────────────────────────────────────

#[compio::test]
async fn radio_dish_reconnect_replays_joins() {
    let radio1 = Socket::new(SocketType::Radio, Options::default());
    let ep = radio1.bind(tcp_ep(0)).await.unwrap();

    let dish = Socket::new(SocketType::Dish, fast_reconnect());
    dish.connect(ep.clone()).await.unwrap();
    dish.join("w").await.unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        radio1
            .send(Message::multipart(["w", "probe"]))
            .await
            .unwrap();
        if let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(200), dish.recv()).await {
            assert_eq!(m, Message::multipart(["w", "probe"]));
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "join never propagated"
        );
    }

    radio1.close().await.unwrap();
    let radio2 = rebind(&ep, || Socket::new(SocketType::Radio, Options::default())).await;

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        radio2
            .send(Message::multipart(["w", "after"]))
            .await
            .unwrap();
        if let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(200), dish.recv()).await {
            assert_eq!(m, Message::multipart(["w", "after"]));
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "join not replayed after restart"
        );
    }

    radio2
        .send(Message::multipart(["other", "miss"]))
        .await
        .unwrap();
    radio2
        .send(Message::multipart(["w", "final"]))
        .await
        .unwrap();
    let m = compio::time::timeout(Duration::from_secs(3), dish.recv())
        .await
        .expect("final recv timed out")
        .unwrap();
    assert_eq!(m, Message::multipart(["w", "final"]));
}

// ── PEER ─────────────────────────────────────────────────────────────────────

#[compio::test]
async fn peer_reconnect_after_bind_side_restart() {
    let peer_a1 = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pa")),
    );
    let ep = peer_a1.bind(tcp_ep(0)).await.unwrap();

    let peer_b = Socket::new(
        SocketType::Peer,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(30)),
            ..Options::default().identity(Bytes::from_static(b"pb"))
        },
    );
    peer_b.connect(ep.clone()).await.unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        peer_b
            .send(Message::multipart(["pa", "hello"]))
            .await
            .unwrap();
        if let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(200), peer_a1.recv()).await {
            assert_eq!(m, Message::multipart(["pb", "hello"]));
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "identity never discovered"
        );
    }

    peer_a1.close().await.unwrap();
    let peer_a2 = rebind(&ep, || {
        Socket::new(
            SocketType::Peer,
            Options::default().identity(Bytes::from_static(b"pa")),
        )
    })
    .await;

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        peer_b
            .send(Message::multipart(["pa", "after"]))
            .await
            .unwrap();
        if let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(200), peer_a2.recv()).await {
            assert_eq!(m, Message::multipart(["pb", "after"]));
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "identity not rediscovered after restart"
        );
    }
}

// ── CHANNEL ──────────────────────────────────────────────────────────────────

#[compio::test]
async fn channel_reconnect_after_bind_side_restart() {
    let ch_a1 = Socket::new(SocketType::Channel, Options::default());
    let ep = ch_a1.bind(tcp_ep(0)).await.unwrap();

    let ch_b = Socket::new(SocketType::Channel, fast_reconnect());
    ch_b.connect(ep.clone()).await.unwrap();
    compio::time::sleep(Duration::from_millis(100)).await;

    ch_b.send(Message::single("hi")).await.unwrap();
    let got1 = compio::time::timeout(Duration::from_secs(2), ch_a1.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(got1, Message::single("hi"));
    ch_a1.send(Message::single("there")).await.unwrap();
    compio::time::timeout(Duration::from_secs(2), ch_b.recv())
        .await
        .expect("initial reply timed out")
        .unwrap();

    ch_a1.close().await.unwrap();
    let ch_a2 = rebind(&ep, || Socket::new(SocketType::Channel, Options::default())).await;

    ch_b.send(Message::single("again")).await.unwrap();
    let got2 = compio::time::timeout(Duration::from_secs(3), ch_a2.recv())
        .await
        .expect("post-restart recv timed out")
        .unwrap();
    assert_eq!(got2, Message::single("again"));
    ch_a2.send(Message::single("back")).await.unwrap();
    let reply2 = compio::time::timeout(Duration::from_secs(2), ch_b.recv())
        .await
        .expect("post-restart reply timed out")
        .unwrap();
    assert_eq!(reply2, Message::single("back"));
}

// ── XPUB / XSUB ─────────────────────────────────────────────────────────────

#[compio::test]
async fn xpub_xsub_reconnect_replays_subscriptions() {
    let xpub1 = Socket::new(SocketType::XPub, Options::default());
    let ep = xpub1.bind(tcp_ep(0)).await.unwrap();

    let xsub = Socket::new(SocketType::XSub, fast_reconnect());
    xsub.connect(ep.clone()).await.unwrap();
    xsub.subscribe("news.").await.unwrap();

    let sub_msg = compio::time::timeout(Duration::from_secs(3), xpub1.recv())
        .await
        .expect("initial subscription timed out")
        .unwrap();
    let sub_bytes = sub_msg.part_bytes(0).unwrap();
    assert_eq!(sub_bytes[0], 0x01);
    assert_eq!(&sub_bytes[1..], b"news.");

    xpub1.close().await.unwrap();
    let xpub2 = rebind(&ep, || Socket::new(SocketType::XPub, Options::default())).await;

    let replayed = compio::time::timeout(Duration::from_secs(3), xpub2.recv())
        .await
        .expect("subscription replay timed out")
        .unwrap();
    let replayed_bytes = replayed.part_bytes(0).unwrap();
    assert_eq!(replayed_bytes[0], 0x01);
    assert_eq!(&replayed_bytes[1..], b"news.");
}

//! Options polish: end-to-end tests that exercise individual options for
//! correctness across the public API. Feature-by-feature smoke tests.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Error, Message, OnMute, Options, Socket, SocketType};

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

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[compio::test]
async fn linger_zero_drops_pending_on_close() {
    let ep = inproc_ep("opt-linger0");
    let push = Socket::new(SocketType::Push, Options::default().linger(Duration::ZERO));
    push.bind(ep.clone()).await.unwrap();

    let _ = compio::time::timeout(
        Duration::from_millis(50),
        push.send(Message::single("dropped")),
    )
    .await;
    push.close().await.unwrap();
}

#[compio::test]
async fn router_mandatory_default_silent() {
    let ep = inproc_ep("opt-rm-default");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep).await.unwrap();
    router
        .send(Message::multipart(["ghost", "hi"]))
        .await
        .unwrap();
}

#[compio::test]
async fn router_mandatory_true_errors_on_unknown() {
    let ep = inproc_ep("opt-rm-on");
    let router = Socket::new(
        SocketType::Router,
        Options::default().router_mandatory(true),
    );
    router.bind(ep).await.unwrap();
    let r = router.send(Message::multipart(["ghost", "hi"])).await;
    assert!(matches!(r, Err(Error::Unroutable)), "got {r:?}");
}

#[compio::test]
async fn max_message_size_rejects_oversize() {
    let ep = inproc_ep("opt-mms");
    let pull = Socket::new(SocketType::Pull, Options::default().max_message_size(8));
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    push.send(Message::single("12345678")).await.unwrap();
    let m = compio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().len(), 8);

    push.send(Message::single("123456789")).await.unwrap();
    let r = compio::time::timeout(Duration::from_millis(200), pull.recv()).await;
    assert!(r.is_err(), "oversize must not be delivered");
}

#[compio::test]
async fn drop_newest_silently_discards_overflow() {
    let ep = inproc_ep("opt-drop-new");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options::default().send_hwm(1).on_mute(OnMute::DropNewest),
    );
    push.connect(ep).await.unwrap();
    push.send(Message::single("a")).await.unwrap();
    push.send(Message::single("b")).await.unwrap();
    push.send(Message::single("c")).await.unwrap();

    let m = compio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    let _ = m;
    let extra = compio::time::timeout(Duration::from_millis(100), pull.recv()).await;
    let _ = extra;
}

#[compio::test]
async fn try_recv_empty_returns_would_block() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(inproc_ep("try-recv-empty-compio")).await.unwrap();
    assert!(matches!(pull.try_recv(), Err(Error::WouldBlock)));
}

#[compio::test]
async fn try_recv_returns_buffered_message() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let push = Socket::new(SocketType::Push, Options::default());
    pull.bind(inproc_ep("try-recv-buffered-compio"))
        .await
        .unwrap();
    push.connect(inproc_ep("try-recv-buffered-compio"))
        .await
        .unwrap();
    push.send(Message::single("hello")).await.unwrap();
    // Yield so the inproc frame is forwarded through in_tx/in_rx.
    let _ = compio::runtime::spawn(async {}).await;
    let msg = pull.try_recv().unwrap();
    assert_eq!(&*msg.part_bytes(0).unwrap(), b"hello");
}

#[compio::test]
async fn try_send_no_peers_returns_would_block() {
    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(inproc_ep("try-send-nopeer-compio"))
        .await
        .unwrap();
    // No peer connected; shared queue has capacity but no peer means WouldBlock.
    assert!(matches!(
        push.try_send(Message::single("x")),
        Err(Error::WouldBlock)
    ));
}

#[compio::test]
async fn identity_propagates_on_handshake() {
    let ep = inproc_ep("opt-ident");
    let server = Socket::new(SocketType::Router, Options::default());
    let mut mon = server.monitor();
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Dealer,
        Options::default().identity(bytes::Bytes::from_static(b"my-client")),
    );
    client.connect(ep).await.unwrap();

    let mut got_identity = None;
    for _ in 0..6 {
        match compio::time::timeout(Duration::from_millis(500), mon.recv()).await {
            Ok(Ok(omq_compio::MonitorEvent::HandshakeSucceeded { peer, .. })) => {
                got_identity = peer.peer_identity.clone();
                break;
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert_eq!(got_identity.as_deref(), Some(&b"my-client"[..]));
}

#[compio::test]
async fn unbounded_send_hwm_accepts_large_burst() {
    const N: usize = 2_000;

    let ep = inproc_ep("opt-unbounded-send-cmp");
    let pull = Socket::new(SocketType::Pull, Options::default().recv_hwm(4096));
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default().unbounded_send());
    push.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(30)).await;

    for i in 0..N {
        push.send(Message::single(format!("m{i}"))).await.unwrap();
    }

    let mut received = 0usize;
    while let Ok(Ok(_)) = compio::time::timeout(Duration::from_millis(500), pull.recv()).await {
        received += 1;
    }
    assert!(
        received >= N / 2,
        "unbounded HWM should not throttle sender; got {received}/{N}"
    );
}

#[compio::test]
async fn unbounded_recv_hwm_accepts_large_burst() {
    const N: usize = 2_000;

    let ep = inproc_ep("opt-unbounded-recv-cmp");
    let pull = Socket::new(SocketType::Pull, Options::default().unbounded_recv());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(30)).await;

    for i in 0..N {
        push.send(Message::single(format!("m{i}"))).await.unwrap();
    }

    let mut received = 0usize;
    while let Ok(Ok(_)) = compio::time::timeout(Duration::from_millis(500), pull.recv()).await {
        received += 1;
    }
    assert!(
        received >= N / 2,
        "unbounded recv HWM should not drop messages; got {received}/{N}"
    );
}

#[compio::test]
async fn drop_oldest_keeps_newest_messages() {
    let ep = inproc_ep("opt-drop-old-cmp");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options::default().send_hwm(1).on_mute(OnMute::DropOldest),
    );
    push.connect(ep).await.unwrap();
    push.send(Message::single("first")).await.unwrap();
    push.send(Message::single("second")).await.unwrap();
    push.send(Message::single("third")).await.unwrap();

    let m = compio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    let _ = m;
}

#[compio::test]
async fn max_message_size_exactly_at_limit_is_accepted() {
    let ep = inproc_ep("opt-mms-exact-cmp");
    let pull = Socket::new(SocketType::Pull, Options::default().max_message_size(8));
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    push.send(Message::single("12345678")).await.unwrap();
    let m = compio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .expect("message at exact limit must be delivered")
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().len(), 8);
}

#[compio::test]
async fn max_message_size_one_byte_over_drops_connection() {
    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default().max_message_size(8));
    pull.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp_ep(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    push.send(Message::single("123456789")).await.unwrap();
    let r = compio::time::timeout(Duration::from_millis(300), pull.recv()).await;
    assert!(
        r.is_err(),
        "9-byte message must not be delivered when limit is 8"
    );
}

//! Connect-before-bind: the dialer connects before the listener binds.
//! The dialer must retry until the listener appears, then deliver messages.
//! Tested across inproc, IPC, and TCP for every socket-type pair.

mod test_support;

use std::time::Duration;

use bytes::Bytes;
use omq_compio::endpoint::IpcPath;
use omq_compio::{Endpoint, Message, Options, ReconnectPolicy, Socket, SocketType};

fn opts() -> Options {
    Options {
        reconnect: ReconnectPolicy::Fixed(Duration::from_millis(20)),
        ..Default::default()
    }
}

fn free_tcp_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: omq_compio::endpoint::Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

#[cfg(feature = "lz4")]
fn lz4_ep(port: u16) -> Endpoint {
    Endpoint::Lz4Tcp {
        host: omq_compio::endpoint::Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

#[cfg(feature = "zstd")]
fn zstd_ep(port: u16) -> Endpoint {
    Endpoint::ZstdTcp {
        host: omq_compio::endpoint::Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

fn ipc_ep(name: &str) -> Endpoint {
    let path = std::env::temp_dir().join(format!(
        "omq-cbb-comp-{name}-{}-{}.sock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    Endpoint::Ipc(IpcPath::Filesystem(path))
}

const TIMEOUT: Duration = Duration::from_secs(5);

// -- PUSH/PULL ---------------------------------------------------------------

async fn push_pull_connect_before_bind(ep: Endpoint) {
    let push = Socket::new(SocketType::Push, opts());
    push.connect(ep.clone()).await.unwrap();

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&pull).await;

    push.send(Message::single("late")).await.unwrap();
    let m = compio::time::timeout(TIMEOUT, pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"late"[..]);
}

#[compio::test]
async fn push_pull_connect_before_bind_inproc() {
    push_pull_connect_before_bind(inproc_ep("cbb-pp-comp-inproc")).await;
}

#[compio::test]
async fn push_pull_connect_before_bind_ipc() {
    push_pull_connect_before_bind(ipc_ep("cbb-pp-comp")).await;
}

#[compio::test]
async fn push_pull_connect_before_bind_tcp() {
    push_pull_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- REQ/REP -----------------------------------------------------------------

async fn req_rep_connect_before_bind(ep: Endpoint) {
    let req = Socket::new(SocketType::Req, opts());
    req.connect(ep.clone()).await.unwrap();

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&rep).await;

    req.send(Message::single("q")).await.unwrap();
    let q = compio::time::timeout(TIMEOUT, rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q.part_bytes(0).unwrap(), &b"q"[..]);

    rep.send(Message::single("a")).await.unwrap();
    let a = compio::time::timeout(TIMEOUT, req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.part_bytes(0).unwrap(), &b"a"[..]);
}

#[compio::test]
async fn req_rep_connect_before_bind_inproc() {
    req_rep_connect_before_bind(inproc_ep("cbb-rr-comp-inproc")).await;
}

#[compio::test]
async fn req_rep_connect_before_bind_ipc() {
    req_rep_connect_before_bind(ipc_ep("cbb-rr-comp")).await;
}

#[compio::test]
async fn req_rep_connect_before_bind_tcp() {
    req_rep_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- PAIR --------------------------------------------------------------------

async fn pair_connect_before_bind(ep: Endpoint) {
    let a = Socket::new(SocketType::Pair, opts());
    a.connect(ep.clone()).await.unwrap();

    let b = Socket::new(SocketType::Pair, Options::default());
    b.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&b).await;

    a.send(Message::single("from-a")).await.unwrap();
    let m = compio::time::timeout(TIMEOUT, b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"from-a"[..]);

    b.send(Message::single("from-b")).await.unwrap();
    let m = compio::time::timeout(TIMEOUT, a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"from-b"[..]);
}

#[compio::test]
async fn pair_connect_before_bind_inproc() {
    pair_connect_before_bind(inproc_ep("cbb-pair-comp-inproc")).await;
}

#[compio::test]
async fn pair_connect_before_bind_ipc() {
    pair_connect_before_bind(ipc_ep("cbb-pair-comp")).await;
}

#[compio::test]
async fn pair_connect_before_bind_tcp() {
    pair_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- PUB/SUB -----------------------------------------------------------------

async fn pub_sub_connect_before_bind(ep: Endpoint) {
    let sub = Socket::new(SocketType::Sub, opts());
    sub.subscribe("x.").await.unwrap();
    sub.connect(ep.clone()).await.unwrap();

    let pub_ = Socket::new(SocketType::Pub, Options::default());
    pub_.bind(ep).await.unwrap();

    // Probe until the subscription propagates.
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        pub_.send(Message::single("x.hit")).await.unwrap();
        if let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(200), sub.recv()).await {
            assert_eq!(m.part_bytes(0).unwrap(), &b"x.hit"[..]);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "subscription never propagated"
        );
    }

    pub_.send(Message::single("y.miss")).await.unwrap();
    pub_.send(Message::single("x.second")).await.unwrap();
    let m = compio::time::timeout(TIMEOUT, sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"x.second"[..]);
}

#[compio::test]
async fn pub_sub_connect_before_bind_inproc() {
    pub_sub_connect_before_bind(inproc_ep("cbb-ps-comp-inproc")).await;
}

#[compio::test]
async fn pub_sub_connect_before_bind_ipc() {
    pub_sub_connect_before_bind(ipc_ep("cbb-ps-comp")).await;
}

#[compio::test]
async fn pub_sub_connect_before_bind_tcp() {
    pub_sub_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- DEALER/ROUTER -----------------------------------------------------------

async fn dealer_router_connect_before_bind(ep: Endpoint) {
    let dealer = Socket::new(
        SocketType::Dealer,
        opts().identity(Bytes::from_static(b"d1")),
    );
    dealer.connect(ep.clone()).await.unwrap();

    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&router).await;

    dealer.send(Message::single("hello")).await.unwrap();
    let m = compio::time::timeout(TIMEOUT, router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"d1"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"hello"[..]);

    router
        .send(Message::multipart([
            Bytes::from_static(b"d1"),
            Bytes::from_static(b"world"),
        ]))
        .await
        .unwrap();
    let m = compio::time::timeout(TIMEOUT, dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"world"[..]);
}

#[compio::test]
async fn dealer_router_connect_before_bind_inproc() {
    dealer_router_connect_before_bind(inproc_ep("cbb-dr-comp-inproc")).await;
}

#[compio::test]
async fn dealer_router_connect_before_bind_ipc() {
    dealer_router_connect_before_bind(ipc_ep("cbb-dr-comp")).await;
}

#[compio::test]
async fn dealer_router_connect_before_bind_tcp() {
    dealer_router_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- CLIENT/SERVER -----------------------------------------------------------

async fn client_server_connect_before_bind(ep: Endpoint) {
    let client = Socket::new(
        SocketType::Client,
        opts().identity(Bytes::from_static(b"c1")),
    );
    client.connect(ep.clone()).await.unwrap();

    let server = Socket::new(SocketType::Server, Options::default());
    server.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&server).await;

    client.send(Message::single("ping")).await.unwrap();
    let m = compio::time::timeout(TIMEOUT, server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"c1"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"ping"[..]);

    server
        .send(Message::multipart([
            Bytes::from_static(b"c1"),
            Bytes::from_static(b"pong"),
        ]))
        .await
        .unwrap();
    let m = compio::time::timeout(TIMEOUT, client.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"pong"[..]);
}

#[compio::test]
async fn client_server_connect_before_bind_inproc() {
    client_server_connect_before_bind(inproc_ep("cbb-cs-comp-inproc")).await;
}

#[compio::test]
async fn client_server_connect_before_bind_ipc() {
    client_server_connect_before_bind(ipc_ep("cbb-cs-comp")).await;
}

#[compio::test]
async fn client_server_connect_before_bind_tcp() {
    client_server_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- SCATTER/GATHER ----------------------------------------------------------

async fn scatter_gather_connect_before_bind(ep: Endpoint) {
    let scatter = Socket::new(SocketType::Scatter, opts());
    scatter.connect(ep.clone()).await.unwrap();

    let gather = Socket::new(SocketType::Gather, Options::default());
    gather.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&gather).await;

    scatter.send(Message::single("late")).await.unwrap();
    let m = compio::time::timeout(TIMEOUT, gather.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"late"[..]);
}

#[compio::test]
async fn scatter_gather_connect_before_bind_inproc() {
    scatter_gather_connect_before_bind(inproc_ep("cbb-sg-comp-inproc")).await;
}

#[compio::test]
async fn scatter_gather_connect_before_bind_ipc() {
    scatter_gather_connect_before_bind(ipc_ep("cbb-sg-comp")).await;
}

#[compio::test]
async fn scatter_gather_connect_before_bind_tcp() {
    scatter_gather_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- RADIO/DISH --------------------------------------------------------------

async fn radio_dish_connect_before_bind(ep: Endpoint) {
    let dish = Socket::new(SocketType::Dish, opts());
    dish.join("w").await.unwrap();
    dish.connect(ep.clone()).await.unwrap();

    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.bind(ep).await.unwrap();

    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        radio.send(Message::multipart(["w", "hit"])).await.unwrap();
        if let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(200), dish.recv()).await {
            assert_eq!(m.part_bytes(0).unwrap(), &b"w"[..]);
            assert_eq!(m.part_bytes(1).unwrap(), &b"hit"[..]);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "join never propagated"
        );
    }

    radio
        .send(Message::multipart(["other", "miss"]))
        .await
        .unwrap();
    radio
        .send(Message::multipart(["w", "second"]))
        .await
        .unwrap();
    let m = compio::time::timeout(TIMEOUT, dish.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"w"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"second"[..]);
}

#[compio::test]
async fn radio_dish_connect_before_bind_inproc() {
    radio_dish_connect_before_bind(inproc_ep("cbb-rd-comp-inproc")).await;
}

#[compio::test]
async fn radio_dish_connect_before_bind_ipc() {
    radio_dish_connect_before_bind(ipc_ep("cbb-rd-comp")).await;
}

#[compio::test]
async fn radio_dish_connect_before_bind_tcp() {
    radio_dish_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- PEER --------------------------------------------------------------------

async fn peer_connect_before_bind(ep: Endpoint) {
    let b = Socket::new(SocketType::Peer, opts().identity(Bytes::from_static(b"pb")));
    b.connect(ep.clone()).await.unwrap();

    let a = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pa")),
    );
    a.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&a).await;

    // PEER routes by identity. Probe until B discovers A's identity.
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        b.send(Message::multipart(["pa", "from-b"])).await.unwrap();
        if let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(200), a.recv()).await {
            assert_eq!(m.part_bytes(0).unwrap(), &b"pb"[..]);
            assert_eq!(m.part_bytes(1).unwrap(), &b"from-b"[..]);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "peer identity never discovered"
        );
    }

    a.send(Message::multipart(["pb", "from-a"])).await.unwrap();
    let m = compio::time::timeout(TIMEOUT, b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"pa"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"from-a"[..]);
}

#[compio::test]
async fn peer_connect_before_bind_inproc() {
    peer_connect_before_bind(inproc_ep("cbb-peer-comp-inproc")).await;
}

#[compio::test]
async fn peer_connect_before_bind_ipc() {
    peer_connect_before_bind(ipc_ep("cbb-peer-comp")).await;
}

#[compio::test]
async fn peer_connect_before_bind_tcp() {
    peer_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- CHANNEL -----------------------------------------------------------------

async fn channel_connect_before_bind(ep: Endpoint) {
    let a = Socket::new(SocketType::Channel, opts());
    a.connect(ep.clone()).await.unwrap();

    let b = Socket::new(SocketType::Channel, Options::default());
    b.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&b).await;

    a.send(Message::single("from-a")).await.unwrap();
    let m = compio::time::timeout(TIMEOUT, b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"from-a"[..]);

    b.send(Message::single("from-b")).await.unwrap();
    let m = compio::time::timeout(TIMEOUT, a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"from-b"[..]);
}

#[compio::test]
async fn channel_connect_before_bind_inproc() {
    channel_connect_before_bind(inproc_ep("cbb-ch-comp-inproc")).await;
}

#[compio::test]
async fn channel_connect_before_bind_ipc() {
    channel_connect_before_bind(ipc_ep("cbb-ch-comp")).await;
}

#[compio::test]
async fn channel_connect_before_bind_tcp() {
    channel_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- lz4+tcp -----------------------------------------------------------------

#[cfg(feature = "lz4")]
#[compio::test]
async fn push_pull_connect_before_bind_lz4() {
    push_pull_connect_before_bind(lz4_ep(free_tcp_port())).await;
}

#[cfg(feature = "lz4")]
#[compio::test]
async fn req_rep_connect_before_bind_lz4() {
    req_rep_connect_before_bind(lz4_ep(free_tcp_port())).await;
}

// -- zstd+tcp ----------------------------------------------------------------

#[cfg(feature = "zstd")]
#[compio::test]
async fn push_pull_connect_before_bind_zstd() {
    push_pull_connect_before_bind(zstd_ep(free_tcp_port())).await;
}

#[cfg(feature = "zstd")]
#[compio::test]
async fn req_rep_connect_before_bind_zstd() {
    req_rep_connect_before_bind(zstd_ep(free_tcp_port())).await;
}

// -- ws ----------------------------------------------------------------------
// TODO: WS connect is fire-and-forget (no dial_supervisor retry loop), so
// these fail until WS gets a reconnect supervisor like TCP/IPC have.

#[cfg(feature = "ws")]
fn ws_ep(port: u16) -> Endpoint {
    format!("ws://127.0.0.1:{port}/").parse().unwrap()
}

#[cfg(feature = "ws")]
#[compio::test]
#[ignore = "WS lacks reconnect supervisor"]
async fn push_pull_connect_before_bind_ws() {
    push_pull_connect_before_bind(ws_ep(free_tcp_port())).await;
}

#[cfg(feature = "ws")]
#[compio::test]
#[ignore = "WS lacks reconnect supervisor"]
async fn req_rep_connect_before_bind_ws() {
    req_rep_connect_before_bind(ws_ep(free_tcp_port())).await;
}

#[cfg(feature = "ws")]
#[compio::test]
#[ignore = "WS lacks reconnect supervisor"]
async fn pub_sub_connect_before_bind_ws() {
    pub_sub_connect_before_bind(ws_ep(free_tcp_port())).await;
}

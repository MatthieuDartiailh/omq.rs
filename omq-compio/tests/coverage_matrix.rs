//! Socket-type × transport coverage matrix for omq-compio.
//!
//! Every cell exercises a minimal round-trip on one backend so the
//! "all 19 types work over every transport that's structurally
//! meaningful" claim is verifiable in CI. Cells that don't make
//! sense (e.g. RADIO/DISH only run over UDP per RFC 48 in this
//! suite; XPUB ↔ XSUB needs the explicit drain step that lives in
//! `xpub_xsub.rs`) are intentionally absent here.

mod test_support;

use std::time::Duration;

use bytes::Bytes;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};
use omq_proto::endpoint::IpcPath;

fn ipc_ep(name: &str) -> Endpoint {
    Endpoint::Ipc(IpcPath::Abstract(format!(
        "omq-compio-cov-{name}-{}-{}",
        std::process::id(),
        rand::random::<u32>()
    )))
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc {
        name: format!(
            "cov-{name}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ),
    }
}

async fn wait() {
    compio::time::sleep(Duration::from_millis(60)).await;
}

async fn push_pull_roundtrip(bind_ep: Endpoint, connect_ep: Endpoint) {
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(bind_ep).await.unwrap();
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(connect_ep).await.unwrap();
    push.send(Message::single("hi")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("hi"));
}

async fn push_pull_roundtrip_tcp() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let p = test_support::bind_loopback(&pull).await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(test_support::tcp_loopback(p)).await.unwrap();
    push.send(Message::single("hi")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("hi"));
}

async fn req_rep_roundtrip(bind_ep: Endpoint, connect_ep: Endpoint) {
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(bind_ep).await.unwrap();
    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(connect_ep).await.unwrap();
    req.send(Message::single("q")).await.unwrap();
    let q = compio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q, Message::single("q"));
    rep.send(Message::single("a")).await.unwrap();
    let a = compio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a, Message::single("a"));
}

async fn req_rep_roundtrip_tcp() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let p = test_support::bind_loopback(&rep).await;
    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(test_support::tcp_loopback(p)).await.unwrap();
    req.send(Message::single("q")).await.unwrap();
    let q = compio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q, Message::single("q"));
    rep.send(Message::single("a")).await.unwrap();
    let a = compio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a, Message::single("a"));
}

async fn dealer_router_roundtrip(bind_ep: Endpoint, connect_ep: Endpoint) {
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(bind_ep).await.unwrap();
    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(Bytes::from_static(b"d1")),
    );
    dealer.connect(connect_ep).await.unwrap();
    dealer.send(Message::single("hi")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::multipart(["d1", "hi"]));
}

async fn dealer_router_roundtrip_tcp() {
    let router = Socket::new(SocketType::Router, Options::default());
    let p = test_support::bind_loopback(&router).await;
    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(Bytes::from_static(b"d1")),
    );
    dealer.connect(test_support::tcp_loopback(p)).await.unwrap();
    dealer.send(Message::single("hi")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::multipart(["d1", "hi"]));
}

async fn pair_roundtrip(bind_ep: Endpoint, connect_ep: Endpoint) {
    let a = Socket::new(SocketType::Pair, Options::default());
    a.bind(bind_ep).await.unwrap();
    let b = Socket::new(SocketType::Pair, Options::default());
    b.connect(connect_ep).await.unwrap();
    a.send(Message::single("x")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("x"));
}

async fn pair_roundtrip_tcp() {
    let a = Socket::new(SocketType::Pair, Options::default());
    let p = test_support::bind_loopback(&a).await;
    let b = Socket::new(SocketType::Pair, Options::default());
    b.connect(test_support::tcp_loopback(p)).await.unwrap();
    a.send(Message::single("x")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("x"));
}

async fn pub_sub_roundtrip(bind_ep: Endpoint, connect_ep: Endpoint) {
    let p = Socket::new(SocketType::Pub, Options::default());
    p.bind(bind_ep).await.unwrap();
    let s = Socket::new(SocketType::Sub, Options::default());
    s.subscribe("").await.unwrap();
    s.connect(connect_ep).await.unwrap();
    // Subscription propagation can race the first publish; loop.
    for _ in 0..30 {
        let _ = p.send(Message::single("hello")).await;
        if let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(50), s.recv()).await {
            assert_eq!(m, Message::single("hello"));
            return;
        }
    }
    panic!("SUB never received");
}

async fn pub_sub_roundtrip_tcp() {
    let p = Socket::new(SocketType::Pub, Options::default());
    let port = test_support::bind_loopback(&p).await;
    let s = Socket::new(SocketType::Sub, Options::default());
    s.subscribe("").await.unwrap();
    s.connect(test_support::tcp_loopback(port)).await.unwrap();
    // Subscription propagation can race the first publish; loop.
    for _ in 0..30 {
        let _ = p.send(Message::single("hello")).await;
        if let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(50), s.recv()).await {
            assert_eq!(m, Message::single("hello"));
            return;
        }
    }
    panic!("SUB never received");
}

async fn client_server_roundtrip(bind_ep: Endpoint, connect_ep: Endpoint) {
    let server = Socket::new(SocketType::Server, Options::default());
    server.bind(bind_ep).await.unwrap();
    let client = Socket::new(
        SocketType::Client,
        Options::default().identity(Bytes::from_static(b"c1")),
    );
    client.connect(connect_ep).await.unwrap();
    client.send(Message::single("ping")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::multipart(["c1", "ping"]));
}

async fn client_server_roundtrip_tcp() {
    let server = Socket::new(SocketType::Server, Options::default());
    let p = test_support::bind_loopback(&server).await;
    let client = Socket::new(
        SocketType::Client,
        Options::default().identity(Bytes::from_static(b"c1")),
    );
    client.connect(test_support::tcp_loopback(p)).await.unwrap();
    client.send(Message::single("ping")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::multipart(["c1", "ping"]));
}

async fn scatter_gather_roundtrip(bind_ep: Endpoint, connect_ep: Endpoint) {
    let g = Socket::new(SocketType::Gather, Options::default());
    g.bind(bind_ep).await.unwrap();
    let s = Socket::new(SocketType::Scatter, Options::default());
    s.connect(connect_ep).await.unwrap();
    s.send(Message::single("m")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), g.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("m"));
}

async fn scatter_gather_roundtrip_tcp() {
    let g = Socket::new(SocketType::Gather, Options::default());
    let p = test_support::bind_loopback(&g).await;
    let s = Socket::new(SocketType::Scatter, Options::default());
    s.connect(test_support::tcp_loopback(p)).await.unwrap();
    s.send(Message::single("m")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), g.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("m"));
}

async fn channel_roundtrip(bind_ep: Endpoint, connect_ep: Endpoint) {
    let a = Socket::new(SocketType::Channel, Options::default());
    a.bind(bind_ep).await.unwrap();
    let b = Socket::new(SocketType::Channel, Options::default());
    b.connect(connect_ep).await.unwrap();
    a.send(Message::single("hi")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("hi"));
}

async fn channel_roundtrip_tcp() {
    let a = Socket::new(SocketType::Channel, Options::default());
    let p = test_support::bind_loopback(&a).await;
    let b = Socket::new(SocketType::Channel, Options::default());
    b.connect(test_support::tcp_loopback(p)).await.unwrap();
    a.send(Message::single("hi")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("hi"));
}

async fn peer_roundtrip(bind_ep: Endpoint, connect_ep: Endpoint) {
    let a = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pa")),
    );
    a.bind(bind_ep).await.unwrap();
    let b = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pb")),
    );
    b.connect(connect_ep).await.unwrap();
    wait().await;
    b.send(Message::multipart(["pa", "hi a"])).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::multipart(["pb", "hi a"]));
}

async fn peer_roundtrip_tcp() {
    let a = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pa")),
    );
    let p = test_support::bind_loopback(&a).await;
    let b = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pb")),
    );
    b.connect(test_support::tcp_loopback(p)).await.unwrap();
    wait().await;
    b.send(Message::multipart(["pa", "hi a"])).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::multipart(["pb", "hi a"]));
}

// =====================================================================
// Inproc cells
// =====================================================================

#[compio::test]
async fn push_pull_inproc() {
    let ep = inproc_ep("pp");
    push_pull_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn req_rep_inproc() {
    let ep = inproc_ep("rr");
    req_rep_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn dealer_router_inproc() {
    let ep = inproc_ep("dr");
    dealer_router_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn pair_inproc() {
    let ep = inproc_ep("pair");
    pair_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn pub_sub_inproc() {
    let ep = inproc_ep("ps");
    pub_sub_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn client_server_inproc() {
    let ep = inproc_ep("cs");
    client_server_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn scatter_gather_inproc() {
    let ep = inproc_ep("sg");
    scatter_gather_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn channel_inproc() {
    let ep = inproc_ep("ch");
    channel_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn peer_inproc() {
    let ep = inproc_ep("pp");
    peer_roundtrip(ep.clone(), ep).await;
}

// =====================================================================
// IPC cells
// =====================================================================

#[compio::test]
async fn push_pull_ipc() {
    let ep = ipc_ep("pp");
    push_pull_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn req_rep_ipc() {
    let ep = ipc_ep("rr");
    req_rep_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn dealer_router_ipc() {
    let ep = ipc_ep("dr");
    dealer_router_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn pair_ipc() {
    let ep = ipc_ep("pair");
    pair_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn pub_sub_ipc() {
    let ep = ipc_ep("ps");
    pub_sub_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn client_server_ipc() {
    let ep = ipc_ep("cs");
    client_server_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn scatter_gather_ipc() {
    let ep = ipc_ep("sg");
    scatter_gather_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn channel_ipc() {
    let ep = ipc_ep("ch");
    channel_roundtrip(ep.clone(), ep).await;
}
#[compio::test]
async fn peer_ipc() {
    let ep = ipc_ep("pp");
    peer_roundtrip(ep.clone(), ep).await;
}

// =====================================================================
// TCP cells
// =====================================================================

#[compio::test]
async fn push_pull_tcp() {
    push_pull_roundtrip_tcp().await;
}
#[compio::test]
async fn req_rep_tcp() {
    req_rep_roundtrip_tcp().await;
}
#[compio::test]
async fn dealer_router_tcp() {
    dealer_router_roundtrip_tcp().await;
}
#[compio::test]
async fn pair_tcp() {
    pair_roundtrip_tcp().await;
}
#[compio::test]
async fn pub_sub_tcp() {
    pub_sub_roundtrip_tcp().await;
}
#[compio::test]
async fn client_server_tcp() {
    client_server_roundtrip_tcp().await;
}
#[compio::test]
async fn scatter_gather_tcp() {
    scatter_gather_roundtrip_tcp().await;
}
#[compio::test]
async fn channel_tcp() {
    channel_roundtrip_tcp().await;
}
#[compio::test]
async fn peer_tcp() {
    peer_roundtrip_tcp().await;
}

// =====================================================================
// Send-before-connect: messages queued before any peer connects must
// be delivered once a peer appears.
// =====================================================================

#[compio::test]
async fn send_before_connect_ipc() {
    let ep = ipc_ep("sbc");
    let push = Socket::new(SocketType::Push, Options::default());
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();
    push.connect(ep).await.unwrap();
    // Send immediately, before the handshake can complete.
    push.send(Message::single("early")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("early"));
}

#[compio::test]
async fn send_before_connect_tcp() {
    let push = Socket::new(SocketType::Push, Options::default());
    let pull = Socket::new(SocketType::Pull, Options::default());
    let p = test_support::bind_loopback(&pull).await;
    push.connect(test_support::tcp_loopback(p)).await.unwrap();
    // Send immediately, before the handshake can complete.
    push.send(Message::single("early")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("early"));
}

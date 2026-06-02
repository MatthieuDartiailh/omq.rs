//! Stress test: connect-before-bind hundreds of times per socket-type/transport
//! combo. Verifies reconnection never hangs.

mod test_support;

use std::time::Duration;

use bytes::Bytes;
use omq_tokio::endpoint::IpcPath;
use omq_tokio::{Endpoint, Message, Options, ReconnectPolicy, Socket, SocketType};

const ROUNDS: usize = 200;
const TIMEOUT: Duration = Duration::from_secs(5);

fn opts() -> Options {
    Options {
        reconnect: ReconnectPolicy::Fixed(Duration::from_millis(20)),
        ..Default::default()
    }
}

fn tcp_ep() -> Endpoint {
    Endpoint::Tcp {
        host: omq_tokio::endpoint::Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port: 0,
    }
}

fn inproc_ep(tag: &str, round: usize) -> Endpoint {
    Endpoint::Inproc {
        name: format!("stress-cbb-{tag}-{round}-{}", rand::random::<u32>()),
    }
}

fn ipc_ep(tag: &str, round: usize) -> Endpoint {
    Endpoint::Ipc(IpcPath::Abstract(format!(
        "omq-stress-cbb-{tag}-{round}-{}-{}",
        std::process::id(),
        rand::random::<u32>(),
    )))
}

enum Transport {
    Tcp,
    Ipc,
    Inproc,
}

fn ep_for(transport: &Transport, tag: &str, round: usize) -> Endpoint {
    match transport {
        Transport::Tcp => tcp_ep(),
        Transport::Ipc => ipc_ep(tag, round),
        Transport::Inproc => inproc_ep(tag, round),
    }
}

async fn stress_push_pull(transport: &Transport, bind_side: &str) {
    let tag = format!("pp-{bind_side}");
    for i in 0..ROUNDS {
        let ep = ep_for(transport, &tag, i);

        let (push, pull) = if bind_side == "push" {
            let push = Socket::new(SocketType::Push, Options::default());
            let bound = push.bind(ep).await.unwrap();
            let pull = Socket::new(SocketType::Pull, opts());
            pull.connect(bound).await.unwrap();
            (push, pull)
        } else {
            let pull = Socket::new(SocketType::Pull, Options::default());
            let bound = pull.bind(ep).await.unwrap();
            let push = Socket::new(SocketType::Push, opts());
            push.connect(bound).await.unwrap();
            (push, pull)
        };

        push.send(Message::single("x")).await.unwrap();
        let m = tokio::time::timeout(TIMEOUT, pull.recv())
            .await
            .expect(&format!("push/pull {bind_side}-binds round {i} timed out"))
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), &b"x"[..]);
    }
}

async fn stress_req_rep(transport: &Transport, bind_side: &str) {
    let tag = format!("rr-{bind_side}");
    for i in 0..ROUNDS {
        let ep = ep_for(transport, &tag, i);

        let (req, rep) = if bind_side == "rep" {
            let rep = Socket::new(SocketType::Rep, Options::default());
            let bound = rep.bind(ep).await.unwrap();
            let req = Socket::new(SocketType::Req, opts());
            req.connect(bound).await.unwrap();
            (req, rep)
        } else {
            let req = Socket::new(SocketType::Req, Options::default());
            let bound = req.bind(ep).await.unwrap();
            let rep = Socket::new(SocketType::Rep, opts());
            rep.connect(bound).await.unwrap();
            (req, rep)
        };

        req.send(Message::single("q")).await.unwrap();
        let m = tokio::time::timeout(TIMEOUT, rep.recv())
            .await
            .expect(&format!(
                "req/rep {bind_side}-binds round {i} recv timed out"
            ))
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), &b"q"[..]);

        rep.send(Message::single("a")).await.unwrap();
        let m = tokio::time::timeout(TIMEOUT, req.recv())
            .await
            .expect(&format!(
                "req/rep {bind_side}-binds round {i} reply timed out"
            ))
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), &b"a"[..]);
    }
}

async fn stress_pub_sub(transport: &Transport, bind_side: &str) {
    let tag = format!("ps-{bind_side}");
    for i in 0..ROUNDS {
        let ep = ep_for(transport, &tag, i);

        let (pub_, sub) = if bind_side == "pub" {
            let pub_ = Socket::new(SocketType::Pub, Options::default());
            let bound = pub_.bind(ep).await.unwrap();
            let sub = Socket::new(SocketType::Sub, opts());
            sub.subscribe("").await.unwrap();
            sub.connect(bound).await.unwrap();
            (pub_, sub)
        } else {
            let sub = Socket::new(SocketType::Sub, Options::default());
            sub.subscribe("").await.unwrap();
            let bound = sub.bind(ep).await.unwrap();
            let pub_ = Socket::new(SocketType::Pub, opts());
            pub_.connect(bound).await.unwrap();
            (pub_, sub)
        };

        let deadline = std::time::Instant::now() + TIMEOUT;
        loop {
            pub_.send(Message::single("m")).await.unwrap();
            if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(100), sub.recv()).await {
                assert_eq!(m.part_bytes(0).unwrap(), &b"m"[..]);
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "pub/sub {bind_side}-binds round {i} sub never received"
            );
        }
    }
}

async fn stress_pair(transport: &Transport) {
    for i in 0..ROUNDS {
        let ep = ep_for(transport, "pair", i);

        let a = Socket::new(SocketType::Pair, Options::default());
        let bound = a.bind(ep).await.unwrap();
        let b = Socket::new(SocketType::Pair, opts());
        b.connect(bound).await.unwrap();

        a.send(Message::single("ab")).await.unwrap();
        let m = tokio::time::timeout(TIMEOUT, b.recv())
            .await
            .expect(&format!("pair round {i} a->b timed out"))
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), &b"ab"[..]);

        b.send(Message::single("ba")).await.unwrap();
        let m = tokio::time::timeout(TIMEOUT, a.recv())
            .await
            .expect(&format!("pair round {i} b->a timed out"))
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), &b"ba"[..]);
    }
}

async fn stress_dealer_router(transport: &Transport, bind_side: &str) {
    let tag = format!("dr-{bind_side}");
    for i in 0..ROUNDS {
        let ep = ep_for(transport, &tag, i);

        let (dealer, router) = if bind_side == "router" {
            let router = Socket::new(SocketType::Router, Options::default());
            let bound = router.bind(ep).await.unwrap();
            let dealer = Socket::new(
                SocketType::Dealer,
                opts().identity(Bytes::from_static(b"d1")),
            );
            dealer.connect(bound).await.unwrap();
            (dealer, router)
        } else {
            let dealer = Socket::new(
                SocketType::Dealer,
                Options::default().identity(Bytes::from_static(b"d1")),
            );
            let bound = dealer.bind(ep).await.unwrap();
            let router = Socket::new(SocketType::Router, opts());
            router.connect(bound).await.unwrap();
            (dealer, router)
        };

        dealer.send(Message::single("hi")).await.unwrap();
        let m = tokio::time::timeout(TIMEOUT, router.recv())
            .await
            .expect(&format!(
                "dealer/router {bind_side}-binds round {i} timed out"
            ))
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), &b"d1"[..]);
        assert_eq!(m.part_bytes(1).unwrap(), &b"hi"[..]);

        router
            .send(Message::multipart([
                Bytes::from_static(b"d1"),
                Bytes::from_static(b"yo"),
            ]))
            .await
            .unwrap();
        let m = tokio::time::timeout(TIMEOUT, dealer.recv())
            .await
            .expect(&format!(
                "dealer/router {bind_side}-binds round {i} reply timed out"
            ))
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), &b"yo"[..]);
    }
}

// ── TCP ─────────────────────────────────────────────────────────

#[tokio::test]
async fn push_pull_tcp_push_binds() {
    stress_push_pull(&Transport::Tcp, "push").await;
}
#[tokio::test]
async fn push_pull_tcp_pull_binds() {
    stress_push_pull(&Transport::Tcp, "pull").await;
}
#[tokio::test]
async fn req_rep_tcp_rep_binds() {
    stress_req_rep(&Transport::Tcp, "rep").await;
}
#[tokio::test]
async fn req_rep_tcp_req_binds() {
    stress_req_rep(&Transport::Tcp, "req").await;
}
#[tokio::test]
async fn pub_sub_tcp_pub_binds() {
    stress_pub_sub(&Transport::Tcp, "pub").await;
}
#[tokio::test]
async fn pub_sub_tcp_sub_binds() {
    stress_pub_sub(&Transport::Tcp, "sub").await;
}
#[tokio::test]
async fn pair_tcp() {
    stress_pair(&Transport::Tcp).await;
}
#[tokio::test]
async fn dealer_router_tcp_router_binds() {
    stress_dealer_router(&Transport::Tcp, "router").await;
}
#[tokio::test]
async fn dealer_router_tcp_dealer_binds() {
    stress_dealer_router(&Transport::Tcp, "dealer").await;
}

// ── IPC ─────────────────────────────────────────────────────────

#[tokio::test]
async fn push_pull_ipc_push_binds() {
    stress_push_pull(&Transport::Ipc, "push").await;
}
#[tokio::test]
async fn push_pull_ipc_pull_binds() {
    stress_push_pull(&Transport::Ipc, "pull").await;
}
#[tokio::test]
async fn req_rep_ipc_rep_binds() {
    stress_req_rep(&Transport::Ipc, "rep").await;
}
#[tokio::test]
async fn req_rep_ipc_req_binds() {
    stress_req_rep(&Transport::Ipc, "req").await;
}
#[tokio::test]
async fn pub_sub_ipc_pub_binds() {
    stress_pub_sub(&Transport::Ipc, "pub").await;
}
#[tokio::test]
async fn pub_sub_ipc_sub_binds() {
    stress_pub_sub(&Transport::Ipc, "sub").await;
}
#[tokio::test]
async fn pair_ipc() {
    stress_pair(&Transport::Ipc).await;
}
#[tokio::test]
async fn dealer_router_ipc_router_binds() {
    stress_dealer_router(&Transport::Ipc, "router").await;
}
#[tokio::test]
async fn dealer_router_ipc_dealer_binds() {
    stress_dealer_router(&Transport::Ipc, "dealer").await;
}

// ── inproc ──────────────────────────────────────────────────────

#[tokio::test]
async fn push_pull_inproc_push_binds() {
    stress_push_pull(&Transport::Inproc, "push").await;
}
#[tokio::test]
async fn push_pull_inproc_pull_binds() {
    stress_push_pull(&Transport::Inproc, "pull").await;
}
#[tokio::test]
async fn req_rep_inproc_rep_binds() {
    stress_req_rep(&Transport::Inproc, "rep").await;
}
#[tokio::test]
async fn req_rep_inproc_req_binds() {
    stress_req_rep(&Transport::Inproc, "req").await;
}
#[tokio::test]
async fn pub_sub_inproc_pub_binds() {
    stress_pub_sub(&Transport::Inproc, "pub").await;
}
#[tokio::test]
async fn pub_sub_inproc_sub_binds() {
    stress_pub_sub(&Transport::Inproc, "sub").await;
}
#[tokio::test]
async fn pair_inproc() {
    stress_pair(&Transport::Inproc).await;
}
#[tokio::test]
async fn dealer_router_inproc_router_binds() {
    stress_dealer_router(&Transport::Inproc, "router").await;
}
#[tokio::test]
async fn dealer_router_inproc_dealer_binds() {
    stress_dealer_router(&Transport::Inproc, "dealer").await;
}

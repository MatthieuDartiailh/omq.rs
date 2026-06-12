//! PLAIN end-to-end integration tests: username/password handshake
//! between two omq-tokio sockets.

#![cfg(feature = "plain")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};
use omq_tokio::endpoint::Host;

// Authentication tests must use a real transport (IPC on Unix, TCP on Windows)
// because inproc bypasses authentication (no wire codec in fast path)
#[cfg(unix)]
fn auth_ep(name: &str) -> Endpoint {
    use omq_tokio::IpcPath;
    Endpoint::Ipc(IpcPath::Abstract(format!(
        "omq-plain-{name}-{}",
        std::process::id()
    )))
}

#[cfg(not(unix))]
fn auth_ep(_name: &str) -> Endpoint {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::atomic::{AtomicU16, Ordering};
    static PORT: AtomicU16 = AtomicU16::new(13000);
    let port = PORT.fetch_add(1, Ordering::SeqCst);
    Endpoint::Tcp {
        host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port,
    }
}

fn accept_alice(peer: &omq_tokio::MechanismPeerInfo) -> bool {
    peer.username.as_deref() == Some("alice") && peer.password.as_deref() == Some("secret")
}

#[tokio::test]
async fn plain_push_pull_roundtrip() {
    let ep = auth_ep("push-pull");

    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "secret"),
    );
    client.connect(ep).await.unwrap();

    client
        .send(Message::single("hello over plain"))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"hello over plain"[..]);
}

#[tokio::test]
async fn plain_multipart_roundtrip() {
    let ep = auth_ep("multipart");

    let pair_a = Socket::new(
        SocketType::Pair,
        Options::default().plain_server(accept_alice),
    );
    pair_a.bind(ep.clone()).await.unwrap();

    let pair_b = Socket::new(
        SocketType::Pair,
        Options::default().plain_client("alice", "secret"),
    );
    pair_b.connect(ep).await.unwrap();

    pair_b
        .send(Message::multipart(["a", "bb", "ccc"]))
        .await
        .unwrap();

    let m = tokio::time::timeout(Duration::from_secs(2), pair_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 3);
    assert_eq!(m.part_bytes(0).unwrap(), &b"a"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"bb"[..]);
    assert_eq!(m.part_bytes(2).unwrap(), &b"ccc"[..]);
}

#[tokio::test]
async fn plain_wrong_credentials_rejected() {
    let ep = auth_ep("wrong-creds");

    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "wrong"),
    );
    client.connect(ep).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let _ = tokio::time::timeout(
        Duration::from_millis(50),
        client.send(Message::single("ghost")),
    )
    .await;
    let r = tokio::time::timeout(Duration::from_millis(200), server.recv()).await;
    assert!(r.is_err(), "wrong credentials must prevent delivery");
}

#[tokio::test]
async fn plain_authenticator_callback_runs() {
    let ep = auth_ep("auth-callback");

    let saw = Arc::new(AtomicBool::new(false));
    let saw_cb = saw.clone();

    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(move |peer| {
            saw_cb.store(true, Ordering::SeqCst);
            accept_alice(peer)
        }),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "secret"),
    );
    client.connect(ep).await.unwrap();

    client.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"hi");
    assert!(saw.load(Ordering::SeqCst), "authenticator must run");
}

#[tokio::test]
async fn plain_req_rep() {
    let ep = auth_ep("req-rep");

    let rep = Socket::new(
        SocketType::Rep,
        Options::default().plain_server(accept_alice),
    );
    rep.bind(ep.clone()).await.unwrap();

    let req = Socket::new(
        SocketType::Req,
        Options::default().plain_client("alice", "secret"),
    );
    req.connect(ep).await.unwrap();

    req.send(Message::single("q")).await.unwrap();
    let q = tokio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q.part_bytes(0).unwrap(), &b"q"[..]);

    rep.send(Message::single("a")).await.unwrap();
    let a = tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.part_bytes(0).unwrap(), &b"a"[..]);
}

#[tokio::test]
async fn plain_dealer_router() {
    let ep = auth_ep("dealer-router");

    let router = Socket::new(
        SocketType::Router,
        Options::default().plain_server(accept_alice),
    );
    router.bind(ep.clone()).await.unwrap();

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default()
            .identity(bytes::Bytes::from_static(b"d1"))
            .plain_client("alice", "secret"),
    );
    dealer.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    dealer.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"d1"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"hi"[..]);
}

#[tokio::test]
async fn plain_pub_sub() {
    let ep = auth_ep("pub-sub");

    let p = Socket::new(
        SocketType::Pub,
        Options::default().plain_server(accept_alice),
    );
    p.bind(ep.clone()).await.unwrap();

    let s = Socket::new(
        SocketType::Sub,
        Options::default().plain_client("alice", "secret"),
    );
    s.subscribe("").await.unwrap();
    s.connect(ep).await.unwrap();

    for _ in 0..30 {
        let _ = p.send(Message::single("hello")).await;
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(50), s.recv()).await {
            assert_eq!(m.part_bytes(0).unwrap(), &b"hello"[..]);
            return;
        }
    }
    panic!("SUB never received over PLAIN");
}

#[tokio::test]
async fn plain_empty_message() {
    let ep = auth_ep("empty-msg");

    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "secret"),
    );
    client.connect(ep).await.unwrap();

    client
        .send(Message::single(bytes::Bytes::new()))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(m.part_bytes(0).unwrap().is_empty());
}

#[tokio::test]
async fn plain_large_message() {
    let ep = auth_ep("large-msg");

    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "secret"),
    );
    client.connect(ep).await.unwrap();

    let data = vec![0xAB_u8; 256 * 1024];
    client.send(Message::single(data.clone())).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(5), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().to_vec(), data);
}

#[tokio::test]
async fn plain_reconnect_after_server_restart() {
    use omq_tokio::options::ReconnectPolicy;

    let ep = auth_ep("reconnect");

    let server1 = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    server1.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default()
            .plain_client("alice", "secret")
            .reconnect(ReconnectPolicy::Fixed(Duration::from_millis(50))),
    );
    client.connect(ep.clone()).await.unwrap();

    client.send(Message::single("before")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server1.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"before"[..]);

    server1.close().await.unwrap();

    let server2 = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    let mut bound = false;
    for _ in 0..20 {
        if server2.bind(ep.clone()).await.is_ok() {
            bound = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound);

    client.send(Message::single("after")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(5), server2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"after"[..]);
}

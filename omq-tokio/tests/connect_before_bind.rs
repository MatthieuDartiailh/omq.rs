//! Connect-before-bind: the dialer connects before the listener binds.
//! The dialer must retry until the listener appears, then deliver messages.
//! Tested across inproc, IPC, and TCP for PUSH/PULL, REQ/REP, and PAIR.

use std::net::TcpListener as StdTcpListener;
use std::time::Duration;

use omq_proto::endpoint::IpcPath;
use omq_tokio::endpoint::Host;
use omq_tokio::options::ReconnectPolicy;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn opts() -> Options {
    Options {
        reconnect: ReconnectPolicy::Fixed(Duration::from_millis(20)),
        ..Default::default()
    }
}

fn free_tcp_port() -> u16 {
    let l = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

fn ipc_ep(name: &str) -> Endpoint {
    let path = std::env::temp_dir().join(format!(
        "omq-cbb-{name}-{}-{}.sock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    Endpoint::Ipc(IpcPath::Filesystem(path))
}

const BIND_DELAY: Duration = Duration::from_millis(100);
const TIMEOUT: Duration = Duration::from_secs(5);

// -- PUSH/PULL ---------------------------------------------------------------

async fn push_pull_connect_before_bind(ep: Endpoint) {
    let push = Socket::new(SocketType::Push, opts());
    push.connect(ep.clone()).await.unwrap();

    tokio::time::sleep(BIND_DELAY).await;

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep).await.unwrap();

    push.send(Message::single("late")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"late"[..]);
}

#[tokio::test]
async fn push_pull_connect_before_bind_inproc() {
    push_pull_connect_before_bind(inproc_ep("cbb-pp-inproc")).await;
}

#[tokio::test]
async fn push_pull_connect_before_bind_ipc() {
    push_pull_connect_before_bind(ipc_ep("cbb-pp")).await;
}

#[tokio::test]
async fn push_pull_connect_before_bind_tcp() {
    push_pull_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- REQ/REP -----------------------------------------------------------------

async fn req_rep_connect_before_bind(ep: Endpoint) {
    let req = Socket::new(SocketType::Req, opts());
    req.connect(ep.clone()).await.unwrap();

    tokio::time::sleep(BIND_DELAY).await;

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep).await.unwrap();

    req.send(Message::single("q")).await.unwrap();
    let q = tokio::time::timeout(TIMEOUT, rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q.part_bytes(0).unwrap(), &b"q"[..]);

    rep.send(Message::single("a")).await.unwrap();
    let a = tokio::time::timeout(TIMEOUT, req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.part_bytes(0).unwrap(), &b"a"[..]);
}

#[tokio::test]
async fn req_rep_connect_before_bind_inproc() {
    req_rep_connect_before_bind(inproc_ep("cbb-rr-inproc")).await;
}

#[tokio::test]
async fn req_rep_connect_before_bind_ipc() {
    req_rep_connect_before_bind(ipc_ep("cbb-rr")).await;
}

#[tokio::test]
async fn req_rep_connect_before_bind_tcp() {
    req_rep_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

// -- PAIR --------------------------------------------------------------------

async fn pair_connect_before_bind(ep: Endpoint) {
    let a = Socket::new(SocketType::Pair, opts());
    a.connect(ep.clone()).await.unwrap();

    tokio::time::sleep(BIND_DELAY).await;

    let b = Socket::new(SocketType::Pair, Options::default());
    b.bind(ep).await.unwrap();

    a.send(Message::single("from-a")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"from-a"[..]);

    b.send(Message::single("from-b")).await.unwrap();
    let m = tokio::time::timeout(TIMEOUT, a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"from-b"[..]);
}

#[tokio::test]
async fn pair_connect_before_bind_inproc() {
    pair_connect_before_bind(inproc_ep("cbb-pair-inproc")).await;
}

#[tokio::test]
async fn pair_connect_before_bind_ipc() {
    pair_connect_before_bind(ipc_ep("cbb-pair")).await;
}

#[tokio::test]
async fn pair_connect_before_bind_tcp() {
    pair_connect_before_bind(tcp_ep(free_tcp_port())).await;
}

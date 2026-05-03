//! Connection-error handling: the server side must survive abrupt
//! client disconnects (pre-handshake and mid-session) and continue to
//! accept and serve new connections normally.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_tokio::endpoint::Host;
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

#[tokio::test]
async fn server_survives_pre_handshake_drop() {
    // A raw TCP client connects but drops the connection before sending
    // any ZMTP greeting. The server must not crash, panic, or reject
    // subsequent legitimate connections.
    use tokio::net::TcpStream;

    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));

    // Rude client: connect and immediately drop.
    for _ in 0..3 {
        let _ = TcpStream::connect(addr).await.unwrap();
        // Drop immediately — sends FIN with no ZMTP bytes.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Legitimate client: full ZMTP session must work.
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp_ep(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    push.send(Message::single("alive")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timed out after rude clients")
        .unwrap();
    assert_eq!(m.parts()[0].coalesce().as_ref(), b"alive");
}

#[tokio::test]
async fn server_survives_mid_session_abrupt_drop() {
    // Client drops the TCP connection abruptly (tokio socket dropped
    // without close) while the server is live. Server must survive and
    // accept the next connection.
    let port = loopback_port();

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    // First client: sends one message then drops.
    {
        let push1 = Socket::new(SocketType::Push, Options::default());
        push1.connect(tcp_ep(port)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        push1.send(Message::single("first")).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_millis(300), pull.recv()).await;
        // push1 drops here — abrupt half-close.
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second client: server must still be healthy.
    let push2 = Socket::new(SocketType::Push, Options::default());
    push2.connect(tcp_ep(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    push2.send(Message::single("second")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timed out after abrupt drop")
        .unwrap();
    assert_eq!(m.parts()[0].coalesce().as_ref(), b"second");
}

#[tokio::test]
async fn reconnect_after_ipc_peer_restarts() {
    // Dialer must reconnect when the IPC listener goes away and a new
    // one appears at the same path.
    use omq_tokio::IpcPath;
    use std::time::Instant;

    let path = format!("/tmp/omq-test-ipc-{}", std::process::id());
    let ep = Endpoint::Ipc(IpcPath::Filesystem(path.clone().into()));

    let pull1 = Socket::new(SocketType::Pull, Options::default());
    pull1.bind(ep.clone()).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options {
            reconnect: omq_tokio::options::ReconnectPolicy::Fixed(Duration::from_millis(30)),
            ..Default::default()
        },
    );
    push.connect(ep.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    push.send(Message::single("before")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull1.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(m.parts()[0].coalesce().as_ref(), b"before");

    // Listener restarts: close pull1 (removes the socket file) then rebind.
    pull1.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if pull2.bind(ep.clone()).await.is_ok() {
            break;
        }
        assert!(Instant::now() <= deadline, "could not rebind IPC endpoint");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    push.send(Message::single("after")).await.unwrap();
    let m2 = tokio::time::timeout(Duration::from_secs(3), pull2.recv())
        .await
        .expect("post-restart recv timed out")
        .unwrap();
    assert_eq!(m2.parts()[0].coalesce().as_ref(), b"after");
}

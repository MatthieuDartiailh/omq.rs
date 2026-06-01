//! IPC (Unix domain socket) end-to-end tests.

mod test_support;

use std::time::Duration;

use omq_tokio::options::ReconnectPolicy;
use omq_tokio::{Endpoint, IpcPath, Message, Options, Socket, SocketType};

fn temp_ipc(name: &str) -> Endpoint {
    let mut dir = std::env::temp_dir();
    dir.push(format!("omq-ipc-test-{name}-{}.sock", std::process::id()));
    Endpoint::Ipc(IpcPath::Filesystem(dir))
}

#[tokio::test]
async fn ipc_push_pull_roundtrip() {
    let ep = temp_ipc("push-pull");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single("hello over ipc")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"hello over ipc"[..]);
}

#[tokio::test]
async fn ipc_req_rep_roundtrip() {
    let ep = temp_ipc("req-rep");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&req).await;

    req.send(Message::single("ping")).await.unwrap();
    let got = rep.recv().await.unwrap();
    assert_eq!(got.part_bytes(0).unwrap(), &b"ping"[..]);
    rep.send(Message::single("pong")).await.unwrap();
    let reply = tokio::time::timeout(Duration::from_millis(500), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.part_bytes(0).unwrap(), &b"pong"[..]);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn ipc_abstract_push_pull_roundtrip() {
    let name = format!(
        "omq-ipc-abs-{}-{}",
        std::process::id(),
        rand::random::<u32>()
    );
    let ep = Endpoint::Ipc(IpcPath::Abstract(name));

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single("hello over abstract ipc"))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"hello over abstract ipc"[..]);
}

#[tokio::test]
async fn ipc_connect_before_bind() {
    // Connect before the socket file exists; the dialer must retry on ENOENT
    // until the listener creates the file, then deliver the message.
    let ep = temp_ipc("connect-before-bind");

    let push = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(20)),
            ..Default::default()
        },
    );
    push.connect(ep.clone()).await.unwrap();

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&pull).await;

    push.send(Message::single("late-bind")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timed out after late bind")
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"late-bind"[..]);
}

#[tokio::test]
async fn ipc_socket_file_cleaned_on_close() {
    let ep = temp_ipc("cleanup");
    let path = match &ep {
        Endpoint::Ipc(IpcPath::Filesystem(p)) => p.clone(),
        _ => unreachable!(),
    };
    let s = Socket::new(SocketType::Pull, Options::default());
    s.bind(ep).await.unwrap();
    assert!(path.exists(), "bind must create the socket file");
    s.close().await.unwrap();
    // The driver task may take a moment to drop the listener; give it a tick.
    for _ in 0..20 {
        if !path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("close should remove the socket file");
}

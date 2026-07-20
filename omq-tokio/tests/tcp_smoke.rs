mod test_support;

use std::time::Duration;

use omq_tokio::{Message, Options, Socket, SocketType};

#[tokio::test]
async fn push_pull_tcp_smoke() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let port = test_support::bind_loopback(&pull).await;
    let ep = test_support::tcp_loopback(port);

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&push).await;

    push.send(Message::single("tcp-push-pull")).await.unwrap();

    let got = tokio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .expect("pull did not receive TCP message")
        .unwrap();
    assert_eq!(got, Message::single("tcp-push-pull"));
}

#[tokio::test]
async fn req_rep_tcp_smoke() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let port = test_support::bind_loopback(&rep).await;
    let ep = test_support::tcp_loopback(port);

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&req).await;

    req.send(Message::single("tcp-request")).await.unwrap();

    let request = tokio::time::timeout(Duration::from_secs(1), rep.recv())
        .await
        .expect("rep did not receive TCP request")
        .unwrap();
    assert_eq!(request, Message::single("tcp-request"));

    rep.send(Message::single("tcp-reply")).await.unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(1), req.recv())
        .await
        .expect("req did not receive TCP reply")
        .unwrap();
    assert_eq!(reply, Message::single("tcp-reply"));
}

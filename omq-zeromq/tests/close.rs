use std::time::Duration;

use zeromq::{PullSocket, PushSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

#[tokio::test]
async fn close_never_connected() {
    let push = PushSocket::new();
    let errors = push.close().await;
    assert!(errors.is_empty());
}

#[tokio::test]
async fn close_after_bind_no_peers() {
    let mut push = PushSocket::new();
    push.bind("tcp://127.0.0.1:0").await.unwrap();
    let errors = push.close().await;
    assert!(errors.is_empty());
}

#[tokio::test]
async fn close_with_active_peer() {
    let mut push = PushSocket::new();
    let mut pull = PullSocket::new();

    let ep = push.bind("tcp://127.0.0.1:0").await.unwrap();
    pull.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(ZmqMessage::from("before-close")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"before-close");

    let errors = push.close().await;
    assert!(errors.is_empty());
}

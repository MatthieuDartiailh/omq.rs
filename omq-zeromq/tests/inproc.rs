use std::time::Duration;

use tokio::time::timeout;
use zeromq::{PullSocket, PushSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn inproc_push_pull() {
    let mut push = PushSocket::new();
    let mut pull = PullSocket::new();

    push.bind("inproc://test-push-pull").await.unwrap();
    pull.connect("inproc://test-push-pull").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(ZmqMessage::from("inproc-msg")).await.unwrap();
    let msg = timeout(TIMEOUT, pull.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"inproc-msg");
}

#[tokio::test]
async fn inproc_multiple_messages() {
    let mut push = PushSocket::new();
    let mut pull = PullSocket::new();

    push.bind("inproc://test-multi").await.unwrap();
    pull.connect("inproc://test-multi").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..100 {
        push.send(ZmqMessage::from(format!("msg-{i}")))
            .await
            .unwrap();
    }

    for i in 0..100 {
        let msg = timeout(TIMEOUT, pull.recv()).await.unwrap().unwrap();
        assert_eq!(msg.get(0).unwrap().as_ref(), format!("msg-{i}").as_bytes());
    }
}

#[tokio::test]
async fn inproc_req_rep() {
    let mut rep = zeromq::RepSocket::new();
    let mut req = zeromq::ReqSocket::new();

    rep.bind("inproc://test-reqrep").await.unwrap();
    req.connect("inproc://test-reqrep").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    req.send(ZmqMessage::from("question")).await.unwrap();
    let msg = timeout(TIMEOUT, rep.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"question");

    rep.send(ZmqMessage::from("answer")).await.unwrap();
    let reply = timeout(TIMEOUT, req.recv()).await.unwrap().unwrap();
    assert_eq!(reply.get(0).unwrap().as_ref(), b"answer");
}

#[tokio::test]
async fn inproc_pub_sub() {
    let mut pub_socket = zeromq::PubSocket::new();
    let mut sub_socket = zeromq::SubSocket::new();

    pub_socket.bind("inproc://test-pubsub").await.unwrap();
    sub_socket.connect("inproc://test-pubsub").await.unwrap();
    sub_socket.subscribe("").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    pub_socket
        .send(ZmqMessage::from("broadcast"))
        .await
        .unwrap();
    let msg = timeout(TIMEOUT, sub_socket.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"broadcast");
}

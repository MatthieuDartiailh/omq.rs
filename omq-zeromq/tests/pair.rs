use std::time::Duration;

use tokio::time::timeout;
use zeromq::{PairSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn pair_bidirectional() {
    let mut pair1 = PairSocket::new();
    let mut pair2 = PairSocket::new();

    let ep = pair1.bind("tcp://127.0.0.1:0").await.unwrap();
    pair2.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    pair1.send(ZmqMessage::from("from-1")).await.unwrap();
    let msg = timeout(TIMEOUT, pair2.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"from-1");

    pair2.send(ZmqMessage::from("from-2")).await.unwrap();
    let msg = timeout(TIMEOUT, pair1.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"from-2");
}

#[tokio::test]
async fn pair_inproc() {
    let mut pair1 = PairSocket::new();
    let mut pair2 = PairSocket::new();

    pair1.bind("inproc://pair-test").await.unwrap();
    pair2.connect("inproc://pair-test").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    pair1.send(ZmqMessage::from("ping")).await.unwrap();
    let msg = timeout(TIMEOUT, pair2.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"ping");

    pair2.send(ZmqMessage::from("pong")).await.unwrap();
    let msg = timeout(TIMEOUT, pair1.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"pong");
}

#[tokio::test]
async fn pair_multiple_messages() {
    let mut pair1 = PairSocket::new();
    let mut pair2 = PairSocket::new();

    let ep = pair1.bind("tcp://127.0.0.1:0").await.unwrap();
    pair2.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..10 {
        pair1
            .send(ZmqMessage::from(format!("msg-{i}")))
            .await
            .unwrap();
    }
    for i in 0..10 {
        let msg = timeout(TIMEOUT, pair2.recv()).await.unwrap().unwrap();
        assert_eq!(msg.get(0).unwrap().as_ref(), format!("msg-{i}").as_bytes());
    }
}

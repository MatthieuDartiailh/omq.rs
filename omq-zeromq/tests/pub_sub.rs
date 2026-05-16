use std::time::Duration;

use tokio::time::timeout;
use zeromq::{PubSocket, Socket, SocketRecv, SocketSend, SubSocket, ZmqMessage};

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn subscribe_and_receive() {
    let mut pub_socket = PubSocket::new();
    let mut sub_socket = SubSocket::new();

    let ep = pub_socket.bind("tcp://127.0.0.1:0").await.unwrap();
    sub_socket.connect(&ep.to_string()).await.unwrap();
    sub_socket.subscribe("").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    pub_socket
        .send(ZmqMessage::from("hello world"))
        .await
        .unwrap();

    let msg = timeout(TIMEOUT, sub_socket.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"hello world");
}

#[tokio::test]
async fn topic_filtering() {
    let mut pub_socket = PubSocket::new();
    let mut sub_socket = SubSocket::new();

    let ep = pub_socket.bind("tcp://127.0.0.1:0").await.unwrap();
    sub_socket.connect(&ep.to_string()).await.unwrap();
    sub_socket.subscribe("topic.a").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    pub_socket
        .send(ZmqMessage::from("topic.a hello"))
        .await
        .unwrap();
    pub_socket
        .send(ZmqMessage::from("topic.b ignore"))
        .await
        .unwrap();
    pub_socket
        .send(ZmqMessage::from("topic.a world"))
        .await
        .unwrap();

    let msg1 = timeout(TIMEOUT, sub_socket.recv()).await.unwrap().unwrap();
    assert_eq!(msg1.get(0).unwrap().as_ref(), b"topic.a hello");

    let msg2 = timeout(TIMEOUT, sub_socket.recv()).await.unwrap().unwrap();
    assert_eq!(msg2.get(0).unwrap().as_ref(), b"topic.a world");

    // topic.b should not arrive
    let result = timeout(Duration::from_millis(100), sub_socket.recv()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn multiple_subscribers() {
    let mut pub_socket = PubSocket::new();
    let ep = pub_socket.bind("tcp://127.0.0.1:0").await.unwrap();

    let mut sub1 = SubSocket::new();
    sub1.connect(&ep.to_string()).await.unwrap();
    sub1.subscribe("").await.unwrap();

    let mut sub2 = SubSocket::new();
    sub2.connect(&ep.to_string()).await.unwrap();
    sub2.subscribe("").await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    pub_socket
        .send(ZmqMessage::from("broadcast"))
        .await
        .unwrap();

    let msg1 = timeout(TIMEOUT, sub1.recv()).await.unwrap().unwrap();
    let msg2 = timeout(TIMEOUT, sub2.recv()).await.unwrap().unwrap();
    assert_eq!(msg1.get(0).unwrap().as_ref(), b"broadcast");
    assert_eq!(msg2.get(0).unwrap().as_ref(), b"broadcast");
}

#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let mut pub_socket = PubSocket::new();
    let mut sub_socket = SubSocket::new();

    let ep = pub_socket.bind("tcp://127.0.0.1:0").await.unwrap();
    sub_socket.connect(&ep.to_string()).await.unwrap();
    sub_socket.subscribe("topic").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    pub_socket
        .send(ZmqMessage::from("topic first"))
        .await
        .unwrap();
    let msg = timeout(TIMEOUT, sub_socket.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"topic first");

    sub_socket.unsubscribe("topic").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    pub_socket
        .send(ZmqMessage::from("topic second"))
        .await
        .unwrap();
    let result = timeout(Duration::from_millis(200), sub_socket.recv()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn multiple_subscriptions() {
    let mut pub_socket = PubSocket::new();
    let mut sub_socket = SubSocket::new();

    let ep = pub_socket.bind("tcp://127.0.0.1:0").await.unwrap();
    sub_socket.connect(&ep.to_string()).await.unwrap();
    sub_socket.subscribe("A").await.unwrap();
    sub_socket.subscribe("B").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    pub_socket
        .send(ZmqMessage::from("A message"))
        .await
        .unwrap();
    pub_socket
        .send(ZmqMessage::from("C ignored"))
        .await
        .unwrap();
    pub_socket
        .send(ZmqMessage::from("B message"))
        .await
        .unwrap();

    let msg1 = timeout(TIMEOUT, sub_socket.recv()).await.unwrap().unwrap();
    assert_eq!(msg1.get(0).unwrap().as_ref(), b"A message");
    let msg2 = timeout(TIMEOUT, sub_socket.recv()).await.unwrap().unwrap();
    assert_eq!(msg2.get(0).unwrap().as_ref(), b"B message");
}

#[tokio::test]
async fn empty_subscription_receives_all() {
    let mut pub_socket = PubSocket::new();
    let mut sub_socket = SubSocket::new();

    let ep = pub_socket.bind("tcp://127.0.0.1:0").await.unwrap();
    sub_socket.connect(&ep.to_string()).await.unwrap();
    sub_socket.subscribe("").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    pub_socket.send(ZmqMessage::from("anything")).await.unwrap();
    pub_socket
        .send(ZmqMessage::from("everything"))
        .await
        .unwrap();

    let msg1 = timeout(TIMEOUT, sub_socket.recv()).await.unwrap().unwrap();
    let msg2 = timeout(TIMEOUT, sub_socket.recv()).await.unwrap().unwrap();
    assert_eq!(msg1.get(0).unwrap().as_ref(), b"anything");
    assert_eq!(msg2.get(0).unwrap().as_ref(), b"everything");
}

#[tokio::test]
async fn late_subscriber_misses_messages() {
    let mut pub_socket = PubSocket::new();
    let ep = pub_socket.bind("tcp://127.0.0.1:0").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    pub_socket.send(ZmqMessage::from("early")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut sub_socket = SubSocket::new();
    sub_socket.connect(&ep.to_string()).await.unwrap();
    sub_socket.subscribe("").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    pub_socket.send(ZmqMessage::from("late")).await.unwrap();

    let msg = timeout(TIMEOUT, sub_socket.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"late");
}

#[tokio::test]
async fn publisher_with_no_subscribers() {
    let mut pub_socket = PubSocket::new();
    let _ep = pub_socket.bind("tcp://127.0.0.1:0").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Should not block or error
    pub_socket.send(ZmqMessage::from("dropped")).await.unwrap();
    pub_socket
        .send(ZmqMessage::from("also dropped"))
        .await
        .unwrap();
}

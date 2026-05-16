#![allow(clippy::similar_names)]

use std::time::Duration;

use tokio::time::timeout;
use zeromq::{Socket, SocketRecv, SocketSend, XPubSocket, XSubSocket, ZmqMessage};

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn xpub_receives_subscription() {
    let mut xpub = XPubSocket::new();
    let mut xsub = XSubSocket::new();

    let ep = xpub.bind("tcp://127.0.0.1:0").await.unwrap();
    xsub.connect(&ep.to_string()).await.unwrap();
    xsub.subscribe("topic").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // XPUB receives subscription command as a message:
    // first byte 0x01 (subscribe) + topic bytes
    let msg = timeout(TIMEOUT, xpub.recv()).await.unwrap().unwrap();
    assert_eq!(msg.len(), 1);
    let frame = msg.get(0).unwrap();
    assert_eq!(frame[0], 0x01); // subscribe
    assert_eq!(&frame[1..], b"topic");
}

#[tokio::test]
async fn xpub_xsub_forwarding() {
    let mut xpub = XPubSocket::new();
    let mut xsub = XSubSocket::new();

    let ep = xpub.bind("tcp://127.0.0.1:0").await.unwrap();
    xsub.connect(&ep.to_string()).await.unwrap();
    xsub.subscribe("").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drain the subscribe notification from XPUB
    let _sub_msg = timeout(TIMEOUT, xpub.recv()).await.unwrap().unwrap();

    // Send data through XPUB
    xpub.send(ZmqMessage::from("data")).await.unwrap();
    let msg = timeout(TIMEOUT, xsub.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"data");
}

#[tokio::test]
async fn xpub_receives_unsubscription() {
    let mut xpub = XPubSocket::new();
    let mut xsub = XSubSocket::new();

    let ep = xpub.bind("tcp://127.0.0.1:0").await.unwrap();
    xsub.connect(&ep.to_string()).await.unwrap();
    xsub.subscribe("topic").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drain subscribe
    let _sub = timeout(TIMEOUT, xpub.recv()).await.unwrap().unwrap();

    xsub.unsubscribe("topic").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // XPUB receives unsubscription: first byte 0x00 (unsubscribe) + topic
    let msg = timeout(TIMEOUT, xpub.recv()).await.unwrap().unwrap();
    let frame = msg.get(0).unwrap();
    assert_eq!(frame[0], 0x00); // unsubscribe
    assert_eq!(&frame[1..], b"topic");
}

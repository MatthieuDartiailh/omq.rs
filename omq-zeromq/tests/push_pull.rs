use std::time::Duration;

use bytes::Bytes;
use tokio::time::timeout;
use zeromq::{Endpoint, PullSocket, PushSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn single_message() {
    let mut push = PushSocket::new();
    let mut pull = PullSocket::new();

    let ep = push.bind("tcp://127.0.0.1:0").await.unwrap();
    pull.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(ZmqMessage::from("hello")).await.unwrap();
    let msg = timeout(TIMEOUT, pull.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"hello");
}

#[tokio::test]
async fn multiframe_message() {
    let mut push = PushSocket::new();
    let mut pull = PullSocket::new();

    let ep = push.bind("tcp://127.0.0.1:0").await.unwrap();
    pull.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut msg = ZmqMessage::new();
    msg.push_back(Bytes::from_static(b"part1"));
    msg.push_back(Bytes::from_static(b"part2"));
    msg.push_back(Bytes::from_static(b"part3"));
    push.send(msg).await.unwrap();

    let received = timeout(TIMEOUT, pull.recv()).await.unwrap().unwrap();
    assert_eq!(received.len(), 3);
    assert_eq!(received.get(0).unwrap().as_ref(), b"part1");
    assert_eq!(received.get(1).unwrap().as_ref(), b"part2");
    assert_eq!(received.get(2).unwrap().as_ref(), b"part3");
}

#[tokio::test]
async fn multiple_messages_in_sequence() {
    let mut push = PushSocket::new();
    let mut pull = PullSocket::new();

    let ep = push.bind("tcp://127.0.0.1:0").await.unwrap();
    pull.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..10 {
        push.send(ZmqMessage::from(format!("msg-{i}")))
            .await
            .unwrap();
    }

    for i in 0..10 {
        let msg = timeout(TIMEOUT, pull.recv()).await.unwrap().unwrap();
        assert_eq!(msg.get(0).unwrap().as_ref(), format!("msg-{i}").as_bytes());
    }
}

#[tokio::test]
async fn multiple_push_to_one_pull() {
    let mut pull = PullSocket::new();
    let ep = pull.bind("tcp://127.0.0.1:0").await.unwrap();

    let mut pushers = Vec::new();
    for _ in 0..3 {
        let mut push = PushSocket::new();
        push.connect(&ep.to_string()).await.unwrap();
        pushers.push(push);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    for (i, push) in pushers.iter_mut().enumerate() {
        push.send(ZmqMessage::from(format!("from-{i}")))
            .await
            .unwrap();
    }

    let mut received = Vec::new();
    for _ in 0..3 {
        let msg = timeout(TIMEOUT, pull.recv()).await.unwrap().unwrap();
        received.push(msg.get(0).unwrap().clone());
    }
    received.sort();
    assert_eq!(received[0].as_ref(), b"from-0");
    assert_eq!(received[1].as_ref(), b"from-1");
    assert_eq!(received[2].as_ref(), b"from-2");
}

#[tokio::test]
async fn one_push_to_multiple_pull() {
    let mut push = PushSocket::new();
    let ep = push.bind("tcp://127.0.0.1:0").await.unwrap();

    let mut pulls: Vec<PullSocket> = Vec::new();
    for _ in 0..3 {
        let mut pull = PullSocket::new();
        pull.connect(&ep.to_string()).await.unwrap();
        pulls.push(pull);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..9 {
        push.send(ZmqMessage::from(format!("msg-{i}")))
            .await
            .unwrap();
    }

    let mut total = 0;
    for pull in &mut pulls {
        while let Ok(Ok(_)) = timeout(Duration::from_millis(200), pull.recv()).await {
            total += 1;
        }
    }
    assert_eq!(total, 9);
}

#[tokio::test]
async fn large_message() {
    let mut push = PushSocket::new();
    let mut pull = PullSocket::new();

    let ep = push.bind("tcp://127.0.0.1:0").await.unwrap();
    pull.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let data = vec![0xAB_u8; 1_000_000];
    push.send(ZmqMessage::from(data.clone())).await.unwrap();

    let msg = timeout(TIMEOUT, pull.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().len(), 1_000_000);
    assert_eq!(msg.get(0).unwrap().as_ref(), data.as_slice());
}

#[tokio::test]
async fn empty_message() {
    let mut push = PushSocket::new();
    let mut pull = PullSocket::new();

    let ep = push.bind("tcp://127.0.0.1:0").await.unwrap();
    pull.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(ZmqMessage::from(Vec::<u8>::new())).await.unwrap();

    let msg = timeout(TIMEOUT, pull.recv()).await.unwrap().unwrap();
    assert_eq!(msg.len(), 1);
    assert!(msg.get(0).unwrap().is_empty());
}

#[tokio::test]
async fn bind_returns_resolved_endpoint() {
    let mut push = PushSocket::new();
    let ep = push.bind("tcp://127.0.0.1:0").await.unwrap();
    match ep {
        Endpoint::Tcp(addr) => {
            assert_ne!(addr.port(), 0);
            assert_eq!(addr.ip().to_string(), "127.0.0.1");
        }
        _ => panic!("expected TCP endpoint"),
    }
}

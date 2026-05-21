#![cfg(feature = "ws")]

use bytes::Bytes;
use omq_compio::Socket;
use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;

fn ws_endpoint(port: u16) -> Endpoint {
    format!("ws://127.0.0.1:{port}/").parse().unwrap()
}

#[compio::test]
async fn push_pull_one_message() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let bound = pull.bind(ws_endpoint(0)).await.unwrap();

    let port = match &bound {
        Endpoint::Ws { port, .. } => *port,
        other => panic!("expected Ws endpoint, got {other:?}"),
    };

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ws_endpoint(port)).await.unwrap();

    compio::time::sleep(std::time::Duration::from_millis(200)).await;

    push.send(Message::from(Bytes::from_static(b"hello ws")))
        .await
        .unwrap();

    let msg = compio::time::timeout(std::time::Duration::from_secs(5), pull.recv())
        .await
        .expect("recv timed out")
        .unwrap();

    assert_eq!(msg.part_bytes(0).unwrap(), &b"hello ws"[..]);
}

#[compio::test]
async fn push_pull_multipart() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let bound = pull.bind(ws_endpoint(0)).await.unwrap();

    let port = match &bound {
        Endpoint::Ws { port, .. } => *port,
        other => panic!("expected Ws endpoint, got {other:?}"),
    };

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ws_endpoint(port)).await.unwrap();

    compio::time::sleep(std::time::Duration::from_millis(200)).await;

    let msg = Message::multipart([
        Bytes::from_static(b"frame1"),
        Bytes::from_static(b"frame2"),
        Bytes::from_static(b"frame3"),
    ]);
    push.send(msg).await.unwrap();

    let received = compio::time::timeout(std::time::Duration::from_secs(5), pull.recv())
        .await
        .expect("recv timed out")
        .unwrap();

    assert_eq!(received.len(), 3);
    assert_eq!(received.part_bytes(0).unwrap(), &b"frame1"[..]);
    assert_eq!(received.part_bytes(1).unwrap(), &b"frame2"[..]);
    assert_eq!(received.part_bytes(2).unwrap(), &b"frame3"[..]);
}

#[compio::test]
async fn push_pull_many_messages() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let bound = pull.bind(ws_endpoint(0)).await.unwrap();

    let port = match &bound {
        Endpoint::Ws { port, .. } => *port,
        other => panic!("expected Ws endpoint, got {other:?}"),
    };

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ws_endpoint(port)).await.unwrap();

    compio::time::sleep(std::time::Duration::from_millis(200)).await;

    let count = 100;
    for i in 0..count {
        push.send(Message::from(Bytes::from(format!("msg-{i}"))))
            .await
            .unwrap();
    }

    for i in 0..count {
        let msg = compio::time::timeout(std::time::Duration::from_secs(5), pull.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        assert_eq!(msg.part_bytes(0).unwrap(), format!("msg-{i}").as_bytes(),);
    }
}

#![cfg(feature = "ws")]

use std::time::Duration;

use omq_compio::Socket;
use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;

fn ws_endpoint(port: u16) -> Endpoint {
    format!("ws://127.0.0.1:{port}/").parse().unwrap()
}

fn get_port(ep: &Endpoint) -> u16 {
    match ep {
        Endpoint::Ws { port, .. } => *port,
        other => panic!("expected Ws endpoint, got {other:?}"),
    }
}

#[compio::test]
async fn ws_pub_sub_basic() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let bound = pub_.bind(ws_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.subscribe("news.").await.unwrap();
    sub.connect(ws_endpoint(port)).await.unwrap();

    compio::time::sleep(Duration::from_millis(300)).await;

    pub_.send(Message::multipart(["news.sports", "goal scored"]))
        .await
        .unwrap();

    let msg = compio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("recv timed out")
        .unwrap();

    assert_eq!(msg.part_bytes(0).unwrap(), &b"news.sports"[..]);
    assert_eq!(msg.part_bytes(1).unwrap(), &b"goal scored"[..]);
}

#[compio::test]
async fn ws_pub_sub_unsubscribe() {
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let bound = pub_.bind(ws_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.subscribe("news.").await.unwrap();
    sub.connect(ws_endpoint(port)).await.unwrap();

    compio::time::sleep(Duration::from_millis(300)).await;

    pub_.send(Message::multipart(["news.sports", "first"]))
        .await
        .unwrap();

    let msg = compio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("recv timed out")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"news.sports"[..]);

    sub.unsubscribe("news.").await.unwrap();
    compio::time::sleep(Duration::from_millis(200)).await;

    pub_.send(Message::multipart(["news.tech", "second"]))
        .await
        .unwrap();

    let result = compio::time::timeout(Duration::from_millis(500), sub.recv()).await;
    assert!(
        result.is_err(),
        "message should not arrive after unsubscribe"
    );
}

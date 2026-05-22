#![cfg(feature = "ws")]

use std::time::Duration;

use bytes::Bytes;
use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_tokio::Socket;

fn ws_endpoint(port: u16) -> Endpoint {
    format!("ws://127.0.0.1:{port}/").parse().unwrap()
}

fn get_port(ep: &Endpoint) -> u16 {
    match ep {
        Endpoint::Ws { port, .. } => *port,
        other => panic!("expected Ws, got {other:?}"),
    }
}

#[tokio::test]
async fn ws_radio_dish_basic() {
    let radio = Socket::new(SocketType::Radio, Options::default());
    let bound = radio.bind(ws_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let dish = Socket::new(SocketType::Dish, Options::default());
    dish.join("weather").await.unwrap();
    dish.connect(ws_endpoint(port)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    radio
        .send(Message::multipart([
            Bytes::from_static(b"weather"),
            Bytes::from_static(b"sunny"),
        ]))
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(5), dish.recv())
        .await
        .expect("recv timed out")
        .unwrap();

    assert_eq!(msg.part_bytes(0).unwrap(), &b"weather"[..]);
    assert_eq!(msg.part_bytes(1).unwrap(), &b"sunny"[..]);
}

#[tokio::test]
async fn ws_radio_dish_leave() {
    let radio = Socket::new(SocketType::Radio, Options::default());
    let bound = radio.bind(ws_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let dish = Socket::new(SocketType::Dish, Options::default());
    dish.join("weather").await.unwrap();
    dish.connect(ws_endpoint(port)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    radio
        .send(Message::multipart([
            Bytes::from_static(b"weather"),
            Bytes::from_static(b"sunny"),
        ]))
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(5), dish.recv())
        .await
        .expect("recv timed out")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"weather"[..]);

    dish.leave("weather").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    radio
        .send(Message::multipart([
            Bytes::from_static(b"weather"),
            Bytes::from_static(b"rain"),
        ]))
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_millis(500), dish.recv()).await;
    assert!(result.is_err(), "message should not arrive after leave");
}

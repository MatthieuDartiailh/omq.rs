#![cfg(all(feature = "ws", feature = "plain"))]

use bytes::Bytes;
use omq_compio::{MechanismPeerInfo, Socket};
use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use std::time::Duration;

fn accept_alice(peer: &MechanismPeerInfo) -> bool {
    peer.username.as_deref() == Some("alice") && peer.password.as_deref() == Some("secret")
}

fn ws_endpoint(port: u16) -> Endpoint {
    format!("ws://127.0.0.1:{port}/").parse().unwrap()
}

fn get_port(ep: &Endpoint) -> u16 {
    match ep {
        Endpoint::Ws { port, .. } => *port,
        other => panic!("expected Ws, got {other:?}"),
    }
}

#[compio::test]
async fn ws_plain_push_pull() {
    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    let bound = server.bind(ws_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "secret"),
    );
    client.connect(ws_endpoint(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(300)).await;

    client
        .send(Message::from(Bytes::from_static(b"hello plain ws")))
        .await
        .unwrap();

    let msg = compio::time::timeout(Duration::from_secs(5), server.recv())
        .await
        .expect("recv timed out")
        .unwrap();
    assert_eq!(msg, Message::single("hello plain ws"));
}

#[compio::test]
async fn ws_plain_rejected() {
    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    let bound = server.bind(ws_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "wrong"),
    );
    client.connect(ws_endpoint(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(300)).await;

    client
        .send(Message::from(Bytes::from_static(b"should not arrive")))
        .await
        .unwrap();

    let result = compio::time::timeout(Duration::from_millis(500), server.recv()).await;
    assert!(
        result.is_err(),
        "expected timeout, message should not arrive"
    );
}

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
async fn ws_reconnect_after_server_restart() {
    let pull1 = Socket::new(SocketType::Pull, Options::default());
    let bound = pull1.bind(ws_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let push1 = Socket::new(SocketType::Push, Options::default());
    push1.connect(ws_endpoint(port)).await.unwrap();

    compio::time::sleep(Duration::from_millis(300)).await;

    push1.send(Message::single("before")).await.unwrap();
    let msg = compio::time::timeout(Duration::from_secs(5), pull1.recv())
        .await
        .expect("recv timed out")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"before"[..]);

    drop(push1);
    pull1.close().await.unwrap();

    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let mut rebound = false;
    for _ in 0..40 {
        if pull2.bind(ws_endpoint(port)).await.is_ok() {
            rebound = true;
            break;
        }
        compio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(rebound, "pull2 failed to bind on same port");

    let push2 = Socket::new(SocketType::Push, Options::default());
    push2.connect(ws_endpoint(port)).await.unwrap();

    compio::time::sleep(Duration::from_millis(300)).await;

    push2.send(Message::single("after")).await.unwrap();
    let msg = compio::time::timeout(Duration::from_secs(5), pull2.recv())
        .await
        .expect("recv after restart timed out")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"after"[..]);
}

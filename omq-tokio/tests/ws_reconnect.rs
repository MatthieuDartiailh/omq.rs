#![cfg(feature = "ws")]

use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::time::Duration;

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
async fn ws_reconnect_after_server_restart() {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let pull1 = Socket::new(SocketType::Pull, Options::default());
    let bound = pull1.bind(ws_endpoint(port)).await.unwrap();
    let port = get_port(&bound);

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ws_endpoint(port)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    push.send(Message::single("before")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), pull1.recv())
        .await
        .expect("recv timed out")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"before"[..]);

    pull1.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let mut bound = false;
    for _ in 0..20 {
        if pull2.bind(ws_endpoint(port)).await.is_ok() {
            bound = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "pull2 failed to bind after pull1 closed");

    tokio::time::sleep(Duration::from_millis(300)).await;

    push.send(Message::single("after")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), pull2.recv())
        .await
        .expect("recv after restart timed out")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"after"[..]);
}

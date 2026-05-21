//! Cross-runtime WS interop: tokio backend <-> compio backend over
//! ws://. Drives compio on a dedicated thread while tokio runs in the
//! test's own runtime.

#![cfg(feature = "ws")]

use std::net::{IpAddr, Ipv4Addr, TcpListener as StdTcpListener};
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use omq_proto::endpoint::Host;

fn free_tcp_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn ws_tokio(port: u16) -> omq_tokio::Endpoint {
    omq_tokio::Endpoint::Ws {
        host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port,
        path: "/".into(),
    }
}

fn ws_compio(port: u16) -> omq_compio::Endpoint {
    omq_compio::Endpoint::Ws {
        host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port,
        path: "/".into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_push_to_compio_pull_ws() {
    let port = free_tcp_port();
    let (bound_tx, bound_rx) = std::sync::mpsc::channel();

    let pull_thread = thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            use omq_compio::{Options, Socket, SocketType};
            let pull = Socket::new(SocketType::Pull, Options::default());
            pull.bind(ws_compio(port)).await.unwrap();
            let _ = bound_tx.send(());
            let mut got = Vec::new();
            for _ in 0..3 {
                let msg = compio::time::timeout(Duration::from_secs(5), pull.recv())
                    .await
                    .expect("recv timed out")
                    .unwrap();
                got.push(msg.part_bytes(0).unwrap().to_vec());
            }
            got
        })
    });

    tokio::task::spawn_blocking(move || bound_rx.recv().unwrap())
        .await
        .unwrap();

    let push = omq_tokio::Socket::new(omq_tokio::SocketType::Push, omq_tokio::Options::default());
    push.connect(ws_tokio(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    for i in 0..3u32 {
        push.send(omq_proto::message::Message::from(Bytes::from(format!(
            "ws-msg-{i}"
        ))))
        .await
        .unwrap();
    }

    let got = pull_thread.join().expect("compio thread panicked");
    assert_eq!(got.len(), 3);
    for (i, data) in got.iter().enumerate() {
        assert_eq!(data, format!("ws-msg-{i}").as_bytes());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compio_push_to_tokio_pull_ws() {
    let port = free_tcp_port();

    let pull = omq_tokio::Socket::new(omq_tokio::SocketType::Pull, omq_tokio::Options::default());
    pull.bind(ws_tokio(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let push_thread = thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            use omq_compio::{Options, Socket, SocketType};
            let push = Socket::new(SocketType::Push, Options::default());
            push.connect(ws_compio(port)).await.unwrap();
            compio::time::sleep(Duration::from_millis(300)).await;
            for i in 0..3u32 {
                push.send(omq_proto::message::Message::from(Bytes::from(format!(
                    "ws-rev-{i}"
                ))))
                .await
                .unwrap();
            }
            compio::time::sleep(Duration::from_millis(500)).await;
        });
    });

    let mut got = Vec::new();
    for _ in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(5), pull.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        got.push(msg.part_bytes(0).unwrap().to_vec());
    }

    push_thread.join().expect("compio thread panicked");
    assert_eq!(got.len(), 3);
    for (i, data) in got.iter().enumerate() {
        assert_eq!(data, format!("ws-rev-{i}").as_bytes());
    }
}

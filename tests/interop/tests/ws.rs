//! Cross-runtime WS interop: tokio backend <-> compio backend over
//! ws://. Drives compio on a dedicated thread while tokio runs in the
//! test's own runtime.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use omq_proto::MonitorEvent;
use omq_proto::endpoint::Host;

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

/// Compio binds PUB, tokio connects SUB. PUB sends a burst; SUB must
/// receive at least one message. Tests the WS accept path on compio.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compio_pub_to_tokio_sub_ws() {
    let (port_tx, port_rx) = mpsc::channel();

    let pub_thread = thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            use omq_compio::{Message, Options, Socket, SocketType};
            let p = Socket::new(SocketType::Pub, Options::default());
            let mut mon = p.monitor();
            p.bind(ws_compio(0)).await.unwrap();
            let port = loop {
                match mon.recv().await {
                    Ok(MonitorEvent::Listening {
                        endpoint: omq_compio::Endpoint::Ws { port, .. },
                    }) => break port,
                    Ok(_) => {}
                    other => panic!("expected Listening, got {other:?}"),
                }
            };
            let _ = port_tx.send(port);
            for _ in 0..50 {
                let _ = p.send(Message::single("ws.hello")).await;
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    });

    let port = tokio::task::spawn_blocking(move || port_rx.recv().unwrap())
        .await
        .unwrap();

    let sub = omq_tokio::Socket::new(omq_tokio::SocketType::Sub, omq_tokio::Options::default());
    sub.subscribe("ws.").await.unwrap();
    sub.connect(ws_tokio(port)).await.unwrap();

    let m = tokio::time::timeout(Duration::from_secs(10), sub.recv())
        .await
        .expect("sub timed out")
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"ws.hello"[..]);
    drop(sub);
    pub_thread.join().expect("compio thread panicked");
}

/// Tokio binds PUSH, compio connects PULL. Tests the WS connect
/// path on compio.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compio_push_to_tokio_pull_ws() {
    let pull = omq_tokio::Socket::new(omq_tokio::SocketType::Pull, omq_tokio::Options::default());
    let mut mon = pull.monitor();
    pull.bind(ws_tokio(0)).await.unwrap();
    let port = loop {
        match mon.recv().await {
            Ok(omq_tokio::MonitorEvent::Listening {
                endpoint: omq_tokio::Endpoint::Ws { port, .. },
            }) => break port,
            Ok(_) => {}
            other => panic!("expected Listening, got {other:?}"),
        }
    };

    let push_thread = thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            use omq_compio::{Options, Socket, SocketType};
            let push = Socket::new(SocketType::Push, Options::default());
            push.connect(ws_compio(port)).await.unwrap();
            compio::time::sleep(Duration::from_millis(500)).await;
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
        let msg = tokio::time::timeout(Duration::from_secs(10), pull.recv())
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

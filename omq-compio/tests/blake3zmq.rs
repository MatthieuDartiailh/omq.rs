//! BLAKE3ZMQ end-to-end integration tests for omq-compio.

#![cfg(feature = "blake3zmq")]

mod test_support;

use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::{Blake3ZmqKeypair, Endpoint, IpcPath, Message, Options, Socket, SocketType};

fn tcp_loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    }
}

fn temp_ipc(name: &str) -> Endpoint {
    Endpoint::Ipc(IpcPath::Abstract(format!(
        "omq-compio-blake3-{name}-{}-{}",
        std::process::id(),
        rand::random::<u32>()
    )))
}

#[compio::test]
async fn blake3zmq_push_pull_roundtrip() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = temp_ipc("blake3-pp");

    let server = Socket::new(
        SocketType::Pull,
        Options::default().blake3zmq_server(server_kp),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().blake3zmq_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    client
        .send(Message::single("hello over blake3zmq"))
        .await
        .unwrap();
    let m = compio::time::timeout(Duration::from_secs(5), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"hello over blake3zmq"[..]);
}

// =====================================================================
// Strategy-bucket coverage: REQ/REP, DEALER/ROUTER, PUB/SUB.
// =====================================================================

#[compio::test]
async fn blake3zmq_req_rep() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = temp_ipc("req-rep");

    let rep = Socket::new(
        SocketType::Rep,
        Options::default().blake3zmq_server(server_kp),
    );
    rep.bind(ep.clone()).await.unwrap();
    let req = Socket::new(
        SocketType::Req,
        Options::default().blake3zmq_client(client_kp, server_pub),
    );
    req.connect(ep).await.unwrap();

    req.send(Message::single("q")).await.unwrap();
    let q = compio::time::timeout(Duration::from_secs(5), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q.part_bytes(0).unwrap(), &b"q"[..]);
    rep.send(Message::single("a")).await.unwrap();
    let a = compio::time::timeout(Duration::from_secs(5), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.part_bytes(0).unwrap(), &b"a"[..]);
}

#[compio::test]
async fn blake3zmq_dealer_router() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = temp_ipc("dr");

    let router = Socket::new(
        SocketType::Router,
        Options::default().blake3zmq_server(server_kp),
    );
    router.bind(ep.clone()).await.unwrap();
    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default()
            .identity(bytes::Bytes::from_static(b"d1"))
            .blake3zmq_client(client_kp, server_pub),
    );
    dealer.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&dealer).await;

    dealer.send(Message::single("hi")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(5), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"d1"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"hi"[..]);
}

#[compio::test]
async fn blake3zmq_pub_sub() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = temp_ipc("ps");

    let p = Socket::new(
        SocketType::Pub,
        Options::default().blake3zmq_server(server_kp),
    );
    p.bind(ep.clone()).await.unwrap();
    let s = Socket::new(
        SocketType::Sub,
        Options::default().blake3zmq_client(client_kp, server_pub),
    );
    s.subscribe("").await.unwrap();
    s.connect(ep).await.unwrap();

    for _ in 0..30 {
        let _ = p.send(Message::single("hello")).await;
        if let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(50), s.recv()).await {
            assert_eq!(m.part_bytes(0).unwrap(), &b"hello"[..]);
            return;
        }
    }
    panic!("SUB never received over BLAKE3ZMQ");
}

/// Regression test for flush_codec_to_wire partial-write ordering bug.
///
/// The bug: on partial TCP write, unwritten codec bytes were placed
/// into `EncodedQueue` instead of staying in the codec. Next
/// iteration flushed new codec data (step 3a) before the old leftover
/// in `EncodedQueue` (step 3b), corrupting the byte stream. This
/// caused decryption failures and connection drops under load.
///
/// Sender runs on a separate thread with its own compio runtime so
/// its driver can saturate the TCP send buffer without cooperative
/// scheduling draining it. Small SO_SNDBUF forces partial writes.
#[test]
fn blake3zmq_large_messages_tcp_partial_write() {
    const MSG_SIZE: usize = 8 * 1024;
    const MSG_COUNT: usize = 2000;

    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;

    let (port_tx, port_rx) = std::sync::mpsc::channel();

    let pull_kp = server_kp;
    let recv_handle = std::thread::spawn(move || {
        let recv_rt = omq_compio::runtime::build_default_runtime().unwrap();
        recv_rt.block_on(async {
            let pull = Socket::new(
                SocketType::Pull,
                Options::default().blake3zmq_server(pull_kp),
            );
            let mut mon = pull.monitor();
            pull.bind(tcp_loopback(0)).await.unwrap();
            let port = match mon.recv().await.unwrap() {
                omq_compio::MonitorEvent::Listening {
                    endpoint: Endpoint::Tcp { port, .. },
                } => port,
                other => panic!("{other:?}"),
            };
            port_tx.send(port).unwrap();

            let payload: Vec<u8> = (0..MSG_SIZE).map(|i| (i % 251) as u8).collect();
            for i in 0..MSG_COUNT {
                let m = compio::time::timeout(Duration::from_secs(10), pull.recv())
                    .await
                    .unwrap_or_else(|_| panic!("timeout waiting for message {i}"))
                    .unwrap_or_else(|e| panic!("recv error on message {i}: {e}"));
                let body = m.part_bytes(0).unwrap();
                assert_eq!(body.len(), MSG_SIZE, "message {i}: wrong length");
                assert_eq!(&body[..], &payload[..], "message {i}: content mismatch");
            }

            let mut accepted = 0usize;
            while let Ok(Ok(ev)) =
                compio::time::timeout(Duration::from_millis(100), mon.recv()).await
            {
                if matches!(ev, omq_compio::MonitorEvent::Accepted { .. }) {
                    accepted += 1;
                }
            }
            assert!(
                accepted <= 1,
                "unexpected reconnections: {accepted} accepts"
            );
        });
    });

    let port = port_rx.recv().unwrap();
    let send_rt = omq_compio::runtime::build_default_runtime().unwrap();
    let _ = send_rt.block_on(async {
        let push = Socket::new(
            SocketType::Push,
            Options::default()
                .linger(Duration::from_secs(10))
                .blake3zmq_client(client_kp, server_pub),
        );
        push.connect(tcp_loopback(port)).await.unwrap();

        let payload: Vec<u8> = (0..MSG_SIZE).map(|i| (i % 251) as u8).collect();
        let msg = Message::single(payload);
        for _ in 0..MSG_COUNT {
            push.send(msg.clone()).await.unwrap();
        }
        push.close().await.unwrap();
    });

    recv_handle.join().unwrap();
}

//! Smoke test verifying the API shape matches what zmq.rs users write.
//! This is a port of the `zmqrs_bench_peer` push/pull pattern.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use zeromq::{PullSocket, PushSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

#[tokio::test]
async fn bench_peer_push_pull_pattern() {
    let mut push = PushSocket::new();

    push.bind("tcp://127.0.0.1:0").await.unwrap();

    // The zmq.rs pattern: create socket, bind/connect with string, send ZmqMessage
    let ep = {
        let mut s = PushSocket::new();
        s.bind("tcp://127.0.0.1:0").await.unwrap()
    };

    // Verify we can use the endpoint string directly
    let addr = ep.to_string();
    assert!(addr.starts_with("tcp://"));
}

#[tokio::test]
async fn zmqrs_api_shape() {
    // This test verifies that the API shape compiles identically to zmq.rs usage.
    // If this compiles, existing zmq.rs code will compile with omq-zeromq.

    // Socket creation (zmq.rs pattern)
    let mut push = PushSocket::new();
    let mut pull = PullSocket::new();

    // Bind returns endpoint (zmq.rs pattern)
    let ep = push.bind("tcp://127.0.0.1:0").await.unwrap();

    // Connect with string (zmq.rs pattern)
    pull.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send ZmqMessage::from (zmq.rs pattern)
    let payload = Bytes::from(vec![b'x'; 64]);
    push.send(ZmqMessage::from(payload.clone())).await.unwrap();

    // Recv (zmq.rs pattern)
    let msg = pull.recv().await.unwrap();
    assert_eq!(msg.get(0).unwrap().len(), 64);

    // Multi-frame message (zmq.rs pattern)
    let mut multi = ZmqMessage::new();
    multi.push_back(Bytes::from_static(b"identity"));
    multi.push_back(Bytes::from_static(b""));
    multi.push_back(Bytes::from_static(b"body"));
    push.send(multi).await.unwrap();

    let received = pull.recv().await.unwrap();
    assert_eq!(received.len(), 3);
    assert_eq!(received.get(0).unwrap().as_ref(), b"identity");
    assert!(received.get(1).unwrap().is_empty());
    assert_eq!(received.get(2).unwrap().as_ref(), b"body");
}

#[tokio::test]
async fn throughput_smoke() {
    let mut push = PushSocket::new();
    let mut pull = PullSocket::new();

    let ep = push.bind("tcp://127.0.0.1:0").await.unwrap();
    pull.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let count = Arc::new(AtomicU64::new(0));
    let count_recv = count.clone();

    let send_handle = tokio::spawn(async move {
        let payload = Bytes::from(vec![b'x'; 64]);
        for _ in 0..1000 {
            push.send(ZmqMessage::from(payload.clone())).await.unwrap();
        }
    });

    let recv_handle = tokio::spawn(async move {
        for _ in 0..1000 {
            let _msg = pull.recv().await.unwrap();
            count_recv.fetch_add(1, Ordering::Relaxed);
        }
    });

    let start = Instant::now();
    send_handle.await.unwrap();
    recv_handle.await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(count.load(Ordering::Relaxed), 1000);
    // Should complete well under 5 seconds
    assert!(elapsed < Duration::from_secs(5));
}

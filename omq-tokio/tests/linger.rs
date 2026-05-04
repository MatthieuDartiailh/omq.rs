//! Linger: `close()` with linger > 0 drains all queued messages before
//! returning. Exercises the send-queue drain path that linger=0 (the
//! default) never touches.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn loopback_port() -> u16 {
    let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn linger_nonzero_drains_queued_messages_inproc() {
    const N: u32 = 20;

    let ep = inproc_ep("linger-drain-inproc-tok");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options::default().linger(Duration::from_secs(2)),
    );
    push.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    for i in 0..N {
        push.send(Message::single(i.to_be_bytes().to_vec()))
            .await
            .unwrap();
    }

    // close() with linger blocks until the queue is drained.
    tokio::time::timeout(Duration::from_secs(3), push.close())
        .await
        .expect("close timed out — linger drain stalled")
        .unwrap();

    for i in 0..N {
        let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        let bytes: [u8; 4] = m.parts()[0].as_bytes().as_ref().try_into().unwrap();
        assert_eq!(
            u32::from_be_bytes(bytes),
            i,
            "message {i} out of order or missing"
        );
    }
}

#[tokio::test]
async fn linger_nonzero_drains_queued_messages_tcp() {
    const N: u32 = 50;

    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options::default().linger(Duration::from_secs(2)),
    );
    push.connect(tcp_ep(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    for i in 0..N {
        push.send(Message::single(i.to_be_bytes().to_vec()))
            .await
            .unwrap();
    }

    tokio::time::timeout(Duration::from_secs(3), push.close())
        .await
        .expect("close timed out — linger drain stalled")
        .unwrap();

    for i in 0..N {
        let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        let bytes: [u8; 4] = m.parts()[0].as_bytes().as_ref().try_into().unwrap();
        assert_eq!(
            u32::from_be_bytes(bytes),
            i,
            "message {i} out of order or missing"
        );
    }
}

#[tokio::test]
async fn linger_forever_waits_until_drained() {
    // linger_forever (None) means "wait indefinitely until queue drains".
    // The receiver runs concurrently in a spawned task so that close()
    // can block until the queue is empty without deadlocking.
    const N: u32 = 20;

    let ep = inproc_ep("linger-forever-tok");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default().linger_forever());
    push.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    for i in 0..N {
        push.send(Message::single(i.to_be_bytes().to_vec()))
            .await
            .unwrap();
    }

    // Drain concurrently so close() doesn't wait on an idle consumer.
    let recv_task = tokio::spawn(async move {
        let mut received = Vec::with_capacity(N as usize);
        for _ in 0..N {
            let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
                .await
                .expect("recv timed out in linger_forever task")
                .unwrap();
            let bytes: [u8; 4] = m.parts()[0].as_bytes().as_ref().try_into().unwrap();
            received.push(u32::from_be_bytes(bytes));
        }
        received
    });

    tokio::time::timeout(Duration::from_secs(2), push.close())
        .await
        .expect("close timed out with linger_forever")
        .unwrap();

    let received = recv_task.await.unwrap();
    for (i, v) in received.into_iter().enumerate() {
        assert_eq!(v, i as u32, "message {i} out of order or missing");
    }
}

#[tokio::test]
async fn linger_zero_returns_immediately_on_close() {
    // Default linger (ZERO): close() does not wait for pending queue;
    // messages that have not yet been delivered are silently dropped.
    // We verify close() returns promptly even with a full queue.
    let ep = inproc_ep("linger-zero-fast-tok");

    let push = Socket::new(SocketType::Push, Options::default()); // linger = ZERO by default
    push.bind(ep.clone()).await.unwrap();

    // No peer — send blocks in actor; close with linger=0 must return quickly.
    let _ = tokio::time::timeout(
        Duration::from_millis(10),
        push.send(Message::single("queued")),
    )
    .await;

    let t0 = std::time::Instant::now();
    push.close().await.unwrap();
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "linger=0 close took too long: {elapsed:?}"
    );
}

#[tokio::test]
async fn linger_completes_within_timeout_after_peer_disconnect() {
    // Queued messages cannot be delivered after the peer disconnects.
    // close() with a finite linger must return within the linger window
    // rather than hanging indefinitely waiting for a peer that is gone.
    const LINGER: Duration = Duration::from_millis(300);

    let port = loopback_port();

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();
    let push = Socket::new(SocketType::Push, Options::default().linger(LINGER));
    push.connect(tcp_ep(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Queue up messages; let the peer receive a few then disconnect.
    for i in 0u32..50 {
        push.send(Message::single(i.to_be_bytes().to_vec()))
            .await
            .unwrap();
    }

    // Drain a handful so the connection is live, then drop the peer.
    for _ in 0..5 {
        let _ = tokio::time::timeout(Duration::from_millis(200), pull.recv()).await;
    }
    pull.close().await.unwrap();

    // close() must return within linger + generous slack; it must not block
    // until the linger timeout if the underlying queue could be drained sooner
    // (and it must not hang indefinitely if the peer is gone).
    let t0 = std::time::Instant::now();
    tokio::time::timeout(LINGER + Duration::from_millis(500), push.close())
        .await
        .expect("close() hung past linger timeout after peer disconnect")
        .unwrap();
    let elapsed = t0.elapsed();
    assert!(
        elapsed <= LINGER + Duration::from_millis(500),
        "close took {elapsed:?}, expected ≤ linger({LINGER:?}) + 500 ms"
    );
}

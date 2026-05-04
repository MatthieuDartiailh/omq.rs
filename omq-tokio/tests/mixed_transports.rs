//! Mixed transports in one socket: PUSH binds on inproc + TCP simultaneously.
//!
//! Verifies that work-stealing round-robin distributes across peers regardless
//! of which transport each peer uses, and that removing a transport's peers
//! reverts distribution to the remaining transport.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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
async fn push_distributes_across_inproc_and_tcp() {
    const N: usize = 100;
    let port = loopback_port();
    let inproc = inproc_ep("mixed-push-tok");

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(inproc.clone()).await.unwrap();
    push.bind(tcp_ep(port)).await.unwrap();

    let pull_inproc = Socket::new(SocketType::Pull, Options::default());
    pull_inproc.connect(inproc).await.unwrap();

    let pull_tcp = Socket::new(SocketType::Pull, Options::default());
    pull_tcp.connect(tcp_ep(port)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..N {
        push.send(Message::single(format!("m{i}"))).await.unwrap();
    }

    let inproc_count = Arc::new(AtomicUsize::new(0));
    let tcp_count = Arc::new(AtomicUsize::new(0));

    let ic = inproc_count.clone();
    let h1 = tokio::spawn(async move {
        loop {
            match tokio::time::timeout(Duration::from_millis(300), pull_inproc.recv()).await {
                Ok(Ok(_)) => {
                    ic.fetch_add(1, Ordering::SeqCst);
                }
                _ => return,
            }
        }
    });
    let tc = tcp_count.clone();
    let h2 = tokio::spawn(async move {
        loop {
            match tokio::time::timeout(Duration::from_millis(300), pull_tcp.recv()).await {
                Ok(Ok(_)) => {
                    tc.fetch_add(1, Ordering::SeqCst);
                }
                _ => return,
            }
        }
    });
    let _ = h1.await;
    let _ = h2.await;

    let total = inproc_count.load(Ordering::SeqCst) + tcp_count.load(Ordering::SeqCst);
    assert_eq!(
        total, N,
        "all {N} messages must arrive across both transports"
    );
    assert!(
        inproc_count.load(Ordering::SeqCst) > 0,
        "inproc peer received nothing"
    );
    assert!(
        tcp_count.load(Ordering::SeqCst) > 0,
        "tcp peer received nothing"
    );
}

#[tokio::test]
async fn push_reverts_to_remaining_after_peer_disconnect() {
    const INIT: usize = 20;
    const AFTER: usize = 30;
    let port = loopback_port();
    let inproc = inproc_ep("mixed-revert-tok");

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(inproc.clone()).await.unwrap();
    push.bind(tcp_ep(port)).await.unwrap();

    let pull_inproc = Socket::new(SocketType::Pull, Options::default());
    pull_inproc.connect(inproc).await.unwrap();

    let pull_tcp = Socket::new(SocketType::Pull, Options::default());
    pull_tcp.connect(tcp_ep(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drain initial traffic so both peers have active sessions.
    for i in 0..INIT {
        push.send(Message::single(format!("init-{i}")))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drop the TCP peer; only inproc remains.
    pull_tcp.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // All subsequent sends must reach the inproc peer without blocking.
    for i in 0..AFTER {
        tokio::time::timeout(
            Duration::from_secs(2),
            push.send(Message::single(format!("after-{i}"))),
        )
        .await
        .expect("send timed out after TCP peer dropped")
        .unwrap();
    }

    let mut got = 0usize;
    while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(300), pull_inproc.recv()).await
    {
        got += 1;
    }
    assert!(
        got >= AFTER,
        "inproc peer must receive all {AFTER} post-disconnect sends; got {got}"
    );
}

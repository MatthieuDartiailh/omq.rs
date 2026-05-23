//! Mixed transports in one socket: PUSH binds on inproc + TCP simultaneously.

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[compio::test]
async fn push_distributes_across_inproc_and_tcp() {
    const N: usize = 100;
    let inproc = inproc_ep("mixed-push-cmp");

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(inproc.clone()).await.unwrap();
    let tcp = push.bind(tcp_ep(0)).await.unwrap();

    let pull_inproc = Socket::new(SocketType::Pull, Options::default());
    pull_inproc.connect(inproc).await.unwrap();

    let pull_tcp = Socket::new(SocketType::Pull, Options::default());
    pull_tcp.connect(tcp).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..N {
        push.send(Message::single(format!("m{i}"))).await.unwrap();
    }

    let inproc_count = Arc::new(AtomicUsize::new(0));
    let tcp_count = Arc::new(AtomicUsize::new(0));

    let ic = inproc_count.clone();
    let h1 = compio::runtime::spawn(async move {
        loop {
            match compio::time::timeout(Duration::from_millis(300), pull_inproc.recv()).await {
                Ok(Ok(_)) => {
                    ic.fetch_add(1, Ordering::SeqCst);
                }
                _ => return,
            }
        }
    });
    let tc = tcp_count.clone();
    let h2 = compio::runtime::spawn(async move {
        loop {
            match compio::time::timeout(Duration::from_millis(300), pull_tcp.recv()).await {
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

#[compio::test]
async fn push_reverts_to_remaining_after_peer_disconnect() {
    const INIT: usize = 20;
    const AFTER: usize = 30;
    let inproc = inproc_ep("mixed-revert-cmp");

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(inproc.clone()).await.unwrap();
    let tcp = push.bind(tcp_ep(0)).await.unwrap();

    let pull_inproc = Socket::new(SocketType::Pull, Options::default());
    pull_inproc.connect(inproc).await.unwrap();

    let pull_tcp = Socket::new(SocketType::Pull, Options::default());
    pull_tcp.connect(tcp).await.unwrap();

    for i in 0..INIT {
        push.send(Message::single(format!("init-{i}")))
            .await
            .unwrap();
    }
    compio::time::sleep(Duration::from_millis(100)).await;

    pull_tcp.close().await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..AFTER {
        compio::time::timeout(
            Duration::from_secs(2),
            push.send(Message::single(format!("after-{i}"))),
        )
        .await
        .expect("send timed out after TCP peer dropped")
        .unwrap();
    }

    let mut got = 0usize;
    while let Ok(Ok(_)) =
        compio::time::timeout(Duration::from_millis(300), pull_inproc.recv()).await
    {
        got += 1;
    }
    assert!(
        got >= AFTER,
        "inproc peer must receive all {AFTER} post-disconnect sends; got {got}"
    );
}

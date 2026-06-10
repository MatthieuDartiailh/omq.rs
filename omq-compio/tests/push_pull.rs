//! Multi-peer PUSH / PULL integration tests and work-stealing demo.

mod test_support;

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

#[compio::test]
async fn push_pull_single_peer() {
    let ep = inproc_ep("pp-single");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single("a")).await.unwrap();
    push.send(Message::single("b")).await.unwrap();
    push.send(Message::single("c")).await.unwrap();

    let m1 = pull.recv().await.unwrap();
    let m2 = pull.recv().await.unwrap();
    let m3 = pull.recv().await.unwrap();
    assert_eq!(m1.part_bytes(0).unwrap(), &b"a"[..]);
    assert_eq!(m2.part_bytes(0).unwrap(), &b"b"[..]);
    assert_eq!(m3.part_bytes(0).unwrap(), &b"c"[..]);
}

#[compio::test]
async fn push_pull_multi_peer_distributes() {
    const N: usize = 300;
    let ep = inproc_ep("pp-multi-3");

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(ep.clone()).await.unwrap();

    let pulls: Vec<Socket> = (0..3)
        .map(|_| Socket::new(SocketType::Pull, Options::default()))
        .collect();
    for p in &pulls {
        p.connect(ep.clone()).await.unwrap();
    }

    for i in 0..N {
        push.send(Message::single(format!("msg-{i}")))
            .await
            .unwrap();
    }

    let counts: Vec<Arc<AtomicUsize>> = (0..pulls.len())
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();
    let mut handles = Vec::new();
    for (p, c) in pulls.into_iter().zip(counts.iter().cloned()) {
        let c = c.clone();
        handles.push(compio::runtime::spawn(async move {
            loop {
                match compio::time::timeout(Duration::from_millis(200), p.recv()).await {
                    Ok(Ok(_)) => {
                        c.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => return,
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }

    let total: usize = counts.iter().map(|c| c.load(Ordering::SeqCst)).sum();
    assert_eq!(total, N, "every message must reach exactly one pull");

    for c in &counts {
        let n = c.load(Ordering::SeqCst);
        assert!(
            n > N / 20,
            "pull got only {n} / {N}; distribution too skewed"
        );
    }
}

#[compio::test]
async fn push_pull_slow_peer_does_not_block_fast() {
    const N: usize = 200;
    let ep = inproc_ep("pp-slow-fast");

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(ep.clone()).await.unwrap();

    let fast = Socket::new(SocketType::Pull, Options::default());
    let slow = Socket::new(SocketType::Pull, Options::default());
    fast.connect(ep.clone()).await.unwrap();
    slow.connect(ep).await.unwrap();

    for i in 0..N {
        push.send(Message::single(format!("m-{i}"))).await.unwrap();
    }

    let fast_count = Arc::new(AtomicUsize::new(0));
    let slow_count = Arc::new(AtomicUsize::new(0));

    let fc = fast_count.clone();
    let fast_task = compio::runtime::spawn(async move {
        loop {
            match compio::time::timeout(Duration::from_millis(300), fast.recv()).await {
                Ok(Ok(_)) => {
                    fc.fetch_add(1, Ordering::SeqCst);
                }
                _ => return,
            }
        }
    });
    let sc = slow_count.clone();
    let slow_task = compio::runtime::spawn(async move {
        loop {
            match compio::time::timeout(Duration::from_millis(500), slow.recv()).await {
                Ok(Ok(_)) => {
                    sc.fetch_add(1, Ordering::SeqCst);
                    compio::time::sleep(Duration::from_millis(2)).await;
                }
                _ => return,
            }
        }
    });

    let _ = fast_task.await;
    let _ = slow_task.await;

    let f = fast_count.load(Ordering::SeqCst);
    let s = slow_count.load(Ordering::SeqCst);
    assert_eq!(f + s, N, "every message must arrive");
    assert!(f > 0 && s > 0, "both peers must receive some messages");
    assert!(
        f >= s,
        "fast peer should never receive fewer than slow (got {f} vs {s})"
    );
}

#[compio::test]
async fn push_pull_under_backpressure_delivers_everything() {
    const N: usize = 1_000;
    let ep = inproc_ep("pp-steal");

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(ep.clone()).await.unwrap();

    let fast = Socket::new(SocketType::Pull, Options::default().recv_hwm(32));
    let slow = Socket::new(SocketType::Pull, Options::default().recv_hwm(32));
    fast.connect(ep.clone()).await.unwrap();
    slow.connect(ep).await.unwrap();

    // Spawn receivers BEFORE the send loop: sends block when queues are full
    // (recv_hwm=32), and in compio's cooperative runtime nothing drains the
    // queues unless receivers are already scheduled.
    let fast_count = Arc::new(AtomicUsize::new(0));
    let slow_count = Arc::new(AtomicUsize::new(0));

    let fc = fast_count.clone();
    let fast_task = compio::runtime::spawn(async move {
        loop {
            match compio::time::timeout(Duration::from_millis(400), fast.recv()).await {
                Ok(Ok(_)) => {
                    fc.fetch_add(1, Ordering::SeqCst);
                }
                _ => return,
            }
        }
    });
    let sc = slow_count.clone();
    let slow_task = compio::runtime::spawn(async move {
        loop {
            match compio::time::timeout(Duration::from_millis(800), slow.recv()).await {
                Ok(Ok(_)) => {
                    sc.fetch_add(1, Ordering::SeqCst);
                    compio::time::sleep(Duration::from_micros(200)).await;
                }
                _ => return,
            }
        }
    });

    let payload = vec![b'x'; 512];
    for i in 0..N {
        let mut m = payload.clone();
        m.extend_from_slice(format!("{i}").as_bytes());
        push.send(Message::single(m)).await.unwrap();
    }

    let _ = fast_task.await;
    let _ = slow_task.await;

    let f = fast_count.load(Ordering::SeqCst);
    let s = slow_count.load(Ordering::SeqCst);
    assert_eq!(f + s, N, "every message must arrive under backpressure");
    assert!(f > 0 && s > 0, "both peers must receive at least some");
    assert!(f >= s, "fast peer must not fall behind slow peer");
}

#[compio::test]
async fn push_send_before_peer_connects_queues() {
    let ep = inproc_ep("pp-before-peer");

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(ep.clone()).await.unwrap();

    for i in 0..5 {
        push.send(Message::single(format!("early-{i}")))
            .await
            .unwrap();
    }

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.connect(ep).await.unwrap();

    for i in 0..5 {
        let m = compio::time::timeout(Duration::from_millis(500), pull.recv())
            .await
            .unwrap()
            .unwrap();
        let expected = format!("early-{i}");
        assert_eq!(m.part_bytes(0).unwrap(), expected.as_bytes());
    }
}

#[compio::test]
async fn push_delivers_to_alive_peer_after_dead_slot() {
    // PUSH binds; PULL1 connects (slot 0), then disconnects (slot stays
    // dead in out_peers), then PULL2 connects (slot 1). PUSH sends —
    // the message must reach PULL2 via the shared queue even though
    // peer_count == 2 (dead slot + alive slot).
    let push = Socket::new(SocketType::Push, Options::default());
    let ep = push.bind(tcp_ep(0)).await.unwrap();

    // Slot 0: connect, send one message, then drop.
    {
        let pull1 = Socket::new(SocketType::Pull, Options::default());
        pull1.connect(ep.clone()).await.unwrap();
        test_support::wait_for_handshake(&pull1).await;
        push.send(Message::single("first")).await.unwrap();
        let m = compio::time::timeout(Duration::from_millis(200), pull1.recv())
            .await
            .expect("pull1 recv timed out")
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"first");
        // pull1 drops here — slot 0 becomes dead.
    }
    compio::time::sleep(Duration::from_millis(500)).await;

    // Slot 1: alive peer.
    let pull2 = Socket::new(SocketType::Pull, Options::default());
    pull2.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&pull2).await;

    push.send(Message::single("second")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull2.recv())
        .await
        .expect("pull2 did not receive message after dead slot")
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"second");
}

/// TCP PUSH/PULL with peer churn: PULL connects, receives, disconnects;
/// new PULL connects and must receive subsequent messages.
#[compio::test]
async fn push_tcp_survives_pull_churn() {
    let push = Socket::new(SocketType::Push, Options::default());
    let port = test_support::bind_loopback(&push).await;

    for round in 0..3u32 {
        let pull = Socket::new(SocketType::Pull, Options::default());
        pull.connect(tcp_ep(port)).await.unwrap();
        compio::time::sleep(Duration::from_millis(100)).await;

        let tag = format!("round-{round}");
        push.send(Message::single(tag.clone())).await.unwrap();

        let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .expect("pull timed out")
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), tag.as_bytes());
        drop(pull);
        compio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// TCP PUSH delivers all messages across multiple TCP PULLs.
#[compio::test]
async fn push_tcp_multi_pull_all_delivered() {
    const N: usize = 300;
    let push = Socket::new(SocketType::Push, Options::default());
    let port = test_support::bind_loopback(&push).await;

    let total = Arc::new(AtomicUsize::new(0));
    let mut recv_tasks = Vec::new();
    for _ in 0..3 {
        let p = Socket::new(SocketType::Pull, Options::default());
        p.connect(tcp_ep(port)).await.unwrap();
        let t = total.clone();
        recv_tasks.push(compio::runtime::spawn(async move {
            while let Ok(Ok(_)) = compio::time::timeout(Duration::from_millis(500), p.recv()).await
            {
                t.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    compio::time::sleep(Duration::from_millis(300)).await;

    for i in 0..N {
        push.send(Message::single(format!("m-{i}"))).await.unwrap();
    }

    for t in recv_tasks {
        let _ = t.await;
    }
    assert_eq!(total.load(Ordering::SeqCst), N, "every message must arrive");
}

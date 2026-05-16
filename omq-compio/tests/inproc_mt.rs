//! Cross-thread inproc SPSC tests. Exercises the ypipe fast path
//! where PUSH and PULL run on separate compio runtimes (separate threads).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

/// Regression: `recv()` called before peer install must still receive
/// once the SPSC path becomes active.
#[test]
fn recv_before_connect_receives_via_spsc() {
    const N: usize = 100;
    let ep = inproc_ep("spsc-recv-before-connect");
    let count = Arc::new(AtomicUsize::new(0));
    let ready = Arc::new(Barrier::new(2));

    let pull_count = count.clone();
    let pull_ready = ready.clone();
    let pull_ep = ep.clone();
    let pull_thread = std::thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let pull = Socket::new(SocketType::Pull, Options::default());
            pull.bind(pull_ep).await.unwrap();
            // Signal: bind is done, recv will start immediately.
            pull_ready.wait();
            for _ in 0..N {
                let r = compio::time::timeout(Duration::from_secs(5), pull.recv()).await;
                let msg = r.expect("recv timed out").unwrap();
                assert!(!msg.is_empty());
                pull_count.fetch_add(1, Ordering::Relaxed);
            }
        });
    });

    let push_thread = std::thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            // Wait for PULL to bind (and start recv).
            ready.wait();
            // Small delay so PULL's recv() enters the in_rx loop before
            // our connect installs the SPSC path.
            compio::time::sleep(Duration::from_millis(10)).await;

            let push = Socket::new(SocketType::Push, Options::default());
            push.connect(ep).await.unwrap();

            for i in 0..N {
                push.send(Message::single(format!("msg-{i}")))
                    .await
                    .unwrap();
            }
        });
    });

    push_thread.join().unwrap();
    pull_thread.join().unwrap();
    assert_eq!(count.load(Ordering::Relaxed), N);
}

/// Regression: when the SPSC ring fills (capacity=1024), the sender
/// falls back to blume. The receiver must also drain blume while in
/// the SPSC loop, otherwise messages are stranded.
#[test]
fn spsc_overflow_to_blume_still_delivers() {
    const N: usize = 3000;
    let ep = inproc_ep("spsc-overflow");
    let count = Arc::new(AtomicUsize::new(0));
    let ready = Arc::new(Barrier::new(2));

    let pull_count = count.clone();
    let pull_ready = ready.clone();
    let pull_ep = ep.clone();
    let pull_thread = std::thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let pull = Socket::new(SocketType::Pull, Options::default());
            pull.bind(pull_ep).await.unwrap();
            pull_ready.wait();
            for _ in 0..N {
                let r = compio::time::timeout(Duration::from_secs(10), pull.recv()).await;
                r.expect("recv timed out").unwrap();
                pull_count.fetch_add(1, Ordering::Relaxed);
            }
        });
    });

    let push_thread = std::thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            ready.wait();
            compio::time::sleep(Duration::from_millis(10)).await;
            let push = Socket::new(SocketType::Push, Options::default());
            push.connect(ep).await.unwrap();
            for i in 0..N {
                push.send(Message::single(format!("{i:06}"))).await.unwrap();
            }
        });
    });

    push_thread.join().unwrap();
    pull_thread.join().unwrap();
    assert_eq!(count.load(Ordering::Relaxed), N);
}

/// SPSC path delivers messages in FIFO order across threads.
#[test]
fn spsc_preserves_ordering() {
    const N: usize = 5000;
    let ep = inproc_ep("spsc-order");
    let ready = Arc::new(Barrier::new(2));

    let pull_ready = ready.clone();
    let pull_ep = ep.clone();
    let pull_thread = std::thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let pull = Socket::new(SocketType::Pull, Options::default());
            pull.bind(pull_ep).await.unwrap();
            pull_ready.wait();
            compio::time::sleep(Duration::from_millis(20)).await;

            for i in 0..N {
                let r = compio::time::timeout(Duration::from_secs(10), pull.recv()).await;
                let msg = r.expect("recv timed out").unwrap();
                let body = msg.part_bytes(0).unwrap();
                let got: usize = std::str::from_utf8(&body).unwrap().parse().unwrap();
                assert_eq!(got, i, "out of order at position {i}");
            }
        });
    });

    let push_thread = std::thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            ready.wait();
            let push = Socket::new(SocketType::Push, Options::default());
            push.connect(ep).await.unwrap();
            compio::time::sleep(Duration::from_millis(30)).await;
            for i in 0..N {
                push.send(Message::single(format!("{i}"))).await.unwrap();
            }
        });
    });

    push_thread.join().unwrap();
    pull_thread.join().unwrap();
}

/// Multi-PUSH cross-thread to one PULL.
#[test]
fn multi_push_cross_thread() {
    const N: usize = 300;
    const PEERS: usize = 3;
    let ep = inproc_ep("spsc-multi-push");
    let count = Arc::new(AtomicUsize::new(0));
    let ready = Arc::new(Barrier::new(1 + PEERS));

    let pull_count = count.clone();
    let pull_ready = ready.clone();
    let pull_ep = ep.clone();
    let pull_thread = std::thread::spawn(move || {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let pull = Socket::new(SocketType::Pull, Options::default());
            pull.bind(pull_ep).await.unwrap();
            pull_ready.wait();
            for _ in 0..(N * PEERS) {
                let r = compio::time::timeout(Duration::from_secs(5), pull.recv()).await;
                r.expect("recv timed out").unwrap();
                pull_count.fetch_add(1, Ordering::Relaxed);
            }
        });
    });

    let mut push_threads = Vec::new();
    for i in 0..PEERS {
        let ep = ep.clone();
        let ready = ready.clone();
        push_threads.push(std::thread::spawn(move || {
            let rt = compio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                ready.wait();
                compio::time::sleep(Duration::from_millis(20)).await;
                let push = Socket::new(SocketType::Push, Options::default());
                push.connect(ep).await.unwrap();
                compio::time::sleep(Duration::from_millis(20)).await;
                for j in 0..N {
                    push.send(Message::single(format!("p{i}-{j}")))
                        .await
                        .unwrap();
                }
            });
        }));
    }

    for t in push_threads {
        t.join().unwrap();
    }
    pull_thread.join().unwrap();
    assert_eq!(count.load(Ordering::Relaxed), N * PEERS);
}

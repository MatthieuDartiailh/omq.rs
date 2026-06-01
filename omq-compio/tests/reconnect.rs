//! Reconnect/backoff: dialer reconnects when a listener appears late,
//! restarts mid-session, or drops abruptly while sends are in-flight.

use std::net::Ipv4Addr;
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::options::ReconnectPolicy;
use omq_compio::{Endpoint, Message, OnMute, Options, Socket, SocketType};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

#[compio::test]
async fn connect_retries_until_listener_appears() {
    // Grab a free port by binding a temporary socket, then close it
    // so the dialer connects to a port with no listener initially.
    let tmp = Socket::new(SocketType::Pull, Options::default());
    let ep = tmp.bind(tcp_ep(0)).await.unwrap();
    tmp.close().await.unwrap();

    // Spawn the bind after a short delay; first dials should fail
    // and get backed off until we appear.
    let pull_ep = ep.clone();
    let bind_handle = compio::runtime::spawn(async move {
        compio::time::sleep(Duration::from_millis(150)).await;
        let pull = Socket::new(SocketType::Pull, Options::default());
        pull.bind(pull_ep).await.unwrap();
        pull
    });

    let opts = Options {
        reconnect: ReconnectPolicy::Exponential {
            min: Duration::from_millis(20),
            max: Duration::from_millis(80),
        },
        ..Default::default()
    };
    let push = Socket::new(SocketType::Push, opts);
    push.connect(ep).await.unwrap();

    push.send(Message::single("eventually")).await.unwrap();
    let pull = bind_handle.await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timeout")
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"eventually"[..]);
}

#[compio::test]
async fn reconnect_after_peer_restart() {
    // Peer (listener side) shuts down mid-session; dialer reconnects to the
    // same endpoint when a fresh listener appears.
    let pull1 = Socket::new(SocketType::Pull, Options::default());
    let ep = pull1.bind(tcp_ep(0)).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(50)),
            ..Default::default()
        },
    );
    push.connect(ep.clone()).await.unwrap();

    // Confirm the session is live before simulating the peer restart.
    push.send(Message::single("before")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull1.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(&*m.part_bytes(0).unwrap(), b"before");

    // Peer restarts: close cleanly. close() cancels listener tasks
    // immediately so the OS port is freed as the runtime processes
    // the io_uring cancellations.
    pull1.close().await.unwrap();

    // Spin until the runtime has processed the listener cancellation
    // and the OS port is free again.
    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let mut bound = false;
    for _ in 0..20 {
        if pull2.bind(ep.clone()).await.is_ok() {
            bound = true;
            break;
        }
        compio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "pull2 failed to bind after pull1 closed");

    push.send(Message::single("after")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull2.recv())
        .await
        .expect("recv after peer restart timed out")
        .unwrap();
    assert_eq!(&*m.part_bytes(0).unwrap(), b"after");
}

#[compio::test]
async fn peer_drop_mid_send_is_handled_cleanly() {
    // Peer is dropped while the push engine has in-flight sends. The socket
    // must not panic or deadlock; it reconnects and resumes delivery.
    let pull1 = Socket::new(SocketType::Pull, Options::default());
    let ep = pull1.bind(tcp_ep(0)).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(50)),
            ..Default::default()
        },
    );
    push.connect(ep.clone()).await.unwrap();

    // Confirm handshake before flooding.
    push.send(Message::single("sync")).await.unwrap();
    compio::time::timeout(Duration::from_secs(2), pull1.recv())
        .await
        .expect("sync recv timed out")
        .unwrap();

    // Flood sends from a background task; pull1 will be closed mid-flood.
    let push2 = push.clone();
    let flood = compio::runtime::spawn(async move {
        for _ in 0..100 {
            let _ = compio::time::timeout(
                Duration::from_millis(20),
                push2.send(Message::single("flood")),
            )
            .await;
        }
    });

    // Close pull1 while the push engine may still have pending writes.
    // close() cancels listener and dialer tasks immediately.
    compio::time::sleep(Duration::from_millis(10)).await;
    pull1.close().await.unwrap();

    // Spin until the runtime has processed the listener cancellation
    // and the OS port is free again.
    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let mut bound = false;
    for _ in 0..20 {
        if pull2.bind(ep.clone()).await.is_ok() {
            bound = true;
            break;
        }
        compio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "pull2 failed to bind after pull1 was dropped");

    // Push must have reconnected; this send must reach pull2.
    push.send(Message::single("after")).await.unwrap();
    compio::time::timeout(Duration::from_secs(2), pull2.recv())
        .await
        .expect("recv after peer drop timed out")
        .unwrap();

    let _ = flood.await;
}

#[compio::test]
async fn exponential_backoff_retry_in_grows() {
    use omq_compio::MonitorEvent;

    // Grab a free port by binding a temporary socket, then close it
    // so the dialer connects to a port with no listener.
    let tmp = Socket::new(SocketType::Pull, Options::default());
    let ep = tmp.bind(tcp_ep(0)).await.unwrap();
    tmp.close().await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Exponential {
                min: Duration::from_millis(20),
                max: Duration::from_millis(200),
            },
            ..Default::default()
        },
    );
    let mut mon = push.monitor();
    push.connect(ep).await.unwrap();

    let mut delays: Vec<Duration> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while delays.len() < 4 {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for ConnectDelayed events"
        );
        let evt = compio::time::timeout(Duration::from_secs(2), mon.recv())
            .await
            .expect("timed out waiting for ConnectDelayed event")
            .unwrap();
        if let MonitorEvent::ConnectDelayed { retry_in, .. } = evt {
            delays.push(retry_in);
        }
    }

    for (i, &d) in delays.iter().enumerate() {
        assert!(
            d >= Duration::from_millis(20),
            "delay[{i}] = {d:?} is below min (20 ms)"
        );
    }
    for i in 1..delays.len() {
        assert!(
            delays[i] >= delays[i - 1],
            "delay[{i}] = {:?} decreased from delay[{}] = {:?}",
            delays[i],
            i - 1,
            delays[i - 1]
        );
    }
}

#[compio::test]
async fn push_hwm_drains_after_reconnect() {
    let pull1 = Socket::new(SocketType::Pull, Options::default());
    let ep = pull1.bind(tcp_ep(0)).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options::default()
            .send_hwm(4)
            .on_mute(OnMute::DropNewest)
            .reconnect(ReconnectPolicy::Fixed(Duration::from_millis(30))),
    );
    push.connect(ep.clone()).await.unwrap();
    compio::time::sleep(Duration::from_millis(100)).await;

    push.send(Message::single("warmup")).await.unwrap();
    compio::time::timeout(Duration::from_secs(2), pull1.recv())
        .await
        .expect("warmup timed out")
        .unwrap();

    pull1.close().await.unwrap();
    compio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..10 {
        let _ = compio::time::timeout(
            Duration::from_millis(50),
            push.send(Message::single(format!("q{i}"))),
        )
        .await;
    }

    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let mut bound = false;
    for _ in 0..40 {
        if pull2.bind(ep.clone()).await.is_ok() {
            bound = true;
            break;
        }
        compio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "pull2 failed to rebind");

    push.send(Message::single("final")).await.unwrap();

    let mut count = 0;
    while let Ok(Ok(_)) = compio::time::timeout(Duration::from_secs(2), pull2.recv()).await {
        count += 1;
        if count > 10 {
            break;
        }
    }
    assert!(count >= 1, "expected at least 1 message, got {count}");
}

//! REQ/REP envelope handling.

use std::net::Ipv4Addr;
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

#[compio::test]
async fn req_rep_roundtrip_over_tcp() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();

    let rep_clone = rep.clone();
    let rep_handle = compio::runtime::spawn(async move {
        for _ in 0..3 {
            let m = rep_clone.recv().await.unwrap();
            let body = m.part_bytes(0).unwrap();
            let mut reply = body.to_vec();
            reply.extend_from_slice(b"-pong");
            rep_clone.send(Message::single(reply)).await.unwrap();
        }
    });

    for i in 0..3u32 {
        let body = format!("ping-{i}");
        req.send(Message::single(body.clone())).await.unwrap();
        let r = compio::time::timeout(Duration::from_secs(2), req.recv())
            .await
            .expect("recv timeout")
            .unwrap();
        let want = format!("{body}-pong");
        assert_eq!(r.part_bytes(0).unwrap(), want.as_bytes());
    }
    let _ = rep_handle.await;
}

#[compio::test]
async fn req_double_send_errors() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();

    req.send(Message::single("first")).await.unwrap();
    let err = req.send(Message::single("second")).await.err().unwrap();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("REQ socket must receive a reply"),
        "expected alternation error, got {msg}"
    );
}

#[compio::test]
async fn rep_send_without_recv_errors() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(tcp_ep(0)).await.unwrap();

    let err = rep.send(Message::single("oops")).await.err().unwrap();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("REP socket must receive a request"),
        "expected alternation error, got {msg}"
    );
}

#[compio::test]
async fn rep_survives_client_disconnect_mid_cycle() {
    // REP receives a request; the client drops before REP sends the
    // reply.  REP must clear its stale envelope and serve the next
    // client correctly.
    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    // First client: sends a request then drops immediately.
    {
        let req1 = Socket::new(SocketType::Req, Options::default());
        req1.connect(ep.clone()).await.unwrap();
        req1.send(Message::single("drop-me")).await.unwrap();

        // Let REP receive the request (stale envelope now held).
        let _ = compio::time::timeout(Duration::from_millis(300), rep.recv()).await;
        // req1 drops here: connection closes before REP replies.
    }

    // Give REP time to detect the disconnect and clear the stale envelope.
    compio::time::sleep(Duration::from_millis(150)).await;

    // Second client: full roundtrip must succeed.
    let req2 = Socket::new(SocketType::Req, Options::default());
    req2.connect(ep).await.unwrap();

    req2.send(Message::single("real")).await.unwrap();
    let got = compio::time::timeout(Duration::from_millis(500), rep.recv())
        .await
        .expect("REP did not receive second client's request")
        .unwrap();
    assert_eq!(got.part_bytes(0).unwrap().as_ref(), b"real");

    rep.send(Message::single("reply")).await.unwrap();
    let reply = compio::time::timeout(Duration::from_millis(500), req2.recv())
        .await
        .expect("REQ2 did not receive reply")
        .unwrap();
    assert_eq!(reply.part_bytes(0).unwrap().as_ref(), b"reply");
}

#[compio::test]
async fn req_rep_1000_cycles_tcp() {
    const CYCLES: usize = 1_000;

    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    let rep_task = compio::runtime::spawn(async move {
        for _ in 0..CYCLES {
            let m = rep.recv().await.unwrap();
            rep.send(m).await.unwrap();
        }
    });

    for i in 0..CYCLES {
        req.send(Message::single(format!("{i}"))).await.unwrap();
        let r = compio::time::timeout(Duration::from_secs(5), req.recv())
            .await
            .unwrap_or_else(|_| panic!("cycle {i} timed out"))
            .unwrap();
        let expected = format!("{i}");
        assert_eq!(r.part_bytes(0).unwrap(), expected.as_bytes(), "cycle {i}");
    }

    rep_task.await.unwrap();
}

#[compio::test]
async fn req_rep_roundtrip_sequential_ipv4() {
    // Same as ipv6_req_rep but with IPv4 - tests the sequential (non-spawned) pattern.
    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();

    req.send(Message::single("ping")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"ping"[..]);

    rep.send(Message::single("pong")).await.unwrap();
    let r = compio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.part_bytes(0).unwrap(), &b"pong"[..]);
}

#[compio::test]
async fn req_rep_roundtrip_sequential_with_yield() {
    // Test if yielding between rep.send and req.recv fixes the deadlock.
    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();

    req.send(Message::single("ping")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"ping"[..]);

    rep.send(Message::single("pong")).await.unwrap();
    // Explicit yield to let REP's driver flush encoded_queue.
    compio::time::sleep(Duration::from_millis(1)).await;
    let r = compio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.part_bytes(0).unwrap(), &b"pong"[..]);
}

#[compio::test]
async fn req_rep_roundtrip_sequential_with_long_yield() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();

    req.send(Message::single("ping")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"ping"[..]);

    rep.send(Message::single("pong")).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;
    let r = compio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.part_bytes(0).unwrap(), &b"pong"[..]);
}

#[compio::test]
async fn req_rep_roundtrip_sequential_spawned_recv() {
    // Pattern: sequential but req.recv() in a spawned task
    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();

    req.send(Message::single("ping")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"ping"[..]);

    rep.send(Message::single("pong")).await.unwrap();

    let req_c = req.clone();
    let recv_task = compio::runtime::spawn(async move {
        compio::time::timeout(Duration::from_secs(2), req_c.recv())
            .await
            .unwrap()
            .unwrap()
    });
    let r = recv_task.await.unwrap();
    assert_eq!(r.part_bytes(0).unwrap(), &b"pong"[..]);
}

#[compio::test]
async fn req_rep_sequential_longer_timeout() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();

    req.send(Message::single("ping")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(5), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"ping"[..]);

    rep.send(Message::single("pong")).await.unwrap();
    // 10 second timeout - if req.recv() eventually succeeds it's a scheduling issue
    let r = compio::time::timeout(Duration::from_secs(10), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.part_bytes(0).unwrap(), &b"pong"[..]);
}

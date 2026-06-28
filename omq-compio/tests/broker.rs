//! Broker pattern: ROUTER (frontend) ↔ DEALER (backend) ↔ REP (worker).
//!
//! Message envelope at the ROUTER level: [`client_id` | "" | body]
//! Message envelope at the DEALER/REP level: [`client_id` | "" | body]
//! REP delivers [body] to the application and re-wraps on reply.

use std::time::Duration;

use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

fn inproc(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[compio::test]
async fn router_dealer_rep_single_cycle() {
    let frontend = inproc("broker-fe-cmp");
    let backend = inproc("broker-be-cmp");

    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(frontend.clone()).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, Options::default());
    dealer.bind(backend.clone()).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(frontend).await.unwrap();

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.connect(backend).await.unwrap();

    let router_c = router.clone();
    let dealer_c = dealer.clone();
    let broker = compio::runtime::spawn(async move {
        let request = compio::time::timeout(Duration::from_secs(2), router_c.recv())
            .await
            .expect("router recv timed out")
            .unwrap();
        dealer_c.send(request).await.unwrap();

        let reply = compio::time::timeout(Duration::from_secs(2), dealer_c.recv())
            .await
            .expect("dealer recv timed out")
            .unwrap();
        router_c.send(reply).await.unwrap();
    });

    req.send(Message::single("work")).await.unwrap();

    let work = compio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(work, Message::single("work"));
    rep.send(Message::single("done")).await.unwrap();

    let reply = compio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, Message::single("done"));

    broker.await.unwrap();
}

#[compio::test]
async fn router_dealer_rep_multiple_rounds() {
    const ROUNDS: usize = 5;

    let frontend = inproc("broker-rounds-fe-cmp");
    let backend = inproc("broker-rounds-be-cmp");

    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(frontend.clone()).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, Options::default());
    dealer.bind(backend.clone()).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(frontend).await.unwrap();

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.connect(backend).await.unwrap();

    let rep_task = compio::runtime::spawn(async move {
        for _ in 0..ROUNDS {
            let m = compio::time::timeout(Duration::from_secs(2), rep.recv())
                .await
                .unwrap()
                .unwrap();
            let body = m.part_bytes(0).unwrap().to_vec();
            let mut reply = b"ack:".to_vec();
            reply.extend_from_slice(&body);
            rep.send(Message::single(reply)).await.unwrap();
        }
    });

    let router_c = router.clone();
    let dealer_c = dealer.clone();
    let broker_task = compio::runtime::spawn(async move {
        for _ in 0..ROUNDS {
            let request = compio::time::timeout(Duration::from_secs(2), router_c.recv())
                .await
                .unwrap()
                .unwrap();
            dealer_c.send(request).await.unwrap();
            let reply = compio::time::timeout(Duration::from_secs(2), dealer_c.recv())
                .await
                .unwrap()
                .unwrap();
            router_c.send(reply).await.unwrap();
        }
    });

    for i in 0..ROUNDS {
        req.send(Message::single(format!("job-{i}"))).await.unwrap();
        let r = compio::time::timeout(Duration::from_secs(2), req.recv())
            .await
            .unwrap()
            .unwrap();
        let got = r.part_bytes(0).unwrap();
        let expected = format!("ack:job-{i}");
        assert_eq!(&*got, expected.as_bytes(), "round {i} mismatch");
    }

    rep_task.await.unwrap();
    broker_task.await.unwrap();
}

#[compio::test]
async fn router_dealer_rep_two_concurrent_clients() {
    let frontend = inproc("broker-multi-fe-cmp");
    let backend = inproc("broker-multi-be-cmp");

    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(frontend.clone()).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, Options::default());
    dealer.bind(backend.clone()).await.unwrap();

    let req1 = Socket::new(SocketType::Req, Options::default());
    req1.connect(frontend.clone()).await.unwrap();
    let req2 = Socket::new(SocketType::Req, Options::default());
    req2.connect(frontend).await.unwrap();

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.connect(backend).await.unwrap();

    // Both clients send simultaneously.
    req1.send(Message::single("alpha")).await.unwrap();
    req2.send(Message::single("beta")).await.unwrap();

    // Broker + worker process two request/reply cycles sequentially.
    for _ in 0..2 {
        let request = compio::time::timeout(Duration::from_secs(3), router.recv())
            .await
            .expect("router recv timed out")
            .unwrap();
        dealer.send(request).await.unwrap();

        let m = compio::time::timeout(Duration::from_secs(3), rep.recv())
            .await
            .expect("rep recv timed out")
            .unwrap();
        let mut ok = b"ok-".to_vec();
        ok.extend_from_slice(&m.part_bytes(0).unwrap());
        rep.send(Message::single(ok)).await.unwrap();

        let reply = compio::time::timeout(Duration::from_secs(3), dealer.recv())
            .await
            .expect("dealer recv timed out")
            .unwrap();
        router.send(reply).await.unwrap();
    }

    // Each client must now have its reply queued.
    let r1 = compio::time::timeout(Duration::from_secs(3), req1.recv())
        .await
        .expect("req1 recv timed out")
        .unwrap()
        .part_bytes(0)
        .unwrap()
        .to_vec();
    let r2 = compio::time::timeout(Duration::from_secs(3), req2.recv())
        .await
        .expect("req2 recv timed out")
        .unwrap()
        .part_bytes(0)
        .unwrap()
        .to_vec();

    assert!(r1.starts_with(b"ok-"), "req1 got bad reply: {r1:?}");
    assert!(r2.starts_with(b"ok-"), "req2 got bad reply: {r2:?}");
    let bodies: std::collections::HashSet<Vec<u8>> = [r1, r2].into_iter().collect();
    assert_eq!(bodies.len(), 2, "both clients must get distinct replies");
}

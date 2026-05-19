//! REQ / REP integration tests.
//!
//! Verifies:
//! - Basic request/reply roundtrip with envelope.
//! - REQ strict alternation: second send without intervening recv errors.
//! - REP strict alternation: send without prior recv errors.
//! - REP envelope restore lets ROUTER-style clients (DEALER) talk to a
//!   REP server.
//! - REP survives a client disconnect mid-cycle and serves the next client.

use std::net::Ipv4Addr;
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Error, Message, Options, Socket, SocketType};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

async fn wait_ready() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn req_rep_basic_roundtrip() {
    let ep = inproc_ep("rr-basic");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();
    wait_ready().await;

    req.send(Message::single("hello")).await.unwrap();

    let request = rep.recv().await.unwrap();
    assert_eq!(request.len(), 1);
    assert_eq!(request.part_bytes(0).unwrap(), &b"hello"[..]);

    rep.send(Message::single("world")).await.unwrap();

    let reply = tokio::time::timeout(Duration::from_millis(500), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.len(), 1);
    assert_eq!(reply.part_bytes(0).unwrap(), &b"world"[..]);
}

#[tokio::test]
async fn req_rejects_double_send() {
    let ep = inproc_ep("rr-req-double");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();
    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();
    wait_ready().await;

    req.send(Message::single("one")).await.unwrap();
    let second = req.send(Message::single("two")).await;
    assert!(matches!(second, Err(Error::Protocol(_))), "got {second:?}");
}

#[tokio::test]
async fn rep_rejects_send_before_recv() {
    let ep = inproc_ep("rr-rep-noreq");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep).await.unwrap();

    let r = rep.send(Message::single("oops")).await;
    assert!(matches!(r, Err(Error::Protocol(_))), "got {r:?}");
}

#[tokio::test]
async fn req_rep_multiple_rounds() {
    let ep = inproc_ep("rr-many");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();
    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();
    wait_ready().await;

    for i in 0..5 {
        req.send(Message::single(format!("q-{i}"))).await.unwrap();
        let got = rep.recv().await.unwrap();
        assert_eq!(got.part_bytes(0).unwrap(), format!("q-{i}").as_bytes());
        rep.send(Message::single(format!("a-{i}"))).await.unwrap();
        let reply = req.recv().await.unwrap();
        assert_eq!(reply.part_bytes(0).unwrap(), format!("a-{i}").as_bytes());
    }
}

#[tokio::test]
async fn dealer_to_rep_envelope() {
    // DEALER sends [empty, body]; REP saves envelope + empty delim and
    // returns just the body to the user. Reply goes back through the
    // envelope correctly.
    let ep = inproc_ep("rr-dealer-rep");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(bytes::Bytes::from_static(b"cli")),
    );
    dealer.connect(ep).await.unwrap();
    wait_ready().await;

    // Emulate a REQ-style send: empty delim + body.
    dealer
        .send(Message::multipart(["", "hello"]))
        .await
        .unwrap();

    let got = rep.recv().await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got.part_bytes(0).unwrap(), &b"hello"[..]);

    rep.send(Message::single("world")).await.unwrap();

    // DEALER receives [empty, body] from REP via the envelope restore.
    let reply = tokio::time::timeout(Duration::from_millis(500), dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.len(), 2);
    assert!(reply.part_bytes(0).unwrap().is_empty());
    assert_eq!(reply.part_bytes(1).unwrap(), &b"world"[..]);
}

#[tokio::test]
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
        tokio::time::sleep(Duration::from_millis(50)).await;
        req1.send(Message::single("drop-me")).await.unwrap();

        // Let REP receive the request (stale envelope now held).
        let _ = tokio::time::timeout(Duration::from_millis(300), rep.recv()).await;
        // req1 drops here: connection closes before REP replies.
    }

    // Give REP time to detect the disconnect and clear the stale envelope.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Second client: full roundtrip must succeed.
    let req2 = Socket::new(SocketType::Req, Options::default());
    req2.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    req2.send(Message::single("real")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), rep.recv())
        .await
        .expect("REP did not receive second client's request")
        .unwrap();
    assert_eq!(got.part_bytes(0).unwrap().as_ref(), b"real");

    rep.send(Message::single("reply")).await.unwrap();
    let reply = tokio::time::timeout(Duration::from_millis(500), req2.recv())
        .await
        .expect("REQ2 did not receive reply")
        .unwrap();
    assert_eq!(reply.part_bytes(0).unwrap().as_ref(), b"reply");
}

#[tokio::test]
async fn req_rep_1000_cycles_tcp() {
    // 1 000 sequential request-reply cycles over TCP.
    // Scales beyond inproc to reveal framing races, backpressure issues,
    // and timer/wake latency at real socket throughput.
    const CYCLES: usize = 1_000;

    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let rep_task = tokio::spawn(async move {
        for _ in 0..CYCLES {
            let m = rep.recv().await.unwrap();
            rep.send(m).await.unwrap(); // echo
        }
    });

    for i in 0..CYCLES {
        req.send(Message::single(format!("{i}"))).await.unwrap();
        let r = tokio::time::timeout(Duration::from_secs(5), req.recv())
            .await
            .unwrap_or_else(|_| panic!("cycle {i} timed out"))
            .unwrap();
        let expected = format!("{i}");
        assert_eq!(r.part_bytes(0).unwrap(), expected.as_bytes(), "cycle {i}");
    }

    rep_task.await.unwrap();
}

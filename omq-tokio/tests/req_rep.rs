//! REQ / REP integration tests.
//!
//! Verifies:
//! - Basic request/reply roundtrip with envelope.
//! - REQ strict alternation: second send without intervening recv errors.
//! - REP strict alternation: send without prior recv errors.
//! - REP envelope restore lets ROUTER-style clients (DEALER) talk to a
//!   REP server.
//! - REP survives a client disconnect mid-cycle and serves the next client.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Error, Message, Options, Socket, SocketType};

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
    assert_eq!(request.parts()[0].coalesce(), &b"hello"[..]);

    rep.send(Message::single("world")).await.unwrap();

    let reply = tokio::time::timeout(Duration::from_millis(500), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.len(), 1);
    assert_eq!(reply.parts()[0].coalesce(), &b"world"[..]);
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
        assert_eq!(got.parts()[0].coalesce(), format!("q-{i}").as_bytes());
        rep.send(Message::single(format!("a-{i}"))).await.unwrap();
        let reply = req.recv().await.unwrap();
        assert_eq!(reply.parts()[0].coalesce(), format!("a-{i}").as_bytes());
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
    assert_eq!(got.parts()[0].coalesce(), &b"hello"[..]);

    rep.send(Message::single("world")).await.unwrap();

    // DEALER receives [empty, body] from REP via the envelope restore.
    let reply = tokio::time::timeout(Duration::from_millis(500), dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.len(), 2);
    assert!(reply.parts()[0].is_empty());
    assert_eq!(reply.parts()[1].coalesce(), &b"world"[..]);
}

#[tokio::test]
async fn rep_survives_client_disconnect_mid_cycle() {
    // REP receives a request; the client drops before REP sends the
    // reply.  REP must clear its stale envelope and serve the next
    // client correctly.
    let port = loopback_port();

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(tcp_ep(port)).await.unwrap();

    // First client: sends a request then drops immediately.
    {
        let req1 = Socket::new(SocketType::Req, Options::default());
        req1.connect(tcp_ep(port)).await.unwrap();
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
    req2.connect(tcp_ep(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    req2.send(Message::single("real")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), rep.recv())
        .await
        .expect("REP did not receive second client's request")
        .unwrap();
    assert_eq!(got.parts()[0].coalesce().as_ref(), b"real");

    rep.send(Message::single("reply")).await.unwrap();
    let reply = tokio::time::timeout(Duration::from_millis(500), req2.recv())
        .await
        .expect("REQ2 did not receive reply")
        .unwrap();
    assert_eq!(reply.parts()[0].coalesce().as_ref(), b"reply");
}

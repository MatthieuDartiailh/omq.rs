//! REQ/REP envelope handling.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

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

#[compio::test]
async fn req_rep_roundtrip_over_tcp() {
    let port = loopback_port();
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(tcp_ep(port)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(tcp_ep(port)).await.unwrap();

    let rep_clone = rep.clone();
    let rep_handle = compio::runtime::spawn(async move {
        for _ in 0..3 {
            let m = rep_clone.recv().await.unwrap();
            let body = m.parts()[0].coalesce();
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
        assert_eq!(r.parts()[0].coalesce(), want.as_bytes());
    }
    let _ = rep_handle.await;
}

#[compio::test]
async fn req_double_send_errors() {
    let port = loopback_port();
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(tcp_ep(port)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(tcp_ep(port)).await.unwrap();

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
    let port = loopback_port();
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(tcp_ep(port)).await.unwrap();

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
    let port = loopback_port();

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(tcp_ep(port)).await.unwrap();

    // First client: sends a request then drops immediately.
    {
        let req1 = Socket::new(SocketType::Req, Options::default());
        req1.connect(tcp_ep(port)).await.unwrap();
        compio::time::sleep(Duration::from_millis(50)).await;
        req1.send(Message::single("drop-me")).await.unwrap();

        // Let REP receive the request (stale envelope now held).
        let _ = compio::time::timeout(Duration::from_millis(300), rep.recv()).await;
        // req1 drops here: connection closes before REP replies.
    }

    // Give REP time to detect the disconnect and clear the stale envelope.
    compio::time::sleep(Duration::from_millis(150)).await;

    // Second client: full roundtrip must succeed.
    let req2 = Socket::new(SocketType::Req, Options::default());
    req2.connect(tcp_ep(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    req2.send(Message::single("real")).await.unwrap();
    let got = compio::time::timeout(Duration::from_millis(500), rep.recv())
        .await
        .expect("REP did not receive second client's request")
        .unwrap();
    assert_eq!(got.parts()[0].coalesce().as_ref(), b"real");

    rep.send(Message::single("reply")).await.unwrap();
    let reply = compio::time::timeout(Duration::from_millis(500), req2.recv())
        .await
        .expect("REQ2 did not receive reply")
        .unwrap();
    assert_eq!(reply.parts()[0].coalesce().as_ref(), b"reply");
}

//! Monitor-event surface tests for omq-compio. Covers the bind /
//! accept / connect / handshake-succeeded / disconnected flow on
//! TCP, plus the closed-on-drop guarantee.

use std::time::Duration;

use omq_compio::{DisconnectReason, Endpoint, Message, MonitorEvent, Options, Socket, SocketType};
use omq_proto::endpoint::Host;

fn tcp_loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    }
}

async fn next_event(m: &mut omq_compio::MonitorStream) -> Option<MonitorEvent> {
    compio::time::timeout(Duration::from_millis(500), m.recv())
        .await
        .ok()
        .and_then(std::result::Result::ok)
}

#[compio::test]
async fn bind_emits_listening() {
    let s = Socket::new(SocketType::Pull, Options::default());
    let mut m = s.monitor();
    s.bind(tcp_loopback(0)).await.unwrap();
    let evt = next_event(&mut m).await.expect("listening");
    assert!(matches!(evt, MonitorEvent::Listening { .. }), "{evt:?}");
}

#[compio::test]
async fn handshake_succeeded_seen_on_both_sides() {
    let server = Socket::new(SocketType::Pull, Options::default());
    let mut server_m = server.monitor();
    server.bind(tcp_loopback(0)).await.unwrap();
    // Pull the bound port out of the Listening event.
    let port = match next_event(&mut server_m).await.unwrap() {
        MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("expected Listening, got {other:?}"),
    };

    let client = Socket::new(SocketType::Push, Options::default());
    let mut client_m = client.monitor();
    client.connect(tcp_loopback(port)).await.unwrap();

    // Each side should see Connected/Accepted then HandshakeSucceeded.
    // We accept any order until we've seen at least one
    // HandshakeSucceeded per side.
    let mut server_done = false;
    let mut client_done = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !(server_done && client_done) {
        assert!(
            std::time::Instant::now() <= deadline,
            "timed out: server_done={server_done} client_done={client_done}"
        );
        if !server_done
            && let Some(MonitorEvent::HandshakeSucceeded { .. }) = next_event(&mut server_m).await
        {
            server_done = true;
        }
        if !client_done
            && let Some(MonitorEvent::HandshakeSucceeded { .. }) = next_event(&mut client_m).await
        {
            client_done = true;
        }
    }
}

#[compio::test]
async fn closed_event_on_socket_drop() {
    let s = Socket::new(SocketType::Pull, Options::default());
    let mut m = s.monitor();
    drop(s);
    let evt = compio::time::timeout(Duration::from_secs(1), m.recv())
        .await
        .expect("monitor recv timeout")
        .expect("monitor recv");
    assert!(matches!(evt, MonitorEvent::Closed), "{evt:?}");
}

// --- Disconnect events on PUB, SUB, REP, REQ (zeromq/zmq.rs#201) ---

async fn drain_until_disconnect(mon: &mut omq_compio::MonitorStream) -> Option<DisconnectReason> {
    for _ in 0..20 {
        match compio::time::timeout(Duration::from_millis(500), mon.recv()).await {
            Ok(Ok(MonitorEvent::Disconnected { reason, .. })) => return Some(reason),
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    None
}

async fn drain_until_handshake(mon: &mut omq_compio::MonitorStream) {
    for _ in 0..10 {
        match compio::time::timeout(Duration::from_millis(500), mon.recv()).await {
            Ok(Ok(MonitorEvent::HandshakeSucceeded { .. })) => return,
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    panic!("HandshakeSucceeded never arrived");
}

fn bound_port(evt: MonitorEvent) -> u16 {
    match evt {
        MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("expected Listening, got {other:?}"),
    }
}

#[compio::test]
async fn sub_sees_disconnect_when_pub_closes() {
    let publisher = Socket::new(SocketType::Pub, Options::default());
    let mut pub_mon = publisher.monitor();
    publisher.bind(tcp_loopback(0)).await.unwrap();
    let port = bound_port(next_event(&mut pub_mon).await.unwrap());

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    let mut sub_mon = subscriber.monitor();
    subscriber.connect(tcp_loopback(port)).await.unwrap();
    drain_until_handshake(&mut sub_mon).await;

    publisher.close().await.unwrap();

    let reason = drain_until_disconnect(&mut sub_mon)
        .await
        .expect("SUB must see Disconnected when PUB closes");
    assert_eq!(reason, DisconnectReason::PeerClosed);
}

#[compio::test]
async fn pub_sees_disconnect_when_sub_closes() {
    let publisher = Socket::new(SocketType::Pub, Options::default());
    let mut pub_mon = publisher.monitor();
    publisher.bind(tcp_loopback(0)).await.unwrap();
    let port = bound_port(next_event(&mut pub_mon).await.unwrap());

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(tcp_loopback(port)).await.unwrap();
    drain_until_handshake(&mut pub_mon).await;

    subscriber.close().await.unwrap();

    let reason = drain_until_disconnect(&mut pub_mon)
        .await
        .expect("PUB must see Disconnected when SUB closes");
    assert_eq!(reason, DisconnectReason::PeerClosed);
}

#[compio::test]
async fn rep_sees_disconnect_when_req_closes() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let mut rep_mon = rep.monitor();
    rep.bind(tcp_loopback(0)).await.unwrap();
    let port = bound_port(next_event(&mut rep_mon).await.unwrap());

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(tcp_loopback(port)).await.unwrap();
    drain_until_handshake(&mut rep_mon).await;

    req.close().await.unwrap();

    let reason = drain_until_disconnect(&mut rep_mon)
        .await
        .expect("REP must see Disconnected when REQ closes");
    assert_eq!(reason, DisconnectReason::PeerClosed);
}

#[compio::test]
async fn req_sees_disconnect_when_rep_closes() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let mut rep_mon = rep.monitor();
    rep.bind(tcp_loopback(0)).await.unwrap();
    let port = bound_port(next_event(&mut rep_mon).await.unwrap());

    let req = Socket::new(SocketType::Req, Options::default());
    let mut req_mon = req.monitor();
    req.connect(tcp_loopback(port)).await.unwrap();
    drain_until_handshake(&mut req_mon).await;

    rep.close().await.unwrap();

    let reason = drain_until_disconnect(&mut req_mon)
        .await
        .expect("REQ must see Disconnected when REP closes");
    assert_eq!(reason, DisconnectReason::PeerClosed);
}

#[compio::test]
async fn pub_sees_disconnect_after_message_exchange() {
    let publisher = Socket::new(SocketType::Pub, Options::default());
    let mut pub_mon = publisher.monitor();
    publisher.bind(tcp_loopback(0)).await.unwrap();
    let port = bound_port(next_event(&mut pub_mon).await.unwrap());

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(tcp_loopback(port)).await.unwrap();
    subscriber.subscribe("").await.unwrap();
    drain_until_handshake(&mut pub_mon).await;
    compio::time::sleep(Duration::from_millis(50)).await;

    publisher.send(Message::single("hello")).await.unwrap();
    let msg = compio::time::timeout(Duration::from_millis(500), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg, Message::single("hello"));

    subscriber.close().await.unwrap();

    let reason = drain_until_disconnect(&mut pub_mon)
        .await
        .expect("PUB must see Disconnected after message exchange");
    assert_eq!(reason, DisconnectReason::PeerClosed);
}

#[compio::test]
async fn req_sees_disconnect_after_roundtrip() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let mut rep_mon = rep.monitor();
    rep.bind(tcp_loopback(0)).await.unwrap();
    let port = bound_port(next_event(&mut rep_mon).await.unwrap());

    let req = Socket::new(SocketType::Req, Options::default());
    let mut req_mon = req.monitor();
    req.connect(tcp_loopback(port)).await.unwrap();
    drain_until_handshake(&mut req_mon).await;

    req.send(Message::single("ping")).await.unwrap();
    let request = compio::time::timeout(Duration::from_millis(500), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request, Message::single("ping"));
    rep.send(Message::single("pong")).await.unwrap();
    let reply = compio::time::timeout(Duration::from_millis(500), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, Message::single("pong"));

    rep.close().await.unwrap();

    let reason = drain_until_disconnect(&mut req_mon)
        .await
        .expect("REQ must see Disconnected after roundtrip");
    assert_eq!(reason, DisconnectReason::PeerClosed);
}

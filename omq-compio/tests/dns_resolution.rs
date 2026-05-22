use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::options::ReconnectPolicy;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

fn tcp_name_ep(name: &str, port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Name(name.into()),
        port,
    }
}

#[compio::test]
async fn tcp_connect_by_hostname() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    // Bind to "localhost" too so listener and connector resolve the same address.
    let ep = pull.bind(tcp_name_ep("localhost", 0)).await.unwrap();
    let port = match &ep {
        Endpoint::Tcp { port, .. } => *port,
        _ => unreachable!(),
    };

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp_name_ep("localhost", port)).await.unwrap();

    push.send(Message::single("dns-connect")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timeout")
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"dns-connect"[..]);
}

#[compio::test]
async fn tcp_bind_by_hostname() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let ep = pull.bind(tcp_name_ep("localhost", 0)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single("dns-bind")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timeout")
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"dns-bind"[..]);
}

#[compio::test]
async fn tcp_connect_unresolvable_fails_immediately() {
    let push = Socket::new(SocketType::Push, Options::default());
    let result = push.connect(tcp_name_ep("nonexistent.invalid", 5555)).await;
    assert!(result.is_err(), "connect to unresolvable host must fail");
}

#[compio::test]
async fn tcp_bind_unresolvable_fails() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let result = pull.bind(tcp_name_ep("nonexistent.invalid", 0)).await;
    assert!(result.is_err(), "bind to unresolvable host must fail");
}

#[compio::test]
async fn reconnect_re_resolves_hostname() {
    // Verify reconnect re-resolves DNS on each attempt. The dial
    // supervisor calls resolve_connect() every iteration, so a
    // transient DNS failure feeds into the backoff loop silently.
    let pull1 = Socket::new(SocketType::Pull, Options::default());
    let ep = pull1.bind(tcp_name_ep("localhost", 0)).await.unwrap();
    let port = match &ep {
        Endpoint::Tcp { port, .. } => *port,
        _ => unreachable!(),
    };

    let push = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(50)),
            ..Default::default()
        },
    );
    push.connect(tcp_name_ep("localhost", port)).await.unwrap();

    push.send(Message::single("before")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull1.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"before"[..]);

    // Kill listener — dial supervisor retries, re-resolving DNS each time.
    pull1.close().await.unwrap();

    // Re-bind on the same port; dialer reconnects via hostname.
    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let mut bound = false;
    for _ in 0..20 {
        if pull2.bind(tcp_name_ep("localhost", port)).await.is_ok() {
            bound = true;
            break;
        }
        compio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "pull2 failed to bind after pull1 closed");

    push.send(Message::single("after")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull2.recv())
        .await
        .expect("recv after reconnect timed out")
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"after"[..]);
}

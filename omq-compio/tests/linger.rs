//! Linger: close() with linger > 0 drains all queued messages before
//! returning.

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

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[compio::test]
async fn linger_nonzero_drains_queued_messages_inproc() {
    let ep = inproc_ep("linger-drain-inproc-cmp");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options::default().linger(Duration::from_secs(2)),
    );
    push.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    const N: u32 = 20;
    for i in 0..N {
        push.send(Message::single(i.to_be_bytes().to_vec()))
            .await
            .unwrap();
    }

    compio::time::timeout(Duration::from_secs(3), push.close())
        .await
        .expect("close timed out — linger drain stalled")
        .unwrap();

    for i in 0..N {
        let m = compio::time::timeout(Duration::from_millis(500), pull.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        let bytes: [u8; 4] = m.parts()[0].coalesce().as_ref().try_into().unwrap();
        assert_eq!(u32::from_be_bytes(bytes), i, "message {i} out of order or missing");
    }
}

#[compio::test]
async fn linger_nonzero_drains_queued_messages_tcp() {
    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options::default().linger(Duration::from_secs(2)),
    );
    push.connect(tcp_ep(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    const N: u32 = 50;
    for i in 0..N {
        push.send(Message::single(i.to_be_bytes().to_vec()))
            .await
            .unwrap();
    }

    compio::time::timeout(Duration::from_secs(3), push.close())
        .await
        .expect("close timed out — linger drain stalled")
        .unwrap();

    for i in 0..N {
        let m = compio::time::timeout(Duration::from_millis(500), pull.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        let bytes: [u8; 4] = m.parts()[0].coalesce().as_ref().try_into().unwrap();
        assert_eq!(u32::from_be_bytes(bytes), i, "message {i} out of order or missing");
    }
}

#[compio::test]
async fn linger_forever_waits_until_drained() {
    // Receiver runs concurrently (compio::runtime::spawn) so close()
    // can block on drain without deadlocking.
    let ep = inproc_ep("linger-forever-cmp");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default().linger_forever());
    push.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    const N: u32 = 20;
    for i in 0..N {
        push.send(Message::single(i.to_be_bytes().to_vec()))
            .await
            .unwrap();
    }

    let recv_task = compio::runtime::spawn(async move {
        let mut received = Vec::with_capacity(N as usize);
        for _ in 0..N {
            let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
                .await
                .expect("recv timed out in linger_forever task")
                .unwrap();
            let bytes: [u8; 4] = m.parts()[0].coalesce().as_ref().try_into().unwrap();
            received.push(u32::from_be_bytes(bytes));
        }
        received
    });

    compio::time::timeout(Duration::from_secs(2), push.close())
        .await
        .expect("close timed out with linger_forever")
        .unwrap();

    let received = recv_task.await.unwrap();
    for (i, v) in received.into_iter().enumerate() {
        assert_eq!(v, i as u32, "message {i} out of order or missing");
    }
}

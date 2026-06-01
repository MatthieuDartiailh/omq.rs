//! Regression test: `close()` must complete even when wire drivers are
//! backpressured (command channel full, TCP write blocked, inbound
//! channel full). Previously `close()` could hang indefinitely because
//! `send_async(Close)` blocked on a full command channel.

mod test_support;

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
async fn close_push_pull_under_backpressure() {
    let pull = Socket::new(SocketType::Pull, Options::default().recv_hwm(4));
    let ep = pull.bind(tcp_ep(0)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default().send_hwm(4));
    push.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&push).await;

    // Saturate the pipeline: send until both hwm and TCP buffer fill.
    for _ in 0..100 {
        let _ = compio::time::timeout(Duration::from_millis(1), push.send(Message::single("fill")))
            .await;
    }

    // Close must complete within 2 seconds even under backpressure.
    compio::time::timeout(Duration::from_secs(2), push.close())
        .await
        .expect("push.close() hung under backpressure")
        .unwrap();

    compio::time::timeout(Duration::from_secs(2), pull.close())
        .await
        .expect("pull.close() hung under backpressure")
        .unwrap();
}

#[compio::test]
async fn close_many_pairs_no_hang() {
    let mut pairs = Vec::new();
    for _ in 0..10 {
        let pull = Socket::new(SocketType::Pull, Options::default().recv_hwm(4));
        let ep = pull.bind(tcp_ep(0)).await.unwrap();
        let push = Socket::new(SocketType::Push, Options::default().send_hwm(4));
        push.connect(ep).await.unwrap();
        test_support::wait_for_handshake(&push).await;
        pairs.push((push, pull));
    }

    for (push, _pull) in &pairs {
        for _ in 0..50 {
            let _ =
                compio::time::timeout(Duration::from_millis(1), push.send(Message::single("x")))
                    .await;
        }
    }

    let close_all = async {
        for (push, pull) in pairs {
            push.close().await.unwrap();
            pull.close().await.unwrap();
        }
    };

    compio::time::timeout(Duration::from_secs(5), close_all)
        .await
        .expect("closing 10 backpressured pairs hung");
}

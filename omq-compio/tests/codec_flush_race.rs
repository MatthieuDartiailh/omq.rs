//! Regression test for the flush_codec_to_wire / flush_codec_output race.
//!
//! Before the fix, sustained TCP send/recv with heartbeats could panic
//! with "advance_transmit beyond pending bytes" because the driver's
//! flush_codec_to_wire cloned chunks without advancing, yielded during
//! the async write, and the recv path's flush_codec_output drained the
//! same chunks before the driver resumed.

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
async fn sustained_send_recv_with_heartbeats() {
    let opts = Options {
        heartbeat_interval: Some(Duration::from_millis(20)),
        heartbeat_timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    };

    let pull = Socket::new(SocketType::Pull, opts.clone().recv_hwm(1000));
    let ep = pull.bind(tcp_ep(0)).await.unwrap();

    let push = Socket::new(SocketType::Push, opts.send_hwm(1000));
    push.connect(ep).await.unwrap();

    compio::time::sleep(Duration::from_millis(50)).await;

    let mut sent = 0u64;
    let mut recvd = 0u64;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);

    while std::time::Instant::now() < deadline {
        for _ in 0..100 {
            if compio::time::timeout(Duration::from_millis(5), push.send(Message::single("x")))
                .await
                .is_ok()
            {
                sent += 1;
            }
        }

        loop {
            match compio::time::timeout(Duration::from_millis(1), pull.recv()).await {
                Ok(Ok(_)) => recvd += 1,
                _ => break,
            }
        }
    }

    push.close().await.unwrap();
    pull.close().await.unwrap();

    assert!(sent > 100, "should have sent messages, got {sent}");
    assert!(recvd > 100, "should have received messages, got {recvd}");
}

#[compio::test]
async fn concurrent_codec_output_and_data() {
    let opts = Options {
        heartbeat_interval: Some(Duration::from_millis(10)),
        heartbeat_timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    };

    let pull = Socket::new(SocketType::Pull, opts.clone().recv_hwm(500));
    let ep = pull.bind(tcp_ep(0)).await.unwrap();

    let push = Socket::new(SocketType::Push, opts.clone().send_hwm(500));
    push.connect(ep.clone()).await.unwrap();

    let push2 = Socket::new(SocketType::Push, opts.send_hwm(500));
    push2.connect(ep).await.unwrap();

    compio::time::sleep(Duration::from_millis(50)).await;

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut total = 0u64;

    while std::time::Instant::now() < deadline {
        let _ =
            compio::time::timeout(Duration::from_millis(2), push.send(Message::single("a"))).await;
        let _ =
            compio::time::timeout(Duration::from_millis(2), push2.send(Message::single("b"))).await;

        for _ in 0..10 {
            match compio::time::timeout(Duration::from_millis(1), pull.recv()).await {
                Ok(Ok(_)) => total += 1,
                _ => break,
            }
        }
    }

    push.close().await.unwrap();
    push2.close().await.unwrap();
    pull.close().await.unwrap();

    assert!(total > 100, "should have exchanged messages, got {total}");
}

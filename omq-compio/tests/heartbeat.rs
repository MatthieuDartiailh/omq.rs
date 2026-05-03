//! Heartbeat tests: PING/PONG keeps connections alive; a silently
//! unresponsive peer is evicted within the heartbeat timeout.

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

// ── ZMTP NULL greeting + READY for a synthetic "PULL" peer ───────────────────

fn null_greeting_as_client() -> [u8; 64] {
    let mut g = [0u8; 64];
    g[0] = 0xFF;
    g[9] = 0x7F;
    g[10] = 3;
    g[11] = 1;
    g[12..16].copy_from_slice(b"NULL");
    g
}

const PULL_READY: &[u8] = &[
    0x04, 0x1A,
    0x05, b'R', b'E', b'A', b'D', b'Y',
    0x0B, b'S', b'o', b'c', b'k', b'e', b't', b'-', b'T', b'y', b'p', b'e',
    0x00, 0x00, 0x00, 0x04,
    b'P', b'U', b'L', b'L',
];

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

fn opts_with_hb() -> Options {
    Options {
        heartbeat_interval: Some(Duration::from_millis(50)),
        heartbeat_timeout: Some(Duration::from_millis(500)),
        ..Default::default()
    }
}

#[compio::test]
async fn heartbeat_keeps_idle_connection_alive() {
    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, opts_with_hb());
    pull.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(SocketType::Push, opts_with_hb());
    push.connect(tcp_ep(port)).await.unwrap();

    push.send(Message::single("first")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .expect("recv timeout")
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"first"[..]);

    // Idle window covers several heartbeat intervals - PINGs fire
    // on both sides, codec PONGs them, neither side observes
    // Timeout, both stay up.
    compio::time::sleep(Duration::from_millis(300)).await;

    push.send(Message::single("after-idle")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .expect("recv timeout post-idle")
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"after-idle"[..]);
}

#[compio::test]
async fn heartbeat_detects_silent_peer() {
    // A raw TCP peer completes the ZMTP NULL handshake as PULL then
    // goes silent. PUSH must evict it within the heartbeat timeout.
    // The raw peer is driven from a blocking std::thread so it doesn't
    // need the compio runtime.
    let port = loopback_port();
    let push = Socket::new(
        SocketType::Push,
        Options {
            heartbeat_interval: Some(Duration::from_millis(50)),
            heartbeat_timeout: Some(Duration::from_millis(150)),
            ..Default::default()
        },
    );
    let mut mon = push.monitor();
    push.bind(tcp_ep(port)).await.unwrap();

    // Wait for Listening so the port is open.
    loop {
        if let Ok(Ok(MonitorEvent::Listening { .. })) =
            compio::time::timeout(Duration::from_secs(1), mon.recv()).await
        {
            break;
        }
    }

    // Spawn silent peer on a blocking thread (avoids compio runtime nesting).
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let silent = std::thread::spawn(move || {
        let mut s = std::net::TcpStream::connect(addr).unwrap();
        s.write_all(&null_greeting_as_client()).unwrap();
        s.write_all(PULL_READY).unwrap();
        let mut buf = [0u8; 512];
        loop {
            match s.read(&mut buf) {
                Ok(0) | Err(_) => break, // PUSH closed the connection
                Ok(_) => {}
            }
        }
    });

    // Wait for HandshakeSucceeded.
    let handshook = loop {
        match compio::time::timeout(Duration::from_secs(2), mon.recv())
            .await
            .ok()
            .and_then(std::result::Result::ok)
        {
            Some(MonitorEvent::HandshakeSucceeded { .. }) => break true,
            Some(_) => {}
            None => break false,
        }
    };
    assert!(handshook, "handshake never completed with silent peer");

    // PUSH must emit Disconnected within heartbeat_interval + timeout + margin.
    let deadline = Duration::from_millis(600);
    let disconnected = compio::time::timeout(deadline, async {
        loop {
            if let Ok(MonitorEvent::Disconnected { .. }) = mon.recv().await {
                return true;
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(disconnected, "PUSH did not evict the silent peer within heartbeat timeout");
    let _ = silent.join();
}

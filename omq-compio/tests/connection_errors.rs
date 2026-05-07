//! Connection-error handling: the server side must survive abrupt
//! client disconnects (pre-handshake and mid-session) and continue to
//! accept and serve new connections normally.

use std::io::Write;
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
async fn server_survives_pre_handshake_drop() {
    // A raw TCP client connects but drops the connection before sending
    // any ZMTP greeting. The server must not crash, panic, or reject
    // subsequent legitimate connections.
    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));

    // Rude clients on blocking threads: connect and immediately drop.
    for _ in 0..3 {
        let h = std::thread::spawn(move || {
            let _ = std::net::TcpStream::connect(addr);
            // Drop immediately — sends FIN with no ZMTP bytes.
        });
        let _ = h.join();
        compio::time::sleep(Duration::from_millis(20)).await;
    }

    // Legitimate client: full ZMTP session must work.
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp_ep(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(100)).await;

    push.send(Message::single("alive")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timed out after rude clients")
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"alive");
}

#[compio::test]
async fn server_survives_mid_session_abrupt_drop() {
    // Client drops the TCP connection abruptly while the server is live.
    // Server must survive and accept the next connection.
    let port = loopback_port();

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    // First client: sends one message then drops.
    {
        let push1 = Socket::new(SocketType::Push, Options::default());
        push1.connect(tcp_ep(port)).await.unwrap();
        compio::time::sleep(Duration::from_millis(50)).await;
        push1.send(Message::single("first")).await.unwrap();
        let _ = compio::time::timeout(Duration::from_millis(300), pull.recv()).await;
        // push1 drops here — abrupt half-close.
    }
    compio::time::sleep(Duration::from_millis(50)).await;

    // Second client: server must still be healthy.
    let push2 = Socket::new(SocketType::Push, Options::default());
    push2.connect(tcp_ep(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;
    push2.send(Message::single("second")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timed out after abrupt drop")
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"second");
}

#[compio::test]
async fn abrupt_reset_mid_greeting_does_not_wedge_server() {
    // A peer that sends a partial greeting then drops must not stall the
    // server's accept loop. The server must still serve the next good client.
    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let h = std::thread::spawn(move || {
        let mut s = std::net::TcpStream::connect(addr).unwrap();
        // Send only the first 5 bytes of a greeting, then drop.
        let partial: [u8; 5] = [0xFF, 0x00, 0x00, 0x00, 0x00];
        let _ = s.write_all(&partial);
    });
    let _ = h.join();
    compio::time::sleep(Duration::from_millis(50)).await;

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp_ep(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(100)).await;

    push.send(Message::single("ok")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timed out after partial-greeting peer")
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"ok");
}

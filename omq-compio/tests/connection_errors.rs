//! Connection-error handling: the server side must survive abrupt
//! client disconnects (pre-handshake and mid-session) and continue to
//! accept and serve new connections normally.

mod test_support;

use std::io::Write;
use std::net::{Ipv4Addr, SocketAddr};
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
async fn server_survives_pre_handshake_drop() {
    // A raw TCP client connects but drops the connection before sending
    // any ZMTP greeting. The server must not crash, panic, or reject
    // subsequent legitimate connections.
    let pull = Socket::new(SocketType::Pull, Options::default());
    let ep = pull.bind(tcp_ep(0)).await.unwrap();
    let port = match &ep {
        Endpoint::Tcp { port, .. } => *port,
        _ => unreachable!(),
    };

    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));

    // Rude clients on blocking threads: connect and immediately drop.
    for _ in 0..3 {
        let h = std::thread::spawn(move || {
            std::net::TcpStream::connect(addr).expect("rude client connect");
        });
        h.join().expect("rude client thread panicked");
        compio::time::sleep(Duration::from_millis(20)).await;
    }

    // Legitimate client: full ZMTP session must work.
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&push).await;

    push.send(Message::single("alive")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timed out after rude clients")
        .unwrap();
    assert_eq!(m, Message::single("alive"));
}

#[compio::test]
async fn server_survives_mid_session_abrupt_drop() {
    // Client drops the TCP connection abruptly while the server is live.
    // Server must survive and accept the next connection.
    let pull = Socket::new(SocketType::Pull, Options::default());
    let ep = pull.bind(tcp_ep(0)).await.unwrap();

    // First client: sends one message then drops.
    {
        let push1 = Socket::new(SocketType::Push, Options::default());
        push1.connect(ep.clone()).await.unwrap();
        test_support::wait_for_handshake(&push1).await;
        push1.send(Message::single("first")).await.unwrap();
        let m = compio::time::timeout(Duration::from_millis(300), pull.recv())
            .await
            .expect("recv timed out for first client")
            .unwrap();
        assert_eq!(m, Message::single("first"));
        // push1 drops here — abrupt half-close.
    }
    compio::time::sleep(Duration::from_millis(50)).await;

    // Second client: server must still be healthy.
    let push2 = Socket::new(SocketType::Push, Options::default());
    push2.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&push2).await;
    push2.send(Message::single("second")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timed out after abrupt drop")
        .unwrap();
    assert_eq!(m, Message::single("second"));
}

#[compio::test]
async fn abrupt_reset_mid_greeting_does_not_wedge_server() {
    // A peer that sends a partial greeting then drops must not stall the
    // server's accept loop. The server must still serve the next good client.
    let pull = Socket::new(SocketType::Pull, Options::default());
    let ep = pull.bind(tcp_ep(0)).await.unwrap();
    let port = match &ep {
        Endpoint::Tcp { port, .. } => *port,
        _ => unreachable!(),
    };

    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let h = std::thread::spawn(move || {
        let mut s = std::net::TcpStream::connect(addr).unwrap();
        // Send only the first 5 bytes of a greeting, then drop.
        let partial: [u8; 5] = [0xFF, 0x00, 0x00, 0x00, 0x00];
        s.write_all(&partial).expect("write partial greeting");
    });
    h.join().expect("partial-greeting thread panicked");
    compio::time::sleep(Duration::from_millis(50)).await;

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&push).await;

    push.send(Message::single("ok")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timed out after partial-greeting peer")
        .unwrap();
    assert_eq!(m, Message::single("ok"));
}

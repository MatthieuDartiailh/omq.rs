//! `ZMQ_STREAM` socket type: raw TCP communication with identity routing.

use std::time::Duration;

use bytes::Bytes;
use compio::io::{AsyncRead, AsyncWrite};
use compio::net::TcpStream;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

fn tcp_ep() -> Endpoint {
    "tcp://127.0.0.1:0".parse().unwrap()
}

#[compio::test]
async fn stream_bind_raw_tcp_client() {
    let sock = Socket::new(SocketType::Stream, Options::default());
    let bound = sock.bind(tcp_ep()).await.unwrap();
    let port = match &bound {
        Endpoint::Tcp { port, .. } => *port,
        _ => panic!("expected tcp endpoint"),
    };

    let client = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // Connect notification: [identity, empty].
    let connect_msg = compio::time::timeout(Duration::from_secs(2), sock.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(connect_msg.len(), 2);
    let identity = connect_msg.part_bytes(0).unwrap();
    assert!(!identity.is_empty());
    assert!(connect_msg.part_bytes(1).unwrap().is_empty());

    // Client sends raw bytes.
    let compio::BufResult(res, _) =
        AsyncWrite::write(&mut &client, Vec::from(b"hello from tcp" as &[u8])).await;
    res.unwrap();

    // STREAM socket receives [identity, data].
    let msg = compio::time::timeout(Duration::from_secs(2), sock.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.len(), 2);
    assert_eq!(msg.part_bytes(0).unwrap(), identity);
    assert_eq!(msg.part_bytes(1).unwrap(), &b"hello from tcp"[..]);

    // Send data back: [identity, data].
    sock.send(Message::multipart([
        identity.clone(),
        Bytes::from_static(b"reply"),
    ]))
    .await
    .unwrap();

    // Client receives raw bytes.
    let mut buf = vec![0u8; 64];
    let compio::BufResult(res, returned) = AsyncRead::read(&mut &client, buf).await;
    buf = returned;
    let n = res.unwrap();
    assert_eq!(&buf[..n], b"reply");
}

#[compio::test]
async fn stream_connect_to_raw_tcp_server() {
    let listener = compio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local = listener.local_addr().unwrap();

    let sock = Socket::new(SocketType::Stream, Options::default());
    let ep: Endpoint = format!("tcp://127.0.0.1:{}", local.port()).parse().unwrap();
    sock.connect(ep).await.unwrap();

    let (server_stream, _) = listener.accept().await.unwrap();

    // Connect notification.
    let connect_msg = compio::time::timeout(Duration::from_secs(2), sock.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(connect_msg.len(), 2);
    let identity = connect_msg.part_bytes(0).unwrap();
    assert!(connect_msg.part_bytes(1).unwrap().is_empty());

    // Send data from STREAM to the raw TCP server.
    sock.send(Message::multipart([
        identity.clone(),
        Bytes::from_static(b"outgoing"),
    ]))
    .await
    .unwrap();

    // Raw TCP server receives.
    let mut buf = vec![0u8; 64];
    let compio::BufResult(res, returned) = AsyncRead::read(&mut &server_stream, buf).await;
    buf = returned;
    let n = res.unwrap();
    assert_eq!(&buf[..n], b"outgoing");

    // Raw TCP server sends back.
    let compio::BufResult(res, _) =
        AsyncWrite::write(&mut &server_stream, Vec::from(b"incoming" as &[u8])).await;
    res.unwrap();

    // STREAM socket receives [identity, data].
    let msg = compio::time::timeout(Duration::from_secs(2), sock.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.len(), 2);
    assert_eq!(msg.part_bytes(0).unwrap(), identity);
    assert_eq!(msg.part_bytes(1).unwrap(), &b"incoming"[..]);
}

#[compio::test]
async fn stream_disconnect_on_close() {
    let sock = Socket::new(SocketType::Stream, Options::default());
    let bound = sock.bind(tcp_ep()).await.unwrap();
    let port = match &bound {
        Endpoint::Tcp { port, .. } => *port,
        _ => panic!("expected tcp endpoint"),
    };

    let client = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // Connect notification.
    let connect_msg = compio::time::timeout(Duration::from_secs(2), sock.recv())
        .await
        .unwrap()
        .unwrap();
    let identity = connect_msg.part_bytes(0).unwrap();

    // Close the raw TCP client.
    drop(client);

    // Disconnect notification: [identity, empty].
    let disc_msg = compio::time::timeout(Duration::from_secs(2), sock.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(disc_msg.len(), 2);
    assert_eq!(disc_msg.part_bytes(0).unwrap(), identity);
    assert!(disc_msg.part_bytes(1).unwrap().is_empty());
}

#[compio::test]
async fn stream_close_by_empty_send() {
    let sock = Socket::new(SocketType::Stream, Options::default());
    let bound = sock.bind(tcp_ep()).await.unwrap();
    let port = match &bound {
        Endpoint::Tcp { port, .. } => *port,
        _ => panic!("expected tcp endpoint"),
    };

    let client = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // Connect notification.
    let connect_msg = compio::time::timeout(Duration::from_secs(2), sock.recv())
        .await
        .unwrap()
        .unwrap();
    let identity = connect_msg.part_bytes(0).unwrap();

    // Send empty frame to disconnect.
    sock.send(Message::multipart([identity.clone(), Bytes::new()]))
        .await
        .unwrap();

    // The TCP client should see EOF.
    compio::time::sleep(Duration::from_millis(100)).await;
    let buf = vec![0u8; 64];
    let compio::BufResult(res, _) = AsyncRead::read(&mut &client, buf).await;
    assert_eq!(res.unwrap(), 0);
}

#[compio::test]
async fn stream_rejects_non_tcp() {
    let sock = Socket::new(SocketType::Stream, Options::default());
    let ep = Endpoint::Inproc {
        name: "nope".into(),
    };
    let res = sock.bind(ep).await;
    assert!(res.is_err());
}

#[compio::test]
async fn stream_rejects_wrong_frame_count() {
    let sock = Socket::new(SocketType::Stream, Options::default());
    let _ = sock.bind(tcp_ep()).await.unwrap();
    let res = sock.send(Message::single("oops")).await;
    assert!(res.is_err());
}

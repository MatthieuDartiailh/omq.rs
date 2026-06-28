#![cfg(feature = "ws")]

use bytes::Bytes;
use omq_compio::Socket;
use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::{Options, WssTls};
use omq_proto::proto::SocketType;
use std::time::Duration;

fn self_signed_tls() -> (Vec<u8>, Vec<u8>) {
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let cert_pem = certified.cert.pem().into_bytes();
    let key_pem = certified.signing_key.serialize_pem().into_bytes();
    (cert_pem, key_pem)
}

fn wss_endpoint(port: u16) -> Endpoint {
    format!("wss://127.0.0.1:{port}/").parse().unwrap()
}

fn get_port(ep: &Endpoint) -> u16 {
    match ep {
        Endpoint::Wss { port, .. } => *port,
        other => panic!("expected Wss, got {other:?}"),
    }
}

#[compio::test]
async fn wss_push_pull() {
    let (cert_pem, key_pem) = self_signed_tls();

    let server_opts = Options {
        wss_tls: WssTls {
            server_cert_pem: Some(cert_pem),
            server_key_pem: Some(key_pem),
            accept_invalid_certs: false,
        },
        ..Options::default()
    };

    let pull = Socket::new(SocketType::Pull, server_opts);
    let bound = pull.bind(wss_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let client_opts = Options {
        wss_tls: WssTls {
            accept_invalid_certs: true,
            ..WssTls::default()
        },
        ..Options::default()
    };

    let push = Socket::new(SocketType::Push, client_opts);
    push.connect(wss_endpoint(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(300)).await;

    push.send(Message::from(Bytes::from_static(b"hello wss")))
        .await
        .unwrap();

    let msg = compio::time::timeout(Duration::from_secs(5), pull.recv())
        .await
        .expect("recv timed out")
        .unwrap();

    assert_eq!(msg, Message::single("hello wss"));
}

#[compio::test]
async fn wss_multipart() {
    let (cert_pem, key_pem) = self_signed_tls();

    let server_opts = Options {
        wss_tls: WssTls {
            server_cert_pem: Some(cert_pem),
            server_key_pem: Some(key_pem),
            accept_invalid_certs: false,
        },
        ..Options::default()
    };

    let pull = Socket::new(SocketType::Pull, server_opts);
    let bound = pull.bind(wss_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let client_opts = Options {
        wss_tls: WssTls {
            accept_invalid_certs: true,
            ..WssTls::default()
        },
        ..Options::default()
    };

    let push = Socket::new(SocketType::Push, client_opts);
    push.connect(wss_endpoint(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(300)).await;

    let msg = Message::multipart([Bytes::from_static(b"frame1"), Bytes::from_static(b"frame2")]);
    push.send(msg).await.unwrap();

    let received = compio::time::timeout(Duration::from_secs(5), pull.recv())
        .await
        .expect("recv timed out")
        .unwrap();

    assert_eq!(received, Message::multipart(["frame1", "frame2"]));
}

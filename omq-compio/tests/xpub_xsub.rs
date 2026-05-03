//! XPUB / XSUB subscription propagation:
//! - SUB → PUB: SUBSCRIBE / CANCEL drives PUB-side filtering.
//! - SUB → XPUB: SUBSCRIBE surfaces as a `\x01<topic>` message at
//!   `XPUB.recv()`, CANCEL as `\x00<topic>`.
//! - XSUB → XPUB: XSUB.subscribe() sends SUBSCRIBE commands upstream.
//! - ZMTP 3.0 compatibility: PUB accepts `\x01<topic>` data-frame subscriptions.

use std::time::Duration;

use omq_compio::{Endpoint, Message, Options, Socket, SocketType};
use omq_proto::endpoint::Host;

fn loopback_port() -> u16 {
    use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
    let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn tcp_loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    }
}

#[compio::test]
async fn pub_filters_by_subscriber_prefix() {
    // Pre-subscribe to monitor BEFORE bind so we catch Listening.
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    let mut pub_mon = pub_.monitor();
    pub_.bind(tcp_loopback(0)).await.unwrap();
    let port = match compio::time::timeout(Duration::from_secs(1), pub_mon.recv())
        .await
        .unwrap()
        .unwrap()
    {
        omq_compio::MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("{other:?}"),
    };

    // Two SUBs: one wants "news.", one wants "sports.".
    let news = Socket::new(SocketType::Sub, Options::default());
    news.subscribe("news.").await.unwrap();
    news.connect(tcp_loopback(port)).await.unwrap();

    let sports = Socket::new(SocketType::Sub, Options::default());
    sports.subscribe("sports.").await.unwrap();
    sports.connect(tcp_loopback(port)).await.unwrap();

    // Drive a few probes so subscriptions propagate. Without this
    // the first sends after connect race the wire SUBSCRIBE.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut news_got = false;
    let mut sports_got = false;
    while !(news_got && sports_got) && std::time::Instant::now() < deadline {
        let _ = pub_.send(Message::single("news.alpha")).await;
        let _ = pub_.send(Message::single("sports.beta")).await;
        if !news_got
            && let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(20), news.recv()).await
        {
            let bytes = m.parts()[0].coalesce();
            assert!(bytes.starts_with(b"news."), "news got non-news: {bytes:?}");
            news_got = true;
        }
        if !sports_got
            && let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(20), sports.recv()).await
        {
            let bytes = m.parts()[0].coalesce();
            assert!(
                bytes.starts_with(b"sports."),
                "sports got non-sports: {bytes:?}"
            );
            sports_got = true;
        }
    }
    assert!(news_got, "news SUB never received its subscription");
    assert!(sports_got, "sports SUB never received its subscription");
}

#[compio::test]
async fn xpub_surfaces_subscribe_messages() {
    let xpub = Socket::new(SocketType::XPub, Options::default());
    let mut xmon = xpub.monitor();
    xpub.bind(tcp_loopback(0)).await.unwrap();
    let port = match compio::time::timeout(Duration::from_secs(1), xmon.recv())
        .await
        .unwrap()
        .unwrap()
    {
        omq_compio::MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("{other:?}"),
    };

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.connect(tcp_loopback(port)).await.unwrap();
    sub.subscribe("foo.").await.unwrap();

    // First message at XPUB should be `\x01foo.`.
    let m = compio::time::timeout(Duration::from_secs(2), xpub.recv())
        .await
        .unwrap()
        .unwrap();
    let body = m.parts()[0].coalesce();
    assert_eq!(&body[..], b"\x01foo.");
}

#[compio::test]
async fn xsub_subscribe_filters_messages_from_xpub() {
    let xpub = Socket::new(SocketType::XPub, Options::default());
    let mut xmon = xpub.monitor();
    xpub.bind(tcp_loopback(0)).await.unwrap();
    let port = match compio::time::timeout(Duration::from_secs(1), xmon.recv())
        .await
        .unwrap()
        .unwrap()
    {
        omq_compio::MonitorEvent::Listening {
            endpoint: Endpoint::Tcp { port, .. },
        } => port,
        other => panic!("{other:?}"),
    };

    let xsub = Socket::new(SocketType::XSub, Options::default());
    xsub.connect(tcp_loopback(port)).await.unwrap();
    xsub.subscribe("news.").await.unwrap();

    let sub_notif = compio::time::timeout(Duration::from_secs(2), xpub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&sub_notif.parts()[0].coalesce()[..], b"\x01news.");

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let _ = xpub.send(Message::single("news.alpha")).await;
        let _ = xpub.send(Message::single("sports.beta")).await;
        if let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(20), xsub.recv()).await {
            let bytes = m.parts()[0].coalesce();
            assert!(
                bytes.starts_with(b"news."),
                "XSUB received non-subscribed message: {bytes:?}"
            );
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "XSUB never received its subscribed messages"
        );
    }
}

#[compio::test]
async fn xsub_send_returns_protocol_error() {
    let xsub = Socket::new(SocketType::XSub, Options::default());
    let r = xsub
        .send(Message::single(b"\x01topic".as_ref()))
        .await;
    assert!(
        matches!(r, Err(omq_compio::Error::Protocol(_))),
        "XSUB send should return Protocol error; got {r:?}"
    );
}

fn zmtp30_sub_greeting() -> [u8; 64] {
    let mut g = [0u8; 64];
    g[0] = 0xFF;
    g[9] = 0x7F;
    g[10] = 3;
    g[11] = 0;
    g[12..16].copy_from_slice(b"NULL");
    g
}

const SUB_READY: &[u8] = &[
    0x04, 0x19,
    0x05, b'R', b'E', b'A', b'D', b'Y',
    0x0B, b'S', b'o', b'c', b'k', b'e', b't', b'-', b'T', b'y', b'p', b'e',
    0x00, 0x00, 0x00, 0x03,
    b'S', b'U', b'B',
];

fn read_zmtp_frame(stream: &mut std::net::TcpStream) -> Vec<u8> {
    use std::io::Read;
    let mut flags = [0u8; 1];
    stream.read_exact(&mut flags).unwrap();
    let body_len = if flags[0] & 0x02 != 0 {
        let mut len_buf = [0u8; 8];
        stream.read_exact(&mut len_buf).unwrap();
        u64::from_be_bytes(len_buf) as usize
    } else {
        let mut len_buf = [0u8; 1];
        stream.read_exact(&mut len_buf).unwrap();
        len_buf[0] as usize
    };
    let mut body = vec![0u8; body_len];
    stream.read_exact(&mut body).unwrap();
    body
}

#[compio::test]
async fn pub_accepts_zmtp30_message_form_subscribe() {
    use std::io::Write;

    let port = loopback_port();
    let pub_ = Socket::new(SocketType::Pub, Options::default());
    pub_.bind(tcp_loopback(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(30)).await;

    let (sub_sent_tx, sub_sent_rx) = flume::bounded::<()>(1);
    let (result_tx, result_rx) = flume::bounded::<Vec<u8>>(1);

    let addr = std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, port));
    std::thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();

        stream.write_all(&zmtp30_sub_greeting()).unwrap();

        let mut peer_greeting = [0u8; 64];
        use std::io::Read;
        stream.read_exact(&mut peer_greeting).unwrap();

        stream.write_all(SUB_READY).unwrap();

        let _ = read_zmtp_frame(&mut stream);

        let sub_frame: &[u8] = &[0x00, 0x04, 0x01, b'f', b'o', b'o'];
        stream.write_all(sub_frame).unwrap();

        let _ = sub_sent_tx.send(());

        match read_zmtp_frame(&mut stream) {
            body => {
                let _ = result_tx.send(body);
            }
        }
    });

    compio::time::timeout(Duration::from_secs(2), sub_sent_rx.recv_async())
        .await
        .expect("raw peer never sent subscription")
        .unwrap();

    compio::time::sleep(Duration::from_millis(100)).await;

    pub_.send(Message::single("foo.topic.1")).await.unwrap();
    pub_.send(Message::single("bar.other")).await.unwrap();

    let received = compio::time::timeout(Duration::from_secs(3), result_rx.recv_async())
        .await
        .expect("raw peer never received a message")
        .unwrap();

    assert!(
        received.starts_with(b"foo."),
        "ZMTP 3.0 message-form subscription must route 'foo.*' messages; got: {received:?}"
    );
}

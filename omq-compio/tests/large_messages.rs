//! Large messages over NULL mechanism TCP.
//!
//! Exercises multi-chunk Payload / scatter-gather framing at payload sizes
//! that span many TCP segments.
//!
//! All tests build the runtime with [`build_default_runtime`] (128 x 32 KiB
//! `BUF_RING` pool). The default `#[compio::test]` runtime uses compio's
//! 8 x 8 KiB defaults, which exhausts the ring on the first ~64 KiB of
//! sustained delivery and terminates the multi-shot recv SQE.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType, build_default_runtime};

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

async fn push_pull_large(size_bytes: usize) {
    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp_ep(port)).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    let payload: Vec<u8> = (0..size_bytes).map(|i| (i & 0xFF) as u8).collect();
    push.send(Message::single(payload.clone())).await.unwrap();

    let m = compio::time::timeout(Duration::from_secs(10), pull.recv())
        .await
        .expect("large message recv timed out")
        .unwrap();
    let got = m.parts()[0].as_bytes();
    assert_eq!(
        got.len(),
        size_bytes,
        "payload length mismatch at {size_bytes} B"
    );
    assert_eq!(
        &*got,
        &payload[..],
        "payload data corrupted at {size_bytes} B"
    );
}

#[test]
fn large_message_64kib() {
    let rt = build_default_runtime().expect("build runtime");
    rt.block_on(push_pull_large(64 * 1024));
}

#[test]
fn large_message_256kib() {
    let rt = build_default_runtime().expect("build runtime");
    rt.block_on(push_pull_large(256 * 1024));
}

#[test]
fn large_message_1mib() {
    let rt = build_default_runtime().expect("build runtime");
    rt.block_on(push_pull_large(1024 * 1024));
}

#[test]
fn large_multipart_over_tcp() {
    let rt = build_default_runtime().expect("build runtime");
    rt.block_on(async {
        let part_size = 256 * 1024;
        let port = loopback_port();
        let rep = Socket::new(SocketType::Rep, Options::default());
        rep.bind(tcp_ep(port)).await.unwrap();
        let req = Socket::new(SocketType::Req, Options::default());
        req.connect(tcp_ep(port)).await.unwrap();
        compio::time::sleep(Duration::from_millis(50)).await;

        let part_a: Vec<u8> = vec![0xAA; part_size];
        let part_b: Vec<u8> = vec![0xBB; part_size];

        req.send(Message::multipart([part_a, part_b]))
            .await
            .unwrap();

        let m = compio::time::timeout(Duration::from_secs(10), rep.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.len(), 2, "expected 2-part message");
        assert_eq!(m.parts()[0].as_bytes().len(), part_size);
        assert_eq!(*m.parts()[0].as_bytes().first().unwrap(), 0xAA);
        assert_eq!(m.parts()[1].as_bytes().len(), part_size);
        assert_eq!(*m.parts()[1].as_bytes().first().unwrap(), 0xBB);
    });
}

#[test]
fn huge_messages_xxhash() {
    use futures::join;
    use xxhash_rust::xxh3::xxh3_128;

    // 800 MiB total across the three messages. With compio's default
    // 8 KiB BUF_RING slots that's ~100k CQEs, blowing past the
    // u16 BUF_RING tail counter (~65k slot-resets) and tripping
    // synchrony's debug-mode overflow check. Use the omq-compio
    // helper which configures 32 KiB slots, pushing the threshold
    // out to ~2 GiB.
    let rt = build_default_runtime().expect("build runtime");
    rt.block_on(async {
        const SIZES: [usize; 3] = [100 * 1024 * 1024, 200 * 1024 * 1024, 500 * 1024 * 1024];

        let port = loopback_port();
        let pull = Socket::new(SocketType::Pull, Options::default());
        pull.bind(tcp_ep(port)).await.unwrap();
        let push = Socket::new(SocketType::Push, Options::default());
        push.connect(tcp_ep(port)).await.unwrap();
        compio::time::sleep(Duration::from_millis(50)).await;

        // Pre-generate all payloads and hashes up front, then run send and recv
        // concurrently via join! — required on this single-thread runtime because
        // all-sends-then-all-recvs fills the kernel socket buffer and deadlocks.
        let payloads: Vec<Vec<u8>> = SIZES
            .iter()
            .map(|&size| {
                (0u64..size as u64)
                    .map(|j| {
                        j.wrapping_mul(6_364_136_223_846_793_005)
                            .wrapping_add(1_442_695_040_888_963_407)
                            .to_be_bytes()[0]
                    })
                    .collect()
            })
            .collect();
        let hashes: Vec<u128> = payloads.iter().map(|p| xxh3_128(p)).collect();

        let send_fut = async {
            for payload in payloads {
                push.send(Message::single(payload)).await.unwrap();
            }
        };
        let recv_fut = async {
            for (i, expected) in hashes.iter().enumerate() {
                let m = compio::time::timeout(Duration::from_mins(2), pull.recv())
                    .await
                    .unwrap_or_else(|_| panic!("recv timed out for message {i}"))
                    .unwrap();
                let got = m.parts()[0].as_bytes();
                assert_eq!(got.len(), SIZES[i], "length mismatch on message {i}");
                assert_eq!(
                    xxh3_128(&got),
                    *expected,
                    "xxh3-128 mismatch on message {i} — payload corrupted in transit"
                );
            }
        };
        join!(send_fut, recv_fut);
    });
}

#[test]
fn large_message_back_to_back() {
    let rt = build_default_runtime().expect("build runtime");
    rt.block_on(async {
        let size = 128 * 1024;
        let port = loopback_port();
        let pull = Socket::new(SocketType::Pull, Options::default());
        pull.bind(tcp_ep(port)).await.unwrap();

        let push = Socket::new(SocketType::Push, Options::default());
        push.connect(tcp_ep(port)).await.unwrap();
        compio::time::sleep(Duration::from_millis(50)).await;

        let p1: Vec<u8> = vec![0x11; size];
        let p2: Vec<u8> = vec![0x22; size];
        push.send(Message::single(p1.clone())).await.unwrap();
        push.send(Message::single(p2.clone())).await.unwrap();

        let m1 = compio::time::timeout(Duration::from_secs(5), pull.recv())
            .await
            .unwrap()
            .unwrap();
        let m2 = compio::time::timeout(Duration::from_secs(5), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&*m1.parts()[0].as_bytes(), &p1[..]);
        assert_eq!(&*m2.parts()[0].as_bytes(), &p2[..]);
    });
}

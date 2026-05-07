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
    let got = m.part_bytes(0).unwrap();
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
        assert_eq!(m.part_bytes(0).unwrap().len(), part_size);
        assert_eq!(*m.part_bytes(0).unwrap().first().unwrap(), 0xAA);
        assert_eq!(m.part_bytes(1).unwrap().len(), part_size);
        assert_eq!(*m.part_bytes(1).unwrap().first().unwrap(), 0xBB);
    });
}

#[test]
fn huge_messages_xxhash() {
    use futures::join;
    use xxhash_rust::xxh3::xxh3_128;

    // Uses build_default_runtime (32 KiB BUF_RING slots) to handle
    // sustained large-message delivery without exhausting slot counters.
    let rt = build_default_runtime().expect("build runtime");
    rt.block_on(async {
        const SIZES: [usize; 3] = [4 * 1024 * 1024, 8 * 1024 * 1024, 100 * 1024 * 1024];

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
            .enumerate()
            .map(|(i, &size)| {
                let seed = (i as u8).wrapping_mul(0xAB).wrapping_add(0x37);
                vec![seed; size]
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
                let got = m.part_bytes(0).unwrap();
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

/// With `large_message_threshold` disabled, the multi-shot path is used
/// for every recv regardless of size. This test confirms data integrity
/// under that mode (i.e. the threshold knob is wired correctly and
/// disabling it still works end-to-end).
#[test]
fn large_message_with_threshold_disabled() {
    let rt = build_default_runtime().expect("build runtime");
    rt.block_on(async {
        let port = loopback_port();
        let opts = Options::default().disable_large_message_path();
        let pull = Socket::new(SocketType::Pull, opts.clone());
        pull.bind(tcp_ep(port)).await.unwrap();
        let push = Socket::new(SocketType::Push, opts);
        push.connect(tcp_ep(port)).await.unwrap();
        compio::time::sleep(Duration::from_millis(50)).await;

        let size = 1024 * 1024;
        let payload: Vec<u8> = (0..size).map(|i| (i & 0xFF) as u8).collect();
        push.send(Message::single(payload.clone())).await.unwrap();
        let m = compio::time::timeout(Duration::from_secs(10), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&*m.part_bytes(0).unwrap(), &payload[..]);
    });
}

/// Sequence small → large → small confirms the codec returns to a
/// clean state after the one-shot path supplies a payload, the
/// multi-shot stream is rebuilt, and a subsequent small frame parses
/// normally via the multi-shot path.
#[test]
fn small_then_large_then_small() {
    let rt = build_default_runtime().expect("build runtime");
    rt.block_on(async {
        let port = loopback_port();
        let pull = Socket::new(SocketType::Pull, Options::default());
        pull.bind(tcp_ep(port)).await.unwrap();
        let push = Socket::new(SocketType::Push, Options::default());
        push.connect(tcp_ep(port)).await.unwrap();
        compio::time::sleep(Duration::from_millis(50)).await;

        let small_a: Vec<u8> = (0..128).map(|i| (i & 0xFF) as u8).collect();
        let large: Vec<u8> = (0..(2 * 1024 * 1024)).map(|i| (i & 0xFF) as u8).collect();
        let small_b: Vec<u8> = (0..256).map(|i| ((i + 7) & 0xFF) as u8).collect();
        push.send(Message::single(small_a.clone())).await.unwrap();
        push.send(Message::single(large.clone())).await.unwrap();
        push.send(Message::single(small_b.clone())).await.unwrap();

        let m1 = compio::time::timeout(Duration::from_secs(5), pull.recv())
            .await
            .unwrap()
            .unwrap();
        let m2 = compio::time::timeout(Duration::from_secs(15), pull.recv())
            .await
            .unwrap()
            .unwrap();
        let m3 = compio::time::timeout(Duration::from_secs(5), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&*m1.part_bytes(0).unwrap(), &small_a[..]);
        assert_eq!(&*m2.part_bytes(0).unwrap(), &large[..]);
        assert_eq!(&*m3.part_bytes(0).unwrap(), &small_b[..]);
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
        assert_eq!(&*m1.part_bytes(0).unwrap(), &p1[..]);
        assert_eq!(&*m2.part_bytes(0).unwrap(), &p2[..]);
    });
}

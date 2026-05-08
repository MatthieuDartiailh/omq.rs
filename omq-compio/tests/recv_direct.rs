//! Stage 5 stripped recv-direct fast path tests.
//!
//! The fast path is activated implicitly on eligible single-peer
//! sockets (Pull / Sub / Rep / Pair / Req); these tests exercise the
//! cancellation, concurrency, heartbeat-coexistence, and reconnect
//! edges where the implementation is most likely to misbehave.

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

/// Drop a `recv()` future after `read_ready` has subscribed but no
/// data has arrived; a fresh `recv()` should still receive the
/// next message correctly. Verifies the RAII `ClaimGuard` resets
/// the recv claim and wakes the driver.
#[compio::test]
async fn cancel_recv_mid_wait_then_recv_succeeds() {
    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(tcp_ep(port)).await.unwrap();

    // Wait for the connection to handshake before kicking off the
    // cancellation race - direct recv only engages post-handshake.
    push.send(Message::single("warm-up")).await.unwrap();
    let warm = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("warm-up timeout")
        .unwrap();
    assert_eq!(warm.part_bytes(0).unwrap(), &b"warm-up"[..]);

    // Abandon recv() mid-flight: it has claimed the recv slot,
    // entered the read_ready/in_rx select, and is parked. Drop
    // forces a Drop on the ClaimGuard which must release the claim.
    let canceled = compio::time::timeout(Duration::from_millis(100), pull.recv()).await;
    assert!(canceled.is_err(), "first recv should have timed out");

    push.send(Message::single("after-cancel")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("second recv timeout")
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"after-cancel"[..]);
}

/// While a `recv()` direct claim is held with no traffic, both
/// sides' driver heartbeat ticks must continue to fire and update
/// the shared `last_input_nanos` so the connection stays up. After
/// the idle window the next `send` still arrives.
#[compio::test]
async fn heartbeat_keeps_connection_alive_under_direct_recv() {
    let o = Options {
        heartbeat_interval: Some(Duration::from_millis(50)),
        heartbeat_timeout: Some(Duration::from_millis(500)),
        ..Default::default()
    };

    let port = loopback_port();
    let pull = Socket::new(SocketType::Pull, o.clone());
    pull.bind(tcp_ep(port)).await.unwrap();
    let push = Socket::new(SocketType::Push, o);
    push.connect(tcp_ep(port)).await.unwrap();

    // Warm-up to confirm direct recv is engaging.
    push.send(Message::single("warm-up")).await.unwrap();
    let _ = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("warm-up timeout")
        .unwrap();

    // Park PULL in a long-running direct recv across several
    // heartbeat windows (~14× the interval). Auto-PONG on each
    // side must keep the connection from timing out.
    let pull_handle = compio::runtime::spawn({
        let pull = pull.clone();
        async move { compio::time::timeout(Duration::from_secs(2), pull.recv()).await }
    });

    compio::time::sleep(Duration::from_millis(700)).await;
    push.send(Message::single("after-idle")).await.unwrap();

    let m = pull_handle.await.unwrap();
    assert!(m.is_ok(), "recv timed out (heartbeat dropped connection)");
    let m = m.unwrap().unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"after-idle"[..]);
}

/// Hammer the recv cancellation path. Sends a long sequence of
/// numbered frames; the receiver alternates between a real `recv()`
/// and a tiny randomised timeout that often fires mid-await,
/// forcing the recv future to be dropped between iterations.
///
/// Before the multi-shot recv migration, dropping a recv future
/// after the kernel had selected a buffer (but before the consumer
/// observed it) leaked those bytes, desyncing ZMTP framing. With the
/// persistent multi-shot SQE, the kernel op survives consumer drops:
/// bytes remain queued in the `BUF_RING` and are picked up by the
/// next `recv()`. This test would intermittently fail on the old
/// code; it must pass deterministically on the new path.
#[test]
fn recv_drop_during_select_does_not_desync() {
    use futures::join;

    const FRAMES: u32 = 5_000;

    let rt = build_default_runtime().expect("build runtime");
    rt.block_on(async {
        let port = loopback_port();
        let pull = Socket::new(SocketType::Pull, Options::default());
        pull.bind(tcp_ep(port)).await.unwrap();
        let push = Socket::new(SocketType::Push, Options::default());
        push.connect(tcp_ep(port)).await.unwrap();
        compio::time::sleep(Duration::from_millis(50)).await;

        let send_fut = async {
            for i in 0..FRAMES {
                let mut payload = Vec::with_capacity(8);
                payload.extend_from_slice(&i.to_be_bytes());
                payload.extend_from_slice(b"-frame");
                push.send(Message::single(payload)).await.unwrap();
            }
        };

        let recv_fut = async {
            // Pseudo-random timeout in [0, 200) microseconds. Linear
            // congruential generator inline so the test has no `rand`
            // crate dependency at runtime; the seed varies the cancel
            // pattern across runs without making outcomes flaky -
            // every frame must arrive regardless.
            let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
            let mut got: Vec<u32> = Vec::with_capacity(FRAMES as usize);
            while (got.len() as u32) < FRAMES {
                seed = seed
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let micros = (seed >> 56) * 200 / 256;
                let outcome =
                    compio::time::timeout(Duration::from_micros(micros), pull.recv()).await;
                if let Ok(msg) = outcome {
                    let m = msg.expect("recv ok");
                    let payload = m.part_bytes(0).unwrap();
                    let seq = u32::from_be_bytes(payload[..4].try_into().unwrap());
                    got.push(seq);
                }
            }
            got
        };

        let ((), got) = join!(send_fut, recv_fut);
        eprintln!("got {} frames total (expected {FRAMES})", got.len());
        for (i, seq) in got.iter().enumerate() {
            if *seq != i as u32 {
                let prev = if i == 0 { -1i64 } else { i64::from(got[i - 1]) };
                let i_i64 = i64::try_from(i).expect("frame index fits i64");
                panic!(
                    "frame {i} desynced: prev={prev}, got seq {seq} (jumped {}) \
                     after dropping recv mid-await",
                    i64::from(*seq) - i_i64,
                );
            }
        }
    });
}

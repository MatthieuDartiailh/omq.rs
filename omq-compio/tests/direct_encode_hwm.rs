//! Regression test: the direct-encode fast path must provide backpressure.
//! Bounded by a 512 KiB byte cap (`DIRECT_CAP`); the cmd channel (bounded
//! at `send_hwm`) provides the per-message backpressure layer.

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

/// Fill the PUSH->PULL pipeline (receiver never reads), then verify
/// that `send()` blocks within a bounded number of messages.
///
/// Uses 128 KiB payloads so kernel TCP buffers (which autoscale up to
/// tcp_wmem max, often 4 MiB on Linux) fill within tens of messages.
#[compio::test]
async fn direct_encode_respects_send_hwm() {
    let hwm: u32 = 16;
    let pull = Socket::new(SocketType::Pull, Options::default().recv_hwm(hwm));
    let ep = pull.bind(tcp_ep(0)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default().send_hwm(hwm));
    push.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    let payload = vec![0u8; 128 * 1024];
    let mut accepted = 0usize;
    for _ in 0..5000 {
        match compio::time::timeout(
            Duration::from_millis(50),
            push.send(Message::from(payload.clone())),
        )
        .await
        {
            Ok(Ok(())) => accepted += 1,
            _ => break,
        }
    }

    // Total buffering: DIRECT_CAP (512 KiB) / 128 KiB = 4 msgs +
    // send_hwm 16 (cmd channel) + kernel TCP buffers (tcp_wmem max is
    // typically 4 MiB on Linux = ~32 msgs at 128 KiB). Total ~52 msgs.
    assert!(
        accepted < 500,
        "accepted {accepted} messages -- expected backpressure well before 500 \
         (128 KiB payloads x send_hwm {hwm})",
    );

    let _ = pull;
}

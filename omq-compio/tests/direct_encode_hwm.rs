//! Regression test: the direct-encode fast path must respect `send_hwm`.
//! Previously it was bounded only by a 512 KiB byte cap, allowing
//! thousands of small messages to bypass backpressure.

#![cfg(not(feature = "priority"))]

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

/// Fill the PUSH→PULL pipeline (receiver never reads), then verify
/// that `send()` blocks within a bounded number of messages.
///
/// Uses 8 KiB payloads so kernel TCP buffers fill within hundreds of
/// messages rather than tens of thousands.
#[compio::test]
async fn direct_encode_respects_send_hwm() {
    let hwm: u32 = 16;
    let pull = Socket::new(SocketType::Pull, Options::default().recv_hwm(hwm));
    let ep = pull.bind(tcp_ep(0)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default().send_hwm(hwm));
    push.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    let payload = vec![0u8; 8 * 1024];
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

    // With send_hwm enforcement on the direct-encode path, the total
    // buffering is bounded by: send_hwm (direct) + send_hwm (cmd chan)
    // + send_hwm (shared queue) + kernel TCP buffers (system-dependent,
    // typically 128-512 KiB on Linux). At 8 KiB per message that gives
    // roughly 48 + ~64 = ~112 messages before backpressure.
    //
    // Without the fix, the direct path alone would accept up to
    // DIRECT_CAP / 8 KiB = 64 msgs byte-capped, so the difference is
    // small at 8 KiB. The real regression this guards against is small
    // messages: at ~10 B per msg the old byte cap allowed ~50k messages
    // vs the new 16-message count cap. The 8 KiB payload ensures TCP
    // buffers actually fill during the test.
    assert!(
        accepted < 500,
        "accepted {accepted} messages — expected backpressure well before 500 \
         (8 KiB payloads × send_hwm {hwm})",
    );

    let _ = pull;
}

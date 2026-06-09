//! Regression tests for the pending-commands drain path.
//!
//! When messages are sent before the handshake completes, they queue in
//! `pending_cmds`. The driver must drain them with a byte cap (interleaving
//! flushes) rather than encoding the entire backlog in one pass, which would
//! buffer hundreds of megabytes for large messages under crypto mechanisms.
//!
//! These tests verify:
//! - All queued messages are delivered (no data loss from batched drain).
//! - FIFO ordering is preserved across the drain boundary.
//! - The fix works for plain, CURVE, and BLAKE3ZMQ paths.

mod test_support;

use std::time::Duration;

use omq_compio::{Endpoint, IpcPath, Message, Options, ReconnectPolicy, Socket, SocketType};

fn ipc_ep(name: &str) -> Endpoint {
    Endpoint::Ipc(IpcPath::Abstract(format!(
        "omq-compio-pending-drain-{name}-{}-{}",
        std::process::id(),
        rand::random::<u32>()
    )))
}

const TIMEOUT: Duration = Duration::from_secs(10);

/// Queue many large messages before the handshake, then verify all arrive
/// in order. With 200 × 128 KiB = 25 MiB queued, the old unbounded drain
/// would buffer everything in one shot. The byte-capped drain flushes in
/// ~1 MiB batches.
#[compio::test]
async fn large_messages_queued_before_handshake_plain() {
    const MSG_SIZE: usize = 128 * 1024;
    const MSG_COUNT: usize = 200;

    let ep = ipc_ep("plain-large");
    let push = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(20)),
            ..Default::default()
        },
    );
    push.connect(ep.clone()).await.unwrap();

    let payload: Vec<u8> = (0..MSG_SIZE).map(|i| (i % 251) as u8).collect();
    let msg = Message::single(payload.clone());
    for _ in 0..MSG_COUNT {
        push.send(msg.clone()).await.unwrap();
    }

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep).await.unwrap();

    for i in 0..MSG_COUNT {
        let m = compio::time::timeout(TIMEOUT, pull.recv())
            .await
            .unwrap_or_else(|_| panic!("timeout waiting for message {i}"))
            .unwrap();
        let body = m.part_bytes(0).unwrap();
        assert_eq!(body.len(), MSG_SIZE, "message {i}: wrong length");
        assert_eq!(&body[..8], &payload[..8], "message {i}: content mismatch");
    }
}

/// Verify FIFO ordering: send numbered messages before handshake, receive
/// them in the same order after.
#[compio::test]
async fn pending_drain_preserves_fifo_order() {
    const MSG_COUNT: usize = 500;

    let ep = ipc_ep("fifo");
    let push = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(20)),
            ..Default::default()
        },
    );
    push.connect(ep.clone()).await.unwrap();

    for i in 0..MSG_COUNT {
        push.send(Message::single(i.to_string())).await.unwrap();
    }

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep).await.unwrap();

    for i in 0..MSG_COUNT {
        let m = compio::time::timeout(TIMEOUT, pull.recv())
            .await
            .unwrap_or_else(|_| panic!("timeout waiting for message {i}"))
            .unwrap();
        let body = m.part_bytes(0).unwrap();
        let got: usize = std::str::from_utf8(&body).unwrap().parse().unwrap();
        assert_eq!(got, i, "out of order: expected {i}, got {got}");
    }
}

// =====================================================================
// BLAKE3ZMQ: crypto path (codec.send_message → out_chunks)
// =====================================================================

#[cfg(feature = "blake3zmq")]
mod blake3zmq_drain {
    use super::*;
    use omq_compio::Blake3ZmqKeypair;

    fn ipc_ep_b3(name: &str) -> Endpoint {
        Endpoint::Ipc(IpcPath::Abstract(format!(
            "omq-compio-b3-drain-{name}-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        )))
    }

    /// Queue 200 × 128 KiB messages through BLAKE3ZMQ before handshake.
    /// Exercises the crypto path where encrypted bytes accumulate in
    /// `codec.out_chunks` (not `EncodedQueue`). The old drain would buffer
    /// ~25 MiB of encrypted frames in one shot.
    #[compio::test]
    async fn blake3zmq_large_pending_drain() {
        const MSG_SIZE: usize = 128 * 1024;
        const MSG_COUNT: usize = 200;

        let server_kp = Blake3ZmqKeypair::generate();
        let client_kp = Blake3ZmqKeypair::generate();
        let server_pub = server_kp.public;
        let ep = ipc_ep_b3("large");

        let push = Socket::new(
            SocketType::Push,
            Options {
                reconnect: ReconnectPolicy::Fixed(Duration::from_millis(20)),
                ..Default::default()
            }
            .blake3zmq_client(client_kp, server_pub),
        );
        push.connect(ep.clone()).await.unwrap();

        let payload: Vec<u8> = (0..MSG_SIZE).map(|i| (i % 251) as u8).collect();
        let msg = Message::single(payload.clone());
        for _ in 0..MSG_COUNT {
            push.send(msg.clone()).await.unwrap();
        }

        let pull = Socket::new(
            SocketType::Pull,
            Options::default().blake3zmq_server(server_kp),
        );
        pull.bind(ep).await.unwrap();

        for i in 0..MSG_COUNT {
            let m = compio::time::timeout(TIMEOUT, pull.recv())
                .await
                .unwrap_or_else(|_| panic!("timeout waiting for message {i}"))
                .unwrap();
            let body = m.part_bytes(0).unwrap();
            assert_eq!(body.len(), MSG_SIZE, "message {i}: wrong length");
            assert_eq!(&body[..8], &payload[..8], "message {i}: content mismatch");
        }
    }

    /// FIFO ordering through BLAKE3ZMQ pending drain.
    #[compio::test]
    async fn blake3zmq_pending_drain_preserves_fifo() {
        const MSG_COUNT: usize = 500;

        let server_kp = Blake3ZmqKeypair::generate();
        let client_kp = Blake3ZmqKeypair::generate();
        let server_pub = server_kp.public;
        let ep = ipc_ep_b3("fifo");

        let push = Socket::new(
            SocketType::Push,
            Options {
                reconnect: ReconnectPolicy::Fixed(Duration::from_millis(20)),
                ..Default::default()
            }
            .blake3zmq_client(client_kp, server_pub),
        );
        push.connect(ep.clone()).await.unwrap();

        for i in 0..MSG_COUNT {
            push.send(Message::single(i.to_string())).await.unwrap();
        }

        let pull = Socket::new(
            SocketType::Pull,
            Options::default().blake3zmq_server(server_kp),
        );
        pull.bind(ep).await.unwrap();

        for i in 0..MSG_COUNT {
            let m = compio::time::timeout(TIMEOUT, pull.recv())
                .await
                .unwrap_or_else(|_| panic!("timeout waiting for message {i}"))
                .unwrap();
            let body = m.part_bytes(0).unwrap();
            let got: usize = std::str::from_utf8(&body).unwrap().parse().unwrap();
            assert_eq!(got, i, "out of order: expected {i}, got {got}");
        }
    }
}

// =====================================================================
// CURVE: same codec path as BLAKE3ZMQ but different mechanism
// =====================================================================

#[cfg(feature = "curve")]
mod curve_drain {
    use super::*;
    use omq_compio::CurveKeypair;

    fn ipc_ep_curve(name: &str) -> Endpoint {
        Endpoint::Ipc(IpcPath::Abstract(format!(
            "omq-compio-curve-drain-{name}-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        )))
    }

    #[compio::test]
    async fn curve_large_pending_drain() {
        const MSG_SIZE: usize = 128 * 1024;
        const MSG_COUNT: usize = 200;

        let server_kp = CurveKeypair::generate();
        let client_kp = CurveKeypair::generate();
        let server_pub = server_kp.public;
        let ep = ipc_ep_curve("large");

        let push = Socket::new(
            SocketType::Push,
            Options {
                reconnect: ReconnectPolicy::Fixed(Duration::from_millis(20)),
                ..Default::default()
            }
            .curve_client(client_kp, server_pub),
        );
        push.connect(ep.clone()).await.unwrap();

        let payload: Vec<u8> = (0..MSG_SIZE).map(|i| (i % 251) as u8).collect();
        let msg = Message::single(payload.clone());
        for _ in 0..MSG_COUNT {
            push.send(msg.clone()).await.unwrap();
        }

        let pull = Socket::new(SocketType::Pull, Options::default().curve_server(server_kp));
        pull.bind(ep).await.unwrap();

        for i in 0..MSG_COUNT {
            let m = compio::time::timeout(TIMEOUT, pull.recv())
                .await
                .unwrap_or_else(|_| panic!("timeout waiting for message {i}"))
                .unwrap();
            let body = m.part_bytes(0).unwrap();
            assert_eq!(body.len(), MSG_SIZE, "message {i}: wrong length");
            assert_eq!(&body[..8], &payload[..8], "message {i}: content mismatch");
        }
    }

    #[compio::test]
    async fn curve_pending_drain_preserves_fifo() {
        const MSG_COUNT: usize = 500;

        let server_kp = CurveKeypair::generate();
        let client_kp = CurveKeypair::generate();
        let server_pub = server_kp.public;
        let ep = ipc_ep_curve("fifo");

        let push = Socket::new(
            SocketType::Push,
            Options {
                reconnect: ReconnectPolicy::Fixed(Duration::from_millis(20)),
                ..Default::default()
            }
            .curve_client(client_kp, server_pub),
        );
        push.connect(ep.clone()).await.unwrap();

        for i in 0..MSG_COUNT {
            push.send(Message::single(i.to_string())).await.unwrap();
        }

        let pull = Socket::new(SocketType::Pull, Options::default().curve_server(server_kp));
        pull.bind(ep).await.unwrap();

        for i in 0..MSG_COUNT {
            let m = compio::time::timeout(TIMEOUT, pull.recv())
                .await
                .unwrap_or_else(|_| panic!("timeout waiting for message {i}"))
                .unwrap();
            let body = m.part_bytes(0).unwrap();
            let got: usize = std::str::from_utf8(&body).unwrap().parse().unwrap();
            assert_eq!(got, i, "out of order: expected {i}, got {got}");
        }
    }
}

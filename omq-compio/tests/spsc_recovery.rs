//! Verify inproc send path works after peer churn (dead slot + new slot).
//! The SPSC fast path only activates for cross-thread inproc, but this
//! validates the `out_peer_count` guard and pipe Vec scan after slot gaps.

use std::time::Duration;

use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

fn inproc(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[compio::test]
async fn send_works_after_peer_drop_and_new_connect() {
    let ep = inproc("compio-spsc-recovery");
    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(ep.clone()).await.unwrap();

    // First peer: connect, send, recv, then drop.
    {
        let pull1 = Socket::new(SocketType::Pull, Options::default());
        pull1.connect(ep.clone()).await.unwrap();

        push.send(Message::from_slice(b"first")).await.unwrap();
        let msg = compio::time::timeout(Duration::from_secs(2), pull1.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.part_bytes(0).unwrap().as_ref(), b"first");
    }
    compio::time::sleep(Duration::from_millis(100)).await;

    // Second peer connects into a different slot. The first slot is
    // dead (None in the pipes Vec). The send fast path must still find
    // the new pipe.
    let pull2 = Socket::new(SocketType::Pull, Options::default());
    pull2.connect(ep.clone()).await.unwrap();

    push.send(Message::from_slice(b"second")).await.unwrap();
    let msg = compio::time::timeout(Duration::from_secs(2), pull2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap().as_ref(), b"second");

    push.close().await.unwrap();
    pull2.close().await.unwrap();
}

//! Regression: binding an inproc name, closing via `signal_close()` (the
//! fallback path when the socket Rc has surviving refs), then rebinding
//! the same name on a new socket must succeed.

use std::time::Duration;

use omq_compio::{Endpoint, Options, Socket, SocketType};

fn inproc(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[compio::test]
async fn rebind_after_signal_close() {
    let ep = inproc("rebind-signal-close");

    let s1 = Socket::new(SocketType::Pull, Options::default());
    s1.bind(ep.clone()).await.unwrap();

    // Simulate the pyomq destroy_socket fallback: Rc::try_unwrap fails,
    // so we call signal_close() and drop the handle without awaiting close.
    let s1_clone = s1.clone();
    s1.signal_close();
    drop(s1);
    drop(s1_clone);

    // Must not fail with "inproc name already bound".
    let s2 = Socket::new(SocketType::Pull, Options::default());
    let bound = compio::time::timeout(Duration::from_secs(2), s2.bind(ep)).await;
    bound.expect("bind timed out").expect("rebind failed");
    s2.close().await.unwrap();
}

#[compio::test]
async fn rebind_after_graceful_close() {
    let ep = inproc("rebind-graceful-close");

    let s1 = Socket::new(SocketType::Pull, Options::default());
    s1.bind(ep.clone()).await.unwrap();
    s1.close().await.unwrap();

    let s2 = Socket::new(SocketType::Pull, Options::default());
    let bound = compio::time::timeout(Duration::from_secs(2), s2.bind(ep)).await;
    bound.expect("bind timed out").expect("rebind failed");
    s2.close().await.unwrap();
}

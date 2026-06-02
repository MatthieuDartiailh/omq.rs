//! Regression: binding an inproc name, closing or dropping the socket,
//! then rebinding the same name on a new socket must succeed.

use std::time::Duration;

use omq_compio::{Endpoint, Options, Socket, SocketType};

fn inproc(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[compio::test]
async fn rebind_after_drop() {
    let ep = inproc("rebind-drop");

    let s1 = Socket::new(SocketType::Pull, Options::default());
    s1.bind(ep.clone()).await.unwrap();
    drop(s1);

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

//! Verify that dropping a socket releases its inproc name so a new
//! socket can rebind the same name.

use std::time::Duration;

use omq_tokio::{Endpoint, Options, Socket, SocketType};

fn inproc(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn rebind_after_close() {
    let ep = inproc("tokio-rebind-close");

    let s1 = Socket::new(SocketType::Pull, Options::default());
    s1.bind(ep.clone()).await.unwrap();
    s1.close().await.unwrap();

    let s2 = Socket::new(SocketType::Pull, Options::default());
    let bound = tokio::time::timeout(Duration::from_secs(2), s2.bind(ep)).await;
    bound.expect("bind timed out").expect("rebind failed");
    s2.close().await.unwrap();
}

#[tokio::test]
async fn rebind_after_drop() {
    let ep = inproc("tokio-rebind-drop");

    let s1 = Socket::new(SocketType::Pull, Options::default());
    s1.bind(ep.clone()).await.unwrap();
    drop(s1);

    // Give the actor a moment to run teardown.
    tokio::time::sleep(Duration::from_millis(10)).await;

    let s2 = Socket::new(SocketType::Pull, Options::default());
    let bound = tokio::time::timeout(Duration::from_secs(2), s2.bind(ep)).await;
    bound.expect("bind timed out").expect("rebind failed");
    s2.close().await.unwrap();
}

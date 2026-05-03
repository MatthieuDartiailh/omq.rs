//! Double-close idempotency: calling `close()` on two clones of the same socket
//! must not panic, deadlock, or return a hard error. The second close sees the
//! driver already gone and returns Ok(()).

use std::time::Duration;

use omq_tokio::{Options, Socket, SocketType};

async fn close_twice(socket_type: SocketType) {
    let a = Socket::new(socket_type, Options::default());
    let b = a.clone();

    tokio::time::timeout(Duration::from_secs(2), a.close())
        .await
        .expect("first close timed out")
        .expect("first close returned error");

    tokio::time::timeout(Duration::from_secs(2), b.close())
        .await
        .expect("second close timed out")
        .expect("second close must not error even if driver is already gone");
}

#[tokio::test]
async fn double_close_push() {
    close_twice(SocketType::Push).await;
}

#[tokio::test]
async fn double_close_pull() {
    close_twice(SocketType::Pull).await;
}

#[tokio::test]
async fn double_close_pub() {
    close_twice(SocketType::Pub).await;
}

#[tokio::test]
async fn double_close_sub() {
    close_twice(SocketType::Sub).await;
}

#[tokio::test]
async fn double_close_req() {
    close_twice(SocketType::Req).await;
}

#[tokio::test]
async fn double_close_rep() {
    close_twice(SocketType::Rep).await;
}

#[tokio::test]
async fn double_close_router() {
    close_twice(SocketType::Router).await;
}

#[tokio::test]
async fn double_close_dealer() {
    close_twice(SocketType::Dealer).await;
}

#[tokio::test]
async fn double_close_pair() {
    close_twice(SocketType::Pair).await;
}

#[tokio::test]
async fn double_close_xpub() {
    close_twice(SocketType::XPub).await;
}

#[tokio::test]
async fn double_close_xsub() {
    close_twice(SocketType::XSub).await;
}

#[tokio::test]
async fn triple_clone_close_last_wins() {
    // Three clones: close A, then C, then B. Each must succeed.
    let a = Socket::new(SocketType::Push, Options::default());
    let b = a.clone();
    let c = a.clone();

    a.close().await.unwrap();
    c.close().await.unwrap();
    b.close().await.unwrap();
}

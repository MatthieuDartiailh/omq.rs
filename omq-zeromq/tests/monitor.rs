use std::time::Duration;

use tokio::time::timeout;
use zeromq::{PullSocket, PushSocket, Socket, SocketEvent};

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn listening_event_after_bind() {
    let mut push = PushSocket::new();
    let mut mon = push.monitor();
    let _ep = push.bind("tcp://127.0.0.1:0").await.unwrap();

    let event = timeout(TIMEOUT, mon.recv()).await.unwrap().unwrap();
    assert!(matches!(event, SocketEvent::Listening));
}

#[tokio::test]
async fn connected_accepted_events() {
    let mut push = PushSocket::new();
    let mut push_mon = push.monitor();
    let ep = push.bind("tcp://127.0.0.1:0").await.unwrap();

    // Drain the Listening event
    let _ = timeout(TIMEOUT, push_mon.recv()).await.unwrap().unwrap();

    let mut pull = PullSocket::new();
    pull.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Push (server) should see Accepted
    let event = timeout(TIMEOUT, push_mon.recv()).await.unwrap().unwrap();
    assert!(matches!(event, SocketEvent::Accepted));
}

#[tokio::test]
async fn disconnected_event_on_close() {
    let mut push = PushSocket::new();
    let ep = push.bind("tcp://127.0.0.1:0").await.unwrap();

    let mut pull = PullSocket::new();
    let mut pull_mon = pull.monitor();
    pull.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drain connection events
    while timeout(Duration::from_millis(100), pull_mon.recv())
        .await
        .is_ok()
    {}

    // Close the push socket to trigger disconnect
    push.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Pull should see a disconnected event eventually
    let mut saw_disconnect = false;
    for _ in 0..10 {
        match timeout(Duration::from_millis(200), pull_mon.recv()).await {
            Ok(Some(SocketEvent::Disconnected)) => {
                saw_disconnect = true;
                break;
            }
            Ok(Some(_)) => {}

            _ => break,
        }
    }
    assert!(saw_disconnect);
}

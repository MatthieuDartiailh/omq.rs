//! `ZGuide` 10 — Binary Star: backup server.
//!
//! SUB connects to the primary's heartbeat endpoint. Phase 1: monitor
//! heartbeats with a 300ms timeout. On timeout (primary is dead),
//! transition to phase 2: serve REQ clients with "backup:{body}" replies.
//!
//!     cargo run -p zguide-compio-10-binary-star --bin zg10_backup [heartbeat_ep] [service_ep]

use std::time::Duration;

use omq::{Endpoint, Message, Options, Socket, SocketType};

fn endpoint_or(args: &[String], index: usize, default: &str) -> Endpoint {
    args.get(index).map_or_else(
        || default.parse().unwrap(),
        |s| s.parse().expect("invalid endpoint"),
    )
}

fn msg_str(msg: &Message, idx: usize) -> String {
    String::from_utf8_lossy(&msg.part_bytes(idx).unwrap()).to_string()
}

#[compio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let heartbeat_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-10-heartbeat");
    let service_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-10-backup");

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.connect(heartbeat_ep).await.unwrap();
    sub.subscribe("HB").await.unwrap();

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(service_ep).await.unwrap();

    // Phase 1: monitor heartbeats.
    let timeout = Duration::from_millis(300);
    loop {
        if !matches!(compio::time::timeout(timeout, sub.recv()).await, Ok(Ok(_))) {
            // Timeout or connection closed: primary is gone.
            println!("backup: primary heartbeat lost -- taking over!");
            break;
        }
    }

    // Phase 2: serve requests.
    loop {
        let msg = rep.recv().await.unwrap();
        let body = msg_str(&msg, 0);
        println!("backup: served {body}");
        rep.send(Message::single(format!("backup:{body}")))
            .await
            .unwrap();
    }
}

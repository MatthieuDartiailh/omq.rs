//! `ZGuide` 03 — Ventilator (task producer).
//!
//! PUSH socket binds and sends N tasks followed by sentinel messages
//! so each worker knows when to stop.
//!
//!     cargo run -p zguide-compio-03-pipeline --bin ventilator [vent_ep] [n_tasks] [n_workers]

use std::time::Duration;

use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn endpoint_or(args: &[String], index: usize, default: &str) -> Endpoint {
    args.get(index).map_or_else(|| default.parse().unwrap(), |s| s.parse().expect("invalid endpoint"))
}

#[compio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let vent_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-03-ventilator");
    let n_tasks: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100);
    let n_workers: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(3);

    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(vent_ep.clone()).await.unwrap();

    // Give workers time to connect.
    compio::time::sleep(Duration::from_millis(200)).await;

    for i in 0..n_tasks {
        push.send(Message::single(format!("task-{i}")))
            .await
            .unwrap();
    }

    for _ in 0..n_workers {
        push.send(Message::single("END")).await.unwrap();
    }

    println!("ventilator: sent {n_tasks} tasks + {n_workers} END sentinels on {vent_ep}");
}

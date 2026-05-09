//! `ZGuide` 02 — SUB subscriber.
//!
//! Connects a SUB socket, subscribes to a topic prefix, and prints
//! matching messages.
//!
//!     cargo run -p zguide-compio-02-pub-sub --bin subscriber [endpoint] [topic] [count]
//!
//! If `count` is given, receives that many messages then exits.
//! Otherwise runs indefinitely (Ctrl-C to stop).

use omq::{Endpoint, Message, Options, Socket, SocketType};

fn endpoint_or(args: &[String], index: usize, default: &str) -> Endpoint {
    args.get(index).map_or_else(|| default.parse().unwrap(), |s| s.parse().expect("invalid endpoint"))
}

fn msg_str(msg: &Message, idx: usize) -> String {
    String::from_utf8_lossy(&msg.part_bytes(idx).unwrap()).to_string()
}

#[compio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let ep = endpoint_or(&args, 1, "ipc://@omq-zguide-02-pubsub");
    let topic: String = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "weather.nyc".to_string());
    let count: Option<usize> = args.get(3).and_then(|s| s.parse().ok());

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.connect(ep.clone()).await.unwrap();
    sub.subscribe(topic.clone()).await.unwrap();

    println!("subscriber: connected to {ep}, topic={topic:?}");

    let limit = count.unwrap_or(usize::MAX);
    for i in 0..limit {
        let msg = sub.recv().await.unwrap();
        let body = msg_str(&msg, 0);
        println!("subscriber[{topic}]: [{i}] {body}");
    }

    println!("subscriber: done ({limit} messages)");
}

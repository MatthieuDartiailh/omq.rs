//! ZGuide 01 — REP worker.
//!
//! Connects to the broker's DEALER backend. Receives requests and
//! replies with an echo prefixed by the worker ID.
//!
//!     cargo run -p zguide-tokio-01-req-rep --bin worker [backend] [id]

use omq::{Endpoint, Message, Options, Socket, SocketType};

fn endpoint_or(args: &[String], index: usize, default: &str) -> Endpoint {
    args.get(index)
        .map(|s| s.parse().expect("invalid endpoint"))
        .unwrap_or_else(|| default.parse().unwrap())
}

fn msg_str(msg: &Message, idx: usize) -> String {
    String::from_utf8_lossy(&msg.part_bytes(idx).unwrap()).to_string()
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let backend_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-01-backend");
    let id = args.get(2).map_or("0", |s| s.as_str());

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.connect(backend_ep).await.unwrap();

    println!("worker-{id}: ready");

    loop {
        let msg = rep.recv().await.unwrap();
        let body = msg_str(&msg, 0);
        let reply = format!("worker-{id}:{body}");
        println!("worker-{id}: {body} -> {reply}");
        rep.send(Message::single(reply)).await.unwrap();
    }
}

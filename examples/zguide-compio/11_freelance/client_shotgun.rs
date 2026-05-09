//! `ZGuide` 11 — Freelance Model 2: shotgun.
//!
//! DEALER connects to all endpoints. Sends a request to each (empty
//! delimiter + body for REP compatibility). Takes the first reply.
//!
//!     cargo run -p zguide-compio-11-freelance --bin zg11_client_shotgun [ep1] [ep2] ...

use std::time::Duration;

use omq::{Endpoint, Message, Options, Socket, SocketType};

fn msg_str(msg: &Message, idx: usize) -> String {
    String::from_utf8_lossy(&msg.part_bytes(idx).unwrap()).to_string()
}

#[compio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let endpoints: Vec<Endpoint> = if args.len() > 1 {
        args[1..]
            .iter()
            .map(|s| s.parse().expect("invalid endpoint"))
            .collect()
    } else {
        vec![
            "ipc://@omq-zguide-11-server1".parse().unwrap(),
            "ipc://@omq-zguide-11-server2".parse().unwrap(),
        ]
    };

    let dealer = Socket::new(SocketType::Dealer, Options::default());
    for ep in &endpoints {
        dealer.connect(ep.clone()).await.unwrap();
    }
    compio::time::sleep(Duration::from_millis(50)).await;

    // Send one request per endpoint (empty delimiter + body).
    for _ in &endpoints {
        dealer
            .send(Message::multipart(["", "shotgun-req"]))
            .await
            .unwrap();
    }

    // Take the first reply.
    match compio::time::timeout(Duration::from_secs(1), dealer.recv()).await {
        Ok(Ok(reply)) => {
            // Reply from REP via DEALER: [delimiter, body].
            let n = reply.len();
            let body = msg_str(&reply, n - 1);
            println!("client: first reply = {body}");
        }
        Ok(Err(e)) => {
            eprintln!("client: recv error: {e}");
        }
        Err(_) => {
            eprintln!("client: timeout waiting for reply");
        }
    }

    println!("client: done");
}

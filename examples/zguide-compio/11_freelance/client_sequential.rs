//! `ZGuide` 11 — Freelance Model 1: sequential failover.
//!
//! For each request, try endpoints in order. Create a fresh REQ per
//! attempt, send, recv with 150ms timeout. On timeout: drop socket,
//! try the next endpoint.
//!
//!     cargo run -p zguide-compio-11-freelance --bin zg11_client_sequential [ep1] [ep2] ...

use std::time::Duration;

use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

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
            "ipc://@omq-zguide-11-server3".parse().unwrap(),
        ]
    };

    for i in 0..3 {
        let body = format!("request-{i}");
        let mut served = false;

        for ep in &endpoints {
            let req = Socket::new(SocketType::Req, Options::default());
            req.connect(ep.clone()).await.unwrap();
            compio::time::sleep(Duration::from_millis(20)).await;

            let send_ok = compio::time::timeout(
                Duration::from_millis(150),
                req.send(Message::single(body.clone())),
            )
            .await;
            if send_ok.is_err() {
                println!("client: send timeout on {ep}, trying next");
                continue;
            }
            send_ok.unwrap().unwrap();

            match compio::time::timeout(Duration::from_millis(150), req.recv()).await {
                Ok(Ok(reply)) => {
                    println!("client: {body} -> {}", msg_str(&reply, 0));
                    served = true;
                    break;
                }
                Ok(Err(e)) => {
                    eprintln!("client: recv error on {ep}: {e}");
                }
                Err(_) => {
                    println!("client: timeout on {ep}, trying next");
                }
            }
        }

        if !served {
            eprintln!("client: all endpoints failed for {body}");
        }
    }

    println!("client: done (3 requests)");
}

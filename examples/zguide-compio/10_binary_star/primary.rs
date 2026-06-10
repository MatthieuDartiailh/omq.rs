//! `ZGuide` 10 — Binary Star: primary server.
//!
//! REP binds a service endpoint, PUB binds a heartbeat endpoint.
//! Two tasks: one sends "HB" heartbeats every 50ms, the other
//! serves REQ clients with "primary:{body}" replies.
//!
//!     cargo run -p zguide-compio-10-binary-star --bin zg10_primary [service_ep] [heartbeat_ep]

use std::time::Duration;

use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

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
    let service_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-10-primary");
    let heartbeat_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-10-heartbeat");

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(service_ep).await.unwrap();

    let pub_socket = Socket::new(SocketType::Pub, Options::default());
    pub_socket.bind(heartbeat_ep).await.unwrap();

    // Heartbeat task: send "HB" every 50ms.
    let pub_c = pub_socket.clone();
    compio::runtime::spawn(async move {
        loop {
            pub_c.send(Message::single("HB")).await.unwrap();
            compio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .detach();

    // Server task: recv on REP, reply with "primary:{body}".
    loop {
        let msg = rep.recv().await.unwrap();
        let body = msg_str(&msg, 0);
        println!("primary: served {body}");
        rep.send(Message::single(format!("primary:{body}")))
            .await
            .unwrap();
    }
}

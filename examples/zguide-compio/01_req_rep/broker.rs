//! `ZGuide` 01 — ROUTER/DEALER broker.
//!
//! Forwards messages between a ROUTER frontend (for clients) and a
//! DEALER backend (for workers). Load-balances requests across workers.
//!
//! compio is single-threaded, so the broker uses a sequential loop:
//! recv from frontend, send to backend, recv from backend, send to
//! frontend.
//!
//!     cargo run -p zguide-compio-01-req-rep --bin broker [frontend] [backend]

use omq_compio::{Endpoint, Options, Socket, SocketType};

fn endpoint_or(args: &[String], index: usize, default: &str) -> Endpoint {
    args.get(index).map_or_else(
        || default.parse().unwrap(),
        |s| s.parse().expect("invalid endpoint"),
    )
}

#[compio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let frontend_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-01-frontend");
    let backend_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-01-backend");

    let frontend = Socket::new(SocketType::Router, Options::default());
    frontend.bind(frontend_ep.clone()).await.unwrap();

    let backend = Socket::new(SocketType::Dealer, Options::default());
    backend.bind(backend_ep.clone()).await.unwrap();

    println!("broker: frontend={frontend_ep} backend={backend_ep}");

    loop {
        let request = frontend.recv().await.unwrap();
        backend.send(request).await.unwrap();

        let reply = backend.recv().await.unwrap();
        frontend.send(reply).await.unwrap();
    }
}

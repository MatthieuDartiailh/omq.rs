//! ZGuide 01 — ROUTER/DEALER broker.
//!
//! Forwards messages between a ROUTER frontend (for clients) and a
//! DEALER backend (for workers). Load-balances requests across workers.
//!
//!     cargo run -p zguide-tokio-01-req-rep --bin broker [frontend] [backend]

use omq::{Endpoint, Options, Socket, SocketType};

fn endpoint_or(args: &[String], index: usize, default: &str) -> Endpoint {
    args.get(index)
        .map(|s| s.parse().expect("invalid endpoint"))
        .unwrap_or_else(|| default.parse().unwrap())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let frontend_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-01-frontend");
    let backend_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-01-backend");

    let frontend = Socket::new(SocketType::Router, Options::default());
    frontend.bind(frontend_ep.clone()).await.unwrap();

    let backend = Socket::new(SocketType::Dealer, Options::default());
    backend.bind(backend_ep.clone()).await.unwrap();

    println!("broker: frontend={frontend_ep} backend={backend_ep}");

    let fe = frontend.clone();
    let be = backend.clone();
    let fwd = tokio::spawn(async move {
        loop {
            let msg = fe.recv().await.unwrap();
            be.send(msg).await.unwrap();
        }
    });

    let fe = frontend;
    let be = backend;
    let ret = tokio::spawn(async move {
        loop {
            let msg = be.recv().await.unwrap();
            fe.send(msg).await.unwrap();
        }
    });

    let _ = tokio::join!(fwd, ret);
}

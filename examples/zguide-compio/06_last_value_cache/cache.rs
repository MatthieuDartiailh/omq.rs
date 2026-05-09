//! `ZGuide` 06 — Last Value Cache (caching proxy).
//!
//! Sits between publishers and subscribers. Caches the latest value per
//! topic and serves snapshots to late joiners via REQ/REP.
//!
//!     cargo run -p zguide-compio-06-last-value-cache --bin cache \
//!         [pub_ep] [sub_ep] [snapshot_ep]

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

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
    let pub_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-06-publisher");
    let sub_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-06-subscriber");
    let snapshot_ep = endpoint_or(&args, 3, "ipc://@omq-zguide-06-snapshot");

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(pub_ep.clone()).await.unwrap();

    let pub_ = Socket::new(SocketType::Pub, Options::default());
    pub_.bind(sub_ep.clone()).await.unwrap();

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(snapshot_ep.clone()).await.unwrap();

    println!("cache: PULL bound to {pub_ep}");
    println!("cache: PUB  bound to {sub_ep}");
    println!("cache: REP  bound to {snapshot_ep}");

    // Single-threaded: use Rc<RefCell<>> instead of Arc<Mutex<>>.
    let cache: Rc<RefCell<HashMap<String, String>>> = Rc::new(RefCell::new(HashMap::new()));

    // Forward task: recv from PULL, cache, forward via PUB.
    let cache_fwd = Rc::clone(&cache);
    let pull_c = pull.clone();
    let pub_c = pub_.clone();
    let forward = compio::runtime::spawn(async move {
        loop {
            let msg = pull_c.recv().await.unwrap();
            let body = msg_str(&msg, 0);
            if let Some((topic, value)) = body.split_once(' ') {
                cache_fwd
                    .borrow_mut()
                    .insert(topic.to_owned(), value.to_owned());
                println!("cache: cached {topic}={value}");
            }
            pub_c.send(Message::single(body)).await.unwrap();
        }
    });

    // Snapshot task: serve cached state on REQ/REP.
    let cache_snap = Rc::clone(&cache);
    let rep_c = rep.clone();
    let snapshot = compio::runtime::spawn(async move {
        loop {
            let msg = rep_c.recv().await.unwrap();
            let body = msg_str(&msg, 0);
            if body == "SNAPSHOT" {
                let payload = {
                    let locked = cache_snap.borrow();
                    let p: String = locked
                        .iter()
                        .map(|(k, v)| format!("{k} {v}"))
                        .collect::<Vec<_>>()
                        .join("\n");
                    println!("cache: snapshot served ({} entries)", locked.len());
                    p
                };
                rep_c.send(Message::single(payload)).await.unwrap();
            }
        }
    });

    // The snapshot task runs concurrently via spawn. Await the forward
    // task (it runs forever); the snapshot task is kept alive by its handle.
    let _snapshot = snapshot;
    forward.await.unwrap();
}

//! `ZGuide` 07 — Clone (server).
//!
//! Maintains a key-value store, publishes updates via PUB, and serves
//! snapshots via REQ/REP. Each update carries a sequence number so
//! clients can merge snapshots with buffered live updates.
//!
//!     cargo run -p zguide-compio-07-clone --bin server \
//!         [updates_ep] [snapshot_ep]

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
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

struct Entry {
    value: String,
    seq: u64,
}

#[compio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let updates_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-07-updates");
    let snapshot_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-07-snapshot");

    let pub_ = Socket::new(SocketType::Pub, Options::default());
    pub_.bind(updates_ep.clone()).await.unwrap();

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(snapshot_ep.clone()).await.unwrap();

    println!("server: PUB bound to {updates_ep}");
    println!("server: REP bound to {snapshot_ep}");

    // Single-threaded: use Rc<RefCell<>> instead of Arc<Mutex<>>.
    let store: Rc<RefCell<HashMap<String, Entry>>> = Rc::new(RefCell::new(HashMap::new()));
    let mut seq: u64 = 0;

    // Snapshot task: serve snapshot requests.
    let store_snap = Rc::clone(&store);
    let rep_c = rep.clone();
    let _snapshot_task = compio::runtime::spawn(async move {
        loop {
            let msg = rep_c.recv().await.unwrap();
            let body = msg_str(&msg, 0);
            if body == "SNAPSHOT" {
                let payload = {
                    let locked = store_snap.borrow();
                    let p: String = locked
                        .iter()
                        .map(|(k, e)| format!("{}|{k}|{}", e.seq, e.value))
                        .collect::<Vec<_>>()
                        .join("\n");
                    println!("server: snapshot served ({} entries)", locked.len());
                    p
                };
                rep_c.send(Message::single(payload)).await.unwrap();
            }
        }
    });

    // Give subscribers time to connect.
    compio::time::sleep(Duration::from_millis(200)).await;

    // Publish initial updates.
    for i in 0..5 {
        seq += 1;
        let cur = seq;

        let key = format!("key-{i}");
        let val = format!("val-{i}");
        store.borrow_mut().insert(
            key.clone(),
            Entry {
                value: val.clone(),
                seq: cur,
            },
        );

        let msg = format!("{cur}|{key}|{val}");
        pub_.send(Message::single(msg)).await.unwrap();
        println!("server: published {key}={val} (seq={cur})");
        compio::time::sleep(Duration::from_millis(20)).await;
    }

    // Pause so client can request snapshot.
    compio::time::sleep(Duration::from_millis(300)).await;

    // Publish post-snapshot updates.
    for i in 0..3 {
        seq += 1;
        let cur = seq;

        let key = format!("key-{i}");
        let val = format!("updated-{i}");
        store.borrow_mut().insert(
            key.clone(),
            Entry {
                value: val.clone(),
                seq: cur,
            },
        );

        let msg = format!("{cur}|{key}|{val}");
        pub_.send(Message::single(msg)).await.unwrap();
        println!("server: published {key}={val} (seq={cur})");
        compio::time::sleep(Duration::from_millis(20)).await;
    }

    // Keep serving snapshots briefly, then exit.
    compio::time::sleep(Duration::from_secs(3)).await;
    println!("server: done");
}

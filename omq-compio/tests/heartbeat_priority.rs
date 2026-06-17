//! Regression: heartbeat must not kill a connection when the peer's
//! PING is waiting unread on the wire. Before the fix the heartbeat
//! arm in `select_biased!` had higher priority than the stream read
//! arm, so under sustained outbound load the driver checked the
//! timeout before processing the incoming PING and killed the
//! connection.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use bytes::Bytes;
use omq_compio::{Message, OnMute, Options, Socket, SocketType, build_default_runtime};

fn block_on_and_drain<F: std::future::Future>(rt: &compio::runtime::Runtime, fut: F) -> F::Output {
    let out = rt.block_on(fut);
    rt.enter(|| while rt.run() {});
    out
}

const MSG_SIZE: usize = 131_072;

#[test]
#[expect(clippy::too_many_lines)]
fn heartbeat_does_not_kill_busy_connection() {
    let ep: omq_compio::Endpoint =
        omq_compio::Endpoint::Ipc(omq_compio::endpoint::IpcPath::Abstract(format!(
            "omq-hb-prio-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        )));

    let recv_count = Arc::new(AtomicUsize::new(0));
    let sent_count = Arc::new(AtomicUsize::new(0));
    let counting = Arc::new(AtomicBool::new(false));
    let sending_done = Arc::new(AtomicBool::new(false));
    let sub_ready = Arc::new(AtomicBool::new(false));
    let bind_barrier = Arc::new(Barrier::new(2));

    let sub_thread = {
        let ep = ep.clone();
        let recv_count = recv_count.clone();
        let sent_count = sent_count.clone();
        let counting = counting.clone();
        let sending_done = sending_done.clone();
        let sub_ready = sub_ready.clone();
        let bind_barrier = bind_barrier.clone();
        std::thread::spawn(move || {
            let rt = build_default_runtime().expect("sub runtime");
            block_on_and_drain(&rt, async move {
                bind_barrier.wait();
                let opts = Options::default().heartbeat_interval(Duration::from_secs(10));
                let s = Socket::new(SocketType::Sub, opts);
                s.connect(ep).await.expect("connect SUB");
                s.subscribe(Bytes::new()).await.expect("subscribe");

                loop {
                    match compio::time::timeout(Duration::from_secs(2), s.recv()).await {
                        Ok(Ok(_)) => {
                            sub_ready.store(true, Ordering::Relaxed);
                            if counting.load(Ordering::Acquire) {
                                recv_count.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        _ => {
                            if sending_done.load(Ordering::Acquire) {
                                break;
                            }
                        }
                    }
                }

                let expected = sent_count.load(Ordering::Acquire);
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while std::time::Instant::now() < deadline {
                    match compio::time::timeout(Duration::from_secs(1), s.recv()).await {
                        Ok(Ok(_)) => {
                            recv_count.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {
                            if recv_count.load(Ordering::Relaxed) >= expected {
                                break;
                            }
                        }
                    }
                }
            });
        })
    };

    let pub_thread = {
        let sending_done = sending_done.clone();
        let sent_count = sent_count.clone();
        let counting = counting.clone();
        let sub_ready = sub_ready.clone();
        std::thread::spawn(move || {
            let rt = build_default_runtime().expect("pub runtime");
            block_on_and_drain(&rt, async move {
                let opts = Options::default()
                    .heartbeat_interval(Duration::from_secs(10))
                    .on_mute(OnMute::Block);
                let pub_ = Socket::new(SocketType::Pub, opts);
                pub_.bind(ep).await.expect("bind PUB");
                bind_barrier.wait();

                let payload = Bytes::from(vec![0xABu8; MSG_SIZE]);

                loop {
                    let _ = pub_.send(Message::single(payload.clone())).await;
                    if sub_ready.load(Ordering::Relaxed) {
                        break;
                    }
                    compio::time::sleep(Duration::from_micros(50)).await;
                }
                compio::time::sleep(Duration::from_millis(100)).await;
                counting.store(true, Ordering::Release);

                let start = std::time::Instant::now();
                let mut sent: usize = 0;
                while start.elapsed() < Duration::from_secs(15) {
                    pub_.send(Message::single(payload.clone())).await.unwrap();
                    sent += 1;
                }

                sent_count.store(sent, Ordering::Release);
                sending_done.store(true, Ordering::Release);

                compio::time::sleep(Duration::from_secs(7)).await;
                pub_.close().await.unwrap();
            });
        })
    };

    pub_thread.join().expect("pub thread panicked");
    sub_thread.join().expect("sub thread panicked");

    let s = sent_count.load(Ordering::Relaxed);
    let r = recv_count.load(Ordering::Relaxed);
    eprintln!("[heartbeat_priority] sent {s}, recvd {r}");
    assert_eq!(r, s, "message loss: heartbeat killed the connection");
}

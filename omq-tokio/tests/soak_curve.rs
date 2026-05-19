#![cfg(all(feature = "soak", feature = "curve"))]

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use omq_tokio::{CurveKeypair, Message, Options, Socket, SocketType};

#[test]
fn soak_curve_sustained() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let sent = Arc::new(AtomicU64::new(0));
    let recvd = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let pull = Socket::new(SocketType::Pull, Options::default().curve_server(server_kp));
        let ep = pull.bind(soak_common::tcp_ep(0)).await.unwrap();

        let push = Socket::new(
            SocketType::Push,
            Options::default()
                .curve_client(client_kp, server_pub)
                .linger(Duration::from_secs(5)),
        );
        push.connect(ep).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let send_sent = sent.clone();
        let send_stop = stop.clone();
        let push_clone = push.clone();
        let send_task = tokio::spawn(async move {
            let mut idx: u64 = 0;
            while !send_stop.load(Ordering::Relaxed) {
                if let Ok(Ok(())) = tokio::time::timeout(
                    Duration::from_secs(2),
                    push_clone.send(Message::single(format!("c{idx}"))),
                )
                .await
                {
                    send_sent.fetch_add(1, Ordering::Relaxed);
                }
                idx += 1;
            }
        });

        let recv_recvd = recvd.clone();
        let recv_stop = stop.clone();
        let pull_clone = pull.clone();
        let recv_task = tokio::spawn(async move {
            while !recv_stop.load(Ordering::Relaxed) {
                if let Ok(Ok(_)) =
                    tokio::time::timeout(Duration::from_secs(2), pull_clone.recv()).await
                {
                    recv_recvd.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        let timer_stop = stop.clone();
        let timer_sent = sent.clone();
        let timer_recvd = recvd.clone();
        let start = Instant::now();
        let mut last_log = start;

        while start.elapsed() < duration {
            tokio::time::sleep(Duration::from_secs(1)).await;

            if last_log.elapsed() >= Duration::from_secs(30) {
                let s = timer_sent.load(Ordering::Relaxed);
                let r = timer_recvd.load(Ordering::Relaxed);
                eprintln!(
                    "[curve] {:.0}s, sent {s}, recvd {r}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }
        timer_stop.store(true, Ordering::Relaxed);

        let _ = send_task.await;
        let _ = recv_task.await;

        let s = sent.load(Ordering::Relaxed);
        let r = recvd.load(Ordering::Relaxed);
        eprintln!(
            "[curve] done: sent {s}, recvd {r} in {:.1}s",
            duration.as_secs_f64(),
        );

        push.close().await.unwrap();
        pull.close().await.unwrap();
    });

    let report = monitor.stop();
    report.assert_no_leak("curve");
}

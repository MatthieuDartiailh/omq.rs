#![cfg(feature = "soak")]

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use omq_tokio::{Message, Options, Socket, SocketType};

const MSG_SIZE: usize = 1024 * 1024;
const CANARY_MAGIC: u64 = 0xDEAD_BEEF_CAFE_F00D;

fn build_payload(seq: u64) -> Vec<u8> {
    let mut buf = vec![0u8; MSG_SIZE];
    buf[..8].copy_from_slice(&CANARY_MAGIC.to_le_bytes());
    buf[8..16].copy_from_slice(&seq.to_le_bytes());
    for (i, slot) in buf.iter_mut().enumerate().skip(16) {
        *slot = (i & 0xFF) as u8;
    }
    buf
}

fn validate_payload(data: &[u8], expected_min_seq: u64) -> u64 {
    assert_eq!(data.len(), MSG_SIZE, "payload size mismatch");

    let magic = u64::from_le_bytes(data[..8].try_into().unwrap());
    let seq = u64::from_le_bytes(data[8..16].try_into().unwrap());

    assert_eq!(
        magic,
        CANARY_MAGIC,
        "CANARY CORRUPT: magic=0x{magic:016x}, seq={seq}, first 32 bytes: {:02x?}\n\
         Receiver lost ZMTP frame sync: payload bytes parsed as frame headers.",
        &data[..32]
    );

    assert!(
        seq >= expected_min_seq,
        "sequence went backwards: got {seq}, expected >= {expected_min_seq} \
         (message reordering or duplication)"
    );

    for (i, &byte) in data.iter().enumerate().skip(16) {
        assert_eq!(
            byte,
            (i & 0xFF) as u8,
            "payload byte corruption at offset {i}: seq={seq}",
        );
    }
    seq
}

#[test]
fn soak_large_message_throughput() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let sent = Arc::new(AtomicU64::new(0));
    let recvd = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let pull = Socket::new(SocketType::Pull, soak_common::soak_options().recv_hwm(4));
        let ep = pull.bind(soak_common::tcp_ep(0)).await.unwrap();

        let push = Socket::new(SocketType::Push, soak_common::soak_options().send_hwm(4));
        push.connect(ep).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let send_sent = sent.clone();
        let send_stop = stop.clone();
        let push_clone = push.clone();
        let send_task = tokio::spawn(async move {
            let mut seq = 0u64;
            while !send_stop.load(Ordering::Relaxed) {
                let payload = build_payload(seq);
                if let Ok(Ok(())) = tokio::time::timeout(
                    Duration::from_secs(2),
                    push_clone.send(Message::single(payload)),
                )
                .await
                {
                    seq += 1;
                    send_sent.store(seq, Ordering::Relaxed);
                }
            }
        });

        let recv_recvd = recvd.clone();
        let recv_stop = stop.clone();
        let pull_clone = pull.clone();
        let recv_task = tokio::spawn(async move {
            let mut last_seq = 0u64;
            while !recv_stop.load(Ordering::Relaxed) {
                if let Ok(Ok(m)) =
                    tokio::time::timeout(Duration::from_secs(2), pull_clone.recv()).await
                {
                    let data = m.part_bytes(0).unwrap();
                    last_seq = validate_payload(&data, last_seq);
                    recv_recvd.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        let start = Instant::now();
        let mut last_log = start;
        let mut tracker = soak_common::ThroughputTracker::new(Duration::from_secs(10));

        while start.elapsed() < duration {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let s = sent.load(Ordering::Relaxed);
            let r = recvd.load(Ordering::Relaxed);
            tracker.record(r);

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[large_throughput] {:.0}s, sent {s}, recvd {r}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }
        stop.store(true, Ordering::Relaxed);
        tracker.assert_stable("large_throughput");

        let _ = send_task.await;
        let _ = recv_task.await;

        let s = sent.load(Ordering::Relaxed);
        let r = recvd.load(Ordering::Relaxed);
        eprintln!(
            "[large_throughput] done: sent {s}, recvd {r} in {:.1}s ({:.1} MiB/s)",
            duration.as_secs_f64(),
            r as f64 * MSG_SIZE as f64 / duration.as_secs_f64() / 1_048_576.0,
        );

        push.close().await.unwrap();
        pull.close().await.unwrap();
    });

    let report = monitor.stop();
    report.assert_no_leak("large_throughput");
}

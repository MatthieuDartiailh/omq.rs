#![cfg(feature = "soak")]

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use omq_tokio::{Message, Socket, SocketType};

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

struct PayloadStats {
    max_seq: u64,
    count: u64,
    reorders: u64,
    max_reorder_distance: u64,
    dropped: u64,
}

impl PayloadStats {
    fn new() -> Self {
        Self {
            max_seq: 0,
            count: 0,
            reorders: 0,
            max_reorder_distance: 0,
            dropped: 0,
        }
    }

    fn validate(&mut self, data: &[u8]) {
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

        for (i, &byte) in data.iter().enumerate().skip(16) {
            assert_eq!(
                byte,
                (i & 0xFF) as u8,
                "payload byte corruption at offset {i}: seq={seq}",
            );
        }

        // Small reordering is expected during connection churn: the
        // wire slot bypass and driver inbox are two independent paths,
        // and a handshake transition can let a later message reach the
        // wire first.
        if seq < self.max_seq {
            let distance = self.max_seq - seq;
            self.reorders += 1;
            self.max_reorder_distance = self.max_reorder_distance.max(distance);
        }
        self.max_seq = self.max_seq.max(seq);
        self.count += 1;
    }

    fn finalize(&mut self, total_sent: u64) {
        self.dropped = total_sent.saturating_sub(self.count);
    }
}

#[test]
#[expect(clippy::too_many_lines)]
fn soak_large_message_throughput() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let sent = Arc::new(AtomicU64::new(0));
    let recvd = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let rt = soak_common::tokio_runtime();
    let stats = Arc::new(std::sync::Mutex::new(PayloadStats::new()));

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
        let recv_stats = stats.clone();
        let recv_task = tokio::spawn(async move {
            while !recv_stop.load(Ordering::Relaxed) {
                if let Ok(Ok(m)) =
                    tokio::time::timeout(Duration::from_secs(2), pull_clone.recv()).await
                {
                    let data = m.part_bytes(0).unwrap();
                    recv_stats.lock().unwrap().validate(&data);
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

    let mut st = stats.lock().unwrap();
    let total_sent = sent.load(Ordering::Relaxed);
    st.finalize(total_sent);
    eprintln!(
        "[large_throughput] reorders: {}, max distance: {}, dropped: {}/{}",
        st.reorders, st.max_reorder_distance, st.dropped, total_sent,
    );
    assert!(
        st.max_reorder_distance <= 16,
        "reorder distance {} exceeds tolerance of 16",
        st.max_reorder_distance,
    );
    let drop_pct = if total_sent > 0 {
        st.dropped as f64 / total_sent as f64 * 100.0
    } else {
        0.0
    };
    assert!(
        drop_pct < 5.0,
        "dropped {:.1}% of messages ({}/{})",
        drop_pct,
        st.dropped,
        total_sent,
    );
}

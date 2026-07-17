//! 1 PUSH → 8 PULL over TCP, 128B messages. Matches the zmq.rs "OMQ-shaped" topology.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Context, Endpoint, Message, Options, Socket, SocketType};

const PEERS: usize = 8;
const SIZE: usize = 128;
const WARMUP_MSGS: usize = 50_000;
const ROUNDS: usize = 5;
const ROUND_MSGS: usize = 500_000;

fn main() {
    let ctx = Context::new();
    ctx.block_on(async move {
        let port = {
            let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
            l.local_addr().unwrap().port()
        };
        let ep = Endpoint::Tcp {
            host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
            port,
        };

        let push = Socket::new(SocketType::Push, Options::default());
        push.bind(ep.clone()).await.unwrap();

        let pulls: Vec<Arc<Socket>> = (0..PEERS)
            .map(|_| {
                let s = Socket::new(SocketType::Pull, Options::default());
                Arc::new(s)
            })
            .collect();

        for p in &pulls {
            p.connect(ep.clone()).await.unwrap();
        }

        // Wait for all connections to complete ZMTP handshake.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let conns = push.connections().await.unwrap_or_default();
            if conns.iter().filter(|c| c.peer_info.is_some()).count() == PEERS {
                break;
            }
            assert!(Instant::now() < deadline, "peers never connected");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let payload = Bytes::from(vec![0u8; SIZE]);

        // Spawn one recv task per PULL.
        let recv_handles: Vec<_> = pulls
            .iter()
            .map(|p| {
                let p = p.clone();
                tokio::spawn(async move { while p.recv().await.is_ok() {} })
            })
            .collect();

        let push = Arc::new(push);

        // Warmup.
        for _ in 0..WARMUP_MSGS {
            push.send(Message::single(payload.clone())).await.unwrap();
        }
        // Give receivers a moment to drain.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Timed rounds.
        let mut best_mbps = 0.0f64;
        for _ in 0..ROUNDS {
            let n = ROUND_MSGS;
            let t = Instant::now();
            for _ in 0..n {
                push.send(Message::single(payload.clone())).await.unwrap();
            }
            let elapsed = t.elapsed();
            let mbps = (n * SIZE) as f64 / elapsed.as_secs_f64() / 1_000_000.0;
            let msgs_s = n as f64 / elapsed.as_secs_f64();
            println!(
                "  {SIZE}B  {mbps:.1} MB/s  {msgs_s:.0} msg/s  ({:.3}s, n={n})",
                elapsed.as_secs_f64()
            );
            if mbps > best_mbps {
                best_mbps = mbps;
            }
        }
        println!("best: {best_mbps:.1} MB/s");

        // Cleanup.
        for h in recv_handles {
            h.abort();
        }
    });
}

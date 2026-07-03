//! Proxy benchmark client: connects PUSH to a proxy frontend and PULL
//! to its backend, runs a warmup exchange, then measures throughput.
//!
//! Usage: `bench_proxy_client_compio` `fe_port` `be_port` `msg_size` `duration_secs`
//!
//! Output (stdout): `count` `elapsed_secs` `msg_size`

use std::num::NonZero;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use omq_compio::endpoint::Host;
use omq_compio::runtime::ProactorBuilderExt as _;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};
use std::net::Ipv4Addr;

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn make_rt(size: usize) -> compio::runtime::Runtime {
    let buf_len = (size + 64).next_power_of_two().max(64 * 1024);
    let mut proactor = compio::driver::ProactorBuilder::new();
    proactor.with_omq_buffer_pool_sized(NonZero::new(64).unwrap(), buf_len);
    compio::runtime::RuntimeBuilder::new()
        .with_proactor(proactor)
        .build()
        .expect("compio runtime")
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let fe_port: u16 = args[1].parse().expect("fe_port");
    let be_port: u16 = args[2].parse().expect("be_port");
    let size: usize = args[3].parse().expect("msg_size");
    let duration_secs: f64 = args[4].parse().expect("duration_secs");
    let duration = Duration::from_secs_f64(duration_secs);

    let stop = Arc::new(AtomicBool::new(false));

    let stop2 = stop.clone();
    let send_handle = std::thread::spawn(move || {
        make_rt(size).block_on(async move {
            let push = Socket::new(SocketType::Push, Options::default());
            push.connect(tcp_ep(fe_port)).await.expect("push connect");
            let payload = if size <= omq_proto::message::MAX_INLINE_MESSAGE {
                Message::from_slice(&vec![b'x'; size])
            } else {
                Message::single(vec![b'x'; size])
            };
            while !stop2.load(Ordering::Relaxed) {
                if push.send(payload.clone()).await.is_err() {
                    break;
                }
            }
        });
    });

    make_rt(size).block_on(async move {
        let pull = Socket::new(SocketType::Pull, Options::default());
        pull.connect(tcp_ep(be_port)).await.expect("pull connect");

        compio::time::sleep(Duration::from_millis(500)).await;

        let t0 = Instant::now();
        let deadline = t0 + duration;
        let mut count: u64 = 0;
        loop {
            pull.recv().await.unwrap();
            count += 1;
            while pull.try_recv().is_ok() {
                count += 1;
            }
            if Instant::now() >= deadline {
                break;
            }
        }
        let elapsed = t0.elapsed().as_secs_f64();
        println!("{count} {elapsed:.6} {size}");

        #[expect(clippy::cast_precision_loss)]
        let rate = count as f64 / elapsed;
        eprintln!("{count} msgs in {elapsed:.3}s = {rate:.0} msg/s");

        stop.store(true, Ordering::Relaxed);
    });

    let _ = send_handle.join();
}

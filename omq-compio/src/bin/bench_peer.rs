//! Two-process throughput and latency peer for omq-compio.
//!
//! Usage:
//!   `bench_peer` push \<endpoint\> \<`msg_size_bytes`\>
//!   `bench_peer` pull \<endpoint\> \<`msg_size_bytes`\> \<`duration_secs`\>
//!   `bench_peer` rep  \<endpoint\> \<`msg_size_bytes`\>
//!   `bench_peer` req  \<endpoint\> \<`msg_size_bytes`\> \<iterations\> \<warmup\>
//!
//! Endpoint: a port number (`tcp://127.0.0.1:<port>`) or a path (`/tmp/foo.sock`).
//!
//! Push: binds, sends \<`msg_size`\> byte messages forever.
//! Pull: connects, warms up for 500 ms, then counts messages for \<duration\>
//!       seconds and prints one line to stdout:
//!         \<count\> \<`elapsed_secs`\> \<`msg_size`\>
//! Rep: binds a REP socket, echoes every received message back forever.
//! Req: connects a REQ socket, runs \<warmup\> warm-up round-trips, then
//!      measures \<iterations\> round-trips and prints one line to stdout:
//!        \<`p50_us`\> \<`p99_us`\> \<`p999_us`\> \<`max_us`\> \<iterations\>

use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType, build_default_runtime};
use std::net::Ipv4Addr;

fn parse_ep(s: &str) -> Endpoint {
    if let Ok(port) = s.parse::<u16>() {
        Endpoint::Tcp {
            host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
            port,
        }
    } else {
        s.parse()
            .expect("valid endpoint (port number or ipc:// path)")
    }
}

extern "C" fn exit_on_signal(_sig: libc::c_int) {
    unsafe { libc::_exit(0) };
}

fn main() {
    unsafe {
        libc::signal(
            libc::SIGTERM,
            exit_on_signal as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGINT,
            exit_on_signal as *const () as libc::sighandler_t,
        );
    }
    let rt = build_default_runtime().expect("compio runtime");
    rt.block_on(async {
        let args: Vec<String> = std::env::args().collect();
        match args.get(1).map(String::as_str) {
            Some("push") => {
                let ep = parse_ep(&args[2]);
                let size: usize = args[3].parse().expect("msg_size");
                run_push(ep, size).await;
            }
            Some("pull") => {
                let ep = parse_ep(&args[2]);
                let size: usize = args[3].parse().expect("msg_size");
                let duration: f64 = args[4].parse().expect("duration_secs");
                run_pull(ep, size, Duration::from_secs_f64(duration)).await;
            }
            Some("inproc") => {
                let name = args[2].clone();
                let size: usize = args[3].parse().expect("msg_size");
                let duration: f64 = args[4].parse().expect("duration_secs");
                run_inproc(name, size, Duration::from_secs_f64(duration)).await;
            }
            Some("inproc-st") => {
                let name = args[2].clone();
                let size: usize = args[3].parse().expect("msg_size");
                let duration: f64 = args[4].parse().expect("duration_secs");
                run_inproc_same_thread(name, size, Duration::from_secs_f64(duration)).await;
            }
            Some("rep") => {
                let ep = parse_ep(&args[2]);
                let size: usize = args[3].parse().expect("msg_size");
                run_rep(ep, size).await;
            }
            Some("req") => {
                let ep = parse_ep(&args[2]);
                let size: usize = args[3].parse().expect("msg_size");
                let iterations: usize = args[4].parse().expect("iterations");
                let warmup: usize = args[5].parse().expect("warmup");
                run_req(ep, size, iterations, warmup).await;
            }
            _ => {
                eprintln!("usage: bench_peer push <endpoint> <size>");
                eprintln!("       bench_peer pull <endpoint> <size> <duration_secs>");
                eprintln!("       bench_peer inproc <name> <size> <duration_secs>");
                eprintln!("       bench_peer inproc-st <name> <size> <duration_secs>");
                eprintln!("       bench_peer rep <endpoint> <size>");
                eprintln!("       bench_peer req <endpoint> <size> <iterations> <warmup>");
                std::process::exit(1);
            }
        }
    });
}

async fn run_push(ep: Endpoint, size: usize) {
    let push = Socket::new(SocketType::Push, bench_options(size));
    push.bind(ep).await.expect("push bind");
    let payload = Bytes::from(vec![b'x'; size]);
    loop {
        push.send(Message::single(payload.clone())).await.unwrap();
    }
}

fn bench_options(msg_size: usize) -> Options {
    let mut o = Options::default();
    if std::env::var_os("OMQ_NO_LARGE_MSG").is_some() {
        o = o.disable_large_message_path();
    }
    if msg_size >= 2 * 1024 * 1024 {
        let buf = msg_size * 2;
        o = o.recv_buffer_size(buf).send_buffer_size(buf);
    }
    o
}

async fn run_inproc(name: String, size: usize, duration: Duration) {
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    };

    let ep = Endpoint::Inproc { name };
    let stop = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(Barrier::new(2));

    let push_ep = ep.clone();
    let push_stop = stop.clone();
    let push_ready = ready.clone();
    std::thread::spawn(move || {
        let rt = build_default_runtime().expect("push runtime");
        rt.block_on(async move {
            let push = Socket::new(SocketType::Push, bench_options(size));
            push.bind(push_ep).await.expect("push bind");
            push_ready.wait();
            let payload = Bytes::from(vec![b'x'; size]);
            while !push_stop.load(Ordering::Relaxed) {
                if push.send(Message::single(payload.clone())).await.is_err() {
                    break;
                }
            }
        });
    });

    ready.wait();
    let pull = Socket::new(SocketType::Pull, bench_options(size));
    pull.connect(ep).await.expect("pull connect");
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
    stop.store(true, Ordering::Relaxed);
    println!("{count} {elapsed:.6} {size}");
}

async fn run_pull(ep: Endpoint, size: usize, duration: Duration) {
    let pull = Socket::new(SocketType::Pull, bench_options(size));
    pull.connect(ep).await.expect("pull connect");

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
}

async fn run_rep(ep: Endpoint, size: usize) {
    let rep = Socket::new(SocketType::Rep, bench_options(size));
    rep.bind(ep).await.expect("rep bind");
    loop {
        let msg = rep.recv().await.unwrap();
        rep.send(msg).await.unwrap();
    }
}

async fn run_req(ep: Endpoint, size: usize, iterations: usize, warmup: usize) {
    let req = Socket::new(SocketType::Req, bench_options(size));
    req.connect(ep).await.expect("req connect");
    compio::time::sleep(Duration::from_millis(200)).await;

    let payload = Bytes::from(vec![b'x'; size]);

    for _ in 0..warmup {
        req.send(Message::single(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
    }

    let mut rtts = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t0 = Instant::now();
        req.send(Message::single(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
        rtts.push(t0.elapsed().as_nanos() as u64);
    }

    rtts.sort_unstable();
    let p50 = percentile(&rtts, 50.0);
    let p99 = percentile(&rtts, 99.0);
    let p999 = percentile(&rtts, 99.9);
    let max = percentile(&rtts, 100.0);
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations}");
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx] as f64 / 1_000.0
}

async fn run_inproc_same_thread(name: String, size: usize, duration: Duration) {
    let ep = Endpoint::Inproc { name };
    let push = Socket::new(SocketType::Push, bench_options(size));
    push.bind(ep.clone()).await.expect("push bind");
    let pull = Socket::new(SocketType::Pull, bench_options(size));
    pull.connect(ep).await.expect("pull connect");

    let payload = Bytes::from(vec![b'x'; size]);
    compio::runtime::spawn(async move {
        loop {
            if push.send(Message::single(payload.clone())).await.is_err() {
                break;
            }
        }
    })
    .detach();

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
}

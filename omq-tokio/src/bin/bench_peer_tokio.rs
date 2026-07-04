//! Two-process benchmark peer for omq-tokio.
//!
//! Throughput (PUSH/PULL):
//!   `bench_peer_tokio` push \<addr\> \<`msg_size`\>
//!   `bench_peer_tokio` pull \<addr\> \<`msg_size`\> \<`duration_secs`\>
//!   `bench_peer_tokio` inproc \<name\> \<`msg_size`\> \<`duration_secs`\>
//!
//! Latency (REQ/REP):
//!   `bench_peer_tokio` rep \<addr\> \<`msg_size`\>
//!   `bench_peer_tokio` req \<addr\> \<`msg_size`\> \<iterations\> \<warmup\>
//!
//! \<addr\>: a port number (`4000`), an `ip:port` pair (`0.0.0.0:4000`),
//!   a full URI (`tcp://0.0.0.0:4000`), or an IPC path (`ipc:///tmp/foo.sock`).
//!
//! Push: binds, sends \<`msg_size`\> byte messages forever.
//! Pull: connects, warms up for 500 ms, then counts messages for \<duration\>
//!       seconds and prints raw stats to stdout (for scripts) and a
//!       human-readable summary to stderr.
//! Rep: binds, echoes every received message back. Killed by SIGTERM.
//! Req: connects, runs warmup + measured round-trips, prints:
//!         \<`p50_us`\> \<`p99_us`\> \<`p999_us`\> \<`max_us`\> \<iterations\>

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};
use std::net::Ipv4Addr;

fn parse_ep(s: &str) -> Endpoint {
    if let Ok(port) = s.parse::<u16>() {
        return Endpoint::Tcp {
            host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
            port,
        };
    }
    if let Some((ip, port_str)) = s.split_once(':')
        && let Ok(addr) = ip.parse::<Ipv4Addr>()
        && let Ok(port) = port_str.parse::<u16>()
    {
        return Endpoint::Tcp {
            host: Host::Ip(addr.into()),
            port,
        };
    }
    s.parse()
        .expect("valid endpoint (port, ip:port, or full URI)")
}

fn print_bound_port(ep: &Endpoint) {
    if let Endpoint::Tcp { port, .. } = ep {
        println!("PORT {port}");
    }
}

extern "C" fn exit_on_signal(_sig: libc::c_int) {
    unsafe { libc::_exit(0) };
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let pull_current_thread = args.get(1).is_some_and(|arg| arg == "pull")
        && std::env::var("OMQ_BENCH_PULL_CURRENT_THREAD").is_ok_and(|v| v == "1");
    let rt = if !pull_current_thread
        && std::env::var("OMQ_BENCH_RUNTIME").is_ok_and(|v| v == "multi_thread")
    {
        #[cfg(feature = "rt-multi-thread")]
        {
            let workers = std::env::var("OMQ_BENCH_WORKERS")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|&v| v > 0)
                .unwrap_or_else(|| {
                    std::thread::available_parallelism().map_or(2, std::num::NonZero::get)
                });
            eprintln!("runtime: multi_thread ({workers} workers)");
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(workers)
                .enable_all()
                .build()
                .expect("tokio runtime")
        }
        #[cfg(not(feature = "rt-multi-thread"))]
        panic!("multi_thread runtime requires --features rt-multi-thread")
    } else {
        eprintln!("runtime: current_thread");
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    };
    rt.block_on(async_main(args));
}

#[expect(clippy::too_many_lines)]
async fn async_main(args: Vec<String>) {
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
    match args.get(1).map(String::as_str) {
        Some("push") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_push(ep, size).await;
        }
        Some("push-fanout") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let peers: usize = args[4].parse().expect("peers");
            run_push_fanout(ep, size, peers).await;
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
        Some("pub") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let peers: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
            run_pub(ep, size, peers).await;
        }
        Some("sub") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_sub(ep, size, Duration::from_secs_f64(duration)).await;
        }
        Some("inproc-pubsub") => {
            let name = args[2].clone();
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            let peers: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(1);
            run_inproc_pubsub(name, size, Duration::from_secs_f64(duration), peers).await;
        }
        Some("push-connect") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_push_connect(ep, size).await;
        }
        Some("pull-bind") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull_bind(ep, size, Duration::from_secs_f64(duration)).await;
        }
        Some("wire-size") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            println!("{}", wire_size(&ep, size));
        }
        #[cfg(feature = "lz4")]
        Some("train-dict") => {
            let path = &args[2];
            let capacity: usize = args.get(3).map_or(2048, |s| s.parse().expect("capacity"));
            let dict = train_json_dict(capacity);
            std::fs::write(path, &dict).expect("write dict file");
            eprintln!("Trained {} byte dict -> {path}", dict.len());
        }
        Some("inproc-latency") => {
            let name = args[2].clone();
            let size: usize = args[3].parse().expect("msg_size");
            let iterations: usize = args[4].parse().expect("iterations");
            let warmup: usize = args[5].parse().expect("warmup");
            run_inproc_latency(name, size, iterations, warmup).await;
        }
        _ => {
            eprintln!("usage: bench_peer_tokio push <addr> <size>");
            eprintln!("       bench_peer_tokio push-fanout <addr> <size> <peers>");
            eprintln!("       bench_peer_tokio pull <addr> <size> <duration_secs>");
            eprintln!("       bench_peer_tokio inproc <name> <size> <duration_secs>");
            eprintln!("       bench_peer_tokio rep <addr> <size>");
            eprintln!("       bench_peer_tokio req <addr> <size> <iterations> <warmup>");
            eprintln!("       bench_peer_tokio inproc-latency <name> <size> <iterations> <warmup>");
            eprintln!("<addr>: port number or full endpoint (tcp:// ipc://)");
            std::process::exit(1);
        }
    }
}

async fn run_pub(ep: Endpoint, size: usize, peers: usize) {
    let pub_ = Socket::new(
        SocketType::Pub,
        bench_options(size).on_mute(omq_tokio::OnMute::Block),
    );
    let monitor = pub_.monitor();
    let bound = pub_.bind(ep).await.expect("pub bind");
    print_bound_port(&bound);
    if peers > 0 {
        wait_for_handshakes(&pub_, monitor, peers).await;
    }
    wait_for_start_barrier().await;
    let payload = bench_payload(size);
    loop {
        pub_.send(Message::single(payload.clone())).await.unwrap();
    }
}

async fn wait_for_start_barrier() {
    let Some(start_at) = std::env::var("OMQ_BENCH_START_AT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
    else {
        return;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64());
    if start_at > now {
        tokio::time::sleep(Duration::from_secs_f64(start_at - now)).await;
    }
}

async fn wait_for_handshakes(sock: &Socket, mut monitor: omq_tokio::MonitorStream, peers: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let connected = sock
            .connections()
            .await
            .expect("connection snapshot")
            .into_iter()
            .filter(|c| c.peer_info.is_some())
            .count();
        if connected >= peers {
            return;
        }
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for {peers} handshaked peers"
        );
        let _ = tokio::time::timeout(deadline - now, monitor.recv()).await;
    }
}

async fn run_push_fanout(ep: Endpoint, size: usize, peers: usize) {
    let push = Socket::new(SocketType::Push, bench_options(size));
    let monitor = push.monitor();
    let bound = push.bind(ep).await.expect("push bind");
    print_bound_port(&bound);
    wait_for_handshakes(&push, monitor, peers).await;
    wait_for_start_barrier().await;

    let payload = bench_payload(size);
    run_push_loop(&push, &payload).await;
}

async fn send_fast(sock: &Socket, msg: Message) {
    match sock.try_send(msg) {
        Ok(()) => {}
        Err(omq_tokio::TrySendError::Full(msg)) => sock.send(msg).await.unwrap(),
        Err(e) => panic!("try_send failed: {e}"),
    }
}

async fn run_push_loop(sock: &Socket, payload: &Bytes) {
    if payload.len() <= omq_tokio::message::MAX_INLINE_MESSAGE {
        loop {
            send_fast(sock, Message::from_slice(payload)).await;
        }
    } else {
        let msg = Message::single(payload.clone());
        loop {
            send_fast(sock, msg.clone()).await;
        }
    }
}

/// Deadline-bounded blocking recv. Returns `true` when a message arrived,
/// `false` if the deadline elapsed first or the socket closed. Bounding the
/// blocking recv keeps a measured loop from hanging forever when a peer
/// delivers zero messages; the hot drain still uses `try_recv` so throughput
/// is unaffected.
async fn recv_before_deadline(sock: &Socket, deadline: Instant) -> bool {
    let now = Instant::now();
    if now >= deadline {
        return false;
    }
    matches!(
        tokio::time::timeout(deadline - now, sock.recv()).await,
        Ok(Ok(_))
    )
}

async fn run_sub(ep: Endpoint, size: usize, duration: Duration) {
    let sub = Socket::new(SocketType::Sub, bench_options(size));
    sub.connect(ep.clone()).await.expect("sub connect");
    sub.subscribe(Bytes::new()).await.expect("subscribe");

    wait_for_start_barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        if !recv_before_deadline(&sub, deadline).await {
            break;
        }
        count += 1;
        while sub.try_recv().is_ok() {
            count += 1;
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
    eprint_pull_summary(&ep, count, elapsed, size);
}

async fn run_inproc_pubsub(name: String, size: usize, duration: Duration, peers: usize) {
    let ep = Endpoint::Inproc { name };
    let pub_ = Socket::new(
        SocketType::Pub,
        bench_options(size).on_mute(omq_tokio::OnMute::Block),
    );
    pub_.bind(ep.clone()).await.expect("pub bind");

    let mut subs = Vec::with_capacity(peers);
    for _ in 0..peers {
        let s = Socket::new(SocketType::Sub, bench_options(size));
        s.connect(ep.clone()).await.expect("sub connect");
        s.subscribe(Bytes::new()).await.expect("subscribe");
        subs.push(s);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = Bytes::from(vec![b'x'; size]);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let pub_handle = tokio::spawn(async move {
        loop {
            if pub_.send(Message::single(payload.clone())).await.is_err() {
                break;
            }
        }
    });

    // Drain all non-measured subscribers so PUB doesn't block on HWM.
    let mut drain_handles = Vec::new();
    let stop_c = stop.clone();
    for s in subs.drain(1..) {
        let stop_c = stop_c.clone();
        drain_handles.push(tokio::spawn(async move {
            while !stop_c.load(std::sync::atomic::Ordering::Relaxed) {
                if s.recv().await.is_err() {
                    break;
                }
            }
        }));
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let measured = subs.into_iter().next().unwrap();
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        measured.recv().await.unwrap();
        count += 1;
        while measured.try_recv().is_ok() {
            count += 1;
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    pub_handle.abort();
    for h in drain_handles {
        h.abort();
    }
    println!("{count} {elapsed:.6} {size}");
}

fn bench_options(msg_size: usize) -> Options {
    let mut o = Options::default();
    if msg_size >= 2 * 1024 * 1024 {
        let buf = msg_size * 2;
        o = o.recv_buffer_size(buf).send_buffer_size(buf);
    }
    if let Ok(path) = std::env::var("OMQ_BENCH_DICT_FILE") {
        let dict = Bytes::from(std::fs::read(&path).expect("read dict file"));
        o = o.compression_dict(dict);
    }
    o
}

fn bench_options_server(msg_size: usize) -> Options {
    let o = bench_options(msg_size);
    match mechanism_env().as_deref() {
        None | Some("null") => o,
        #[cfg(feature = "plain")]
        Some("plain") => o.plain_server(|_| true),
        #[cfg(feature = "curve")]
        Some("curve") => o.curve_server(bench_curve_server_keypair()),
        #[cfg(feature = "blake3zmq")]
        Some("blake3zmq") => o.blake3zmq_server(bench_b3_server_keypair()),
        Some(other) => panic!("unknown OMQ_BENCH_MECHANISM: {other}"),
    }
}

fn bench_options_client(msg_size: usize) -> Options {
    let o = bench_options(msg_size);
    match mechanism_env().as_deref() {
        None | Some("null") => o,
        #[cfg(feature = "plain")]
        Some("plain") => o.plain_client("bench", "bench"),
        #[cfg(feature = "curve")]
        Some("curve") => {
            let client_kp = bench_curve_client_keypair();
            let server_pub = bench_curve_server_keypair().public;
            o.curve_client(client_kp, server_pub)
        }
        #[cfg(feature = "blake3zmq")]
        Some("blake3zmq") => {
            let client_kp = bench_b3_client_keypair();
            let server_pub = bench_b3_server_keypair().public;
            o.blake3zmq_client(client_kp, server_pub)
        }
        Some(other) => panic!("unknown OMQ_BENCH_MECHANISM: {other}"),
    }
}

fn mechanism_env() -> Option<String> {
    std::env::var("OMQ_BENCH_MECHANISM")
        .ok()
        .map(|s| s.to_ascii_lowercase())
}

#[cfg(feature = "curve")]
fn bench_curve_server_keypair() -> omq_tokio::CurveKeypair {
    let secret = omq_tokio::CurveSecretKey::from_bytes([0x01; 32]);
    let public = secret.derive_public();
    omq_tokio::CurveKeypair { public, secret }
}

#[cfg(feature = "curve")]
fn bench_curve_client_keypair() -> omq_tokio::CurveKeypair {
    let secret = omq_tokio::CurveSecretKey::from_bytes([0x02; 32]);
    let public = secret.derive_public();
    omq_tokio::CurveKeypair { public, secret }
}

#[cfg(feature = "blake3zmq")]
fn bench_b3_server_keypair() -> omq_tokio::Blake3ZmqKeypair {
    omq_tokio::Blake3ZmqKeypair::from_secret(omq_tokio::Blake3ZmqSecretKey([0x03; 32]))
}

#[cfg(feature = "blake3zmq")]
fn bench_b3_client_keypair() -> omq_tokio::Blake3ZmqKeypair {
    omq_tokio::Blake3ZmqKeypair::from_secret(omq_tokio::Blake3ZmqSecretKey([0x04; 32]))
}

async fn run_push(ep: Endpoint, size: usize) {
    let push = Socket::new(SocketType::Push, bench_options_server(size));
    let bound = push.bind(ep).await.expect("push bind");
    print_bound_port(&bound);
    wait_for_start_barrier().await;
    let payload = bench_payload(size);
    run_push_loop(&push, &payload).await;
}

async fn run_push_connect(ep: Endpoint, size: usize) {
    let push = Socket::new(SocketType::Push, bench_options_client(size));
    push.connect(ep).await.expect("push connect");
    let payload = bench_payload(size);
    run_push_loop(&push, &payload).await;
}

async fn run_pull_bind(ep: Endpoint, size: usize, duration: Duration) {
    let pull = Socket::new(SocketType::Pull, bench_options_server(size));
    let bound = pull.bind(ep.clone()).await.expect("pull bind");
    print_bound_port(&bound);

    wait_for_start_barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    drain_pending(&pull);

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        if !recv_before_deadline(&pull, deadline).await {
            break;
        }
        count += 1;
        while pull.try_recv().is_ok() {
            count += 1;
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
    eprint_pull_summary(&ep, count, elapsed, size);
}

async fn run_inproc(name: String, size: usize, duration: Duration) {
    let ep = Endpoint::Inproc { name };
    let push = Socket::new(SocketType::Push, bench_options(size));
    push.bind(ep.clone()).await.expect("push bind");
    let pull = Socket::new(SocketType::Pull, bench_options(size));
    pull.connect(ep).await.expect("pull connect");

    let payload = Bytes::from(vec![b'x'; size]);
    tokio::spawn(async move {
        loop {
            if push.send(Message::single(payload.clone())).await.is_err() {
                break;
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        if !recv_before_deadline(&pull, deadline).await {
            break;
        }
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

async fn run_pull(ep: Endpoint, size: usize, duration: Duration) {
    let pull = Socket::new(SocketType::Pull, bench_options_client(size));
    pull.connect(ep.clone()).await.expect("pull connect");

    wait_for_start_barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    drain_pending(&pull);

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        if !recv_before_deadline(&pull, deadline).await {
            break;
        }
        count += 1;
        while pull.try_recv().is_ok() {
            count += 1;
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
    eprint_pull_summary(&ep, count, elapsed, size);
}

fn drain_pending(sock: &Socket) {
    while sock.try_recv().is_ok() {}
}

#[expect(clippy::cast_precision_loss)]
fn eprint_pull_summary(ep: &Endpoint, count: u64, elapsed: f64, size: usize) {
    let total_bytes = u128::from(count) * size as u128;
    let msgs_per_sec = count as f64 / elapsed;
    let bytes_per_sec = total_bytes as f64 / elapsed;
    let mib_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    let mbit_per_sec = bytes_per_sec * 8.0 / 1_000_000.0;
    let total_mib = total_bytes as f64 / (1024.0 * 1024.0);

    eprintln!();
    eprintln!("=== PULL ===");
    eprintln!("  Endpoint    : {ep}");
    eprintln!("  Msg size    : {} B", with_commas(&size.to_string()));
    eprintln!("  Elapsed     : {elapsed:.3} s");
    eprintln!("  Messages    : {}", with_commas(&count.to_string()));
    eprintln!(
        "  Throughput  : {} msg/s",
        with_commas(&format!("{msgs_per_sec:.0}"))
    );
    eprintln!(
        "  Bandwidth   : {} MiB/s  ({} Mbit/s)",
        with_commas(&format!("{mib_per_sec:.2}")),
        with_commas(&format!("{mbit_per_sec:.2}"))
    );
    eprintln!(
        "  Total       : {} MiB",
        with_commas(&format!("{total_mib:.2}"))
    );
    eprintln!();
}

fn with_commas(s: &str) -> String {
    let (int_part, dec_part) = s.find('.').map_or((s, ""), |i| s.split_at(i));
    let (sign, digits) = int_part
        .strip_prefix('-')
        .map_or(("", int_part), |d| ("-", d));
    let mut out = String::with_capacity(s.len() + digits.len() / 3 + 1);
    out.push_str(sign);
    let len = digits.len();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out.push_str(dec_part);
    out
}

async fn run_inproc_latency(name: String, size: usize, iterations: usize, warmup: usize) {
    let ep = Endpoint::Inproc { name };

    let rep_ep = ep.clone();
    tokio::spawn(async move {
        let rep = Socket::new(SocketType::Rep, Options::default());
        rep.bind(rep_ep).await.expect("rep bind");
        loop {
            let msg = rep.recv().await.unwrap();
            rep.send(msg).await.unwrap();
        }
    });

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.expect("req connect");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = Bytes::from(vec![b'x'; size]);

    for _ in 0..warmup {
        req.send(Message::single(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
    }

    let mut rtts = Vec::with_capacity(iterations);
    let wall_start = Instant::now();
    for _ in 0..iterations {
        let t0 = Instant::now();
        req.send(Message::single(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
        rtts.push(t0.elapsed().as_nanos() as u64);
    }
    let elapsed = wall_start.elapsed().as_secs_f64();

    rtts.sort_unstable();
    let p50 = percentile(&rtts, 50.0);
    let p99 = percentile(&rtts, 99.0);
    let p999 = percentile(&rtts, 99.9);
    let max = percentile(&rtts, 100.0);
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations} 0 {elapsed:.6}");
}

async fn run_rep(ep: Endpoint, size: usize) {
    let rep = Socket::new(SocketType::Rep, bench_options(size));
    let bound = rep.bind(ep).await.expect("rep bind");
    print_bound_port(&bound);
    loop {
        let msg = rep.recv().await.unwrap();
        rep.send(msg).await.unwrap();
    }
}

async fn run_req(ep: Endpoint, size: usize, iterations: usize, warmup: usize) {
    let req = Socket::new(SocketType::Req, bench_options(size));
    req.connect(ep).await.expect("req connect");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = Bytes::from(vec![b'x'; size]);

    for _ in 0..warmup {
        req.send(Message::single(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
    }

    let t_wall = Instant::now();
    let cpu_before = cpu_time_secs();
    let mut rtts = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t0 = Instant::now();
        req.send(Message::single(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
        rtts.push(t0.elapsed().as_nanos() as u64);
    }
    let cpu = cpu_time_secs() - cpu_before;
    let elapsed = t_wall.elapsed().as_secs_f64();

    rtts.sort_unstable();
    let p50 = percentile(&rtts, 50.0);
    let p99 = percentile(&rtts, 99.0);
    let p999 = percentile(&rtts, 99.9);
    let max = percentile(&rtts, 100.0);
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations} {cpu:.6} {elapsed:.6}");
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx] as f64 / 1_000.0
}

#[expect(clippy::cast_precision_loss)]
#[cfg(unix)]
fn cpu_time_secs() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        let usage = usage.assume_init();
        let user = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1e6;
        let sys = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1e6;
        user + sys
    }
}

#[cfg(windows)]
fn cpu_time_secs() -> f64 {
    // Windows doesn't have rusage; return 0.0 as a placeholder.
    0.0
}

fn bench_payload(size: usize) -> Bytes {
    if std::env::var("OMQ_BENCH_PAYLOAD").as_deref() == Ok("json") {
        json_payload_random(size)
    } else {
        Bytes::from(vec![b'x'; size])
    }
}

#[cfg(feature = "lz4")]
fn json_payload_seeded(target_bytes: usize, seed: u32) -> Bytes {
    let mut out = String::with_capacity(target_bytes + 512);
    let mut state: u32 = seed;
    while out.len() < target_bytes {
        json_record(&mut out, &mut state);
    }
    out.truncate(target_bytes);
    Bytes::from(out)
}

fn json_payload_random(target_bytes: usize) -> Bytes {
    let mut out = String::with_capacity(target_bytes + 512);
    let mut state: u32 = rand_seed();
    while out.len() < target_bytes {
        json_record(&mut out, &mut state);
    }
    out.truncate(target_bytes);
    Bytes::from(out)
}

fn rand_seed() -> u32 {
    let mut buf = [0u8; 4];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| {
            use std::io::Read;
            f.read_exact(&mut buf)?;
            Ok(())
        })
        .expect("/dev/urandom");
    u32::from_ne_bytes(buf)
}

fn xorshift32(state: &mut u32) -> u32 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    x
}

fn json_record(out: &mut String, state: &mut u32) {
    const LEVELS: &[&str] = &["DEBUG", "INFO", "WARN", "ERROR", "TRACE"];
    const SERVICES: &[&str] = &[
        "api-gateway",
        "auth-svc",
        "order-svc",
        "payment-svc",
        "notify-svc",
        "inventory-svc",
        "shipping-svc",
        "billing-svc",
        "search-svc",
        "user-svc",
        "session-svc",
        "analytics-svc",
        "cache-svc",
        "config-svc",
        "audit-svc",
        "rate-limiter",
    ];
    const METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"];
    const PATHS: &[&str] = &[
        "/v1/widgets",
        "/v1/users",
        "/v1/orders",
        "/v2/events",
        "/v1/health",
        "/v1/sessions",
        "/v1/payments",
        "/v2/search",
        "/v1/inventory",
        "/v1/shipping",
        "/v1/analytics",
        "/v2/config",
    ];
    const REGIONS: &[&str] = &[
        "us-east-1",
        "us-west-2",
        "eu-west-1",
        "ap-south-1",
        "eu-central-1",
        "ap-northeast-1",
        "sa-east-1",
        "ca-central-1",
    ];
    const STATUSES: &[u16] = &[
        200, 201, 202, 204, 301, 302, 304, 400, 401, 403, 404, 405, 409, 422, 429, 500, 502, 503,
        504,
    ];
    const MSGS: &[&str] = &[
        "request handled successfully",
        "resource created",
        "cache miss, fetched from origin",
        "rate limit approaching threshold",
        "upstream timeout, retrying",
        "authentication token refreshed",
        "database connection pool exhausted",
        "circuit breaker tripped",
        "message queued for async processing",
        "TLS handshake completed",
        "request routed to fallback backend",
        "payload validation passed",
        "idempotency key matched existing result",
        "graceful shutdown initiated",
        "health check passed all probes",
        "retry attempt succeeded after backoff",
    ];

    let trace_id = xorshift32(state);
    let span_id = xorshift32(state);
    let user_id = xorshift32(state);
    let r = xorshift32(state) as usize;
    let level = LEVELS[r % LEVELS.len()];
    let service = SERVICES[(r >> 4) % SERVICES.len()];
    let method = METHODS[(r >> 8) % METHODS.len()];
    let path = PATHS[(r >> 12) % PATHS.len()];
    let region = REGIONS[(r >> 16) % REGIONS.len()];
    let status = STATUSES[(r >> 20) % STATUSES.len()];
    let latency = (xorshift32(state) % 5000) + 1;
    let r2 = xorshift32(state) as usize;
    let msg = MSGS[r2 % MSGS.len()];
    let host_id = xorshift32(state);
    let _ = write!(
        out,
        r#"{{"ts":"2026-04-27T12:34:56.{trace_id:08x}Z","level":"{level}","service":"{service}","trace_id":"{trace_id:08x}{span_id:08x}","span_id":"{span_id:08x}","user_id":"u-{user_id:08x}","method":"{method}","path":"{path}/{trace_id:08x}","status":{status},"latency_ms":{latency},"region":"{region}","host":"{service}-{host_id:08x}.svc.cluster.local","msg":"{msg}"}}{nl}"#,
        nl = '\n',
    );
}

#[cfg(feature = "lz4")]
fn train_json_dict(capacity: usize) -> Vec<u8> {
    use omq_proto::proto::transform::lz4::DictTrainer;

    let mut trainer = DictTrainer::new(capacity);
    // Bias toward common message sizes (512B, 1 KiB) with a few at other sizes.
    let sample_sizes: &[(usize, usize)] = &[
        (64, 2),
        (128, 2),
        (256, 4),
        (512, 8),
        (1024, 8),
        (2048, 4),
        (4096, 4),
    ];
    for &(size, count) in sample_sizes {
        let payload = json_payload_seeded(size, 0);
        for _ in 0..count {
            trainer.add_sample(&payload);
        }
    }
    trainer.train()
}

fn wire_size(ep: &Endpoint, size: usize) -> usize {
    use omq_proto::proto::transform::MessageEncoder;
    let options = bench_options(size);
    let Some((mut enc, _dec)) = MessageEncoder::for_endpoint(ep, &options) else {
        return size;
    };
    let payload = json_payload_random(size);
    let msg = Message::single(payload.clone());
    let frames = enc.encode(&msg).unwrap();
    frames.last().map_or(size, Message::byte_len)
}

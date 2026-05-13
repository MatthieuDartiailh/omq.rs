//! Shared bench scaffolding for omq-compio. Mirrors
//! `omq-tokio/benches/common/mod.rs` so results across backends are
//! directly comparable: same payload sizes, prime + calibrate +
//! best-of-N shape, output formatting, and JSONL schema.
//!
//! Each pattern lives in its own bench file (`push_pull.rs`, etc.) that
//! `#[path = "common/mod.rs"] mod common;`s this module.

#![allow(dead_code)]

use std::fs::OpenOptions;
use std::io::Write as _;
use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use omq_compio::{Endpoint, IpcPath};

pub(crate) const DEFAULT_SIZES: &[usize] = &[128, 2_048, 8_192];
pub(crate) const ALL_SIZES: &[usize] = &[32, 128, 512, 2_048, 8_192, 32_768, 131_072];
pub(crate) const DEFAULT_TRANSPORTS: &[&str] = &["inproc", "ipc", "tcp"];

pub(crate) const PRIME_ITERS: usize = 2_000;
pub(crate) const WARMUP_DURATION: Duration = Duration::from_millis(100);
pub(crate) const WARMUP_MIN_ITERS: usize = 1_000;
pub(crate) const DEFAULT_ROUND_DURATION: Duration = Duration::from_millis(500);
pub(crate) const DEFAULT_ROUNDS: usize = 3;

/// Per-round timed budget. Override with `OMQ_BENCH_ROUND_MS` for
/// longer rounds (overnight, less-noisy runs).
pub(crate) fn round_duration() -> Duration {
    std::env::var("OMQ_BENCH_ROUND_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map_or(DEFAULT_ROUND_DURATION, Duration::from_millis)
}

/// Number of timed rounds per cell. Override with `OMQ_BENCH_ROUNDS`.
pub(crate) fn rounds() -> usize {
    std::env::var("OMQ_BENCH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or(DEFAULT_ROUNDS)
}

/// Hard ceiling per cell — a hang guard, not a tight bound. The 30s
/// base covers TCP setup, ZMTP handshake, subscription propagation,
/// and wait_connected's own 30s deadline. The 2x round budget absorbs
/// calibration overshoot.
pub(crate) fn run_timeout() -> Duration {
    let r = rounds() as u32;
    let per = round_duration();
    per.saturating_mul(r * 2) + Duration::from_secs(30)
}

pub(crate) fn results_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("benches");
    let suffix = std::env::var("OMQ_BENCH_RESULTS_SUFFIX").unwrap_or_default();
    if suffix.is_empty() {
        p.push("results.jsonl");
    } else {
        p.push(format!("results_{suffix}.jsonl"));
    }
    p
}

pub(crate) fn run_id() -> String {
    std::env::var("OMQ_BENCH_RUN_ID").unwrap_or_else(|_| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        format!("ts-{now}")
    })
}

pub(crate) fn sizes() -> Vec<usize> {
    if let Ok(s) = std::env::var("OMQ_BENCH_SIZES") {
        return s.split(',').filter_map(|t| t.trim().parse().ok()).collect();
    }
    if std::env::args().any(|a| a == "--all-sizes") {
        return ALL_SIZES.to_vec();
    }
    DEFAULT_SIZES.to_vec()
}

pub(crate) fn transports() -> Vec<String> {
    if let Ok(s) = std::env::var("OMQ_BENCH_TRANSPORTS") {
        return s.split(',').map(|t| t.trim().to_string()).collect();
    }
    #[allow(unused_mut)]
    let mut ts: Vec<String> = DEFAULT_TRANSPORTS
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    #[cfg(feature = "lz4")]
    ts.push("lz4+tcp".to_string());
    #[cfg(feature = "zstd")]
    ts.push("zstd+tcp".to_string());
    ts
}

pub(crate) fn peers_override() -> Option<Vec<usize>> {
    std::env::var("OMQ_BENCH_PEERS")
        .ok()
        .map(|s| s.split(',').filter_map(|t| t.trim().parse().ok()).collect())
}

pub(crate) fn endpoint(transport: &str, seq: usize) -> Endpoint {
    match transport {
        "inproc" => Endpoint::Inproc {
            name: format!("bench-{seq}"),
        },
        "ipc" => {
            let mut dir = std::env::temp_dir();
            dir.push(format!(
                "omq-compio-bench-{}-{seq}.sock",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&dir);
            Endpoint::Ipc(IpcPath::Filesystem(dir))
        }
        "tcp" | "lz4+tcp" | "zstd+tcp" => {
            let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .expect("bench: failed to allocate a tcp port");
            let port = l.local_addr().unwrap().port();
            drop(l);
            let host = omq_compio::endpoint::Host::Ip(Ipv4Addr::LOCALHOST.into());
            match transport {
                "tcp" => Endpoint::Tcp { host, port },
                #[cfg(feature = "lz4")]
                "lz4+tcp" => Endpoint::Lz4Tcp { host, port },
                #[cfg(feature = "zstd")]
                "zstd+tcp" => Endpoint::ZstdTcp { host, port },
                _ => panic!(
                    "bench: transport '{transport}' requires its feature; \
                     rerun with `--features {}`",
                    transport.trim_end_matches("tcp").trim_end_matches('+')
                ),
            }
        }
        other => panic!("bench: unknown transport {other}"),
    }
}

/// Run `fut` on `runtime` and then drain pending tasks before dropping the
/// runtime. Without this, every cell leaks the io_uring fd plus every TCP /
/// unix-socket fd owned by tasks that were still parked at end-of-cell.
///
/// Why: compio 0.12.0-rc.1's `Runtime::drop` early-returns when
/// `Rc::strong_count(&self.0) > 1`, and `compio::time::sleep` / `timeout`
/// install a `CancelToken` that holds a `Runtime` clone for as long as the
/// future is parked. `JoinHandle::drop` (called via `Socket::drop` ->
/// `dialers.clear()`) only marks the task cancelled; the future is dropped
/// the next time the executor ticks the task. `block_on` ticks once after
/// the user future returns, which is not enough when cancellations cascade
/// (dial supervisor -> driver -> pump). Spinning `runtime.run()` until
/// `has_hot()` goes false flushes the cascade so every `CancelToken`'s
/// `Runtime` clone is released, `Runtime::drop` reaches `strong_count` == 1,
/// and `executor.clear()` finally runs.
///
/// Upstream fix: compio-rs/compio#911 (replaces `Runtime` clones with
/// `Rc<RefCell<Proactor>>`). Remove this workaround once that merges.
pub(crate) fn block_on_and_drain<F>(runtime: compio::runtime::Runtime, fut: F) -> F::Output
where
    F: std::future::Future,
{
    let out = runtime.block_on(fut);
    while runtime.run() {}
    drop(runtime);
    out
}

pub(crate) async fn wait_connected(socks: &[&omq_compio::Socket]) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let mut pending = 0usize;
        for s in socks {
            let conns = s.connections().await.unwrap_or_default();
            if !conns.iter().any(|c| c.peer_info.is_some()) {
                pending += 1;
            }
        }
        if pending == 0 {
            return;
        }
        if Instant::now() > deadline {
            // Distinguish "TCP connect never landed" (peer slot missing
            // entirely) from "connected but greeting stuck" (peer slot
            // present, peer_info still None). Helps localise hangs.
            let mut detail = Vec::with_capacity(socks.len());
            for (i, s) in socks.iter().enumerate() {
                let conns = s.connections().await.unwrap_or_default();
                detail.push(format!(
                    "#{i}: slots={} info_some={}",
                    conns.len(),
                    conns.iter().filter(|c| c.peer_info.is_some()).count()
                ));
            }
            panic!(
                "bench: {pending}/{} peer(s) never reached peer_info=Some \
                 within 15s. per-socket: [{}]",
                socks.len(),
                detail.join(", ")
            );
        }
        compio::time::sleep(Duration::from_millis(5)).await;
    }
}

pub(crate) async fn wait_subscribed(pub_: &omq_compio::Socket, subs: &[&omq_compio::Socket]) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut pending: Vec<usize> = (0..subs.len()).collect();
    while !pending.is_empty() {
        assert!(
            Instant::now() <= deadline,
            "bench: subscriptions never propagated"
        );
        let _ = pub_.send(omq_compio::Message::single("")).await;
        let mut still: Vec<usize> = Vec::new();
        for &i in &pending {
            match compio::time::timeout(Duration::from_millis(20), subs[i].recv()).await {
                Ok(Ok(_)) => {}
                _ => still.push(i),
            }
        }
        pending = still;
    }
}

pub(crate) async fn measure_min_of<F, Fut>(msg_size: usize, align: usize, burst: F) -> Cell
where
    F: Fn(usize) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    burst(PRIME_ITERS).await;

    let round_dur = round_duration();
    let n_rounds = rounds();
    let mut n = WARMUP_MIN_ITERS;
    let final_n = loop {
        let t = Instant::now();
        burst(n).await;
        let elapsed = t.elapsed();
        if elapsed >= WARMUP_DURATION {
            let rate = n as f64 / elapsed.as_secs_f64();
            let target = (rate * round_dur.as_secs_f64()) as usize;
            let aligned = (target.max(WARMUP_MIN_ITERS) / align.max(1)) * align.max(1);
            break aligned.max(align.max(1));
        }
        n = n.saturating_mul(4);
    };

    let mut times = Vec::with_capacity(n_rounds);
    for _ in 0..n_rounds {
        let t = Instant::now();
        burst(final_n).await;
        times.push(t.elapsed());
    }
    let elapsed = *times.iter().min().expect("at least one round");
    let mbps = (final_n * msg_size) as f64 / elapsed.as_secs_f64() / 1_000_000.0;
    let msgs_s = final_n as f64 / elapsed.as_secs_f64();
    Cell {
        n: final_n,
        elapsed,
        mbps,
        msgs_s,
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Cell {
    pub n: usize,
    pub elapsed: Duration,
    pub mbps: f64,
    pub msgs_s: f64,
}

pub(crate) fn print_header(label: &str) {
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .unwrap_or_else(|_| "unknown".into())
        .trim()
        .to_string();
    println!(
        "{label} | omq-compio {} | {} | {kernel}",
        env!("CARGO_PKG_VERSION"),
        rustc_version_runtime(),
    );
    println!();
}

pub(crate) fn print_subheader(transport: &str, peers: usize) {
    let s = if peers > 1 { "s" } else { "" };
    println!("--- {transport} ({peers} peer{s}) ---");
}

pub(crate) fn print_cell(msg_size: usize, c: Cell) {
    println!(
        "  {:>6}  {:>8.1} MB/s  {:>8.0} msg/s  ({:.2}s, n={})",
        format!("{msg_size}B"),
        c.mbps,
        c.msgs_s,
        c.elapsed.as_secs_f64(),
        c.n,
    );
}

pub(crate) fn append_jsonl(pattern: &str, transport: &str, peers: usize, msg_size: usize, c: Cell) {
    if std::env::var_os("OMQ_BENCH_NO_WRITE").is_some() {
        return;
    }
    let path = results_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let row = format!(
        r#"{{"run_id":"{run}","pattern":"{pattern}","transport":"{transport}","peers":{peers},"msg_size":{msg_size},"msg_count":{n},"elapsed":{el},"mbps":{mbps},"msgs_s":{msgs_s}}}"#,
        run = run_id(),
        pattern = pattern,
        transport = transport,
        peers = peers,
        msg_size = msg_size,
        n = c.n,
        el = c.elapsed.as_secs_f64(),
        mbps = c.mbps,
        msgs_s = c.msgs_s,
    );
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&path) {
        let _ = writeln!(f, "{row}");
    }
}

fn rustc_version_runtime() -> String {
    std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_else(|| "rustc ?".into())
        .trim()
        .to_string()
}

/// Compio runs everything on the current thread; no multi-thread
/// builder is needed (or available). Each bench `main()` constructs
/// one runtime via `#[compio::main]`.
pub(crate) async fn with_timeout<T>(label: &str, fut: impl std::future::Future<Output = T>) -> T {
    let to = run_timeout();
    compio::time::timeout(to, fut)
        .await
        .unwrap_or_else(|_| panic!("BENCH TIMEOUT: {label} exceeded {to:?}"))
}

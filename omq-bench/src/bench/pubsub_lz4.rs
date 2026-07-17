use crate::cli::PubsubLz4Args;
use crate::jsonl::{self, PubsubLz4Row};
use crate::process;

use std::path::PathBuf;
use std::time::Duration;

const CHART_SIZES: &[u64] = &[
    16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131_072, 262_144,
    524_288,
];
const QUICK_SIZES: &[u64] = &[64, 1024, 16384];
const PEERS: u64 = 32;

fn size_label(n: u64) -> String {
    if n >= 1_048_576 {
        format!("{} MiB", n / 1_048_576)
    } else if n >= 1024 {
        format!("{} KiB", n / 1024)
    } else {
        format!("{n} B")
    }
}

static mut PORT_COUNTER: u16 = 17500;

fn next_port() -> u16 {
    unsafe {
        let p = PORT_COUNTER;
        PORT_COUNTER += 1;
        p
    }
}

fn build_peer() -> PathBuf {
    eprintln!("  building bench_peer_tokio (lz4)...");
    let status = std::process::Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "omq-tokio",
            "--bin",
            "bench_peer_tokio",
            "--features",
            "lz4",
            "-q",
        ])
        .status()
        .expect("failed to run cargo build");
    assert!(status.success(), "build failed");
    PathBuf::from("target/release/bench_peer_tokio")
}

fn get_wire_size(binary: &str, transport: &str, size: u64, dict_file: Option<&str>) -> u64 {
    let port = next_port();
    let ep = format!("{transport}://127.0.0.1:{port}");
    let size_str = size.to_string();
    let mut env = vec![("OMQ_BENCH_PAYLOAD", "json")];
    if let Some(df) = dict_file {
        env.push(("OMQ_BENCH_DICT_FILE", df));
    }
    let output = process::capture(
        &[binary, "wire-size", &ep, &size_str],
        &env,
        None,
        Duration::from_secs(10),
    )
    .unwrap_or_default();
    output.trim().parse().unwrap_or(size)
}

fn train_dict(binary: &str, path: &str, capacity: u64) {
    let cap_str = capacity.to_string();
    process::capture(
        &[binary, "train-dict", path, &cap_str],
        &[],
        None,
        Duration::from_secs(30),
    );
}

fn run_cell(
    binary: &str,
    transport: &str,
    size: u64,
    peers: u64,
    duration: f64,
    dict_file: Option<&str>,
) -> Option<(f64, f64, f64)> {
    let size_str = size.to_string();
    let dur_str = format!("{duration:.1}");
    let drain_dur_str = format!("{:.1}", duration + 3.0);
    let port = next_port();
    let ep = format!("{transport}://127.0.0.1:{port}");

    let mut pub_env: Vec<(&str, &str)> = vec![("OMQ_BENCH_PAYLOAD", "json")];
    if let Some(df) = dict_file {
        pub_env.push(("OMQ_BENCH_DICT_FILE", df));
    }

    let peers_str = peers.to_string();
    let mut pub_proc = process::spawn(
        &[binary, "pub", &ep, &size_str, &peers_str],
        &pub_env,
        Some(process::MEASURED_CPU),
    );

    std::thread::sleep(Duration::from_millis(200));

    let drain_transport = if transport.contains("lz4") {
        "tcp"
    } else {
        transport
    };

    // Spawn drain subscribers (peers - 1).
    let mut drains = Vec::new();
    for _ in 0..(peers - 1) {
        let drain_ep = format!("{drain_transport}://127.0.0.1:{port}");
        let proc = process::spawn(
            &[binary, "sub", &drain_ep, &size_str, &drain_dur_str],
            &[],
            None,
        );
        drains.push(proc);
    }

    std::thread::sleep(Duration::from_millis(500));

    // Measured subscriber.
    let mut sub_env: Vec<(&str, &str)> = vec![("OMQ_BENCH_PAYLOAD", "json")];
    if let Some(df) = dict_file {
        sub_env.push(("OMQ_BENCH_DICT_FILE", df));
    }

    let output = process::capture(
        &[binary, "sub", &ep, &size_str, &dur_str],
        &sub_env,
        Some(process::OTHER_CPU),
        Duration::from_secs(duration as u64 + 30),
    );

    let pub_cpu = process::read_proc_cpu(pub_proc.pid());
    pub_proc.kill();

    for mut d in drains {
        d.kill();
    }

    let output = output?;
    let parts: Vec<&str> = output.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }
    let count: f64 = parts[0].parse().ok()?;
    let elapsed: f64 = parts[1].parse().ok()?;
    if elapsed <= 0.0 || count <= 0.0 {
        return None;
    }
    let msgs_s = count / elapsed;
    let mbps = (count * size as f64) / elapsed / 1_000_000.0;
    let aggregate_mbps = mbps * peers as f64;
    Some((msgs_s, aggregate_mbps, pub_cpu))
}

fn make_run_id() -> String {
    let output = std::process::Command::new("date")
        .args(["-u", "+%Y%m%dT%H%M%SZ"])
        .output()
        .expect("failed to run date");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn run(args: PubsubLz4Args) {
    let sizes = if let Some(ref s) = args.sizes {
        s.clone()
    } else if args.quick {
        QUICK_SIZES.to_vec()
    } else {
        CHART_SIZES.to_vec()
    };

    let duration = if args.quick { 1.5 } else { args.duration };
    let rounds = if args.quick { 1 } else { args.rounds };

    let transports: Vec<&str> = args.transports.split(',').collect();
    let binary = build_peer();
    let binary_str = binary.to_str().unwrap();
    let run_id = make_run_id();
    let jsonl_path = jsonl::cache_dir().join("results_pubsub_lz4.jsonl");

    eprintln!("PubSub LZ4 benchmark");
    eprintln!(
        "Transports: {transports:?}, sizes: {}, peers: {PEERS}, rounds: {rounds}",
        sizes.len()
    );

    for transport in &transports {
        eprintln!("\n=== {transport} ===");

        for &size in &sizes {
            eprint!("{:>8}", size_label(size));

            let wire_bytes = get_wire_size(binary_str, transport, size, None);

            let mut best: Option<(f64, f64, f64)> = None;
            for _ in 0..rounds {
                if let Some(result) = run_cell(binary_str, transport, size, PEERS, duration, None)
                    && best.as_ref().is_none_or(|b| result.0 > b.0)
                {
                    best = Some(result);
                }
            }

            if let Some((msgs_s, mbps, cpu_time)) = best {
                let row = PubsubLz4Row {
                    run_id: run_id.clone(),
                    pattern: "pubsub_lz4".to_string(),
                    transport: transport.to_string(),
                    peers: PEERS,
                    msg_size: size,
                    wire_bytes,
                    msg_count: Some(msgs_s * duration),
                    elapsed: Some(duration),
                    cpu_time: Some(cpu_time),
                    msgs_s: Some(msgs_s),
                    mbps: Some(mbps),
                    dict_size: None,
                };
                jsonl::append_jsonl(&jsonl_path, &row);
                eprint!("  {msgs_s:>10.0} msg/s  {mbps:>8.1} MB/s");
            } else {
                eprint!("  {:>10}", "-");
            }
            eprintln!();
        }
    }

    // Dict wire-size sweep.
    for &dict_cap in &args.dict_sizes {
        let dict_path = format!("/tmp/omq-bench-dict-{dict_cap}.bin");
        eprintln!("\nTraining dict (capacity={dict_cap})...");
        train_dict(binary_str, &dict_path, dict_cap);

        eprintln!("Dict wire sizes (dict_size={dict_cap}):");
        for &size in &sizes {
            let wire_bytes = get_wire_size(binary_str, "lz4+tcp", size, Some(&dict_path));
            let row = PubsubLz4Row {
                run_id: run_id.clone(),
                pattern: "pubsub_lz4_dict".to_string(),
                transport: "lz4+tcp".to_string(),
                peers: PEERS,
                msg_size: size,
                wire_bytes,
                msg_count: None,
                elapsed: None,
                cpu_time: None,
                msgs_s: None,
                mbps: None,
                dict_size: Some(dict_cap),
            };
            jsonl::append_jsonl(&jsonl_path, &row);
            eprintln!("  {}: {} -> {wire_bytes} bytes", size_label(size), size);
        }

        std::fs::remove_file(&dict_path).ok();
    }

    eprintln!("\nResults appended to {}", jsonl_path.display());
}

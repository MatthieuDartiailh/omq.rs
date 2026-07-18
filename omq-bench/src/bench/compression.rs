use crate::cli::CompressionArgs;
use crate::jsonl::{self, CompressionRow};
use crate::process;

use std::path::PathBuf;
use std::time::Duration;

const CHART_SIZES: &[u64] = &[
    16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131_072, 262_144,
    524_288,
];

static mut PORT_COUNTER: u16 = 18500;

fn next_port() -> u16 {
    unsafe {
        let p = PORT_COUNTER;
        PORT_COUNTER += 1;
        p
    }
}

fn size_label(n: u64) -> String {
    if n >= 1_048_576 {
        format!("{} MiB", n / 1_048_576)
    } else if n >= 1024 {
        format!("{} KiB", n / 1024)
    } else {
        format!("{n} B")
    }
}

fn build_peer() -> PathBuf {
    let bin = PathBuf::from("target/release/omq_bench_peer_tokio");
    if bin.exists() {
        return bin;
    }
    eprintln!("  building omq_bench_peer_tokio (lz4)...");
    let status = std::process::Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "omq-tokio",
            "--bin",
            "omq_bench_peer_tokio",
            "--features",
            "lz4",
            "-q",
        ])
        .status()
        .expect("failed to run cargo build");
    assert!(status.success(), "build failed");
    bin
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
    duration: f64,
    dict_file: Option<&str>,
) -> Option<(f64, f64, f64)> {
    let size_str = size.to_string();
    let dur_str = format!("{duration:.1}");
    let port = next_port();
    let ep = format!("{transport}://127.0.0.1:{port}");

    let mut env: Vec<(&str, &str)> = vec![("OMQ_BENCH_PAYLOAD", "json")];
    if let Some(df) = dict_file {
        env.push(("OMQ_BENCH_DICT_FILE", df));
    }

    let mut push_proc = process::spawn(
        &[binary, "push", &ep, &size_str],
        &env,
        Some(process::MEASURED_CPU),
    );

    std::thread::sleep(Duration::from_millis(200));

    let pull_output = process::capture(
        &[binary, "pull", &ep, &size_str, &dur_str],
        &env,
        Some(process::OTHER_CPU),
        Duration::from_secs(duration as u64 + 30),
    );

    let push_cpu = process::read_proc_cpu(push_proc.pid());
    push_proc.kill();

    let output = pull_output?;
    let parts: Vec<&str> = output.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }
    let count: f64 = parts[0].parse().ok()?;
    let elapsed: f64 = parts[1].parse().ok()?;
    let pull_cpu: f64 = parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(0.0);
    if elapsed <= 0.0 || count <= 0.0 {
        return None;
    }
    let msgs_s = count / elapsed;
    let mbps = (count * size as f64) / elapsed / 1_000_000.0;
    let cpu_time = push_cpu + pull_cpu;
    Some((msgs_s, mbps, cpu_time))
}

fn make_run_id() -> String {
    let output = std::process::Command::new("date")
        .args(["-u", "+%Y%m%dT%H%M%SZ"])
        .output()
        .expect("failed to run date");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn run(args: CompressionArgs) {
    let sizes = if let Some(ref s) = args.sizes {
        s.clone()
    } else {
        CHART_SIZES.to_vec()
    };

    let transports: Vec<&str> = args.transports.split(',').collect();
    let binary = build_peer();
    let binary_str = binary.to_str().unwrap();
    let run_id = make_run_id();
    let jsonl_path = jsonl::cache_dir().join("results_compression_tokio.jsonl");

    eprintln!("Compression benchmark");

    // Non-dict sweep.
    run_sweep(
        binary_str,
        &transports,
        &sizes,
        args.duration,
        &run_id,
        None,
        "compression_json",
        None,
        &jsonl_path,
    );

    // Dict sweeps.
    for &dict_cap in &args.dict_sizes {
        let dict_path = format!("/tmp/omq-bench-comp-dict-{dict_cap}.bin");
        eprintln!("\nTraining dict (capacity={dict_cap})...");
        train_dict(binary_str, &dict_path, dict_cap);

        run_sweep(
            binary_str,
            &["lz4+tcp"],
            &sizes,
            args.duration,
            &run_id,
            Some(&dict_path),
            "compression_json_dict",
            Some(dict_cap),
            &jsonl_path,
        );

        std::fs::remove_file(&dict_path).ok();
    }

    eprintln!("\nResults appended to {}", jsonl_path.display());
}

#[expect(clippy::too_many_arguments)]
fn run_sweep(
    binary: &str,
    transports: &[&str],
    sizes: &[u64],
    duration: f64,
    run_id: &str,
    dict_file: Option<&str>,
    pattern: &str,
    dict_size: Option<u64>,
    jsonl_path: &std::path::Path,
) {
    for transport in transports {
        eprintln!("\n=== {transport} {pattern} ===");

        for &size in sizes {
            eprint!("{:>8}", size_label(size));

            let wire_bytes = get_wire_size(binary, transport, size, dict_file);

            match run_cell(binary, transport, size, duration, dict_file) {
                Some((msgs_s, mbps, cpu_time)) => {
                    let row = CompressionRow {
                        run_id: run_id.to_string(),
                        pattern: pattern.to_string(),
                        transport: transport.to_string(),
                        peers: 1,
                        msg_size: size,
                        wire_bytes,
                        msg_count: Some(msgs_s * duration),
                        elapsed: Some(duration),
                        cpu_time: Some(cpu_time),
                        mbps: Some(mbps),
                        msgs_s: Some(msgs_s),
                        dict_size,
                    };
                    jsonl::append_jsonl(jsonl_path, &row);
                    eprint!("  {msgs_s:>10.0} msg/s  {mbps:>8.1} MB/s");
                }
                None => {
                    eprint!("  {:>10}", "-");
                }
            }
            eprintln!();
        }
    }
}

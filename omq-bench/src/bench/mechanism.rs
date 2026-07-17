use crate::cli::MechanismArgs;
use crate::coord::CoordSocket;
use crate::jsonl::{self, MechanismRow};
use crate::process;

use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_SIZES: &[u64] = &[64, 1024, 16384];
const CHART_SIZES: &[u64] = &[
    16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131_072, 262_144,
    524_288,
];
const MECHANISMS: &[&str] = &["PLAIN", "CURVE"];

fn size_label(n: u64) -> String {
    if n >= 1_048_576 {
        format!("{} MiB", n / 1_048_576)
    } else if n >= 1024 {
        format!("{} KiB", n / 1024)
    } else {
        format!("{n} B")
    }
}

fn build_peer(backend: &str) -> PathBuf {
    let features = "plain,curve";
    let bin_name = format!("bench_peer_{backend}");
    let crate_name = format!("omq-{backend}");
    eprintln!("  building {bin_name}...");
    let status = std::process::Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            &crate_name,
            "--bin",
            &bin_name,
            "--features",
            features,
            "-q",
        ])
        .status()
        .expect("failed to run cargo build");
    assert!(status.success(), "build failed");
    PathBuf::from(format!("target/release/{bin_name}"))
}

fn run_cell(
    binary: &str,
    mechanism: &str,
    size: u64,
    duration: f64,
    rounds: u32,
) -> Option<(f64, f64)> {
    let mut best: Option<(f64, f64)> = None;
    for _ in 0..rounds {
        if let Some(result) = run_once(binary, mechanism, size, duration)
            && best.as_ref().is_none_or(|b| result.0 > b.0)
        {
            best = Some(result);
        }
    }
    best
}

fn run_once(binary: &str, mechanism: &str, size: u64, duration: f64) -> Option<(f64, f64)> {
    let size_str = size.to_string();
    let coord = CoordSocket::bind_new();

    let mut push_proc = process::spawn(
        &[binary, "push", "tcp://127.0.0.1:0", &size_str],
        &[
            ("OMQ_BENCH_MECHANISM", mechanism),
            ("OMQ_BENCH_COORD", coord.endpoint()),
        ],
        Some(process::MEASURED_CPU),
    );

    let port = coord.recv_ready_port(Duration::from_secs(10))?;

    let dur_str = format!("{duration:.1}");
    let addr = format!("tcp://127.0.0.1:{port}");

    let output = process::capture(
        &[binary, "pull", &addr, &size_str, &dur_str],
        &[("OMQ_BENCH_MECHANISM", mechanism)],
        Some(process::OTHER_CPU),
        Duration::from_secs(duration as u64 + 30),
    )?;

    push_proc.kill();

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
    Some((msgs_s, mbps))
}

fn make_run_id() -> String {
    std::env::var("OMQ_BENCH_RUN_ID").unwrap_or_else(|_| {
        let output = std::process::Command::new("date")
            .args(["-u", "+%Y%m%dT%H%M%SZ"])
            .output()
            .expect("failed to run date");
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    })
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn run(args: MechanismArgs) {
    let sizes = if let Some(ref s) = args.sizes {
        s.clone()
    } else if args.chart_sizes {
        CHART_SIZES.to_vec()
    } else {
        DEFAULT_SIZES.to_vec()
    };

    let binary = build_peer(&args.backend);
    let binary_str = binary.to_str().unwrap();
    let run_id = make_run_id();
    let jsonl_path = jsonl::cache_dir().join(format!("results_{}.jsonl", args.backend));

    eprintln!("Mechanism benchmark: {}", args.backend);
    eprintln!(
        "Sizes: {:?}",
        sizes.iter().map(|s| size_label(*s)).collect::<Vec<_>>()
    );

    eprint!("{:>8}", "");
    for &mech in MECHANISMS {
        eprint!("  {mech:>12}");
    }
    eprintln!();

    for &size in &sizes {
        eprint!("{:>8}", size_label(size));
        for &mechanism in MECHANISMS {
            match run_cell(binary_str, mechanism, size, args.duration, args.rounds) {
                Some((msgs_s, mbps)) => {
                    let row = MechanismRow {
                        run_id: run_id.clone(),
                        pattern: "mechanism".to_string(),
                        transport: mechanism.to_string(),
                        peers: 1,
                        msg_size: size,
                        msg_count: msgs_s * args.duration,
                        elapsed: args.duration,
                        mbps,
                        msgs_s,
                    };
                    jsonl::append_jsonl(&jsonl_path, &row);

                    if size >= 1024 {
                        eprint!("  {:>12.1}", mbps / 1000.0);
                    } else {
                        eprint!("  {:>12.0}", msgs_s / 1000.0);
                    }
                }
                None => {
                    eprint!("  {:>12}", "-");
                }
            }
        }
        eprintln!();
    }

    eprintln!("\nResults appended to {}", jsonl_path.display());
}

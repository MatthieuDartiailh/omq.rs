#![allow(dead_code, unreachable_pub)]

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use omq_tokio::Endpoint;
use omq_tokio::endpoint::Host;

// ---------------------------------------------------------------------------
// Duration
// ---------------------------------------------------------------------------

pub fn soak_duration() -> Duration {
    let secs: u64 = std::env::var("OMQ_SOAK_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(600);
    Duration::from_secs(secs.max(5))
}

// ---------------------------------------------------------------------------
// Endpoint helpers
// ---------------------------------------------------------------------------

pub fn loopback_port() -> u16 {
    let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

pub fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

pub fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

// ---------------------------------------------------------------------------
// Resource monitor (background std::thread)
// ---------------------------------------------------------------------------

pub struct ResourceMonitor {
    stop: Arc<AtomicBool>,
    #[allow(clippy::type_complexity)]
    handle: Option<std::thread::JoinHandle<(Vec<(Instant, usize)>, Vec<(Instant, usize)>)>>,
}

impl ResourceMonitor {
    pub fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let handle = std::thread::spawn(move || {
            let mut rss_samples = Vec::new();
            let mut fd_samples = Vec::new();
            while !stop2.load(Ordering::Relaxed) {
                let now = Instant::now();
                if let Some(rss) = read_rss_bytes() {
                    rss_samples.push((now, rss));
                }
                if let Some(fds) = read_fd_count() {
                    fd_samples.push((now, fds));
                }
                std::thread::sleep(Duration::from_secs(1));
            }
            (rss_samples, fd_samples)
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    pub fn stop(mut self) -> ResourceReport {
        self.stop.store(true, Ordering::Relaxed);
        let (rss_samples, fd_samples) = self.handle.take().unwrap().join().unwrap();
        ResourceReport {
            rss_samples,
            fd_samples,
        }
    }
}

impl Drop for ResourceMonitor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn read_rss_bytes() -> Option<usize> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let rss_pages: usize = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(rss_pages * 4096)
}

fn read_fd_count() -> Option<usize> {
    Some(std::fs::read_dir("/proc/self/fd").ok()?.count())
}

// ---------------------------------------------------------------------------
// Resource report
// ---------------------------------------------------------------------------

pub struct ResourceReport {
    pub rss_samples: Vec<(Instant, usize)>,
    pub fd_samples: Vec<(Instant, usize)>,
}

impl ResourceReport {
    pub fn assert_no_leak(&self, label: &str) {
        self.assert_rss_stable(label);
        self.assert_fd_stable(label);
    }

    fn assert_rss_stable(&self, label: &str) {
        let n = self.rss_samples.len();
        if n < 10 {
            eprintln!("[{label}] too few RSS samples ({n}) to check for leaks");
            return;
        }

        let warmup = n / 5;
        let post_warmup = &self.rss_samples[warmup..];

        let baseline_end = post_warmup.len() / 10;
        let baseline_end = baseline_end.max(1);
        let baseline: usize = post_warmup[..baseline_end]
            .iter()
            .map(|(_, v)| v)
            .sum::<usize>()
            / baseline_end;

        let tail_start = post_warmup.len() * 4 / 5;
        let tail = &post_warmup[tail_start..];
        let tail_max = tail.iter().map(|(_, v)| *v).max().unwrap_or(0);

        let growth_pct = if baseline > 0 {
            ((tail_max as f64 - baseline as f64) / baseline as f64) * 100.0
        } else {
            0.0
        };

        let peak_rss = self.rss_samples.iter().map(|(_, v)| *v).max().unwrap_or(0);

        eprintln!(
            "[{label}] RSS: baseline {:.1} MiB, tail max {:.1} MiB, peak {:.1} MiB, growth {growth_pct:.1}%",
            baseline as f64 / 1_048_576.0,
            tail_max as f64 / 1_048_576.0,
            peak_rss as f64 / 1_048_576.0,
        );

        let threshold = if n >= 120 { 25.0 } else { 100.0 };

        let growth_mib = (tail_max as f64 - baseline as f64) / 1_048_576.0;

        assert!(
            growth_pct < threshold || growth_mib < 10.0,
            "[{label}] RSS leak detected: grew {growth_pct:.1}% / {growth_mib:.1} MiB \
             from baseline ({:.1} MiB -> {:.1} MiB)",
            baseline as f64 / 1_048_576.0,
            tail_max as f64 / 1_048_576.0,
        );
    }

    fn assert_fd_stable(&self, label: &str) {
        let n = self.fd_samples.len();
        if n < 10 {
            eprintln!("[{label}] too few FD samples ({n}) to check for leaks");
            return;
        }

        let warmup = n / 5;
        let post_warmup = &self.fd_samples[warmup..];

        if post_warmup.len() < 2 {
            return;
        }

        let first_t = post_warmup.first().unwrap().0;
        let elapsed_secs = post_warmup
            .last()
            .unwrap()
            .0
            .duration_since(first_t)
            .as_secs_f64();
        if elapsed_secs < 1.0 {
            return;
        }

        let n_f = post_warmup.len() as f64;
        let sum_x: f64 = post_warmup
            .iter()
            .map(|(t, _)| t.duration_since(first_t).as_secs_f64())
            .sum();
        let sum_y: f64 = post_warmup.iter().map(|(_, v)| *v as f64).sum();
        let dot_xy: f64 = post_warmup
            .iter()
            .map(|(t, v)| t.duration_since(first_t).as_secs_f64() * *v as f64)
            .sum();
        let sum_xx: f64 = post_warmup
            .iter()
            .map(|(t, _)| {
                let x = t.duration_since(first_t).as_secs_f64();
                x * x
            })
            .sum();

        let slope = (n_f * dot_xy - sum_x * sum_y) / (n_f * sum_xx - sum_x * sum_x);

        let fd_min = post_warmup.iter().map(|(_, v)| *v).min().unwrap_or(0);
        let fd_max = post_warmup.iter().map(|(_, v)| *v).max().unwrap_or(0);

        eprintln!("[{label}] FDs: range {fd_min}..{fd_max}, slope {slope:.4} FDs/s");

        let threshold = if post_warmup.len() >= 120 { 0.05 } else { 1.0 };
        assert!(
            slope < threshold,
            "[{label}] FD leak detected: slope {slope:.4} FDs/s (range {fd_min}..{fd_max})"
        );
    }
}

// ---------------------------------------------------------------------------
// Throughput tracker
// ---------------------------------------------------------------------------

pub struct ThroughputTracker {
    #[allow(dead_code)]
    window: Duration,
    samples: Vec<(Instant, u64)>,
}

impl ThroughputTracker {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            samples: Vec::new(),
        }
    }

    pub fn record(&mut self, cumulative_count: u64) {
        self.samples.push((Instant::now(), cumulative_count));
    }

    pub fn assert_stable(&self, label: &str) {
        if self.samples.len() < 4 {
            eprintln!("[{label}] too few throughput samples to check stability");
            return;
        }

        let rates = self.windowed_rates();
        if rates.len() < 4 {
            return;
        }

        let warmup = rates.len() / 5;
        let post_warmup = &rates[warmup..];
        if post_warmup.len() < 2 {
            return;
        }

        let baseline_end = (post_warmup.len() / 5).max(1);
        let baseline: f64 = post_warmup[..baseline_end].iter().sum::<f64>() / baseline_end as f64;

        let tail_start = post_warmup.len() * 4 / 5;
        let tail = &post_warmup[tail_start..];
        let tail_avg: f64 = tail.iter().sum::<f64>() / tail.len() as f64;

        eprintln!(
            "[{label}] throughput: baseline {baseline:.0} msg/s, tail avg {tail_avg:.0} msg/s"
        );

        if baseline > 0.0 {
            assert!(
                tail_avg >= baseline * 0.5,
                "[{label}] throughput degraded: {tail_avg:.0} msg/s < 50% of baseline {baseline:.0} msg/s"
            );
        }
    }

    fn windowed_rates(&self) -> Vec<f64> {
        let mut rates = Vec::new();
        for i in 1..self.samples.len() {
            let dt = self.samples[i]
                .0
                .duration_since(self.samples[i - 1].0)
                .as_secs_f64();
            if dt > 0.0 {
                let dcount = self.samples[i].1.saturating_sub(self.samples[i - 1].1);
                rates.push(dcount as f64 / dt);
            }
        }
        rates
    }
}

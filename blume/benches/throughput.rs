use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const ROUND_MS: u64 = 500;
const ROUNDS: usize = 3;
const CAP: usize = 1024;

#[derive(Clone)]
struct Payload {
    _data: Arc<[u8]>,
}

impl Payload {
    fn new(size: usize) -> Self {
        Self {
            _data: vec![0xABu8; size].into(),
        }
    }
}

struct Cell {
    msgs: usize,
    elapsed: Duration,
}

impl Cell {
    fn msg_per_sec(&self) -> f64 {
        self.msgs as f64 / self.elapsed.as_secs_f64()
    }
}

fn fmt_rate(rate: f64) -> String {
    if rate >= 1_000_000.0 {
        format!("{:.2}M", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.0}k", rate / 1_000.0)
    } else {
        format!("{rate:.0}")
    }
}

// ---------------------------------------------------------------------------
// blume benches
// ---------------------------------------------------------------------------

fn blume_try(size: usize) -> Cell {
    let payload = Payload::new(size);
    let (tx, rx) = blume::bounded::<Payload>(CAP);

    let tx_payload = payload;
    let sender = thread::spawn(move || {
        loop {
            match tx.try_send(tx_payload.clone()) {
                Ok(()) => {}
                Err(blume::TrySendError::Full(_)) => thread::yield_now(),
                Err(blume::TrySendError::Disconnected(_)) => break,
            }
        }
    });

    let deadline = Instant::now() + Duration::from_millis(ROUND_MS);
    let mut count = 0usize;
    while Instant::now() < deadline {
        for _ in 0..256 {
            if rx.try_recv().is_ok() {
                count += 1;
            } else {
                thread::yield_now();
                break;
            }
        }
    }
    let elapsed = Duration::from_millis(ROUND_MS);
    drop(rx);
    let _ = sender.join();
    Cell {
        msgs: count,
        elapsed,
    }
}

fn blume_async(size: usize) -> Cell {
    let payload = Payload::new(size);
    let (tx, rx) = blume::bounded::<Payload>(CAP);

    let tx_payload = payload;
    let sender = thread::spawn(move || {
        futures_lite::future::block_on(async {
            loop {
                if tx.send_async(tx_payload.clone()).await.is_err() {
                    return;
                }
            }
        });
    });

    futures_lite::future::block_on(async {
        let deadline = Instant::now() + Duration::from_millis(ROUND_MS);
        let mut count = 0usize;
        while Instant::now() < deadline {
            match rx.recv_async().await {
                Ok(_) => count += 1,
                Err(_) => break,
            }
        }
        drop(rx);
        let _ = sender.join();
        Cell {
            msgs: count,
            elapsed: Duration::from_millis(ROUND_MS),
        }
    })
}

fn blume_batch(size: usize) -> Cell {
    let payload = Payload::new(size);
    let (tx, rx) = blume::bounded::<Payload>(CAP);

    let tx_payload = payload;
    let sender = thread::spawn(move || {
        futures_lite::future::block_on(async {
            loop {
                if tx.send_async(tx_payload.clone()).await.is_err() {
                    return;
                }
            }
        });
    });

    futures_lite::future::block_on(async {
        let mut buf = Vec::with_capacity(CAP);
        let deadline = Instant::now() + Duration::from_millis(ROUND_MS);
        let mut count = 0usize;
        while Instant::now() < deadline {
            buf.clear();
            match rx.recv_batch(&mut buf).await {
                Ok(n) => count += n,
                Err(_) => break,
            }
        }
        drop(rx);
        let _ = sender.join();
        Cell {
            msgs: count,
            elapsed: Duration::from_millis(ROUND_MS),
        }
    })
}

// ---------------------------------------------------------------------------
// flume benches
// ---------------------------------------------------------------------------

fn flume_try(size: usize) -> Cell {
    let payload = Payload::new(size);
    let (tx, rx) = flume::bounded::<Payload>(CAP);

    let tx_payload = payload;
    let sender = thread::spawn(move || {
        loop {
            match tx.try_send(tx_payload.clone()) {
                Ok(()) => {}
                Err(flume::TrySendError::Full(_)) => thread::yield_now(),
                Err(flume::TrySendError::Disconnected(_)) => break,
            }
        }
    });

    let deadline = Instant::now() + Duration::from_millis(ROUND_MS);
    let mut count = 0usize;
    while Instant::now() < deadline {
        for _ in 0..256 {
            if rx.try_recv().is_ok() {
                count += 1;
            } else {
                thread::yield_now();
                break;
            }
        }
    }
    let elapsed = Duration::from_millis(ROUND_MS);
    drop(rx);
    let _ = sender.join();
    Cell {
        msgs: count,
        elapsed,
    }
}

fn flume_async(size: usize) -> Cell {
    let payload = Payload::new(size);
    let (tx, rx) = flume::bounded::<Payload>(CAP);

    let tx_payload = payload;
    let sender = thread::spawn(move || {
        futures_lite::future::block_on(async {
            loop {
                if tx.send_async(tx_payload.clone()).await.is_err() {
                    return;
                }
            }
        });
    });

    futures_lite::future::block_on(async {
        let deadline = Instant::now() + Duration::from_millis(ROUND_MS);
        let mut count = 0usize;
        while Instant::now() < deadline {
            match rx.recv_async().await {
                Ok(_) => count += 1,
                Err(_) => break,
            }
        }
        drop(rx);
        let _ = sender.join();
        Cell {
            msgs: count,
            elapsed: Duration::from_millis(ROUND_MS),
        }
    })
}

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

fn run_cell<F: Fn(usize) -> Cell>(name: &str, size: usize, f: F) -> Cell {
    let mut best: Option<Cell> = None;
    for _ in 0..ROUNDS {
        let cell = f(size);
        if best.as_ref().is_none_or(|b| cell.msgs > b.msgs) {
            best = Some(cell);
        }
    }
    let cell = best.unwrap();
    println!(
        "  {name:<20} {size:>6}B  {rate:>10} msg/s",
        rate = fmt_rate(cell.msg_per_sec()),
    );
    cell
}

fn main() {
    println!(
        "blume vs flume | 1 sender, 1 receiver, cross-thread | \
         bounded({CAP}) | {ROUNDS}×{ROUND_MS}ms rounds (min)\n"
    );

    let sizes = [0, 32, 128, 512, 2048];

    println!("--- try_send / try_recv ---");
    for &size in &sizes {
        let b = run_cell("blume try", size, blume_try);
        let f = run_cell("flume try", size, flume_try);
        let ratio = b.msg_per_sec() / f.msg_per_sec();
        println!("  {ratio:>42.2}× blume/flume\n");
    }

    println!("--- send_async / recv_async ---");
    for &size in &sizes {
        let b = run_cell("blume async", size, blume_async);
        let f = run_cell("flume async", size, flume_async);
        let ratio = b.msg_per_sec() / f.msg_per_sec();
        println!("  {ratio:>42.2}× blume/flume\n");
    }

    println!("--- blume recv_batch vs blume async vs flume async ---");
    for &size in &sizes {
        let bb = run_cell("blume batch", size, blume_batch);
        let ba = run_cell("blume async", size, blume_async);
        let fa = run_cell("flume async", size, flume_async);
        let ratio_batch = bb.msg_per_sec() / fa.msg_per_sec();
        let ratio_async = ba.msg_per_sec() / fa.msg_per_sec();
        println!("  {ratio_batch:>42.2}× batch/flume  {ratio_async:.2}× async/flume\n",);
    }
}

//! Fast local performance gate for the core TCP paths.
//!
//! Thresholds are read from `.perf_hw`, which is intentionally ignored.
//! Without that file, this command reports measurements but does not fail.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::{Context, ContextConfig, Endpoint, Message, Options, SocketType};

const WARMUP: Duration = Duration::from_millis(100);
const MEASURE: Duration = Duration::from_millis(750);

#[derive(Clone)]
struct Sample {
    name: String,
    value: f64,
    unit: &'static str,
}

fn payload(size: usize) -> Message {
    let bytes = vec![0x5a; size];
    if size <= omq_tokio::message::MAX_INLINE_MESSAGE {
        Message::from_slice(&bytes)
    } else {
        Message::single(Bytes::from(bytes))
    }
}

fn tcp_zero() -> Endpoint {
    "tcp://127.0.0.1:0".parse().expect("valid TCP endpoint")
}

fn read_thresholds() -> HashMap<String, f64> {
    let Ok(contents) = std::fs::read_to_string(".perf_hw") else {
        return HashMap::new();
    };
    let mut section = String::new();
    let mut thresholds = HashMap::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            section = name.trim().to_string();
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if let Ok(value) = value.trim().parse() {
            thresholds.insert(format!("{section}.{}", key.trim()), value);
        }
    }
    thresholds
}

async fn reqrep_latency() -> f64 {
    let req_ctx = Context::current();
    let rep_ctx = Context::current();
    let rep = rep_ctx.socket(SocketType::Rep, Options::default());
    let req = req_ctx.socket(SocketType::Req, Options::default());
    let endpoint = rep.bind(tcp_zero()).await.expect("REP bind");
    req.connect(endpoint).await.expect("REQ connect");
    req.wait_connected(1, Duration::from_secs(1))
        .await
        .expect("REQ connect timeout");

    let echo = tokio::spawn(async move {
        loop {
            let msg = rep.recv().await.expect("REP recv");
            rep.send(msg).await.expect("REP send");
        }
    });
    let msg = payload(256);
    for _ in 0..100 {
        req.send(msg.clone()).await.expect("REQ warmup send");
        req.recv().await.expect("REQ warmup recv");
    }
    let mut samples = Vec::with_capacity(500);
    for _ in 0..500 {
        let start = Instant::now();
        req.send(msg.clone()).await.expect("REQ send");
        req.recv().await.expect("REQ recv");
        samples.push(start.elapsed().as_secs_f64() * 1_000_000.0);
    }
    echo.abort();
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

async fn try_send_until(sock: &omq_tokio::Socket, mut msg: Message, deadline: Instant) -> bool {
    loop {
        match sock.try_send(msg) {
            Ok(()) => return true,
            Err(omq_tokio::TrySendError::Full(returned)) => {
                msg = returned;
                if Instant::now() >= deadline {
                    return false;
                }
                tokio::task::yield_now().await;
            }
            Err(error) => panic!("perf send failed: {error}"),
        }
    }
}

async fn count_messages(sock: omq_tokio::Socket, deadline: Instant) -> u64 {
    let mut count = 0_u64;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if !matches!(
            tokio::time::timeout(remaining, sock.recv()).await,
            Ok(Ok(_))
        ) {
            break;
        }
        count += 1;
        while sock.try_recv().is_ok() {
            count += 1;
        }
        tokio::task::yield_now().await;
    }
    count
}

async fn pushpull(size: usize, io_threads: usize) -> f64 {
    tokio::task::spawn_blocking(move || {
        let pull_ctx = Context::with_config(ContextConfig { io_threads });
        let push_ctx = Context::with_config(ContextConfig { io_threads });
        let pull = pull_ctx.blocking_socket(SocketType::Pull, Options::default());
        let push = push_ctx.blocking_socket(SocketType::Push, Options::default());
        let endpoint = pull.bind(tcp_zero()).expect("PULL bind");
        push.connect(endpoint).expect("PUSH connect");
        pull.wait_connected(1, Duration::from_secs(1))
            .expect("PULL connect timeout");

        let done = Arc::new(AtomicBool::new(false));
        let receiver_done = done.clone();
        let receiver = thread::spawn(move || {
            thread::sleep(WARMUP);
            let mut count = 0_u64;
            loop {
                match pull.try_recv() {
                    Ok(_) => {
                        count += 1;
                        while pull.try_recv().is_ok() {
                            count += 1;
                        }
                    }
                    Err(_) if receiver_done.load(Ordering::Acquire) => break,
                    Err(_) => thread::yield_now(),
                }
            }
            count
        });

        thread::sleep(WARMUP);
        let msg = payload(size);
        let deadline = Instant::now() + MEASURE;
        while Instant::now() < deadline {
            let mut pending = msg.clone();
            loop {
                match push.try_send(pending) {
                    Ok(()) => break,
                    Err(omq_tokio::TrySendError::Full(returned)) => {
                        pending = returned;
                        thread::yield_now();
                    }
                    Err(error) => panic!("PUSH send failed: {error}"),
                }
            }
        }
        push.send(msg).expect("PUSH wakeup send");
        done.store(true, Ordering::Release);
        let count = receiver.join().expect("PULL thread");
        push_ctx.term();
        pull_ctx.term();
        count as f64 / MEASURE.as_secs_f64()
    })
    .await
    .expect("PUSH/PULL task")
}

async fn pubsub(size: usize, io_threads: usize) -> f64 {
    const PEERS: usize = 4;
    let pub_ctx = Context::with_config(ContextConfig { io_threads });
    let sub_ctx = Context::with_config(ContextConfig { io_threads });
    let publisher = pub_ctx.socket(SocketType::Pub, Options::default());
    let endpoint = publisher.bind(tcp_zero()).await.expect("PUB bind");
    let mut subscribers = Vec::with_capacity(PEERS);
    for _ in 0..PEERS {
        let subscriber = sub_ctx.socket(SocketType::Sub, Options::default());
        subscriber
            .connect(endpoint.clone())
            .await
            .expect("SUB connect");
        subscriber
            .subscribe(Bytes::new())
            .await
            .expect("SUB subscribe");
        subscribers.push(subscriber);
    }
    publisher
        .wait_subscribed(PEERS as u64, Duration::from_secs(1))
        .await
        .expect("SUB subscribe timeout");

    let mut receivers = Vec::with_capacity(PEERS);
    for subscriber in subscribers {
        receivers.push(tokio::spawn(count_messages(
            subscriber,
            Instant::now() + WARMUP + MEASURE,
        )));
    }
    let msg = payload(size);
    let warmup_deadline = Instant::now() + WARMUP;
    'warmup: loop {
        for _ in 0..256 {
            if !try_send_until(&publisher, msg.clone(), warmup_deadline).await {
                break 'warmup;
            }
        }
        if Instant::now() >= warmup_deadline {
            break;
        }
        tokio::task::yield_now().await;
    }
    let deadline = Instant::now() + MEASURE;
    'measure: loop {
        for _ in 0..256 {
            if !try_send_until(&publisher, msg.clone(), deadline).await {
                break 'measure;
            }
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::task::yield_now().await;
    }
    let mut rx_count = 0;
    for receiver in receivers {
        rx_count += receiver.await.expect("SUB task");
    }
    pub_ctx.term();
    sub_ctx.term();
    rx_count as f64 / MEASURE.as_secs_f64()
}

fn verify(sample: &Sample, thresholds: &HashMap<String, f64>) -> bool {
    let key = &sample.name;
    println!("{:<24} {:>12.2} {}", sample.name, sample.value, sample.unit);
    match thresholds.get(key) {
        Some(limit) if sample.unit == "us" && sample.value > *limit => {
            eprintln!(
                "FAIL {key}: {:.2} above configured {:.2} {}",
                sample.value, limit, sample.unit
            );
            false
        }
        Some(limit) if sample.unit == "us" => {
            println!("  threshold (max): {:.2} {}", limit, sample.unit);
            true
        }
        Some(limit) if sample.value < *limit => {
            eprintln!(
                "FAIL {key}: {:.2} below configured {:.2} {}",
                sample.value, limit, sample.unit
            );
            false
        }
        Some(limit) => {
            let direction = if sample.unit == "us" { "max" } else { "min" };
            println!("  threshold ({direction}): {:.2} {}", limit, sample.unit);
            true
        }
        None => true,
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let thresholds = read_thresholds();
    let mut ok = true;
    let latency = reqrep_latency().await;
    ok &= verify(
        &Sample {
            name: "reqrep_ct.p50_256b_us".to_string(),
            value: latency,
            unit: "us",
        },
        &thresholds,
    );
    for io_threads in [1, 2] {
        for (size, suffix) in [(16, "16b"), (1024, "1k"), (16 * 1024, "16k")] {
            let value = pushpull(size, io_threads).await;
            ok &= verify(
                &Sample {
                    name: format!("pushpull_{io_threads}io.{suffix}_msgs_s"),
                    value,
                    unit: "msg/s",
                },
                &thresholds,
            );
        }
        for (size, suffix) in [(16, "16b"), (4096, "4k")] {
            let value = pubsub(size, io_threads).await;
            ok &= verify(
                &Sample {
                    name: format!("pubsub_{io_threads}io.{suffix}_msgs_s"),
                    value,
                    unit: "msg/s",
                },
                &thresholds,
            );
        }
    }
    if thresholds.is_empty() {
        eprintln!("warning: .perf_hw missing; measurements not threshold-checked");
    }
    if !ok {
        std::process::exit(1);
    }
}

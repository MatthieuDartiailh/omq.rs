//! Inproc PUSH/PULL with one application thread per socket and one IO thread.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use omq_tokio::{Context, ContextConfig, Endpoint, Message, Options, SocketType};

#[path = "common/mod.rs"]
mod common;

const PATTERN: &str = "inproc_threads";
const TIMER_CHECK_INTERVAL: usize = 10_000;

fn endpoint(seq: usize) -> Endpoint {
    Endpoint::Inproc {
        name: format!("bench-inproc-threads-{seq}"),
    }
}

fn round(ctx: &Context, size: usize, seq: usize, duration: Duration) -> common::Cell {
    let ep = endpoint(seq);
    let pull = ctx.blocking_socket(SocketType::Pull, Options::default());
    let push = ctx.blocking_socket(SocketType::Push, Options::default());
    pull.bind(ep.clone()).expect("bind PULL");
    push.connect(ep).expect("connect PUSH");
    push.wait_connected(1, Duration::from_secs(5))
        .expect("wait for PUSH/PULL connection");

    let payload = vec![b'x'; size];
    let template = Message::from_slice(&payload);
    let barrier = Arc::new(Barrier::new(2));
    let stop = Arc::new(AtomicBool::new(false));
    let cpu_before = common::process_cpu_time();

    let sender_barrier = barrier.clone();
    let sender_stop = stop.clone();
    let sender_push = push.clone();
    let sender = thread::spawn(move || {
        sender_barrier.wait();
        while !sender_stop.load(Ordering::Acquire) {
            let mut msg = template.clone();
            loop {
                match sender_push.try_send(msg) {
                    Ok(()) => break,
                    Err(omq_proto::TrySendError::Full(returned)) => {
                        msg = returned;
                        if sender_stop.load(Ordering::Acquire) {
                            return;
                        }
                        std::hint::spin_loop();
                    }
                    Err(_) => return,
                }
            }
        }
    });

    let receiver_barrier = barrier;
    let receiver_stop = stop.clone();
    let receiver_pull = pull.clone();
    let receiver = thread::spawn(move || {
        receiver_barrier.wait();
        let start = Instant::now();
        let deadline = start + duration;
        let mut count = 0usize;
        let mut until_timer_check = TIMER_CHECK_INTERVAL;
        loop {
            match receiver_pull.try_recv() {
                Ok(_) => count += 1,
                Err(omq_proto::error::Error::WouldBlock) => std::hint::spin_loop(),
                Err(_) => break,
            }
            until_timer_check -= 1;
            if until_timer_check == 0 {
                if Instant::now() >= deadline {
                    break;
                }
                until_timer_check = TIMER_CHECK_INTERVAL;
            }
        }
        receiver_stop.store(true, Ordering::Release);
        (count, start.elapsed())
    });

    let (count, elapsed) = receiver.join().expect("receiver thread");
    sender.join().expect("sender thread");
    let cpu_time = common::process_cpu_time().saturating_sub(cpu_before);
    let _ = pull.close();
    let _ = push.close();
    let elapsed_secs = elapsed.as_secs_f64();
    common::Cell {
        n: count,
        elapsed,
        mbps: count as f64 * size as f64 / elapsed_secs / 1e6,
        msgs_s: count as f64 / elapsed_secs,
        cpu_time,
    }
}

fn main() {
    let ctx = Context::with_config(ContextConfig { io_threads: 1 });
    println!("runtime: 1 dedicated background IO thread\n");
    common::print_header("INPROC PUSH/PULL: TWO APPLICATION THREADS");

    let rounds = common::rounds();
    let duration = common::round_duration();
    let mut seq = 0usize;
    for size in common::sizes() {
        let mut best = None;
        for _ in 0..rounds {
            seq += 1;
            let cell = round(&ctx, size, seq, duration);
            if best.is_none_or(|current: common::Cell| cell.elapsed < current.elapsed) {
                best = Some(cell);
            }
        }
        let cell = best.expect("at least one benchmark round");
        common::print_cell(size, cell);
        common::append_jsonl(PATTERN, "inproc-threads", 1, size, cell);
    }
}

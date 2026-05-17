use std::thread;
use std::time::Instant;

fn bench(cap: usize, batch_size: usize, n: u64, item_bytes: usize) {
    match item_bytes {
        8 => bench_inner::<u64>(cap, batch_size, n),
        128 => bench_inner::<[u8; 128]>(cap, batch_size, n),
        _ => unreachable!(),
    }
}

fn bench_inner<T: Copy + Send + 'static>(cap: usize, batch_size: usize, n: u64) {
    let (mut producer, mut consumer) = yring::spsc::<T>(cap);
    let val: T = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<T>();

    let start = Instant::now();

    let sender = thread::spawn(move || {
        let mut pending = 0u64;
        for _ in 0..n {
            while producer.push(val).is_err() {
                producer.flush();
                thread::yield_now();
            }
            pending += 1;
            if pending >= batch_size as u64 {
                producer.flush();
                pending = 0;
            }
        }
        producer.flush();
    });

    let mut received = 0u64;
    while received < n {
        if consumer.prefetch() > 0 {
            while consumer.pop().is_some() {
                received += 1;
            }
        } else {
            thread::yield_now();
        }
    }

    sender.join().unwrap();
    let elapsed = start.elapsed();
    let items_per_sec = n as f64 / elapsed.as_secs_f64();
    let mb_per_sec = n as f64 * size as f64 / elapsed.as_secs_f64() / 1_000_000.0;

    println!(
        "  cap={cap:<6} batch={batch_size:<4} {:>7.1}M items/s  {:>5.0} MB/s  ({:.2?})",
        items_per_sec / 1_000_000.0,
        mb_per_sec,
        elapsed,
    );
}

fn main() {
    let n = 10_000_000;

    println!("yring throughput ({n} items)");
    println!();

    println!("--- u64 (8 bytes) ---");
    for cap in [256, 1024, 4096] {
        for batch in [1, 16, 64, 256] {
            bench(cap, batch, n, 8);
        }
        println!();
    }

    println!("--- [u8; 128] (128 bytes) ---");
    for cap in [1024, 4096] {
        for batch in [1, 16, 64, 256] {
            bench(cap, batch, n, 128);
        }
        println!();
    }
}

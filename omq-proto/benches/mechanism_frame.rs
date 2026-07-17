//! Per-frame CURVE overhead microbench.
//!
//! Measures the raw cryptographic cost of sealing one ZMTP frame
//! payload under CURVE. Wrapper overhead (nonce assembly,
//! counter increment, AAD construction) is sub-nanosecond and not
//! distinguished here - we go straight at the primitive:
//!
//! - **CURVE**      one `crypto_box::SalsaBox` seal (XSalsa20 +
//!   Poly1305 16-byte tag, RFC 26).
//!
//! Run: `cargo bench -p omq-proto --bench mechanism_frame --features 'curve'`

use std::hint::black_box;
use std::time::Instant;

use crypto_box::{SalsaBox, SecretKey, aead::Aead};

const DEFAULT_SIZES: &[usize] = &[128, 2_048, 8_192];
const ALL_SIZES: &[usize] = &[32, 128, 512, 2_048, 8_192, 32_768, 131_072];

fn sizes() -> Vec<usize> {
    if let Ok(s) = std::env::var("OMQ_BENCH_SIZES") {
        return s.split(',').filter_map(|t| t.trim().parse().ok()).collect();
    }
    if std::env::args().any(|a| a == "--all-sizes") {
        return ALL_SIZES.to_vec();
    }
    DEFAULT_SIZES.to_vec()
}

/// Target wall-time per cell. The bench picks an iteration count that
/// roughly hits this - large payloads run fewer iters, small ones run
/// more, so each cell takes ~the same time and the table doesn't
/// stretch out into 30-minute territory.
const TARGET_NS_PER_CELL: u64 = 200_000_000; // 200 ms

fn main() {
    println!("CURVE per-frame microbench (XSalsa20Poly1305)");
    println!(
        "target wall-time per cell: ~{} ms\n",
        TARGET_NS_PER_CELL / 1_000_000
    );

    println!("  {:>6} | {:>14}", "size", "CURVE ns/op");
    println!("  {}", "-".repeat(24));

    let secret_a = SecretKey::generate(&mut crypto_box::aead::OsRng);
    let secret_b = SecretKey::generate(&mut crypto_box::aead::OsRng);
    let salsa = SalsaBox::new(&secret_b.public_key(), &secret_a);
    let nonce_curve = crypto_box::Nonce::from(black_box([0x22u8; 24]));

    let active_sizes = sizes();
    let mut rows = Vec::with_capacity(active_sizes.len());
    for size in active_sizes {
        let plain = vec![0xACu8; size];

        let curve_ns = bench(|| {
            black_box(
                salsa
                    .encrypt(&nonce_curve, black_box(plain.as_slice()))
                    .unwrap(),
            );
        });

        println!("  {:>6} | {:>14}", size, format!("{curve_ns:>5} ns"));
        rows.push((size, curve_ns));
    }

    println!();
    println!("  {:>6} | {:>14}", "size", "CURVE MiB/s");
    println!("  {}", "-".repeat(24));
    for (size, curve_ns) in rows {
        println!("  {:>6} | {:>14}", size, mibps(size, curve_ns));
    }
}

fn bench(mut f: impl FnMut()) -> u64 {
    // Warm up + size-up: keep doubling until one batch takes ~10 ms,
    // then run enough batches to hit TARGET_NS_PER_CELL total.
    let mut iters: u64 = 1;
    loop {
        let start = Instant::now();
        for _ in 0..iters {
            f();
        }
        let probe = start.elapsed().as_nanos() as u64;
        if probe > 10_000_000 || iters > 1 << 24 {
            let total_iters = ((TARGET_NS_PER_CELL / probe.max(1)).max(1) * iters).max(iters);
            let start = Instant::now();
            for _ in 0..total_iters {
                f();
            }
            let elapsed = start.elapsed().as_nanos() as u64;
            return elapsed / total_iters.max(1);
        }
        iters *= 2;
    }
}

fn mibps(size: usize, ns: u64) -> String {
    if ns == 0 {
        return "    -    ".into();
    }
    let bytes_per_sec = (size as f64) / (ns as f64) * 1e9;
    format!("{:>9.0}", bytes_per_sec / (1024.0 * 1024.0))
}

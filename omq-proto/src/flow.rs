//! Shared outbound flow-control knobs.
//!
//! Both backends drain a shared outbound queue in batches and flush via
//! a single writev. Two caps bound each batch so one busy consumer can
//! never starve its siblings:
//!
//! - a **message cap** ([`fair_share`]): each driver takes at most its
//!   fair share of the queued messages, leaving the rest for the other
//!   peers draining the same queue.
//! - a **byte cap** ([`max_batch_bytes`]): independent of message count,
//!   stop pulling once the batch has buffered this many bytes so a few
//!   large messages don't monopolize the writev or outgrow the kernel
//!   send buffer.
//!
//! Per-peer yield intervals and direct-encode admission caps stay in the
//! backends: those are tuned to each runtime (multi-thread tokio vs.
//! single-thread compio) and don't share a formula.

use std::sync::OnceLock;

/// Max bytes one shared-queue batch buffers before flushing.
///
/// 1 MiB folds large messages into bigger writev calls without
/// outgrowing typical kernel TCP send buffers. Smaller caps (e.g.
/// 256 KiB) under-utilize writev for 32 KiB+ messages and let the
/// per-syscall overhead dominate; larger caps add latency without extra
/// throughput once the kernel send buffer is the bottleneck.
///
/// Override at runtime via `OMQ_BATCH_BYTES`. Read once and cached.
#[must_use]
pub fn max_batch_bytes() -> usize {
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("OMQ_BATCH_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1024 * 1024)
    })
}

/// Fair share of the current queue for one consumer.
///
/// Single peer: full batch (`cap`), no competition. Multiple peers: each
/// consumer takes at most `queue_len / peers` to leave work for the
/// others, but always at least 1 and never more than `cap`.
#[must_use]
pub fn fair_share(queue_len: usize, peers: usize, cap: usize) -> usize {
    if peers <= 1 {
        return cap;
    }
    (queue_len / peers).clamp(1, cap)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_peer_gets_full_cap() {
        assert_eq!(fair_share(0, 1, 256), 256);
        assert_eq!(fair_share(1000, 0, 256), 256);
        assert_eq!(fair_share(5, 1, 64), 64);
    }

    #[test]
    fn multi_peer_splits_with_floor_and_cap() {
        assert_eq!(fair_share(100, 4, 256), 25);
        assert_eq!(fair_share(3, 8, 256), 1); // floor of 1
        assert_eq!(fair_share(10_000, 2, 256), 256); // capped
    }
}

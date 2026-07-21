//! Coalesced data-available notification.
//!
//! Implements the libzmq ypipe signaling discipline: one wake per
//! batch, not one wake per push. Used by `PeerTransmitSlot`, `SendPipe`,
//! `FallbackQueue`, and fan-out lane workers.
//!
//! Protocol:
//! - **Producer** calls [`DataSignal::mark`] after each push. Only the
//!   first mark since the last clear fires `notify_one`.
//! - **Consumer** calls [`DataSignal::clear`] after draining, then
//!   [`DataSignal::rearm_if_nonempty`] to cover the race where a
//!   producer pushes between clear and the next `notified().await`.

use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// Coalesced data-available notification.
#[derive(Debug)]
pub(crate) struct DataSignal {
    pending: AtomicBool,
    notify: Notify,
}

impl DataSignal {
    pub(crate) fn new() -> Self {
        Self {
            pending: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    /// Producer: mark data available.
    /// Wakes one waiter only on the false-to-true transition.
    #[inline]
    pub(crate) fn mark(&self) {
        if !self.pending.swap(true, Ordering::Release) {
            self.notify.notify_one();
        }
    }

    /// Consumer: clear the pending flag after draining.
    #[inline]
    pub(crate) fn clear(&self) {
        self.pending.store(false, Ordering::Release);
    }

    /// Consumer: if the source is non-empty after `clear()`, re-fire
    /// the notification to cover the race where a producer pushes
    /// between `clear` and the next `notified().await`.
    /// Returns `true` when this call fired a wake.
    #[inline]
    pub(crate) fn rearm_if_nonempty(&self, is_empty: bool) -> bool {
        if !is_empty
            && self
                .pending
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.notify.notify_one();
            true
        } else {
            false
        }
    }

    /// Consumer self-reschedule: fire `notify_one` unconditionally.
    ///
    /// Use when the consumer knows data remains (e.g. budget exhausted)
    /// and needs to wake itself on the next select iteration. Unlike
    /// `mark()`, this does not check `pending` because the consumer
    /// has not cleared it (the slot is non-empty).
    #[inline]
    pub(crate) fn reschedule(&self) {
        self.notify.notify_one();
    }

    /// Wake all waiters unconditionally. Shutdown / `mark_dead` path.
    pub(crate) fn wake_all(&self) {
        self.pending.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub(crate) fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::time::{Duration, timeout};

    use super::*;

    #[tokio::test]
    async fn first_mark_wakes() {
        let sig = Arc::new(DataSignal::new());
        let s = sig.clone();
        let handle =
            tokio::spawn(async move { timeout(Duration::from_secs(1), s.notified()).await });
        tokio::task::yield_now().await;
        sig.mark();
        assert!(handle.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn second_mark_coalesces() {
        let sig = Arc::new(DataSignal::new());
        sig.mark();
        sig.mark();
        let s = sig.clone();
        let handle =
            tokio::spawn(async move { timeout(Duration::from_secs(1), s.notified()).await });
        assert!(handle.await.unwrap().is_ok());

        sig.clear();
        let s2 = sig.clone();
        let handle2 =
            tokio::spawn(async move { timeout(Duration::from_millis(20), s2.notified()).await });
        assert!(
            handle2.await.unwrap().is_err(),
            "no wake after clear without new mark",
        );
    }

    #[tokio::test]
    async fn rearm_fires_when_nonempty() {
        let sig = Arc::new(DataSignal::new());
        sig.mark();
        let s = sig.clone();
        let _ = timeout(Duration::from_secs(1), s.notified()).await;
        sig.clear();
        assert!(sig.rearm_if_nonempty(false));

        let s2 = sig.clone();
        let handle =
            tokio::spawn(async move { timeout(Duration::from_secs(1), s2.notified()).await });
        assert!(handle.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn rearm_silent_when_empty() {
        let sig = Arc::new(DataSignal::new());
        sig.mark();
        let s = sig.clone();
        let _ = timeout(Duration::from_secs(1), s.notified()).await;
        sig.clear();
        assert!(!sig.rearm_if_nonempty(true));

        let s2 = sig.clone();
        let handle =
            tokio::spawn(async move { timeout(Duration::from_millis(20), s2.notified()).await });
        assert!(
            handle.await.unwrap().is_err(),
            "rearm with is_empty=true must not wake",
        );
    }

    #[tokio::test]
    async fn wake_all_wakes_multiple() {
        let sig = Arc::new(DataSignal::new());
        let mut handles = Vec::new();
        for _ in 0..3 {
            let s = sig.clone();
            handles.push(tokio::spawn(async move {
                timeout(Duration::from_secs(1), s.notified()).await
            }));
        }
        tokio::task::yield_now().await;
        sig.wake_all();
        for h in handles {
            assert!(h.await.unwrap().is_ok());
        }
    }
}

//! Async wrapper over the core SPSC ring.
//!
//! `AsyncProducer::flush()` wakes the consumer when the ring transitions
//! from empty to non-empty. `AsyncConsumer` implements `futures_core::Stream`.
//! No runtime dependency; works with any executor.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;
use futures_core::Stream;

use crate::{FlushResult, Padded, Ring};

struct AsyncRing<T> {
    ring: Ring<T>,
    waker: Padded<AtomicWaker>,
}

// SAFETY: AsyncRing<T> is Send because the inner Ring<T> is Send and
// AtomicWaker is Send+Sync.
unsafe impl<T: Send> Send for AsyncRing<T> {}
// SAFETY: AsyncRing<T> is Sync for the same reasons as Ring<T> (atomics +
// SPSC protocol for slot access) plus AtomicWaker which is Sync.
unsafe impl<T: Send> Sync for AsyncRing<T> {}

impl<T> Drop for AsyncRing<T> {
    fn drop(&mut self) {
        self.ring.drop_remaining();
        // Zero out counters so Ring::Drop is a no-op (no double-free).
        *self.ring.head.0.get_mut() = 0;
        *self.ring.flush.0.get_mut() = 0;
    }
}

/// Async sending half. Wakes the consumer on flush when the ring was empty.
pub struct AsyncProducer<T> {
    ring: Arc<AsyncRing<T>>,
    tail: usize,
    cached_head: usize,
}

// SAFETY: AsyncProducer<T> is Send because it is single-owner (not Sync) and
// the underlying AsyncRing is Send+Sync.
unsafe impl<T: Send> Send for AsyncProducer<T> {}

/// Async receiving half. Implements [`Stream`].
pub struct AsyncConsumer<T> {
    ring: Arc<AsyncRing<T>>,
    head: usize,
    cached_flush: usize,
}

// SAFETY: AsyncConsumer<T> is Send because it is single-owner (not Sync) and
// the underlying AsyncRing is Send+Sync.
unsafe impl<T: Send> Send for AsyncConsumer<T> {}

/// Create an async bounded SPSC ring with the given capacity (rounded up to
/// next power of two).
pub fn async_spsc<T>(capacity: usize) -> (AsyncProducer<T>, AsyncConsumer<T>) {
    let ring = Arc::new(AsyncRing {
        ring: Ring::new(capacity),
        waker: Padded(AtomicWaker::new()),
    });
    (
        AsyncProducer {
            ring: ring.clone(),
            tail: 0,
            cached_head: 0,
        },
        AsyncConsumer {
            ring,
            head: 0,
            cached_flush: 0,
        },
    )
}

impl<T> AsyncProducer<T> {
    /// Write a value to the ring. Zero atomics. Returns `Err(val)` if full.
    #[inline]
    pub fn push(&mut self, val: T) -> Result<(), T> {
        self.ring
            .ring
            .push(&mut self.tail, &mut self.cached_head, val)
    }

    /// Make all pushed items visible and wake the consumer if the ring was empty.
    #[inline]
    pub fn flush(&mut self) -> FlushResult {
        let r = self.ring.ring.flush_to(self.tail, &mut self.cached_head);
        if matches!(
            r,
            FlushResult::Flushed {
                was_empty: true,
                ..
            }
        ) {
            self.ring.waker.0.wake();
        }
        r
    }

    /// Push + flush in one call.
    #[inline]
    pub fn push_and_flush(&mut self, val: T) -> Result<FlushResult, T> {
        self.push(val)?;
        Ok(self.flush())
    }

    #[inline]
    pub fn is_full(&mut self) -> bool {
        self.ring.ring.is_full(self.tail, &mut self.cached_head)
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.ring.ring.capacity()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ring.ring.producer_len(self.tail)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ring.ring.producer_is_empty(self.tail)
    }
}

impl<T> AsyncConsumer<T> {
    /// Pop one item from the prefetched window. Zero atomics.
    /// Call [`release`](Self::release) after draining a batch.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        self.ring.ring.pop(&mut self.head, self.cached_flush)
    }

    /// Publish consumed position so the producer can reuse slots.
    #[inline]
    pub fn release(&mut self) {
        self.ring.ring.release(self.head);
    }

    /// Load all items flushed since the last prefetch. One Acquire load.
    #[inline]
    pub fn prefetch(&mut self) -> usize {
        self.ring.ring.prefetch(&mut self.cached_flush)
    }

    /// Prefetch + pop + release in one call.
    #[inline]
    pub fn prefetch_and_pop(&mut self) -> Option<T> {
        if self.head == self.cached_flush {
            self.prefetch();
        }
        let val = self.pop();
        if val.is_some() {
            self.release();
        }
        val
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ring
            .ring
            .consumer_is_empty(self.head, self.cached_flush)
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.ring.ring.capacity()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ring.ring.consumer_len(self.head)
    }
}

impl<T> Stream for AsyncConsumer<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if let Some(val) = this.prefetch_and_pop() {
            return Poll::Ready(Some(val));
        }

        this.ring.waker.0.register(cx.waker());

        // Re-check after registering to avoid lost wakes.
        if let Some(val) = this.prefetch_and_pop() {
            Poll::Ready(Some(val))
        } else if Arc::strong_count(&this.ring) == 1 {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl<T> Drop for AsyncConsumer<T> {
    fn drop(&mut self) {
        self.release();
    }
}

impl<T> Drop for AsyncProducer<T> {
    fn drop(&mut self) {
        self.flush();
        self.ring.waker.0.wake();
    }
}

impl<T> std::fmt::Debug for AsyncProducer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncProducer")
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}

impl<T> std::fmt::Debug for AsyncConsumer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncConsumer")
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use futures_lite::StreamExt;

    use super::*;

    #[test]
    fn async_push_pop() {
        let (mut p, mut c) = async_spsc::<u32>(4);
        p.push(1).unwrap();
        p.push(2).unwrap();
        assert!(c.prefetch_and_pop().is_none());
        p.flush();
        assert_eq!(c.prefetch_and_pop(), Some(1));
        assert_eq!(c.prefetch_and_pop(), Some(2));
    }

    #[test]
    fn stream_impl() {
        futures_lite::future::block_on(async {
            let (mut p, mut c) = async_spsc::<u32>(8);
            p.push(10).unwrap();
            p.push(20).unwrap();
            p.push(30).unwrap();
            p.flush();

            assert_eq!(c.next().await, Some(10));
            assert_eq!(c.next().await, Some(20));
            assert_eq!(c.next().await, Some(30));
        });
    }

    #[test]
    fn stream_wakes_on_flush() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let (mut p, mut c) = async_spsc::<u32>(8);
        let done = Arc::new(AtomicBool::new(false));
        let done2 = done.clone();

        let handle = std::thread::spawn(move || {
            futures_lite::future::block_on(async {
                let val = c.next().await;
                done2.store(true, Ordering::Release);
                val
            })
        });

        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(!done.load(Ordering::Acquire));

        p.push(42).unwrap();
        p.flush();

        let val = handle.join().unwrap();
        assert_eq!(val, Some(42));
    }

    #[test]
    fn cross_thread_stream() {
        let (mut p, c) = async_spsc::<u64>(1024);
        let n = 50_000u64;

        let receiver = std::thread::spawn(move || {
            futures_lite::future::block_on(async {
                futures_lite::pin!(c);
                let mut received = 0u64;
                while let Some(v) = c.next().await {
                    assert_eq!(v, received);
                    received += 1;
                    if received == n {
                        break;
                    }
                }
                received
            })
        });

        for i in 0..n {
            while p.push(i).is_err() {
                p.flush();
                std::thread::yield_now();
            }
            if i % 64 == 63 {
                p.flush();
            }
        }
        p.flush();

        let count = receiver.join().unwrap();
        assert_eq!(count, n);
    }

    #[test]
    fn alternating_push_pop_wakes() {
        use std::sync::mpsc;

        let (mut p, c) = async_spsc::<u32>(8);
        let (tx, rx) = mpsc::sync_channel::<u32>(0);

        let handle = std::thread::spawn(move || {
            futures_lite::future::block_on(async {
                futures_lite::pin!(c);
                for _ in 0..5 {
                    let val = c.next().await.unwrap();
                    tx.send(val).unwrap();
                }
            });
        });

        for i in 0..5 {
            p.push_and_flush(i).unwrap();
            let val = rx.recv_timeout(std::time::Duration::from_secs(3)).unwrap();
            assert_eq!(val, i);
        }

        handle.join().unwrap();
    }
}

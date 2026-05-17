//! Async wrapper over the core SPSC ring.
//!
//! `AsyncProducer::flush()` wakes the consumer when the ring transitions
//! from empty to non-empty. `AsyncConsumer` implements `futures_core::Stream`.
//! No runtime dependency; works with any executor.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;
use futures_core::Stream;

use crate::{FlushResult, Padded, Ring};

struct AsyncRing<T> {
    ring: Ring<T>,
    waker: Padded<AtomicWaker>,
}

unsafe impl<T: Send> Send for AsyncRing<T> {}
unsafe impl<T: Send> Sync for AsyncRing<T> {}

impl<T> Drop for AsyncRing<T> {
    fn drop(&mut self) {
        let head = *self.ring.head.0.get_mut();
        let flush = *self.ring.flush.0.get_mut();
        for i in head..flush {
            unsafe {
                self.ring.buf[i & self.ring.mask]
                    .get_mut()
                    .assume_init_drop();
            }
        }
        // Zero out the ring's counters so its own Drop doesn't double-free.
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

unsafe impl<T: Send> Send for AsyncProducer<T> {}

/// Async receiving half. Implements [`Stream`].
pub struct AsyncConsumer<T> {
    ring: Arc<AsyncRing<T>>,
    head: usize,
    cached_flush: usize,
}

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
        if self.tail - self.cached_head >= self.ring.ring.capacity() {
            self.cached_head = self.ring.ring.head.0.load(Ordering::Acquire);
            if self.tail - self.cached_head >= self.ring.ring.capacity() {
                return Err(val);
            }
        }
        unsafe {
            (*self.ring.ring.buf[self.tail & self.ring.ring.mask].get()).write(val);
        }
        self.tail += 1;
        Ok(())
    }

    /// Make all pushed items visible and wake the consumer if the ring was empty.
    #[inline]
    pub fn flush(&mut self) -> FlushResult {
        let prev_flush = self.ring.ring.flush.0.load(Ordering::Relaxed);
        if self.tail == prev_flush {
            return FlushResult::NothingToFlush;
        }
        let count = self.tail - prev_flush;
        let was_empty = prev_flush == self.cached_head;
        self.ring.ring.flush.0.store(self.tail, Ordering::Release);
        if was_empty {
            self.ring.waker.0.wake();
        }
        FlushResult::Flushed { count, was_empty }
    }

    /// Push + flush in one call.
    #[inline]
    pub fn push_and_flush(&mut self, val: T) -> Result<FlushResult, T> {
        self.push(val)?;
        Ok(self.flush())
    }

    #[inline]
    pub fn is_full(&mut self) -> bool {
        if self.tail - self.cached_head >= self.ring.ring.capacity() {
            self.cached_head = self.ring.ring.head.0.load(Ordering::Acquire);
            self.tail - self.cached_head >= self.ring.ring.capacity()
        } else {
            false
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.ring.ring.capacity()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.tail
            .wrapping_sub(self.ring.ring.head.0.load(Ordering::Acquire))
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tail == self.ring.ring.head.0.load(Ordering::Acquire)
    }
}

impl<T> AsyncConsumer<T> {
    /// Pop one item from the prefetched window. Zero atomics.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.head == self.cached_flush {
            return None;
        }
        let val = unsafe {
            (*self.ring.ring.buf[self.head & self.ring.ring.mask].get()).assume_init_read()
        };
        self.head += 1;
        self.ring.ring.head.0.store(self.head, Ordering::Release);
        Some(val)
    }

    /// Load all items flushed since the last prefetch. One Acquire load.
    #[inline]
    pub fn prefetch(&mut self) -> usize {
        let new_flush = self.ring.ring.flush.0.load(Ordering::Acquire);
        let count = new_flush.wrapping_sub(self.cached_flush);
        self.cached_flush = new_flush;
        count
    }

    /// Prefetch + pop in one call.
    #[inline]
    pub fn prefetch_and_pop(&mut self) -> Option<T> {
        if self.head == self.cached_flush {
            self.prefetch();
        }
        self.pop()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head == self.cached_flush
            && self.ring.ring.flush.0.load(Ordering::Acquire) == self.head
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.ring.ring.capacity()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ring
            .ring
            .flush
            .0
            .load(Ordering::Acquire)
            .wrapping_sub(self.head)
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
        } else {
            Poll::Pending
        }
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
}

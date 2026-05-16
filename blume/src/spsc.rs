//! Lock-free bounded SPSC ring with ypipe-style batched flush/prefetch.
//!
//! Three pointers:
//! - `head`: consumer read position (`AtomicUsize`, consumer-owned)
//! - `tail`: writer position (plain usize, producer-only, no atomic)
//! - `flush`: last flushed position (`AtomicUsize`, producer writes, consumer reads)
//!
//! `push` writes to the ring with zero atomics. `flush` makes all
//! pending writes visible with one Release store. `pop` reads with
//! zero atomics. `prefetch` loads all flushed items with one Acquire
//! load. Result: 1 atomic per batch on each side.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[repr(align(64))]
struct Padded<T>(T);

struct Ring<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    /// Consumer read position. Written by consumer, read by producer.
    head: Padded<AtomicUsize>,
    /// Last flushed position. Written by producer, read by consumer.
    flush: Padded<AtomicUsize>,
}

unsafe impl<T: Send> Send for Ring<T> {}
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T> Ring<T> {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");
        let cap = capacity.next_power_of_two();
        let buf: Vec<UnsafeCell<MaybeUninit<T>>> =
            (0..cap).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect();
        Self {
            buf: buf.into_boxed_slice(),
            mask: cap - 1,
            head: Padded(AtomicUsize::new(0)),
            flush: Padded(AtomicUsize::new(0)),
        }
    }

    fn capacity(&self) -> usize {
        self.mask + 1
    }
}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        let head = *self.head.0.get_mut();
        let flush = *self.flush.0.get_mut();
        for i in head..flush {
            unsafe {
                self.buf[i & self.mask].get_mut().assume_init_drop();
            }
        }
    }
}

/// Result of a flush operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushResult {
    /// Items were flushed. The consumer may have been idle and needs waking.
    Flushed { count: usize, was_empty: bool },
    /// Nothing to flush (tail == flush already).
    NothingToFlush,
}

/// Sending half. `Send` but not `Sync`.
pub struct Producer<T> {
    ring: Arc<Ring<T>>,
    /// Private write position. No atomic; only the producer touches it.
    tail: usize,
    /// Cached copy of the consumer's `head` to avoid Acquire loads.
    cached_head: usize,
}

impl<T> Producer<T> {
    pub fn ring_addr(&self) -> usize {
        Arc::as_ptr(&self.ring) as usize
    }
}

unsafe impl<T: Send> Send for Producer<T> {}

/// Receiving half. `Send` but not `Sync`.
pub struct Consumer<T> {
    ring: Arc<Ring<T>>,
    /// Private read position. Only the consumer touches it.
    head: usize,
    /// Cached copy of `flush`. Updated by `prefetch()`.
    cached_flush: usize,
}

impl<T> Consumer<T> {
    pub fn ring_addr(&self) -> usize {
        Arc::as_ptr(&self.ring) as usize
    }
}

unsafe impl<T: Send> Send for Consumer<T> {}

/// Create a bounded SPSC ring with the given capacity (rounded up to
/// next power of two).
pub fn spsc<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let ring = Arc::new(Ring::new(capacity));
    (
        Producer { ring: ring.clone(), tail: 0, cached_head: 0 },
        Consumer { ring, head: 0, cached_flush: 0 },
    )
}

impl<T> Producer<T> {
    /// Write a value to the ring. Zero atomics. Returns `Err(val)` if full.
    /// The value is NOT visible to the consumer until [`flush`](Self::flush).
    #[inline]
    pub fn push(&mut self, val: T) -> Result<(), T> {
        if self.tail - self.cached_head >= self.ring.capacity() {
            self.cached_head = self.ring.head.0.load(Ordering::Acquire);
            if self.tail - self.cached_head >= self.ring.capacity() {
                return Err(val);
            }
        }
        unsafe {
            (*self.ring.buf[self.tail & self.ring.mask].get()).write(val);
        }
        self.tail += 1;
        Ok(())
    }

    /// Make all pushed items visible to the consumer. One Release store.
    #[inline]
    pub fn flush(&mut self) -> FlushResult {
        let prev_flush = self.ring.flush.0.load(Ordering::Relaxed);
        if self.tail == prev_flush {
            return FlushResult::NothingToFlush;
        }
        let count = self.tail - prev_flush;
        let was_empty = prev_flush == self.cached_head;
        self.ring.flush.0.store(self.tail, Ordering::Release);
        FlushResult::Flushed { count, was_empty }
    }

    /// Push + flush in one call (convenience for single-item sends).
    #[inline]
    pub fn push_and_flush(&mut self, val: T) -> Result<FlushResult, T> {
        self.push(val)?;
        Ok(self.flush())
    }

    #[inline]
    pub fn is_full(&mut self) -> bool {
        if self.tail - self.cached_head >= self.ring.capacity() {
            self.cached_head = self.ring.head.0.load(Ordering::Acquire);
            self.tail - self.cached_head >= self.ring.capacity()
        } else {
            false
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.tail.wrapping_sub(self.ring.head.0.load(Ordering::Acquire))
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tail == self.ring.head.0.load(Ordering::Acquire)
    }
}

impl<T> Consumer<T> {
    /// Pop one item. Zero atomics; reads from the prefetched window.
    /// Returns `None` when the prefetched window is exhausted. Call
    /// [`prefetch`](Self::prefetch) to load newly flushed items.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.head == self.cached_flush {
            return None;
        }
        let val = unsafe {
            (*self.ring.buf[self.head & self.ring.mask].get()).assume_init_read()
        };
        self.head += 1;
        // Publish consumed position so producer can reuse slots.
        self.ring.head.0.store(self.head, Ordering::Release);
        Some(val)
    }

    /// Load all items flushed since the last prefetch. One Acquire load.
    /// Returns the count of newly available items.
    #[inline]
    pub fn prefetch(&mut self) -> usize {
        let new_flush = self.ring.flush.0.load(Ordering::Acquire);
        let count = new_flush.wrapping_sub(self.cached_flush);
        self.cached_flush = new_flush;
        count
    }

    /// Convenience: prefetch + pop. For callers that don't need batching.
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
            && self.ring.flush.0.load(Ordering::Acquire) == self.head
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ring.flush.0.load(Ordering::Acquire).wrapping_sub(self.head)
    }
}

impl<T> std::fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}

impl<T> std::fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_pop_basic() {
        let (mut p, mut c) = spsc::<u32>(4);
        assert!(c.prefetch_and_pop().is_none());
        p.push(1).unwrap();
        p.push(2).unwrap();
        // Not visible yet.
        assert!(c.prefetch_and_pop().is_none());
        p.flush();
        assert_eq!(c.prefetch_and_pop(), Some(1));
        assert_eq!(c.prefetch_and_pop(), Some(2));
        assert!(c.prefetch_and_pop().is_none());
    }

    #[test]
    fn push_and_flush() {
        let (mut p, mut c) = spsc::<u32>(4);
        p.push_and_flush(42).unwrap();
        assert_eq!(c.prefetch_and_pop(), Some(42));
    }

    #[test]
    fn batch_prefetch() {
        let (mut p, mut c) = spsc::<u32>(8);
        for i in 0..5 {
            p.push(i).unwrap();
        }
        assert_eq!(c.prefetch(), 0); // not flushed yet
        p.flush();
        assert_eq!(c.prefetch(), 5);
        for i in 0..5 {
            assert_eq!(c.pop(), Some(i));
        }
        assert!(c.pop().is_none());
    }

    #[test]
    fn flush_reports_was_empty() {
        let (mut p, mut c) = spsc::<u32>(4);
        p.push(1).unwrap();
        let r = p.flush();
        assert_eq!(r, FlushResult::Flushed { count: 1, was_empty: true });

        p.push(2).unwrap();
        let r = p.flush();
        // Consumer hasn't read yet, so was_empty depends on cached_head
        assert!(matches!(r, FlushResult::Flushed { count: 1, .. }));

        c.prefetch_and_pop();
        c.prefetch_and_pop();
        p.push(3).unwrap();
        // Force head cache refresh
        p.push(4).unwrap();
        let _ = p.push(5);
        let _ = p.push(6);
        // After consumer consumed, the producer might see stale cached_head
        let r = p.flush();
        assert!(matches!(r, FlushResult::Flushed { .. }));
    }

    #[test]
    fn full_ring() {
        let (mut p, mut c) = spsc::<u32>(4);
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert!(p.push(99).is_err());
        p.flush();
        assert_eq!(c.prefetch_and_pop(), Some(0));
        p.push(99).unwrap();
        p.flush();
        for i in 1..=4 {
            let expected = if i < 4 { i } else { 99 };
            assert_eq!(c.prefetch_and_pop(), Some(expected));
        }
    }

    #[test]
    fn wraps_around() {
        let (mut p, mut c) = spsc::<u32>(2);
        for round in 0..100 {
            p.push(round * 2).unwrap();
            p.push(round * 2 + 1).unwrap();
            p.flush();
            assert_eq!(c.prefetch_and_pop(), Some(round * 2));
            assert_eq!(c.prefetch_and_pop(), Some(round * 2 + 1));
        }
    }

    #[test]
    fn capacity_rounds_up() {
        let (p, _c) = spsc::<u8>(3);
        assert_eq!(p.capacity(), 4);
        let (p, _c) = spsc::<u8>(5);
        assert_eq!(p.capacity(), 8);
        let (p, _c) = spsc::<u8>(1);
        assert_eq!(p.capacity(), 1);
    }

    #[test]
    fn drop_remaining() {
        use std::sync::atomic::AtomicUsize;
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        #[derive(Debug)]
        struct Counted;
        impl Drop for Counted {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }
        DROPS.store(0, Ordering::Relaxed);
        let (mut p, c) = spsc::<Counted>(4);
        p.push(Counted).unwrap();
        p.push(Counted).unwrap();
        p.push(Counted).unwrap();
        p.flush();
        drop(p);
        drop(c);
        assert_eq!(DROPS.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn cross_thread() {
        let (mut p, mut c) = spsc::<u64>(1024);
        let n = 100_000u64;
        let sender = std::thread::spawn(move || {
            for i in 0..n {
                while p.push(i).is_err() {
                    p.flush();
                    std::thread::yield_now();
                }
                p.flush();
            }
        });
        let mut received = 0u64;
        while received < n {
            if c.prefetch() > 0 {
                while let Some(v) = c.pop() {
                    assert_eq!(v, received);
                    received += 1;
                }
            } else {
                std::thread::yield_now();
            }
        }
        sender.join().unwrap();
    }
}

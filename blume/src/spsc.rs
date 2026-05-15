//! Lock-free bounded SPSC (single-producer, single-consumer) ring buffer.
//!
//! No CAS operations: the producer exclusively owns the `tail` index and
//! the consumer exclusively owns the `head` index. Each side reads the
//! other's index with `Acquire` and writes its own with `Release`. This
//! Acquire/Release pair is the full synchronization contract.
//!
//! Head and tail live on separate cache lines to avoid false sharing.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[repr(align(64))]
struct Padded<T>(T);

struct Ring<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    head: Padded<AtomicUsize>,
    tail: Padded<AtomicUsize>,
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
            tail: Padded(AtomicUsize::new(0)),
        }
    }

    fn capacity(&self) -> usize {
        self.mask + 1
    }
}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        let head = *self.head.0.get_mut();
        let tail = *self.tail.0.get_mut();
        for i in head..tail {
            unsafe {
                self.buf[i & self.mask].get_mut().assume_init_drop();
            }
        }
    }
}

/// Sending half of an SPSC ring. `Send` but not `Sync`: only one thread
/// may push at a time.
pub struct Producer<T> {
    ring: Arc<Ring<T>>,
    cached_head: usize,
}

impl<T> Producer<T> {
    /// Address of the shared ring (for debug identity checks).
    pub fn ring_addr(&self) -> usize {
        Arc::as_ptr(&self.ring) as usize
    }
}

unsafe impl<T: Send> Send for Producer<T> {}

/// Receiving half of an SPSC ring. `Send` but not `Sync`: only one
/// thread may pop at a time.
pub struct Consumer<T> {
    ring: Arc<Ring<T>>,
    cached_tail: usize,
}

impl<T> Consumer<T> {
    /// Address of the shared ring (for debug identity checks).
    pub fn ring_addr(&self) -> usize {
        Arc::as_ptr(&self.ring) as usize
    }
}

unsafe impl<T: Send> Send for Consumer<T> {}

/// Create a bounded SPSC ring with the given capacity (rounded up to
/// next power of two). Returns the producer and consumer halves.
pub fn spsc<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let ring = Arc::new(Ring::new(capacity));
    (
        Producer { ring: ring.clone(), cached_head: 0 },
        Consumer { ring, cached_tail: 0 },
    )
}

impl<T> Producer<T> {
    /// Try to push a value. Returns `Err(val)` if full.
    #[inline]
    pub fn push(&mut self, val: T) -> Result<(), T> {
        let tail = self.ring.tail.0.load(Ordering::Relaxed);
        if tail - self.cached_head >= self.ring.capacity() {
            self.cached_head = self.ring.head.0.load(Ordering::Acquire);
            if tail - self.cached_head >= self.ring.capacity() {
                return Err(val);
            }
        }
        unsafe {
            (*self.ring.buf[tail & self.ring.mask].get()).write(val);
        }
        self.ring.tail.0.store(tail + 1, Ordering::Release);
        Ok(())
    }

    /// Number of items currently in the ring (approximate).
    #[inline]
    pub fn len(&self) -> usize {
        let tail = self.ring.tail.0.load(Ordering::Relaxed);
        let head = self.ring.head.0.load(Ordering::Acquire);
        tail.wrapping_sub(head)
    }

    /// True when the ring has no items.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// True when the ring is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() >= self.ring.capacity()
    }

    /// The capacity of the ring.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }
}

impl<T> Consumer<T> {
    /// Try to pop a value. Returns `None` if empty.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        let head = self.ring.head.0.load(Ordering::Relaxed);
        if self.cached_tail == head {
            self.cached_tail = self.ring.tail.0.load(Ordering::Acquire);
            if self.cached_tail == head {
                return None;
            }
        }
        let val = unsafe {
            (*self.ring.buf[head & self.ring.mask].get()).assume_init_read()
        };
        self.ring.head.0.store(head + 1, Ordering::Release);
        Some(val)
    }

    /// Number of items currently in the ring (approximate).
    #[inline]
    pub fn len(&self) -> usize {
        let tail = self.ring.tail.0.load(Ordering::Acquire);
        let head = self.ring.head.0.load(Ordering::Relaxed);
        tail.wrapping_sub(head)
    }

    /// True when the ring has no items.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The capacity of the ring.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }
}

impl<T> std::fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .finish()
    }
}

impl<T> std::fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_pop_basic() {
        let (mut p, mut c) = spsc::<u32>(4);
        assert!(c.pop().is_none());
        p.push(1).unwrap();
        p.push(2).unwrap();
        assert_eq!(c.pop(), Some(1));
        assert_eq!(c.pop(), Some(2));
        assert!(c.pop().is_none());
    }

    #[test]
    fn full_ring() {
        let (mut p, mut c) = spsc::<u32>(4);
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert!(p.push(99).is_err());
        assert_eq!(c.pop(), Some(0));
        p.push(99).unwrap();
        for i in 1..=4 {
            let expected = if i < 4 { i } else { 99 };
            assert_eq!(c.pop(), Some(expected));
        }
    }

    #[test]
    fn wraps_around() {
        let (mut p, mut c) = spsc::<u32>(2);
        for round in 0..100 {
            p.push(round * 2).unwrap();
            p.push(round * 2 + 1).unwrap();
            assert_eq!(c.pop(), Some(round * 2));
            assert_eq!(c.pop(), Some(round * 2 + 1));
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
                    std::thread::yield_now();
                }
            }
        });
        let mut received = 0u64;
        while received < n {
            if let Some(v) = c.pop() {
                assert_eq!(v, received);
                received += 1;
            } else {
                std::thread::yield_now();
            }
        }
        sender.join().unwrap();
    }
}

use std::cell::{Cell, UnsafeCell};

use omq_proto::encoded_queue::EncodedQueue;

/// Single-threaded interior mutability without runtime overhead.
///
/// Wraps `UnsafeCell<T>` behind a safe `get(&self) -> &mut T` accessor.
/// Sound only when all access is confined to a single thread, which is
/// guaranteed by compio's cooperative single-threaded runtime.
///
/// In debug builds, a thread-ID check catches accidental cross-thread access.
pub(crate) struct LocalCell<T> {
    inner: UnsafeCell<T>,
    #[cfg(debug_assertions)]
    owner: std::thread::ThreadId,
}

// SAFETY: compio is single-threaded cooperative. All access happens on
// the runtime thread that created the cell, so no concurrent mutation is
// possible. In debug builds, every `get()` call asserts the current thread
// matches the creating thread. In release builds this assertion is compiled
// out for zero overhead. The type is `pub(crate)` and only used inside
// compio driver internals. Callers must not move a LocalCell to another thread.
unsafe impl<T: Send> Sync for LocalCell<T> {}

impl<T> LocalCell<T> {
    pub(crate) fn new(val: T) -> Self {
        Self {
            inner: UnsafeCell::new(val),
            #[cfg(debug_assertions)]
            owner: std::thread::current().id(),
        }
    }

    // Callers must not hold overlapping borrows. The cooperative runtime
    // prevents preemption, so this only happens if get() is called while
    // a previous &mut T is still live in the same call stack. A guard-based
    // API (like EncodedQueueCell) would enforce this statically but would
    // require changing all call sites.
    #[inline]
    #[expect(clippy::mut_from_ref)]
    pub(crate) fn get(&self) -> &mut T {
        #[cfg(debug_assertions)]
        debug_assert_eq!(
            std::thread::current().id(),
            self.owner,
            "LocalCell accessed from wrong thread"
        );
        // SAFETY: compio's single-threaded runtime guarantees no
        // concurrent access. The debug_assert above catches misuse
        // in test/debug builds.
        unsafe { &mut *self.inner.get() }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for LocalCell<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalCell").finish_non_exhaustive()
    }
}

/// Non-atomic interior-mutable wrapper for `EncodedQueue`.
///
/// Sound only on a single thread (compio's cooperative runtime).
/// Replaces `Mutex<EncodedQueue>` to avoid atomic CAS on every
/// lock/unlock in the send hot path.
pub(crate) struct EncodedQueueCell {
    borrowed: Cell<bool>,
    inner: UnsafeCell<EncodedQueue>,
}

impl EncodedQueueCell {
    pub(crate) fn with_arena_threshold(arena_threshold: usize) -> Self {
        Self {
            borrowed: Cell::new(false),
            inner: UnsafeCell::new(EncodedQueue::with_arena_threshold(arena_threshold)),
        }
    }

    #[inline]
    pub(crate) fn try_borrow_mut(&self) -> Option<EncodedQueueGuard<'_>> {
        if self.borrowed.get() {
            return None;
        }
        self.borrowed.set(true);
        Some(EncodedQueueGuard { cell: self })
    }

    #[inline]
    pub(crate) fn borrow_mut(&self) -> EncodedQueueGuard<'_> {
        assert!(!self.borrowed.get(), "EncodedQueueCell: already borrowed");
        self.borrowed.set(true);
        EncodedQueueGuard { cell: self }
    }
}

pub(crate) struct EncodedQueueGuard<'a> {
    cell: &'a EncodedQueueCell,
}

impl std::ops::Deref for EncodedQueueGuard<'_> {
    type Target = EncodedQueue;

    #[inline]
    fn deref(&self) -> &EncodedQueue {
        // SAFETY: the borrow flag prevents concurrent access. The
        // guard's lifetime is bounded by the cell's, so the pointer
        // remains valid.
        unsafe { &*self.cell.inner.get() }
    }
}

impl std::ops::DerefMut for EncodedQueueGuard<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut EncodedQueue {
        // SAFETY: &mut self guarantees exclusive guard access. The
        // borrow flag prevents a second guard from being created.
        unsafe { &mut *self.cell.inner.get() }
    }
}

impl Drop for EncodedQueueGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.cell.borrowed.set(false);
    }
}

#[expect(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for EncodedQueueCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedQueueCell")
            .field("borrowed", &self.borrowed.get())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_cell_mutates_in_place() {
        let cell = LocalCell::new(vec![1_u8]);
        cell.get().push(2);
        assert_eq!(cell.get().as_slice(), &[1, 2]);
    }

    #[test]
    fn encoded_queue_cell_rejects_overlapping_borrow() {
        let cell = EncodedQueueCell::with_arena_threshold(1024);
        let _guard = cell.borrow_mut();
        assert!(cell.try_borrow_mut().is_none());
    }

    #[test]
    fn encoded_queue_cell_releases_borrow_on_drop() {
        let cell = EncodedQueueCell::with_arena_threshold(1024);
        {
            let mut guard = cell.borrow_mut();
            guard.push_pre_encoded(b"abc");
            assert_eq!(guard.total_bytes(), 3);
        }
        assert!(cell.try_borrow_mut().is_some());
    }

    #[test]
    fn encoded_queue_cell_releases_borrow_during_panic() {
        let cell = EncodedQueueCell::with_arena_threshold(1024);
        let old_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = cell.borrow_mut();
            panic!("drop guard during unwind");
        }));
        std::panic::set_hook(old_hook);

        assert!(result.is_err());
        assert!(cell.try_borrow_mut().is_some());
    }

    #[test]
    fn encoded_queue_cell_releases_borrow_when_guard_is_moved() {
        let cell = EncodedQueueCell::with_arena_threshold(1024);
        let guard = cell.borrow_mut();
        let moved = Some(guard);
        drop(moved);

        assert!(cell.try_borrow_mut().is_some());
    }
}

use std::cell::UnsafeCell;

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

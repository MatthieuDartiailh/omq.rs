use std::cell::UnsafeCell;

/// Single-threaded interior mutability without runtime overhead.
///
/// Wraps `UnsafeCell<T>` behind a safe `get(&self) -> &mut T` accessor.
/// Sound because the ZMQ C API contract requires that each socket is
/// accessed from at most one thread at a time.
///
/// In debug builds, a thread-ID check catches accidental cross-thread access.
pub(crate) struct LocalCell<T> {
    inner: UnsafeCell<T>,
    #[cfg(debug_assertions)]
    owner: std::thread::ThreadId,
}

// SAFETY: ZMQ's C API contract guarantees single-threaded socket
// access. No concurrent mutation is possible under correct usage.
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
        // SAFETY: ZMQ's single-threaded socket contract guarantees
        // no concurrent access to these fields.
        unsafe { &mut *self.inner.get() }
    }
}

impl<T> std::fmt::Debug for LocalCell<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalCell").finish_non_exhaustive()
    }
}

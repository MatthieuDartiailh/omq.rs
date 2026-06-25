use std::cell::UnsafeCell;

/// Single-threaded interior mutability without runtime overhead.
///
/// Wraps `UnsafeCell<T>` behind a safe `get(&self) -> &mut T` accessor.
/// Sound because the ZMQ C API contract requires that each socket is
/// accessed from at most one thread at a time.
pub(crate) struct LocalCell<T>(UnsafeCell<T>);

// SAFETY: ZMQ's C API contract guarantees single-threaded socket
// access. No concurrent mutation is possible under correct usage.
unsafe impl<T: Send> Sync for LocalCell<T> {}

impl<T> LocalCell<T> {
    pub(crate) fn new(val: T) -> Self {
        Self(UnsafeCell::new(val))
    }

    #[inline]
    #[expect(clippy::mut_from_ref)]
    pub(crate) fn get(&self) -> &mut T {
        // SAFETY: ZMQ's single-threaded socket contract guarantees
        // no concurrent access to these fields.
        unsafe { &mut *self.0.get() }
    }
}

impl<T> std::fmt::Debug for LocalCell<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalCell").finish_non_exhaustive()
    }
}

//! Compatibility shim: swap std types for loom types under `cfg(loom)`.

#[cfg(loom)]
pub(crate) use loom::cell::UnsafeCell;
#[cfg(not(loom))]
pub(crate) use std::cell::UnsafeCell;

#[cfg(loom)]
pub(crate) use loom::sync::Arc;
#[cfg(not(loom))]
pub(crate) use std::sync::Arc;

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[cfg(not(loom))]
pub(crate) trait UnsafeCellExt<T> {
    fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R;
}

#[cfg(not(loom))]
impl<T> UnsafeCellExt<T> for std::cell::UnsafeCell<T> {
    #[inline]
    fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.get())
    }
}

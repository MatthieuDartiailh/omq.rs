//! Runtime-builder helpers for sizing compio's `BUF_RING` buffer pool.
//!
//! omq-compio drives recv via compio's multi-shot recv with provided
//! buffers (`BUF_RING`). The pool is per-runtime, sized once at builder
//! time, and serves every connection on that runtime. compio's default
//! is 8 buffers x 8 KiB, which is too small for non-trivial workloads:
//! each in-flight CQE occupies a slot, and a single gigabit TCP burst
//! can leave ~200 KiB queued in the kernel's recv buffer between
//! consumer drains.
//!
//! Apply [`ProactorBuilderExt::with_omq_buffer_pool`] before passing the
//! `ProactorBuilder` to [`compio::runtime::RuntimeBuilder::with_proactor`]
//! to use omq-compio's recommended sizing (128 buffers x 32 KiB =
//! 4 MiB per runtime). For high-fan-out `PUB` or 10 `GbE` deployments,
//! call [`ProactorBuilderExt::with_omq_buffer_pool_sized`] instead.
//!
//! Example:
//!
//! ```no_run
//! use compio::driver::ProactorBuilder;
//! use compio::runtime::RuntimeBuilder;
//! use omq_compio::ProactorBuilderExt;
//!
//! let mut proactor = ProactorBuilder::new();
//! proactor.with_omq_buffer_pool();
//! let runtime = RuntimeBuilder::new()
//!     .with_proactor(proactor)
//!     .build()
//!     .expect("build runtime");
//! ```
//!
//! Requires Linux >= 6.0 (multi-shot recv with provided buffers).

use std::num::NonZero;

use compio::driver::ProactorBuilder;

/// Number of buffers in the `BUF_RING` pool used by omq-compio's recv path.
pub const DEFAULT_BUFFER_POOL_COUNT: u16 = 128;

/// Length in bytes of each buffer in the `BUF_RING` pool.
pub const DEFAULT_BUFFER_POOL_LEN: usize = 32 * 1024;

/// Extension methods on [`compio::driver::ProactorBuilder`] for
/// configuring the `BUF_RING` pool that omq-compio's recv path consumes.
pub trait ProactorBuilderExt: sealed::Sealed {
    /// Apply omq-compio's recommended buffer-pool sizing
    /// ([`DEFAULT_BUFFER_POOL_COUNT`] buffers x [`DEFAULT_BUFFER_POOL_LEN`] bytes).
    fn with_omq_buffer_pool(&mut self) -> &mut Self;

    /// Apply explicit buffer-pool sizing. `count` is rounded up to the
    /// next power of two by compio.
    fn with_omq_buffer_pool_sized(&mut self, count: NonZero<u16>, len: usize) -> &mut Self;
}

impl ProactorBuilderExt for ProactorBuilder {
    fn with_omq_buffer_pool(&mut self) -> &mut Self {
        let count = NonZero::new(DEFAULT_BUFFER_POOL_COUNT).expect("nonzero default count");
        self.with_omq_buffer_pool_sized(count, DEFAULT_BUFFER_POOL_LEN)
    }

    fn with_omq_buffer_pool_sized(&mut self, count: NonZero<u16>, len: usize) -> &mut Self {
        self.buffer_pool_size(count).buffer_pool_buffer_len(len)
    }
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for compio::driver::ProactorBuilder {}
}

/// Build a [`compio::runtime::Runtime`] with omq-compio's recommended
/// `BUF_RING` pool sizing applied. Equivalent to constructing a
/// `RuntimeBuilder`, calling [`ProactorBuilderExt::with_omq_buffer_pool`]
/// on a fresh `ProactorBuilder`, and invoking
/// [`compio::runtime::RuntimeBuilder::with_proactor`] before `build`.
pub fn build_default_runtime() -> std::io::Result<compio::runtime::Runtime> {
    let mut proactor = ProactorBuilder::new();
    proactor.with_omq_buffer_pool();
    compio::runtime::RuntimeBuilder::new()
        .with_proactor(proactor)
        .build()
}

//! Runtime-builder helpers for sizing compio's `BUF_RING` buffer pool.
//!
//! omq-compio drives recv via io_uring multi-shot recv with provided
//! buffers (`BUF_RING`). The pool is per-runtime, shared by every
//! connection on that runtime, and sized once at builder time.
//! compio's own default (8 buffers x 8 KiB) is too small for
//! non-trivial workloads.
//!
//! Apply [`ProactorBuilderExt::with_omq_buffer_pool`] to use
//! omq-compio's recommended sizing (64 buffers x 64 KiB = 4 MiB per
//! runtime). For high-fan-out deployments, call
//! [`ProactorBuilderExt::with_omq_buffer_pool_sized`] instead.
//!
//! ## How the pool interacts with recv
//!
//! **Small messages** (below [`Options::large_message_threshold`],
//! default 128 KiB) stay on the multi-shot path: one
//! `copy_from_slice` per CQE into `Bytes`, one zero-copy
//! `Bytes::slice` out of the codec. Total: 1 memcpy per message.
//!
//! **Large messages** (above the threshold) use the accumulation
//! path: subsequent CQEs are copied directly from the buffer into a
//! single pre-allocated `BytesMut`, bypassing the codec's chunked
//! buffer. If the payload exceeds the pool capacity, the kernel
//! exhausts the pool and terminates the multi-shot SQE with
//! `ENOBUFS`. The recv path transitions to one-shot mode and a
//! single `read_until` pulls the remaining bytes in one syscall.
//! Consecutive large messages stay in one-shot mode with zero
//! pool involvement. A small message re-arms multi-shot.
//!
//! Any message size works with any pool configuration. The pool
//! affects only `ENOBUFS` frequency on small-message bursts — large
//! messages transition to one-shot automatically.
//!
//! ## Pool sizing
//!
//! The pool absorbs small-message bursts. The kernel fills all
//! available buffers in a single `io_uring_enter` cycle; if the pool
//! is exhausted, the multi-shot SQE terminates with `ENOBUFS` and
//! must be re-armed. More buffers = more burst capacity before
//! rearm. The default 64 buffers handles ~10-20 concurrent
//! connections without rearm pressure.
//!
//! The pool does **not** need to be sized for large messages. A
//! 2 GiB message works fine with the default 4 MiB pool — the
//! first 4 MiB is accumulated from CQEs, then `ENOBUFS` triggers
//! a one-shot `read_until` for the remaining ~2 GiB.
//!
//! | Scenario | Recommended call | Pool RAM |
//! |---|---|---|
//! | General use | [`with_omq_buffer_pool`](ProactorBuilderExt::with_omq_buffer_pool) (64 x 64 KiB) | 4 MiB |
//! | High fan-out (100+ connections) | `with_omq_buffer_pool_sized(256, 64 * 1024)` | 16 MiB |
//!
//! [`Options::large_message_threshold`]: omq_proto::options::Options::large_message_threshold
//!
//! ## Example
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
pub const DEFAULT_BUFFER_POOL_COUNT: u16 = 64;

/// Length in bytes of each buffer in the `BUF_RING` pool.
pub const DEFAULT_BUFFER_POOL_LEN: usize = 64 * 1024;

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

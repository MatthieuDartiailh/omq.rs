#[cfg(any(feature = "lz4", feature = "zstd"))]
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use omq_proto::proto::transform::MessageEncoder;

#[cfg(feature = "lz4")]
use omq_proto::proto::transform::Lz4Stream;
#[cfg(feature = "zstd")]
use omq_proto::proto::transform::ZstdCCtx;

/// Variant tag so the pool knows which sub-pool to return a context to.
/// `Inline` is used for small messages that were encoded on the submitting
/// thread without borrowing a pool context.
pub(crate) enum RawCtx {
    Inline,
    #[cfg(feature = "lz4")]
    Lz4(Lz4Stream),
    #[cfg(feature = "zstd")]
    Zstd(ZstdCCtx<'static>),
}

/// Runtime-global pool of reusable compression contexts, shared across
/// all connections on a socket. Sized to `available_parallelism()`.
pub(crate) struct CompressionPool {
    #[cfg(feature = "lz4")]
    lz4: Mutex<Vec<Lz4Stream>>,
    #[cfg(feature = "zstd")]
    zstd: Mutex<Vec<ZstdCCtx<'static>>>,
    outstanding: AtomicUsize,
    cap: usize,
}

impl std::fmt::Debug for CompressionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompressionPool")
            .field("cap", &self.cap)
            .field("outstanding", &self.outstanding.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl CompressionPool {
    pub(crate) fn new() -> Self {
        let cap = std::thread::available_parallelism().map_or(2, std::num::NonZero::get);
        Self {
            #[cfg(feature = "lz4")]
            lz4: Mutex::new(Vec::new()),
            #[cfg(feature = "zstd")]
            zstd: Mutex::new(Vec::new()),
            outstanding: AtomicUsize::new(0),
            cap,
        }
    }

    /// Try to acquire a raw compression context matching the encoder's
    /// variant. Returns `None` if the pool is at capacity (all contexts
    /// are in flight). New contexts are created on demand up to `cap`.
    #[allow(unused_variables, clippy::unused_self, dead_code)]
    pub(crate) fn try_take(&self, encoder: &MessageEncoder) -> Option<RawCtx> {
        match encoder {
            #[cfg(feature = "lz4")]
            MessageEncoder::Lz4(_) => {
                if let Some(s) = self.lz4.lock().unwrap().pop() {
                    self.outstanding.fetch_add(1, Ordering::Relaxed);
                    return Some(RawCtx::Lz4(s));
                }
                if self.outstanding.load(Ordering::Relaxed) < self.cap {
                    self.outstanding.fetch_add(1, Ordering::Relaxed);
                    return Some(RawCtx::Lz4(Lz4Stream::new()));
                }
                None
            }
            #[cfg(feature = "zstd")]
            MessageEncoder::Zstd(_) => {
                if let Some(c) = self.zstd.lock().unwrap().pop() {
                    self.outstanding.fetch_add(1, Ordering::Relaxed);
                    return Some(RawCtx::Zstd(c));
                }
                if self.outstanding.load(Ordering::Relaxed) < self.cap {
                    self.outstanding.fetch_add(1, Ordering::Relaxed);
                    return Some(RawCtx::Zstd(ZstdCCtx::create()));
                }
                None
            }
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => None,
        }
    }

    #[allow(clippy::needless_pass_by_value, clippy::unused_self)]
    pub(crate) fn put(&self, ctx: RawCtx) {
        match ctx {
            RawCtx::Inline => {}
            #[cfg(feature = "lz4")]
            RawCtx::Lz4(s) => {
                self.lz4.lock().unwrap().push(s);
                self.outstanding.fetch_sub(1, Ordering::Relaxed);
            }
            #[cfg(feature = "zstd")]
            RawCtx::Zstd(c) => {
                self.zstd.lock().unwrap().push(c);
                self.outstanding.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

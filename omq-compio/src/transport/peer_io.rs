//! Per-peer shared I/O state.
//!
//! Wire connections expose their codec + writer + reader + transform
//! behind one async [`Mutex`] so the driver task and the direct
//! send/recv fast paths can all drive them. Reads happen under the
//! lock so the driver and a direct-recv caller can't race the same
//! buffer.
//!
//! [`Mutex`]: async_lock::Mutex
//!
//! The reader / writer halves are stored as concrete `enum` variants
//! over the small set of supported transports (TCP, Unix). This
//! gives static dispatch on the per-call hot path - matched at
//! `read` / `write_vectored` call site - and avoids the heap-
//! allocated future that a `Box<dyn Future>` trait object would
//! require per call (the original `Box<dyn DynWriter>` /
//! `Box<dyn DynReader>` shape allocated once per send + once per
//! read, which dominated PUSH/PULL throughput at small message
//! sizes).

use std::io;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use compio::BufResult;
use compio::driver::op::{RecvFlags, RecvMulti};
use compio::driver::{BufferRef, ToSharedFd};
use compio::io::AsyncWrite;
use compio::net::{OwnedWriteHalf, TcpStream, UnixStream};
use compio::runtime::fd::AsyncFd;
use compio::runtime::{Runtime, submit_multi};
use futures::Stream;

use omq_proto::proto::connection::Connection;
use omq_proto::proto::transform::MessageDecoder;

/// Multi-shot recv stream type. Each `next().await` yields a
/// [`BufferRef`] from the runtime's `BUF_RING` pool. The kernel keeps
/// pulling from the same persistent SQE, so cancelling a consumer
/// future does NOT cancel the SQE - bytes accumulate in the ring and
/// are picked up by the next poll.
pub(crate) type RecvStream = Pin<Box<dyn Stream<Item = io::Result<BufferRef>> + 'static>>;

/// Wire reader half. One variant per concrete transport. Static
/// dispatch via `match` inside `read` - no `Box<dyn ...>`, no
/// per-call heap allocation.
///
/// Holds an [`AsyncFd`] (rather than `OwnedReadHalf`) so the recv path
/// can use compio's managed-buffer / multi-shot recv APIs - those are
/// implemented for `AsyncFd<T>` but not for `OwnedReadHalf<T>`.
pub(crate) enum WireReader {
    Tcp(AsyncFd<TcpStream>),
    Ipc(AsyncFd<UnixStream>),
}

impl std::fmt::Debug for WireReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireReader").finish_non_exhaustive()
    }
}

impl WireReader {
    /// Build a multi-shot recv stream backed by compio's `BUF_RING` pool.
    ///
    /// The returned stream owns its own [`SharedFd`] clone, so it does
    /// not borrow from `self`; storing it elsewhere (e.g. on
    /// `DirectIoState`) is safe.
    ///
    /// `len = 0` means each CQE delivers up to one full buffer's
    /// worth (the per-buffer size configured on the runtime's
    /// `ProactorBuilder`).
    ///
    /// [`SharedFd`]: compio::driver::SharedFd
    pub(crate) fn build_recv_stream(&self) -> io::Result<RecvStream> {
        let pool = Runtime::current().buffer_pool()?;
        match self {
            Self::Tcp(fd) => {
                let op = RecvMulti::new(fd.to_shared_fd(), &pool, 0, RecvFlags::empty())?;
                Ok(Box::pin(submit_multi(op).into_managed(pool)))
            }
            Self::Ipc(fd) => {
                let op = RecvMulti::new(fd.to_shared_fd(), &pool, 0, RecvFlags::empty())?;
                Ok(Box::pin(submit_multi(op).into_managed(pool)))
            }
        }
    }
}

impl From<AsyncFd<TcpStream>> for WireReader {
    fn from(r: AsyncFd<TcpStream>) -> Self {
        Self::Tcp(r)
    }
}

impl From<AsyncFd<UnixStream>> for WireReader {
    fn from(r: AsyncFd<UnixStream>) -> Self {
        Self::Ipc(r)
    }
}

/// Wire writer half. Mirrors [`WireReader`].
pub(crate) enum WireWriter {
    Tcp(OwnedWriteHalf<TcpStream>),
    Ipc(OwnedWriteHalf<UnixStream>),
}

impl std::fmt::Debug for WireWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireWriter").finish_non_exhaustive()
    }
}

impl WireWriter {
    /// Vectored write of owned `Bytes` chunks. compio's `Vec<Bytes>`
    /// implements `IoVectoredBuf` (via the `bytes` feature on
    /// compio-buf), so the codec's owned chunks go straight into
    /// the syscall - no manual `iovec` construction.
    ///
    /// Returns the buffer alongside the byte count so callers can
    /// inspect unwritten chunks on partial writes without cloning.
    pub(crate) async fn write_vectored(
        &mut self,
        bufs: Vec<Bytes>,
    ) -> (std::io::Result<usize>, Vec<Bytes>) {
        match self {
            Self::Tcp(w) => {
                let BufResult(res, bufs) = w.write_vectored(bufs).await;
                (res, bufs)
            }
            Self::Ipc(w) => {
                let BufResult(res, bufs) = w.write_vectored(bufs).await;
                (res, bufs)
            }
        }
    }
}

impl From<OwnedWriteHalf<TcpStream>> for WireWriter {
    fn from(w: OwnedWriteHalf<TcpStream>) -> Self {
        Self::Tcp(w)
    }
}

impl From<OwnedWriteHalf<UnixStream>> for WireWriter {
    fn from(w: OwnedWriteHalf<UnixStream>) -> Self {
        Self::Ipc(w)
    }
}

/// Per-peer codec + reader + decoder, intended to live behind a
/// shared async mutex.
///
/// The writer half lives separately in [`DirectIoState::writer`] so
/// the driver can release the codec lock before calling
/// `write_vectored` — that lets the fast-path sender encode the next
/// message while the I/O is in flight.
///
/// The encoder lives in [`DirectIoState::encoder`] under its own
/// async mutex so it can be locked independently of this (reader-side)
/// lock, eliminating contention between the sender and the read loop.
pub(crate) struct PeerIo {
    pub(crate) codec: Connection,
    pub(crate) decoder: Option<MessageDecoder>,
    pub(crate) reader: WireReader,
    /// Flipped to `true` once `Event::HandshakeSucceeded` has been
    /// observed. The direct send fast path bails out (falling back to
    /// `cmd_tx`) until this is set, since pre-handshake the codec
    /// rejects `send_message`.
    pub(crate) handshake_done: bool,
}

impl std::fmt::Debug for PeerIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerIo")
            .field("handshake_done", &self.handshake_done)
            .field("pending_bytes", &self.codec.pending_transmit_size())
            .finish_non_exhaustive()
    }
}

/// Sync mutex chosen deliberately. The `peer_io` codec is single-threaded
/// (compio runtime is thread-pinned) and the discipline is "never hold
/// `peer_io` across an `.await`". With that invariant the lock is only
/// taken between yields, so `.lock()` cannot block waiting on a parked
/// holder. This is what makes the recv path cancel-safe: between the
/// stream pulling a `BufferRef` and `handle_input` consuming it, there
/// is no `.await` — so a future drop in that window is impossible.
pub(crate) type SharedPeerIo = Arc<std::sync::Mutex<PeerIo>>;

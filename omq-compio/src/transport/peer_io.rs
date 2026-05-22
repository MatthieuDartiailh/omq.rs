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

use bytes::{Bytes, BytesMut};
use compio::BufResult;
use compio::driver::op::{RecvFlags, RecvMulti};
use compio::driver::{BufferRef, ToSharedFd};
use compio::io::{AsyncRead, AsyncWrite};
use compio::net::{OwnedWriteHalf, TcpStream, UnixStream};
use compio::runtime::fd::AsyncFd;
use compio::runtime::{CancelToken, Runtime, submit_multi};
use futures::Stream;

use omq_proto::proto::connection::Connection;
use omq_proto::proto::transform::MessageDecoder;

/// Multi-shot recv stream type. Each `next().await` yields a
/// [`BufferRef`] from the runtime's `BUF_RING` pool. The kernel keeps
/// pulling from the same persistent SQE, so cancelling a consumer
/// future does NOT cancel the SQE - bytes accumulate in the ring and
/// are picked up by the next poll.
pub(crate) type RecvStream = Pin<Box<dyn Stream<Item = io::Result<BufferRef>> + 'static>>;

/// Multi-shot recv stream paired with a [`CancelToken`] that targets
/// the same underlying io_uring op key.
///
/// Every `stream.next().await` poll site MUST wrap with
/// `.with_cancel(cancel.clone())` so the `SubmitMulti`'s first poll
/// registers its op key with the token. After registration,
/// `cancel.clone().cancel()` submits an `IORING_OP_ASYNC_CANCEL`
/// targeting that op. Subsequent polls of the stream then drain any
/// CQEs that were already in flight before the cancel landed; the
/// stream terminates with an `ECANCELED` error or a final `None`.
/// This is the basis of the large-frame "cancel-and-drain → one-shot"
/// path: drained bytes are accounted as part of the supplied payload,
/// so no kernel-consumed bytes are lost in the cancel race window.
pub(crate) struct CancellableRecvStream {
    pub(crate) stream: RecvStream,
    pub(crate) cancel: CancelToken,
}

impl std::fmt::Debug for CancellableRecvStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellableRecvStream")
            .finish_non_exhaustive()
    }
}

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
    #[cfg(feature = "ws")]
    Wss(super::ws::SharedTls),
}

impl std::fmt::Debug for WireReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireReader").finish_non_exhaustive()
    }
}

impl WireReader {
    pub(crate) fn supports_multishot(&self) -> bool {
        matches!(self, Self::Tcp(_) | Self::Ipc(_))
    }

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
    pub(crate) fn build_recv_stream(&self) -> io::Result<CancellableRecvStream> {
        let pool = Runtime::current().buffer_pool()?;
        let cancel = CancelToken::new();
        let stream: RecvStream = match self {
            Self::Tcp(fd) => {
                let op = RecvMulti::new(fd.to_shared_fd(), &pool, 0, RecvFlags::empty())?;
                Box::pin(submit_multi(op).into_managed(pool))
            }
            Self::Ipc(fd) => {
                let op = RecvMulti::new(fd.to_shared_fd(), &pool, 0, RecvFlags::empty())?;
                Box::pin(submit_multi(op).into_managed(pool))
            }
            #[cfg(feature = "ws")]
            Self::Wss(_) => unreachable!("TLS streams use one-shot reads"),
        };
        Ok(CancellableRecvStream { stream, cancel })
    }

    /// Clone the underlying fd into an owned [`WireRecvFd`]. The clone
    /// shares the kernel fd via `SharedFd` reference counts, so it can
    /// be used for one-shot reads without holding the [`PeerIo`] mutex
    /// across the await — the caller drops the lock before issuing the
    /// recv.
    pub(crate) fn fd_clone(&self) -> WireRecvFd {
        match self {
            Self::Tcp(fd) => WireRecvFd::Tcp(fd.clone()),
            Self::Ipc(fd) => WireRecvFd::Ipc(fd.clone()),
            #[cfg(feature = "ws")]
            Self::Wss(shared) => WireRecvFd::Wss(shared.clone()),
        }
    }
}

/// Owned recv-only view over a wire fd, produced by
/// [`WireReader::fd_clone`]. Used by the large-frame one-shot path:
/// the recv stream lock has been swapped to `None`, the caller clones
/// the fd, drops the codec lock, and reads exactly `payload_len`
/// bytes into a sized destination buffer.
pub(crate) enum WireRecvFd {
    Tcp(AsyncFd<TcpStream>),
    Ipc(AsyncFd<UnixStream>),
    #[cfg(feature = "ws")]
    Wss(super::ws::SharedTls),
}

impl std::fmt::Debug for WireRecvFd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireRecvFd").finish_non_exhaustive()
    }
}

impl WireRecvFd {
    /// Append reads to `dst` until `dst.len() >= target_total`. Loops
    /// over short reads via `split_off` / `unsplit` so the destination
    /// stays one contiguous allocation (single-chunk `Bytes` after
    /// `freeze`).
    ///
    /// `dst` must have `capacity() >= target_total`. The split/unsplit
    /// pattern relies on both parts sharing one allocation, so an
    /// under-sized buffer would force a reallocation copy.
    ///
    /// Returns [`io::ErrorKind::UnexpectedEof`] if the peer closes
    /// before the target is reached.
    pub(crate) async fn read_until(
        &self,
        dst: &mut BytesMut,
        target_total: usize,
    ) -> io::Result<()> {
        debug_assert!(dst.capacity() >= target_total);
        while dst.len() < target_total {
            let tail = dst.split_off(dst.len());
            let BufResult(res, returned) = match self {
                Self::Tcp(fd) => {
                    let mut r: &AsyncFd<TcpStream> = fd;
                    AsyncRead::read(&mut r, tail).await
                }
                Self::Ipc(fd) => {
                    let mut r: &AsyncFd<UnixStream> = fd;
                    AsyncRead::read(&mut r, tail).await
                }
                #[cfg(feature = "ws")]
                Self::Wss(shared) => {
                    let mut tls = shared.lock().await;
                    AsyncRead::read(&mut *tls, tail).await
                }
            };
            let n = res?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            dst.unsplit(returned);
        }
        Ok(())
    }

    /// Single non-exact read into `buf`. Returns however many bytes the
    /// kernel delivered. Returns `UnexpectedEof` if the peer closed.
    pub(crate) async fn read_some(&self, buf: BytesMut) -> io::Result<bytes::Bytes> {
        let BufResult(res, buf) = match self {
            Self::Tcp(fd) => {
                let mut r: &AsyncFd<TcpStream> = fd;
                AsyncRead::read(&mut r, buf).await
            }
            Self::Ipc(fd) => {
                let mut r: &AsyncFd<UnixStream> = fd;
                AsyncRead::read(&mut r, buf).await
            }
            #[cfg(feature = "ws")]
            Self::Wss(shared) => {
                let mut tls = shared.lock().await;
                AsyncRead::read(&mut *tls, buf).await
            }
        };
        let n = res?;
        if n == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        Ok(buf.freeze())
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
    #[cfg(feature = "ws")]
    Wss(super::ws::SharedTls),
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
            #[cfg(feature = "ws")]
            Self::Wss(shared) => {
                let mut tls = shared.lock().await;
                let BufResult(res, bufs) = AsyncWrite::write_vectored(&mut *tls, bufs).await;
                if res.is_ok() {
                    let _ = AsyncWrite::flush(&mut *tls).await;
                }
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

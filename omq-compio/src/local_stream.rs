use crate::socket::RecvStreamState;
use crate::transport::peer_io::SharedPeerIo;

/// `Send + Sync`-asserting wrapper around a multi-shot recv stream.
///
/// compio's multi-shot recv stream contains [`compio::driver::SharedFd`]
/// which is `Rc`-based for single-thread efficiency, hence `!Send`. We
/// store such a stream behind an [`async_lock::Mutex`] inside the
/// `Arc<DirectIoState>` so the driver task and the user's
/// `try_direct_recv` task can share it.
///
/// # Safety
///
/// `compio::runtime::Runtime` is thread-pinned: every `compio::runtime::spawn`
/// places the future on the local runtime's thread, and `Socket` is documented
/// as pinned to its creating runtime. Cross-runtime sends in omq-compio go
/// through `flume` mpsc, never moving the `Arc<DirectIoState>` itself.
/// Therefore the inner `Rc` refcount is only ever touched on a single thread,
/// and asserting `Send + Sync` on this wrapper does not introduce a data race
/// in any usage pattern omq-compio supports.
pub(crate) struct LocalStream(async_lock::Mutex<Option<RecvStreamState>>);

// SAFETY: see `LocalStream` doc comment above.
unsafe impl Send for LocalStream {}
// SAFETY: see `LocalStream` doc comment above.
unsafe impl Sync for LocalStream {}

impl LocalStream {
    pub(crate) fn new(state: Option<RecvStreamState>) -> Self {
        Self(async_lock::Mutex::new(state))
    }

    pub(crate) async fn lock(&self) -> async_lock::MutexGuard<'_, Option<RecvStreamState>> {
        self.0.lock().await
    }

    /// Replace the stream with a freshly-built one. Used to re-arm
    /// after the kernel terminates a multi-shot recv SQE - typically
    /// `ENOBUFS` under sustained delivery on a small `BUF_RING` pool.
    /// The previous stream's lingering op is cancelled when its slot
    /// drops.
    #[expect(clippy::await_holding_lock)]
    pub(crate) async fn rearm(&self, peer_io: &SharedPeerIo) -> std::io::Result<()> {
        let io = peer_io.lock().expect("peer_io");
        if !io.reader.supports_multishot() {
            drop(io);
            *self.0.lock().await = Some(RecvStreamState::OneShot);
            return Ok(());
        }
        let new_stream = io.reader.build_recv_stream();
        drop(io);
        *self.0.lock().await = Some(RecvStreamState::MultiShot(new_stream));
        Ok(())
    }
}

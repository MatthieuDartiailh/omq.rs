use std::cell::Cell;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use event_listener::Event;

use omq_proto::proto::transform::MessageEncoder;

use crate::transport::peer_io::{CancellableRecvStream, PeerIo, SharedPeerIo, WireWriter};

use super::inner::{LocalStream, RecvStreamState};
use omq_proto::encoded_queue::EncodedQueue;

pub(crate) const DIRECT_ARENA_THRESHOLD_DEFAULT: usize = 1024;

#[allow(clippy::struct_excessive_bools)]
pub(crate) struct DirectIoState {
    pub(crate) peer_io: SharedPeerIo,
    pub(crate) writer: async_lock::Mutex<WireWriter>,
    pub(crate) transmit_ready: Event,
    pub(crate) recv_stream: LocalStream,
    pub(crate) recv_claim: AtomicU8,
    pub(crate) recv_state_changed: Event,
    pub(crate) recv_codec_ready: Event,
    pub(crate) eof_signal: Event,
    pub(crate) last_input_nanos: AtomicU64,
    pub(crate) hb_epoch: Instant,
    pub(crate) handshake_done: Cell<bool>,
    pub(crate) has_transform: bool,
    pub(crate) uses_crypto: bool,
    pub(crate) transform_passthrough: Option<(Bytes, usize)>,
    pub(crate) encoder: async_lock::Mutex<Option<MessageEncoder>>,
    pub(crate) encoded_queue: EncodedQueueCell,
    pub(crate) driver_in_select: Cell<bool>,
    /// Set when a sender has already issued a `transmit_ready` notify for
    /// the driver's current park. Coalesces the per-message notify: while
    /// the cooperative driver is parked, a sender burst would otherwise
    /// call `Event::notify` (an internal-lock op) on every message even
    /// though the first wakeup already arms the driver. Reset by the
    /// driver at the top of its loop once it is running again.
    pub(crate) transmit_notified: Cell<bool>,
    pub(crate) direct_msg_count: Cell<usize>,
    pub(crate) socket_closing: Cell<bool>,
    pub(crate) large_recv_pending: AtomicUsize,
    pub(crate) pending_acc: Mutex<Option<BytesMut>>,
    pub(crate) large_message_threshold: usize,
    pub(crate) arena_threshold: usize,
    pub(crate) multishot_rearms: AtomicUsize,
    #[cfg(feature = "ws")]
    pub(crate) is_ws: bool,
    #[cfg(feature = "ws")]
    pub(crate) ws_masked: bool,
}

impl std::fmt::Debug for DirectIoState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectIoState")
            .field("recv_claim", &self.recv_claim.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) enum OneShotLargeRecvOutcome {
    Skipped,
    RearmMultiShot,
    Took,
    AccumulatePayload,
    IoErr(std::io::Error),
    ProtoErr(omq_proto::error::Error),
}

pub(crate) async fn try_one_shot_large_recv(
    state: &Arc<DirectIoState>,
    sguard: &mut async_lock::MutexGuard<'_, Option<RecvStreamState>>,
) -> OneShotLargeRecvOutcome {
    use bytes::BytesMut;

    if state.large_message_threshold == 0 {
        return OneShotLargeRecvOutcome::RearmMultiShot;
    }
    #[cfg(feature = "ws")]
    if state.is_ws {
        return OneShotLargeRecvOutcome::RearmMultiShot;
    }

    let already_one_shot = matches!(sguard.as_ref(), Some(RecvStreamState::OneShot));

    let one_shot_acc = {
        let Ok(mut io) = state.peer_io.lock() else {
            return OneShotLargeRecvOutcome::RearmMultiShot;
        };
        if io.codec.has_frame_transform() {
            return OneShotLargeRecvOutcome::RearmMultiShot;
        }
        let info = match io.codec.peek_next_frame_payload_size() {
            Ok(Some(info)) => info,
            Ok(None) => return OneShotLargeRecvOutcome::Skipped,
            Err(e) => return OneShotLargeRecvOutcome::ProtoErr(e),
        };
        if info.payload_len < state.large_message_threshold {
            return OneShotLargeRecvOutcome::RearmMultiShot;
        }
        if !already_one_shot {
            return OneShotLargeRecvOutcome::AccumulatePayload;
        }
        let Some((plen, prefix)) = io.codec.begin_supplied_payload_with_prefix() else {
            return OneShotLargeRecvOutcome::RearmMultiShot;
        };
        let mut acc = BytesMut::with_capacity(plen);
        acc.extend_from_slice(prefix.as_slice());
        (plen, acc)
    };

    one_shot_with_prefix(state, sguard, one_shot_acc.0, one_shot_acc.1).await
}

async fn one_shot_with_prefix(
    state: &Arc<DirectIoState>,
    sguard: &mut async_lock::MutexGuard<'_, Option<RecvStreamState>>,
    payload_len: usize,
    mut acc: BytesMut,
) -> OneShotLargeRecvOutcome {
    **sguard = Some(RecvStreamState::OneShot);

    if acc.len() < payload_len {
        let fd = {
            let Ok(io) = state.peer_io.lock() else {
                return OneShotLargeRecvOutcome::Skipped;
            };
            io.reader.fd_clone()
        };
        if let Err(e) = fd.read_until(&mut acc, payload_len).await {
            return OneShotLargeRecvOutcome::IoErr(e);
        }
    }
    state.last_input_nanos.store(
        state.hb_epoch.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );

    let payload_bytes = acc.freeze();
    {
        let Ok(mut io) = state.peer_io.lock() else {
            return OneShotLargeRecvOutcome::IoErr(std::io::Error::other("peer_io"));
        };
        if let Err(e) = io.codec.supply_payload(payload_bytes) {
            return OneShotLargeRecvOutcome::ProtoErr(e);
        }
    }
    OneShotLargeRecvOutcome::Took
}

pub(crate) async fn one_shot_recv_and_feed(
    state: &Arc<DirectIoState>,
    sguard: &mut async_lock::MutexGuard<'_, Option<RecvStreamState>>,
) -> OneShotLargeRecvOutcome {
    use bytes::BytesMut;
    let fd = {
        let Ok(io) = state.peer_io.lock() else {
            return OneShotLargeRecvOutcome::IoErr(std::io::Error::other("peer_io"));
        };
        io.reader.fd_clone()
    };

    let bytes = match fd.read_some(BytesMut::with_capacity(65536)).await {
        Ok(b) => b,
        Err(e) => return OneShotLargeRecvOutcome::IoErr(e),
    };
    state.last_input_nanos.store(
        state.hb_epoch.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );
    {
        let Ok(mut io) = state.peer_io.lock() else {
            return OneShotLargeRecvOutcome::IoErr(std::io::Error::other("peer_io"));
        };
        if let Err(e) = io.codec.handle_input(bytes) {
            return OneShotLargeRecvOutcome::ProtoErr(e);
        }
    }

    loop {
        match try_one_shot_large_recv(state, sguard).await {
            OneShotLargeRecvOutcome::Skipped => {
                let bytes = match fd.read_some(BytesMut::with_capacity(65536)).await {
                    Ok(b) => b,
                    Err(e) => break OneShotLargeRecvOutcome::IoErr(e),
                };
                if bytes.is_empty() {
                    break OneShotLargeRecvOutcome::IoErr(std::io::Error::other("eof"));
                }
                state.last_input_nanos.store(
                    state.hb_epoch.elapsed().as_nanos() as u64,
                    Ordering::Relaxed,
                );
                let Ok(mut io) = state.peer_io.lock() else {
                    break OneShotLargeRecvOutcome::IoErr(std::io::Error::other("peer_io"));
                };
                if let Err(e) = io.codec.handle_input(bytes) {
                    break OneShotLargeRecvOutcome::ProtoErr(e);
                }
                drop(io);
            }
            OneShotLargeRecvOutcome::RearmMultiShot => {
                let Ok(io) = state.peer_io.lock() else {
                    break OneShotLargeRecvOutcome::IoErr(std::io::Error::other("peer_io"));
                };
                if !io.reader.supports_multishot() {
                    drop(io);
                    break OneShotLargeRecvOutcome::Took;
                }
                let new_stream = io.reader.build_recv_stream();
                drop(io);
                **sguard = Some(RecvStreamState::MultiShot(new_stream));
                state.multishot_rearms.fetch_add(1, Ordering::Relaxed);
                break OneShotLargeRecvOutcome::Took;
            }
            other => break other,
        }
    }
}

impl DirectIoState {
    /// Bump the direct-encode message counter and wake the driver if it
    /// is parked in `select_biased!`. Called after every successful
    /// direct-encode (flat, gather, prefixed, transform, or WebSocket).
    #[inline]
    pub(crate) fn signal_encoded(&self) {
        self.direct_msg_count.set(self.direct_msg_count.get() + 1);
        // Coalesce: only the first sender after the driver parks needs to
        // wake it. Subsequent messages in the same cooperative burst skip
        // the event_listener notify (an internal-lock op, ~16% of the
        // small-message fan-out send path per perf). The driver resets
        // `transmit_notified` when it resumes.
        if self.driver_in_select.get() && !self.transmit_notified.replace(true) {
            self.transmit_ready.notify(1);
        }
    }

    #[inline]
    pub(crate) fn signal_eof(&self) {
        self.eof_signal.notify(usize::MAX);
    }

    #[inline]
    pub(crate) fn lock_io(&self) -> std::sync::MutexGuard<'_, PeerIo> {
        self.peer_io.lock().expect("peer_io")
    }

    #[expect(clippy::too_many_arguments)]
    #[allow(clippy::fn_params_excessive_bools)]
    pub(crate) fn new(
        peer_io: SharedPeerIo,
        writer: WireWriter,
        recv_stream: Option<CancellableRecvStream>,
        has_transform: bool,
        transform_passthrough: Option<(Bytes, usize)>,
        encoder: Option<MessageEncoder>,
        uses_crypto: bool,
        large_message_threshold: usize,
        arena_threshold: usize,
        #[cfg(feature = "ws")] is_ws: bool,
        #[cfg(feature = "ws")] ws_masked: bool,
    ) -> Arc<Self> {
        let initial_recv_state = match recv_stream {
            Some(s) => Some(RecvStreamState::MultiShot(s)),
            None => Some(RecvStreamState::OneShot),
        };
        #[expect(clippy::arc_with_non_send_sync)]
        Arc::new(Self {
            peer_io,
            writer: async_lock::Mutex::new(writer),
            transmit_ready: Event::new(),
            recv_stream: LocalStream(async_lock::Mutex::new(initial_recv_state)),
            recv_claim: AtomicU8::new(0),
            recv_state_changed: Event::new(),
            recv_codec_ready: Event::new(),
            eof_signal: Event::new(),
            last_input_nanos: AtomicU64::new(0),
            hb_epoch: Instant::now(),
            handshake_done: Cell::new(false),
            has_transform,
            uses_crypto,
            transform_passthrough,
            encoder: async_lock::Mutex::new(encoder),
            encoded_queue: EncodedQueueCell::with_arena_threshold(arena_threshold),
            driver_in_select: Cell::new(false),
            transmit_notified: Cell::new(false),
            direct_msg_count: Cell::new(0),
            socket_closing: Cell::new(false),
            large_recv_pending: AtomicUsize::new(0),
            pending_acc: Mutex::new(None),
            large_message_threshold,
            arena_threshold,
            multishot_rearms: AtomicUsize::new(0),
            #[cfg(feature = "ws")]
            is_ws,
            #[cfg(feature = "ws")]
            ws_masked,
        })
    }
}

/// Non-atomic interior-mutable wrapper for `EncodedQueue`.
///
/// Sound only on a single thread (compio's cooperative runtime).
/// Replaces `Mutex<EncodedQueue>` to avoid atomic CAS on every
/// lock/unlock in the send hot path.
pub(crate) struct EncodedQueueCell {
    borrowed: Cell<bool>,
    inner: std::cell::UnsafeCell<EncodedQueue>,
}

impl EncodedQueueCell {
    fn with_arena_threshold(arena_threshold: usize) -> Self {
        Self {
            borrowed: Cell::new(false),
            inner: std::cell::UnsafeCell::new(EncodedQueue::with_arena_threshold(arena_threshold)),
        }
    }

    #[inline]
    pub(crate) fn try_borrow_mut(&self) -> Option<EncodedQueueGuard<'_>> {
        if self.borrowed.get() {
            return None;
        }
        self.borrowed.set(true);
        Some(EncodedQueueGuard { cell: self })
    }

    #[inline]
    pub(crate) fn borrow_mut(&self) -> EncodedQueueGuard<'_> {
        assert!(!self.borrowed.get(), "EncodedQueueCell: already borrowed");
        self.borrowed.set(true);
        EncodedQueueGuard { cell: self }
    }
}

pub(crate) struct EncodedQueueGuard<'a> {
    cell: &'a EncodedQueueCell,
}

impl std::ops::Deref for EncodedQueueGuard<'_> {
    type Target = EncodedQueue;
    #[inline]
    fn deref(&self) -> &EncodedQueue {
        // SAFETY: the borrow flag prevents concurrent access. The
        // guard's lifetime is bounded by the cell's, so the pointer
        // remains valid.
        unsafe { &*self.cell.inner.get() }
    }
}

impl std::ops::DerefMut for EncodedQueueGuard<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut EncodedQueue {
        // SAFETY: &mut self guarantees exclusive guard access. The
        // borrow flag prevents a second guard from being created.
        unsafe { &mut *self.cell.inner.get() }
    }
}

impl Drop for EncodedQueueGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.cell.borrowed.set(false);
    }
}

#[expect(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for EncodedQueueCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedQueueCell")
            .field("borrowed", &self.borrowed.get())
            .finish()
    }
}

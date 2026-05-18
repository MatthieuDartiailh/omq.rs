use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use event_listener::Event;

use omq_proto::proto::transform::MessageEncoder;

use crate::transport::peer_io::{CancellableRecvStream, SharedPeerIo, WireWriter};

use super::encoded_queue::EncodedQueue;
use super::inner::{LocalStream, RecvStreamState};

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
    pub(crate) handshake_done: AtomicBool,
    #[cfg_attr(feature = "priority", allow(dead_code))]
    pub(crate) has_transform: bool,
    #[cfg_attr(feature = "priority", allow(dead_code))]
    pub(crate) uses_crypto: bool,
    #[cfg_attr(feature = "priority", allow(dead_code))]
    pub(crate) transform_passthrough: Option<(Bytes, usize)>,
    pub(crate) encoder: async_lock::Mutex<Option<MessageEncoder>>,
    pub(crate) encoded_queue: Mutex<EncodedQueue>,
    pub(crate) driver_in_select: AtomicBool,
    pub(crate) large_recv_pending: AtomicUsize,
    pub(crate) pending_acc: Mutex<Option<BytesMut>>,
    pub(crate) large_message_threshold: usize,
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
        return OneShotLargeRecvOutcome::Skipped;
    }

    let already_one_shot = matches!(sguard.as_ref(), Some(RecvStreamState::OneShot));

    let one_shot_acc = {
        let Ok(mut io) = state.peer_io.lock() else {
            return OneShotLargeRecvOutcome::Skipped;
        };
        let info = match io.codec.peek_next_frame_payload_size() {
            Ok(Some(info)) => info,
            Ok(None) => return OneShotLargeRecvOutcome::Skipped,
            Err(e) => return OneShotLargeRecvOutcome::ProtoErr(e),
        };
        if info.payload_len < state.large_message_threshold {
            return OneShotLargeRecvOutcome::Skipped;
        }
        if !already_one_shot {
            return OneShotLargeRecvOutcome::AccumulatePayload;
        }
        let Some((plen, prefix)) = io.codec.begin_supplied_payload_with_prefix() else {
            return OneShotLargeRecvOutcome::Skipped;
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

    match try_one_shot_large_recv(state, sguard).await {
        OneShotLargeRecvOutcome::Skipped => {
            let new_stream = {
                let Ok(io) = state.peer_io.lock() else {
                    return OneShotLargeRecvOutcome::IoErr(std::io::Error::other("peer_io"));
                };
                match io.reader.build_recv_stream() {
                    Ok(s) => s,
                    Err(e) => return OneShotLargeRecvOutcome::IoErr(e),
                }
            };
            **sguard = Some(RecvStreamState::MultiShot(new_stream));
            OneShotLargeRecvOutcome::Took
        }
        other => other,
    }
}

impl DirectIoState {
    #[inline]
    pub(crate) fn signal_eof(&self) {
        self.eof_signal.notify(usize::MAX);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        peer_io: SharedPeerIo,
        writer: WireWriter,
        recv_stream: CancellableRecvStream,
        has_transform: bool,
        transform_passthrough: Option<(Bytes, usize)>,
        encoder: Option<MessageEncoder>,
        uses_crypto: bool,
        large_message_threshold: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            peer_io,
            writer: async_lock::Mutex::new(writer),
            transmit_ready: Event::new(),
            recv_stream: LocalStream(async_lock::Mutex::new(Some(RecvStreamState::MultiShot(
                recv_stream,
            )))),
            recv_claim: AtomicU8::new(0),
            recv_state_changed: Event::new(),
            recv_codec_ready: Event::new(),
            eof_signal: Event::new(),
            last_input_nanos: AtomicU64::new(0),
            hb_epoch: Instant::now(),
            handshake_done: AtomicBool::new(false),
            has_transform,
            uses_crypto,
            transform_passthrough,
            encoder: async_lock::Mutex::new(encoder),
            encoded_queue: Mutex::new(EncodedQueue::new()),
            driver_in_select: AtomicBool::new(false),
            large_recv_pending: AtomicUsize::new(0),
            pending_acc: Mutex::new(None),
            large_message_threshold,
        })
    }
}

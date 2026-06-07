use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use smallvec::SmallVec;
use tokio::sync::Notify;

use omq_proto::encoded_queue::EncodedQueue;
use omq_proto::message::Message;

const DIRECT_CAP: usize = 2 * 1024 * 1024;
const DIRECT_MSG_MAX: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum TryEncodeResult {
    Ok,
    Dead,
    Full,
    Ineligible,
}

pub(crate) struct PeerEncodeSlot {
    eq: Mutex<EncodedQueue>,
    pub(crate) transmit_notify: Notify,
    pub(crate) drain_notify: Notify,
    pub(crate) driver_in_select: AtomicBool,
    pub(crate) handshake_done: AtomicBool,
    pending: AtomicBool,
    #[allow(dead_code)]
    pub(crate) has_transform: bool,
    #[allow(dead_code)]
    pub(crate) transform_passthrough: Option<(Bytes, usize)>,
    pub(crate) dead: AtomicBool,
    pub(crate) peer_id: u64,
}

impl std::fmt::Debug for PeerEncodeSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerEncodeSlot")
            .field("peer_id", &self.peer_id)
            .field(
                "handshake_done",
                &self.handshake_done.load(Ordering::Relaxed),
            )
            .field("dead", &self.dead.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

#[allow(dead_code)]
impl PeerEncodeSlot {
    pub(crate) fn new(
        peer_id: u64,
        has_transform: bool,
        transform_passthrough: Option<(Bytes, usize)>,
    ) -> Arc<Self> {
        Arc::new(Self {
            eq: Mutex::new(EncodedQueue::new()),
            transmit_notify: Notify::new(),
            drain_notify: Notify::new(),
            driver_in_select: AtomicBool::new(false),
            handshake_done: AtomicBool::new(false),
            pending: AtomicBool::new(false),
            has_transform,
            transform_passthrough,
            dead: AtomicBool::new(false),
            peer_id,
        })
    }

    pub(crate) fn try_encode(&self, msg: &Message) -> TryEncodeResult {
        if self.dead.load(Ordering::Acquire) {
            return TryEncodeResult::Dead;
        }
        if !self.handshake_done.load(Ordering::Acquire) {
            return TryEncodeResult::Ineligible;
        }

        if msg.byte_len() > DIRECT_MSG_MAX {
            return TryEncodeResult::Ineligible;
        }

        if !self.has_transform {
            let mut eq = self.eq.lock().expect("encode_slot eq poisoned");
            if eq.total_bytes() >= DIRECT_CAP {
                return TryEncodeResult::Full;
            }
            eq.encode_arena(msg);
            drop(eq);
            self.signal_encoded();
            return TryEncodeResult::Ok;
        }

        if let Some((ref sentinel, threshold)) = self.transform_passthrough
            && msg.iter().all(|part| part.len() < threshold)
        {
            let mut eq = self.eq.lock().expect("encode_slot eq poisoned");
            if eq.total_bytes() >= DIRECT_CAP {
                return TryEncodeResult::Full;
            }
            eq.encode_prefixed_arena(sentinel, msg);
            drop(eq);
            self.signal_encoded();
            return TryEncodeResult::Ok;
        }

        TryEncodeResult::Ineligible
    }

    pub(crate) fn try_push_encoded(&self, chunks: &[Bytes]) -> TryEncodeResult {
        if self.dead.load(Ordering::Acquire) {
            return TryEncodeResult::Dead;
        }
        let mut eq = self.eq.lock().expect("encode_slot eq poisoned");
        if eq.total_bytes() >= DIRECT_CAP {
            return TryEncodeResult::Full;
        }
        eq.push_shared_chunks(chunks);
        drop(eq);
        self.signal_encoded();
        TryEncodeResult::Ok
    }

    fn signal_encoded(&self) {
        if !self.pending.swap(true, Ordering::Release) {
            self.transmit_notify.notify_one();
        }
    }

    pub(crate) fn drain_into_vec(&self, buf: &mut Vec<Bytes>, max_chunks: usize) {
        let mut eq = self.eq.lock().expect("encode_slot eq poisoned");
        eq.drain_into_vec(buf, max_chunks);
        self.pending.store(false, Ordering::Relaxed);
    }

    pub(crate) fn is_empty(&self) -> bool {
        let eq = self.eq.lock().expect("encode_slot eq poisoned");
        eq.is_empty()
    }

    pub(crate) fn mark_dead(&self) {
        self.dead.store(true, Ordering::Release);
        self.drain_notify.notify_waiters();
    }
}

pub(crate) fn pre_encode(msg: &Message) -> SmallVec<[Bytes; 4]> {
    let mut eq = EncodedQueue::one_shot();
    eq.encode_auto(msg);
    let mut chunks = Vec::new();
    eq.drain_into_vec(&mut chunks, 1024);
    SmallVec::from_vec(chunks)
}

#[allow(dead_code)]
pub(crate) fn pre_encode_prefixed(sentinel: &Bytes, msg: &Message) -> SmallVec<[Bytes; 4]> {
    let mut eq = EncodedQueue::one_shot();
    eq.encode_prefixed_auto(sentinel, msg);
    let mut chunks = Vec::new();
    eq.drain_into_vec(&mut chunks, 1024);
    SmallVec::from_vec(chunks)
}

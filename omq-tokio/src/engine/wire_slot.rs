// During connection churn (heartbeat timeout, peer restart, network
// blip) a small number of messages may be reordered. The wire slot
// bypass and the driver inbox are two independent paths into the same
// TCP stream. When a new connection's handshake completes, one
// in-flight message may still be in the inbox while the next message
// takes the wire slot fast path and reaches the wire first.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use smallvec::SmallVec;
use tokio::sync::Notify;

use omq_proto::encoded_queue::EncodedQueue;
use omq_proto::message::Message;

pub(crate) const WIRE_SLOT_CAP_DEFAULT: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum TryEncodeResult {
    Ok,
    Dead,
    Full,
    Ineligible,
}

pub(crate) struct PeerWireSlot {
    eq: Mutex<EncodedQueue>,
    cap: usize,
    pub(crate) data_ready: Notify,
    pub(crate) space_available: Notify,
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

impl std::fmt::Debug for PeerWireSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerWireSlot")
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
impl PeerWireSlot {
    pub(crate) fn new(
        peer_id: u64,
        has_transform: bool,
        transform_passthrough: Option<(Bytes, usize)>,
        arena_threshold: usize,
        cap: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            eq: Mutex::new(EncodedQueue::with_arena_threshold(arena_threshold)),
            cap,
            data_ready: Notify::new(),
            space_available: Notify::new(),
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

        if !self.has_transform {
            let mut eq = self.eq.lock().expect("wire_slot eq poisoned");
            if eq.total_bytes() >= self.cap {
                return TryEncodeResult::Full;
            }
            eq.encode_auto(msg);
            drop(eq);
            self.signal_encoded();
            return TryEncodeResult::Ok;
        }

        if let Some((ref sentinel, threshold)) = self.transform_passthrough
            && msg.iter().all(|part| part.len() < threshold)
        {
            let mut eq = self.eq.lock().expect("wire_slot eq poisoned");
            if eq.total_bytes() >= self.cap {
                return TryEncodeResult::Full;
            }
            eq.encode_prefixed_auto(sentinel, msg);
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
        let mut eq = self.eq.lock().expect("wire_slot eq poisoned");
        if eq.total_bytes() >= self.cap {
            return TryEncodeResult::Full;
        }
        eq.push_shared_chunks(chunks);
        drop(eq);
        self.signal_encoded();
        TryEncodeResult::Ok
    }

    pub(crate) fn try_push_pre_encoded(&self, data: &[u8]) -> TryEncodeResult {
        if self.dead.load(Ordering::Acquire) {
            return TryEncodeResult::Dead;
        }
        let mut eq = self.eq.lock().expect("wire_slot eq poisoned");
        if eq.total_bytes() >= self.cap {
            return TryEncodeResult::Full;
        }
        eq.push_pre_encoded(data);
        drop(eq);
        self.signal_encoded();
        TryEncodeResult::Ok
    }

    fn signal_encoded(&self) {
        if !self.pending.swap(true, Ordering::Release) {
            self.data_ready.notify_one();
        }
    }

    pub(crate) fn drain_into_vec(&self, buf: &mut Vec<Bytes>, max_chunks: usize) {
        let mut eq = self.eq.lock().expect("wire_slot eq poisoned");
        eq.drain_into_vec(buf, max_chunks);
        self.pending.store(false, Ordering::Relaxed);
    }

    pub(crate) fn has_space(&self) -> bool {
        if self.dead.load(Ordering::Acquire) {
            return false;
        }
        let eq = self.eq.lock().expect("wire_slot eq poisoned");
        eq.total_bytes() < self.cap
    }

    pub(crate) fn is_empty(&self) -> bool {
        let eq = self.eq.lock().expect("wire_slot eq poisoned");
        eq.is_empty()
    }

    pub(crate) fn mark_dead(&self) {
        self.dead.store(true, Ordering::Release);
        self.space_available.notify_waiters();
    }
}

#[allow(dead_code)]
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

// During connection churn (heartbeat timeout, peer restart, network
// blip) a small number of messages may be reordered. The wire slot
// bypass and the driver inbox are two independent paths into the same
// TCP stream. When a new connection's handshake completes, one
// in-flight message may still be in the inbox while the next message
// takes the wire slot fast path and reaches the wire first.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::Notify;

use omq_proto::encoded_queue::EncodedQueue;
use omq_proto::message::Message;

pub(crate) const WIRE_SLOT_CAP_DEFAULT: usize = 2 * 1024 * 1024;
pub(crate) const WIRE_SLOT_MSG_CAP_DEFAULT: usize = crate::routing::SHARED_MAX_BATCH_MSGS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub(crate) handshake_done: AtomicBool,
    /// Coalesces `data_ready` notifications: only the first encode since
    /// the last drain actually calls `notify_one()`, avoiding thundering
    /// herd on the `Notify` when many messages arrive between drains.
    pending: AtomicBool,
    pub(crate) has_transform: bool,
    pub(crate) transform_passthrough: Option<(Bytes, usize)>,
    #[cfg(feature = "ws")]
    is_ws: bool,
    #[cfg(feature = "ws")]
    ws_masked: bool,
    pub(crate) dead: AtomicBool,
    pub(crate) peer_id: u64,
    pub(crate) consecutive_full: AtomicU32,
    queued_msgs: AtomicUsize,
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

impl PeerWireSlot {
    pub(crate) fn new(
        peer_id: u64,
        has_transform: bool,
        transform_passthrough: Option<(Bytes, usize)>,
        arena_threshold: usize,
        cap: usize,
        #[cfg(feature = "ws")] is_ws: bool,
        #[cfg(feature = "ws")] ws_masked: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            eq: Mutex::new(EncodedQueue::with_arena_threshold(arena_threshold)),
            cap,
            data_ready: Notify::new(),
            space_available: Notify::new(),
            handshake_done: AtomicBool::new(false),
            pending: AtomicBool::new(false),
            has_transform,
            transform_passthrough,
            #[cfg(feature = "ws")]
            is_ws,
            #[cfg(feature = "ws")]
            ws_masked,
            dead: AtomicBool::new(false),
            peer_id,
            consecutive_full: AtomicU32::new(0),
            queued_msgs: AtomicUsize::new(0),
        })
    }

    #[cfg(feature = "ws")]
    pub(crate) fn is_ws(&self) -> bool {
        self.is_ws
    }

    #[inline]
    pub(crate) fn try_encode(&self, msg: &Message) -> TryEncodeResult {
        if self.dead.load(Ordering::Acquire) {
            return TryEncodeResult::Dead;
        }
        if !self.handshake_done.load(Ordering::Acquire) {
            return TryEncodeResult::Ineligible;
        }

        #[cfg(feature = "ws")]
        if self.is_ws {
            if self.has_transform {
                return TryEncodeResult::Ineligible;
            }
            let mut eq = self.eq.lock().expect("wire_slot eq poisoned");
            if self.is_full(&eq) {
                return TryEncodeResult::Full;
            }
            eq.encode_ws(msg, self.ws_masked);
            self.queued_msgs.fetch_add(1, Ordering::Relaxed);
            drop(eq);
            self.signal_encoded();
            return TryEncodeResult::Ok;
        }

        if !self.has_transform {
            let mut eq = self.eq.lock().expect("wire_slot eq poisoned");
            if self.is_full(&eq) {
                return TryEncodeResult::Full;
            }
            eq.encode_auto(msg);
            self.queued_msgs.fetch_add(1, Ordering::Relaxed);
            drop(eq);
            self.signal_encoded();
            return TryEncodeResult::Ok;
        }

        if let Some((ref sentinel, threshold)) = self.transform_passthrough
            && msg.iter().all(|part| part.len() < threshold)
        {
            let mut eq = self.eq.lock().expect("wire_slot eq poisoned");
            if self.is_full(&eq) {
                return TryEncodeResult::Full;
            }
            eq.encode_prefixed_auto(sentinel, msg);
            self.queued_msgs.fetch_add(1, Ordering::Relaxed);
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
        if self.is_full(&eq) {
            return TryEncodeResult::Full;
        }
        eq.push_shared_chunks(chunks);
        self.queued_msgs.fetch_add(1, Ordering::Relaxed);
        drop(eq);
        self.signal_encoded();
        TryEncodeResult::Ok
    }

    pub(crate) fn try_push_pre_encoded_no_signal(&self, data: &[u8]) -> TryEncodeResult {
        if self.dead.load(Ordering::Acquire) {
            return TryEncodeResult::Dead;
        }
        let mut eq = self.eq.lock().expect("wire_slot eq poisoned");
        if self.is_full(&eq) {
            return TryEncodeResult::Full;
        }
        eq.push_pre_encoded(data);
        self.queued_msgs.fetch_add(1, Ordering::Relaxed);
        TryEncodeResult::Ok
    }

    pub(crate) fn signal_encoded(&self) {
        if !self.pending.swap(true, Ordering::Release) {
            self.data_ready.notify_one();
        }
    }

    pub(crate) fn drain_into_vec(&self, buf: &mut Vec<Bytes>, max_chunks: usize) {
        let mut eq = self.eq.lock().expect("wire_slot eq poisoned");
        eq.drain_into_vec(buf, max_chunks);
        if eq.is_empty() {
            self.pending.store(false, Ordering::Relaxed);
            self.queued_msgs.store(0, Ordering::Relaxed);
        } else if !buf.is_empty() {
            self.queued_msgs
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    Some(n.saturating_sub(max_chunks))
                })
                .ok();
        }
    }

    pub(crate) fn has_space(&self) -> bool {
        if self.dead.load(Ordering::Acquire) {
            return false;
        }
        let eq = self.eq.lock().expect("wire_slot eq poisoned");
        !self.is_full(&eq)
    }

    pub(crate) fn is_empty(&self) -> bool {
        let eq = self.eq.lock().expect("wire_slot eq poisoned");
        eq.is_empty()
    }

    pub(crate) fn mark_dead(&self) {
        self.dead.store(true, Ordering::Release);
        self.space_available.notify_waiters();
    }

    fn is_full(&self, eq: &EncodedQueue) -> bool {
        eq.total_bytes() >= self.cap
            || self.queued_msgs.load(Ordering::Relaxed) >= WIRE_SLOT_MSG_CAP_DEFAULT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_slot_caps_queued_messages_independent_of_bytes() {
        let slot = PeerWireSlot::new(
            1,
            false,
            None,
            omq_proto::encoded_queue::ARENA_THRESHOLD,
            WIRE_SLOT_CAP_DEFAULT,
        );
        slot.handshake_done.store(true, Ordering::Release);
        let msg = Message::from("x");

        for _ in 0..WIRE_SLOT_MSG_CAP_DEFAULT {
            assert_eq!(slot.try_encode(&msg), TryEncodeResult::Ok);
        }
        assert_eq!(slot.try_encode(&msg), TryEncodeResult::Full);

        let mut chunks = Vec::new();
        slot.drain_into_vec(&mut chunks, 1024);
        assert_eq!(slot.try_encode(&msg), TryEncodeResult::Ok);
    }
}

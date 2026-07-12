// During connection churn (heartbeat timeout, peer restart, network
// blip) a small number of messages may be reordered. The wire slot
// bypass and the driver inbox are two independent paths into the same
// TCP stream. When a new connection's handshake completes, one
// in-flight message may still be in the inbox while the next message
// takes the wire slot fast path and reaches the wire first.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use tokio::sync::Notify;

use super::signal::DataSignal;
use omq_proto::frame_buffer::FrameBuffer;
use omq_proto::handle_frame::{
    HandleFrameCaps, HandleFrameDecision, HandleFrameState, decide_handle_frame,
};
use omq_proto::message::Message;

pub(crate) const TRANSMIT_SLOT_CAP_DEFAULT: usize = 512 * 1024;
#[cfg(test)]
pub(crate) const TRANSMIT_SLOT_MSG_CAP_DEFAULT: usize = 1000;
pub(crate) const TRANSMIT_SLOT_INLINE_CAP: usize = 72;
const TRANSMIT_SLOT_LWM_DIVISOR: usize = 2;

type FanOutReactivation = Arc<dyn Fn(u64) + Send + Sync + 'static>;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) enum TransmitChunk {
    Inline {
        buf: [u8; TRANSMIT_SLOT_INLINE_CAP],
        len: u16,
    },
    Shared(Arc<[Bytes]>),
}

impl TransmitChunk {
    #[allow(dead_code)]
    pub(crate) fn inline(data: &[u8]) -> Self {
        debug_assert!(data.len() <= TRANSMIT_SLOT_INLINE_CAP);
        let mut buf = [0; TRANSMIT_SLOT_INLINE_CAP];
        buf[..data.len()].copy_from_slice(data);
        Self::Inline {
            buf,
            len: data.len() as u16,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn shared(chunks: Arc<[Bytes]>) -> Self {
        Self::Shared(chunks)
    }

    fn byte_len(&self) -> usize {
        match self {
            Self::Inline { len, .. } => *len as usize,
            Self::Shared(chunks) => chunks.iter().map(Bytes::len).sum(),
        }
    }

    fn drain_into(self, out: &mut Vec<Bytes>, inline: &mut BytesMut) {
        match self {
            Self::Inline { buf, len } => {
                inline.extend_from_slice(&buf[..len as usize]);
            }
            Self::Shared(chunks) => {
                flush_inline(inline, out);
                out.extend(chunks.iter().cloned());
            }
        }
    }
}

fn flush_inline(inline: &mut BytesMut, out: &mut Vec<Bytes>) {
    if !inline.is_empty() {
        out.push(inline.split().freeze());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TryFrameResult {
    Ok,
    Dead,
    Full,
    Ineligible,
}

pub(crate) struct PeerTransmitSlot {
    eq: Mutex<FrameBuffer>,
    ring_rx: Mutex<yring::Consumer<TransmitChunk>>,
    cap: usize,
    msg_cap: usize,
    pub(crate) data_signal: DataSignal,
    pub(crate) space_available: Notify,
    pub(crate) handshake_done: AtomicBool,
    pub(crate) has_transform: bool,
    pub(crate) transform_passthrough: Option<(Bytes, usize)>,
    #[cfg(feature = "ws")]
    is_ws: bool,
    #[cfg(feature = "ws")]
    ws_masked: bool,
    pub(crate) dead: AtomicBool,
    pub(crate) peer_id: u64,
    queued_msgs: AtomicUsize,
    queued_ring_bytes: AtomicUsize,
    fanout_dict_shipped: AtomicBool,
    fanout_active: AtomicBool,
    above_lwm: AtomicBool,
    fanout_reactivation: Mutex<Option<FanOutReactivation>>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DrainOutcome {
    pub(crate) space_available: bool,
}

impl std::fmt::Debug for PeerTransmitSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerTransmitSlot")
            .field("peer_id", &self.peer_id)
            .field(
                "handshake_done",
                &self.handshake_done.load(Ordering::Relaxed),
            )
            .field("dead", &self.dead.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl PeerTransmitSlot {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        peer_id: u64,
        has_transform: bool,
        transform_passthrough: Option<(Bytes, usize)>,
        arena_threshold: usize,
        cap: usize,
        msg_cap: usize,
        #[cfg(feature = "ws")] is_ws: bool,
        #[cfg(feature = "ws")] ws_masked: bool,
    ) -> (Arc<Self>, yring::Producer<TransmitChunk>) {
        let (ring_tx, ring_rx) = yring::spsc(msg_cap.max(1));
        let slot = Arc::new(Self {
            eq: Mutex::new(FrameBuffer::with_arena_threshold(arena_threshold)),
            ring_rx: Mutex::new(ring_rx),
            cap,
            msg_cap: msg_cap.max(1),
            data_signal: DataSignal::new(),
            space_available: Notify::new(),
            handshake_done: AtomicBool::new(false),
            has_transform,
            transform_passthrough,
            #[cfg(feature = "ws")]
            is_ws,
            #[cfg(feature = "ws")]
            ws_masked,
            dead: AtomicBool::new(false),
            peer_id,
            queued_msgs: AtomicUsize::new(0),
            queued_ring_bytes: AtomicUsize::new(0),
            fanout_dict_shipped: AtomicBool::new(false),
            fanout_active: AtomicBool::new(true),
            above_lwm: AtomicBool::new(false),
            fanout_reactivation: Mutex::new(None),
        });
        (slot, ring_tx)
    }

    #[cfg(feature = "ws")]
    pub(crate) fn is_ws(&self) -> bool {
        self.is_ws
    }

    #[inline]
    pub(crate) fn try_encode(&self, msg: &Message) -> TryFrameResult {
        if self.dead.load(Ordering::Acquire) {
            return TryFrameResult::Dead;
        }
        if !self.handshake_done.load(Ordering::Acquire) {
            return TryFrameResult::Ineligible;
        }

        let mut eq = self.eq.lock().expect("transmit_slot eq poisoned");
        let decision = decide_handle_frame(
            HandleFrameState {
                uses_crypto: false,
                handshake_done: true,
                has_transform: self.has_transform,
                transform_passthrough: self.transform_passthrough.as_ref(),
                #[cfg(feature = "ws")]
                is_ws: self.is_ws,
                #[cfg(not(feature = "ws"))]
                is_ws: false,
                queued_bytes: eq.total_bytes() + self.queued_ring_bytes.load(Ordering::Relaxed),
                queued_messages: self.queued_msgs.load(Ordering::Relaxed),
            },
            HandleFrameCaps {
                byte_cap: self.cap,
                message_cap: self.msg_cap,
            },
            msg,
        );
        match decision {
            HandleFrameDecision::Plain => eq.frame(msg),
            #[cfg(feature = "ws")]
            HandleFrameDecision::WebSocket => eq.frame_ws(msg, self.ws_masked),
            #[cfg(not(feature = "ws"))]
            HandleFrameDecision::WebSocket => unreachable!("ws disabled"),
            HandleFrameDecision::TransformPassthrough { sentinel } => {
                eq.frame_prefixed(sentinel, msg);
            }
            HandleFrameDecision::Full => {
                self.above_lwm.store(true, Ordering::Relaxed);
                return TryFrameResult::Full;
            }
            HandleFrameDecision::Ineligible => return TryFrameResult::Ineligible,
        }
        self.queued_msgs.fetch_add(1, Ordering::Relaxed);
        self.mark_above_lwm_if_needed(eq.total_bytes(), self.queued_msgs.load(Ordering::Relaxed));
        drop(eq);
        self.signal_encoded();
        TryFrameResult::Ok
    }

    pub(crate) fn try_push_encoded(&self, chunks: &[Bytes]) -> TryFrameResult {
        if self.dead.load(Ordering::Acquire) {
            return TryFrameResult::Dead;
        }
        let bytes = chunks.iter().map(Bytes::len).sum();
        let mut eq = self.eq.lock().expect("transmit_slot eq poisoned");
        if self.is_full(&eq) || eq.total_bytes().saturating_add(bytes) >= self.cap {
            self.above_lwm.store(true, Ordering::Relaxed);
            return TryFrameResult::Full;
        }
        eq.push_shared_chunks(chunks);
        self.queued_msgs.fetch_add(1, Ordering::Relaxed);
        self.mark_above_lwm_if_needed(
            eq.total_bytes() + self.queued_ring_bytes.load(Ordering::Relaxed),
            self.queued_msgs.load(Ordering::Relaxed),
        );
        drop(eq);
        self.signal_encoded();
        TryFrameResult::Ok
    }

    pub(crate) fn try_push_pre_framed_no_signal(&self, data: &[u8]) -> TryFrameResult {
        if self.dead.load(Ordering::Acquire) {
            return TryFrameResult::Dead;
        }
        let mut eq = self.eq.lock().expect("transmit_slot eq poisoned");
        if self.is_full(&eq) {
            self.above_lwm.store(true, Ordering::Relaxed);
            return TryFrameResult::Full;
        }
        eq.push_pre_framed(data);
        self.queued_msgs.fetch_add(1, Ordering::Relaxed);
        self.mark_above_lwm_if_needed(
            eq.total_bytes() + self.queued_ring_bytes.load(Ordering::Relaxed),
            self.queued_msgs.load(Ordering::Relaxed),
        );
        TryFrameResult::Ok
    }

    #[allow(dead_code)]
    pub(crate) fn try_push_ring_item(
        &self,
        tx: &mut yring::Producer<TransmitChunk>,
        item: TransmitChunk,
    ) -> TryFrameResult {
        if self.dead.load(Ordering::Acquire) {
            return TryFrameResult::Dead;
        }
        if !self.handshake_done.load(Ordering::Acquire) {
            return TryFrameResult::Ineligible;
        }
        let bytes = item.byte_len();
        if self
            .queued_ring_bytes
            .load(Ordering::Relaxed)
            .saturating_add(bytes)
            >= self.cap
            || self.queued_msgs.load(Ordering::Relaxed) >= self.msg_cap
        {
            self.above_lwm.store(true, Ordering::Relaxed);
            return TryFrameResult::Full;
        }
        if tx.is_full() {
            self.above_lwm.store(true, Ordering::Relaxed);
            return TryFrameResult::Full;
        }
        if tx.push(item).is_err() {
            self.above_lwm.store(true, Ordering::Relaxed);
            return TryFrameResult::Full;
        }
        self.queued_ring_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.queued_msgs.fetch_add(1, Ordering::Relaxed);
        self.mark_above_lwm_if_needed(
            self.queued_ring_bytes.load(Ordering::Relaxed),
            self.queued_msgs.load(Ordering::Relaxed),
        );
        TryFrameResult::Ok
    }

    #[allow(dead_code)]
    pub(crate) fn flush_ring(&self, tx: &mut yring::Producer<TransmitChunk>) {
        tx.flush();
        self.signal_encoded();
    }

    pub(crate) fn signal_encoded(&self) {
        self.data_signal.mark();
    }

    #[inline]
    pub(crate) fn fanout_dict_shipped(&self) -> bool {
        self.fanout_dict_shipped.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn mark_fanout_dict_shipped(&self) {
        self.fanout_dict_shipped.store(true, Ordering::Release);
    }

    #[inline]
    pub(crate) fn fanout_active(&self) -> bool {
        self.fanout_active.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn deactivate_fanout(&self) -> bool {
        self.fanout_active
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn set_fanout_reactivation(&self, cb: FanOutReactivation) {
        *self
            .fanout_reactivation
            .lock()
            .expect("transmit_slot fanout_reactivation poisoned") = Some(cb);
    }

    /// Arena-only fast path: if all queued data is in the
    /// [`FrameBuffer`] arena (no external `Bytes`, no ring items),
    /// copy the arena bytes into `out` and clear the arena,
    /// preserving its capacity. Returns `None` if the fast path
    /// does not apply.
    pub(crate) fn try_drain_arena_only(&self, out: &mut Vec<u8>) -> Option<DrainOutcome> {
        {
            let rx = self.ring_rx.lock().expect("transmit_slot ring_rx poisoned");
            if !rx.is_empty() {
                return None;
            }
        }
        let mut eq = self.eq.lock().expect("transmit_slot eq poisoned");
        if !eq.has_arena_only() {
            return None;
        }
        out.extend_from_slice(eq.arena_bytes());
        eq.clear_arena();
        drop(eq);

        self.data_signal.clear();
        self.queued_msgs.store(0, Ordering::Relaxed);
        self.queued_ring_bytes.store(0, Ordering::Relaxed);
        let below_lwm = self.is_below_lwm(0, 0);
        let space_available = below_lwm && self.above_lwm.swap(false, Ordering::AcqRel);
        if below_lwm
            && self
                .fanout_active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            && let Some(cb) = self
                .fanout_reactivation
                .lock()
                .expect("transmit_slot fanout_reactivation poisoned")
                .clone()
        {
            cb(self.peer_id);
        }
        Some(DrainOutcome { space_available })
    }

    pub(crate) fn drain(&self, buf: &mut Vec<Bytes>, max_chunks: usize) -> DrainOutcome {
        let mut eq = self.eq.lock().expect("transmit_slot eq poisoned");
        let before_chunks = buf.len();
        eq.drain(buf, max_chunks);
        let eq_drained_chunks = buf.len() - before_chunks;
        let eq_empty = eq.is_empty();
        let eq_bytes = eq.total_bytes();
        drop(eq);

        let mut popped_items = 0usize;
        let mut popped_bytes = 0usize;
        if buf.len() < max_chunks {
            let mut rx = self.ring_rx.lock().expect("transmit_slot ring_rx poisoned");
            rx.prefetch();
            let mut inline = BytesMut::with_capacity(TRANSMIT_SLOT_INLINE_CAP * 4);
            while buf.len() < max_chunks {
                let Some(item) = rx.pop() else {
                    break;
                };
                popped_items += 1;
                popped_bytes += item.byte_len();
                item.drain_into(buf, &mut inline);
            }
            flush_inline(&mut inline, buf);
            if popped_items > 0 {
                rx.release();
            }
        }

        if popped_items > 0 {
            self.queued_ring_bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    Some(n.saturating_sub(popped_bytes))
                })
                .ok();
            self.queued_msgs
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    Some(n.saturating_sub(popped_items))
                })
                .ok();
        }

        if eq_drained_chunks > 0 {
            self.queued_msgs
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    Some(n.saturating_sub(eq_drained_chunks))
                })
                .ok();
        }

        if eq_empty && self.ring_is_empty() {
            self.data_signal.clear();
            self.queued_msgs.store(0, Ordering::Relaxed);
            self.queued_ring_bytes.store(0, Ordering::Relaxed);
        }
        let queued_bytes = eq_bytes + self.queued_ring_bytes.load(Ordering::Relaxed);
        let queued_msgs = self.queued_msgs.load(Ordering::Relaxed);
        let below_lwm = self.is_below_lwm(queued_bytes, queued_msgs);
        let space_available = below_lwm && self.above_lwm.swap(false, Ordering::AcqRel);
        if below_lwm
            && self
                .fanout_active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            && let Some(cb) = self
                .fanout_reactivation
                .lock()
                .expect("transmit_slot fanout_reactivation poisoned")
                .clone()
        {
            cb(self.peer_id);
        }
        DrainOutcome { space_available }
    }

    pub(crate) fn is_empty(&self) -> bool {
        let eq = self.eq.lock().expect("transmit_slot eq poisoned");
        eq.is_empty() && self.ring_is_empty()
    }

    pub(crate) fn mark_dead(&self) {
        self.dead.store(true, Ordering::Release);
        {
            let mut eq = self.eq.lock().expect("transmit_slot eq poisoned");
            *eq = FrameBuffer::one_shot();
        }
        {
            let mut rx = self.ring_rx.lock().expect("transmit_slot ring_rx poisoned");
            rx.prefetch();
            while rx.pop().is_some() {}
            rx.release();
        }
        self.queued_msgs.store(0, Ordering::Relaxed);
        self.queued_ring_bytes.store(0, Ordering::Relaxed);
        self.fanout_active.store(false, Ordering::Relaxed);
        self.above_lwm.store(false, Ordering::Relaxed);
        self.data_signal.wake_all();
        self.space_available.notify_waiters();
    }

    fn is_full(&self, eq: &FrameBuffer) -> bool {
        eq.total_bytes() + self.queued_ring_bytes.load(Ordering::Relaxed) >= self.cap
            || self.queued_msgs.load(Ordering::Relaxed) >= self.msg_cap
    }

    fn mark_above_lwm_if_needed(&self, queued_bytes: usize, queued_messages: usize) {
        if !self.is_below_lwm(queued_bytes, queued_messages) {
            self.above_lwm.store(true, Ordering::Relaxed);
        }
    }

    fn is_below_lwm(&self, queued_bytes: usize, queued_messages: usize) -> bool {
        queued_bytes <= self.cap / TRANSMIT_SLOT_LWM_DIVISOR
            && queued_messages <= self.msg_cap / TRANSMIT_SLOT_LWM_DIVISOR
    }

    fn ring_is_empty(&self) -> bool {
        self.ring_rx
            .lock()
            .expect("transmit_slot ring_rx poisoned")
            .is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transmit_slot_caps_queued_messages_independent_of_bytes() {
        let (slot, _tx) = PeerTransmitSlot::new(
            1,
            false,
            None,
            omq_proto::frame_buffer::ARENA_THRESHOLD,
            TRANSMIT_SLOT_CAP_DEFAULT,
            TRANSMIT_SLOT_MSG_CAP_DEFAULT,
            #[cfg(feature = "ws")]
            false,
            #[cfg(feature = "ws")]
            false,
        );
        slot.handshake_done.store(true, Ordering::Release);
        let msg = Message::from("x");

        for _ in 0..TRANSMIT_SLOT_MSG_CAP_DEFAULT {
            assert_eq!(slot.try_encode(&msg), TryFrameResult::Ok);
        }
        assert_eq!(slot.try_encode(&msg), TryFrameResult::Full);

        let mut chunks = Vec::new();
        slot.drain(&mut chunks, 1024);
        assert_eq!(slot.try_encode(&msg), TryFrameResult::Ok);
    }
}

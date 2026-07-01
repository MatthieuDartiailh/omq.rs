//! Shared wire-slot cache between the socket handle and actor.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use omq_proto::error::Result;
use omq_proto::message::Message;
use omq_proto::proto::SocketType;
use omq_proto::routing::{SendCategory, send_category};

use super::handle::TrySendError;
use crate::engine::wire_slot::{PeerWireSlot, TryEncodeResult};
use crate::routing::SendSubmitter;

type WireSlotHolder = Arc<Mutex<Option<Arc<PeerWireSlot>>>>;
type RrSlots = Arc<RrSlotsInner>;

/// Multi-peer round-robin wire slots, shared between the handle (reads)
/// and the actor (writes on peer add/remove). When more than one wire
/// peer is active on a round-robin socket and none are inproc, the actor
/// fills `slots` with every peer's [`PeerWireSlot`]; the handle picks the
/// next one per send via `cursor`, giving strict round-robin distribution
/// over the per-peer direct-encode fast path. Empty for single-peer /
/// inproc-mixed / identity-routed sockets (those fall back to the shared
/// work-stealing queue).
#[derive(Debug, Default)]
struct RrSlotsInner {
    slots: Mutex<Vec<Arc<PeerWireSlot>>>,
    cursor: AtomicUsize,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct WireSlotCache {
    single: WireSlotHolder,
    rr: RrSlots,
}

impl WireSlotCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn clear_single(&self) {
        *self.single.lock().expect("wire_slot") = None;
    }

    pub(crate) fn rebuild<I>(&self, socket_type: SocketType, peer_count: usize, peer_slots: I)
    where
        I: IntoIterator<Item = Option<Arc<PeerWireSlot>>>,
    {
        let cat = send_category(socket_type);
        if !matches!(cat, SendCategory::RoundRobin | SendCategory::Exclusive) {
            return;
        }

        let slots: Vec<_> = peer_slots.into_iter().collect();
        let mut guard = self.single.lock().expect("wire_slot");
        let mut rr = self.rr.slots.lock().expect("rr_slots");
        rr.clear();

        if peer_count == 1
            && let Some(Some(slot)) = slots.first()
            && slot.handshake_done.load(Ordering::Acquire)
        {
            *guard = Some(slot.clone());
            return;
        }

        *guard = None;
        // Multi-peer round-robin: populate per-peer wire slots so the handle
        // dispatches directly to one peer at a time. Only when every peer is
        // a wire peer (no inproc) - inproc peers have no wire slot and would
        // be starved if the round-robin only cycled wire slots. Mixed or
        // inproc-only sets fall back to the shared queue (rr left empty).
        if matches!(cat, SendCategory::RoundRobin)
            && peer_count > 1
            && slots.iter().all(|slot| {
                slot.as_ref()
                    .is_some_and(|s| s.handshake_done.load(Ordering::Acquire))
            })
        {
            rr.extend(slots.into_iter().flatten());
        }
    }

    /// Synchronous single-peer wire encode fast path. Returns true if
    /// the message was encoded into the peer's `EncodedQueue`.
    #[inline]
    pub(crate) fn try_send(&self, msg: &Message) -> bool {
        if let Some(ref slot) = self.single_slot() {
            return slot.try_encode(msg) == TryEncodeResult::Ok;
        }
        false
    }

    pub(crate) fn single_exists(&self) -> bool {
        self.single.lock().expect("wire_slot").is_some()
    }

    pub(crate) fn single_dead(&self) -> bool {
        self.single
            .lock()
            .expect("wire_slot")
            .as_ref()
            .is_some_and(|s| s.dead.load(Ordering::Acquire))
    }

    pub(crate) async fn send_round_robin(
        &self,
        msg: Message,
        submitter: &SendSubmitter,
        starvation_threshold: u32,
    ) -> Result<()> {
        let starved = {
            let slots = self.rr.slots.lock().expect("rr_slots");
            let n = slots.len();
            let mut starved_slot = None;
            for _ in 0..n {
                let i = self.rr.cursor.fetch_add(1, Ordering::Relaxed) % n;
                match slots[i].try_encode(&msg) {
                    TryEncodeResult::Ok => {
                        slots[i].consecutive_full.store(0, Ordering::Relaxed);
                        return Ok(());
                    }
                    TryEncodeResult::Full => {
                        let prev = slots[i].consecutive_full.fetch_add(1, Ordering::Relaxed);
                        if prev >= starvation_threshold {
                            starved_slot = Some(slots[i].clone());
                            break;
                        }
                    }
                    TryEncodeResult::Dead | TryEncodeResult::Ineligible => {}
                }
            }
            starved_slot
        };

        if let Some(slot) = starved {
            let notified = slot.space_available.notified();
            if slot.try_encode(&msg) == TryEncodeResult::Ok {
                slot.consecutive_full.store(0, Ordering::Relaxed);
                return Ok(());
            }
            let _ = tokio::time::timeout(std::time::Duration::from_millis(1), notified).await;
            if slot.try_encode(&msg) == TryEncodeResult::Ok {
                slot.consecutive_full.store(0, Ordering::Relaxed);
                return Ok(());
            }
        }

        submitter.send(msg).await
    }

    /// Single-peer async slow path: handles backpressure (Full -> wait
    /// for space) and falls back to the shared queue for ineligible peers.
    pub(crate) async fn send_single_slow(
        &self,
        msg: Message,
        submitter: &SendSubmitter,
    ) -> Result<()> {
        let slot = self.single_slot();
        if let Some(ref slot) = slot {
            loop {
                match slot.try_encode(&msg) {
                    TryEncodeResult::Ok => return Ok(()),
                    TryEncodeResult::Dead | TryEncodeResult::Ineligible => break,
                    TryEncodeResult::Full => {
                        let notified = slot.space_available.notified();
                        if slot.try_encode(&msg) == TryEncodeResult::Ok {
                            return Ok(());
                        }
                        notified.await;
                    }
                }
            }
        }
        submitter.send(msg).await
    }

    pub(crate) fn try_send_single(
        &self,
        msg: &Message,
    ) -> core::result::Result<bool, TrySendError> {
        let Some(slot) = self.single_slot() else {
            return Ok(false);
        };
        match slot.try_encode(msg) {
            TryEncodeResult::Ok => Ok(true),
            TryEncodeResult::Full => Err(TrySendError::Full(msg.clone())),
            TryEncodeResult::Dead | TryEncodeResult::Ineligible => Ok(false),
        }
    }

    fn single_slot(&self) -> Option<Arc<PeerWireSlot>> {
        self.single.lock().expect("wire_slot").clone()
    }
}

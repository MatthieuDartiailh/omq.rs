//! Shared wire-slot cache between the socket handle and actor.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwapOption;
use omq_proto::error::Result;
use omq_proto::message::Message;
use omq_proto::proto::SocketType;
use omq_proto::routing::{SendCategory, send_category};

use super::handle::TrySendError;
use crate::engine::wire_slot::{PeerWireSlot, TryEncodeResult};
use crate::routing::SendSubmitter;

#[derive(Clone, Debug)]
pub(crate) struct WireSlotCache {
    single: Arc<ArcSwapOption<PeerWireSlot>>,
    single_available: Arc<AtomicBool>,
}

impl WireSlotCache {
    pub(crate) fn new() -> Self {
        Self {
            single: Arc::new(ArcSwapOption::empty()),
            single_available: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn clear_single(&self) {
        self.single_available.store(false, Ordering::Release);
        self.single.store(None);
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
        if peer_count == 1
            && let Some(Some(slot)) = slots.first()
            && slot.handshake_done.load(Ordering::Acquire)
        {
            self.single.store(Some(slot.clone()));
            self.single_available.store(true, Ordering::Release);
            return;
        }

        self.clear_single();
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
        self.single_available.load(Ordering::Acquire)
    }

    pub(crate) fn single_dead(&self) -> bool {
        if !self.single_exists() {
            return false;
        }
        self.single
            .load()
            .as_ref()
            .is_some_and(|s| s.dead.load(Ordering::Acquire))
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
        if !self.single_exists() {
            return None;
        }
        self.single.load_full()
    }
}

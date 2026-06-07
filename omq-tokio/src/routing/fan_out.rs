//! Fan-out send: per-peer `PeerWireSlot`, filtered by subscription.
//!
//! PUB and XPUB filter by SUBSCRIBE-driven prefix set; RADIO filters
//! by joined groups. On every `send`, the message is encoded once
//! (via `pre_encode`), then the pre-encoded chunks are pushed into
//! each matching peer's `EncodedQueue`. The driver flushes to the wire.

use std::sync::{Arc, Mutex};

use rustc_hash::{FxHashMap, FxHashSet};

use smallvec::SmallVec;

use bytes::Bytes;

use crate::engine::wire_slot::{self, TryEncodeResult};
use crate::engine::{DriverCommand, DriverHandle};
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;

use super::peer_send::PeerSend;
use super::subscription::SubscriptionSet;

/// Filter mode for a fan-out send strategy.
#[derive(Debug, Clone, Copy)]
pub(crate) enum FanOutMode {
    /// PUB / XPUB: prefix-match against peer subscriptions.
    SubscriptionPrefix,
    /// RADIO: exact-match against peer joined groups.
    Group,
}

#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    inner: Arc<Mutex<FanOutInner>>,
    mode: FanOutMode,
}

impl Submitter {
    pub(crate) fn try_send(
        &self,
        msg: Message,
    ) -> core::result::Result<(), crate::socket::handle::TrySendError> {
        let (forwarded, group) = match self.mode {
            FanOutMode::SubscriptionPrefix => (msg, None),
            FanOutMode::Group => {
                if msg.len() != 2 {
                    return Err(crate::socket::handle::TrySendError::Error(Error::Protocol(
                        "RADIO send requires [group, body] (2 parts)".into(),
                    )));
                }
                let group_bytes = msg.part_bytes(0).unwrap_or_default();
                if group_bytes.len() > u8::MAX as usize {
                    return Err(crate::socket::handle::TrySendError::Error(Error::Protocol(
                        "RADIO group name too long (max 255 bytes)".into(),
                    )));
                }
                let group = String::from_utf8_lossy(&group_bytes).into_owned();
                (msg, Some(group))
            }
        };

        let targets = self.collect_targets(&forwarded, group.as_deref());
        dispatch_to_targets(&targets, &forwarded);
        Ok(())
    }

    #[expect(clippy::unused_async)]
    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        let (forwarded, group) = match self.mode {
            FanOutMode::SubscriptionPrefix => (msg, None),
            FanOutMode::Group => {
                if msg.len() != 2 {
                    return Err(Error::Protocol(
                        "RADIO send requires [group, body] (2 parts)".into(),
                    ));
                }
                let group_bytes = msg.part_bytes(0).unwrap_or_default();
                if group_bytes.len() > u8::MAX as usize {
                    return Err(Error::Protocol(
                        "RADIO group name too long (max 255 bytes)".into(),
                    ));
                }
                let group = String::from_utf8_lossy(&group_bytes).into_owned();
                (msg, Some(group))
            }
        };

        let targets = self.collect_targets(&forwarded, group.as_deref());
        dispatch_to_targets(&targets, &forwarded);
        Ok(())
    }

    fn collect_targets(&self, msg: &Message, group: Option<&str>) -> SmallVec<[PeerSend; 8]> {
        let g = self.inner.lock().expect("fanout inner poisoned");
        if g.all_subscribe_all && matches!(self.mode, FanOutMode::SubscriptionPrefix) {
            g.all_targets.clone()
        } else {
            g.peers
                .values()
                .filter(|p| match (self.mode, group) {
                    (FanOutMode::Group, Some(grp)) => p.any_groups || p.groups.contains(grp),
                    (FanOutMode::SubscriptionPrefix, _) => {
                        p.subscriptions.matches(&first_frame_bytes(msg))
                    }
                    (FanOutMode::Group, None) => false,
                })
                .map(|p| p.target.clone())
                .collect()
        }
    }
}

/// Fan-out send strategy.
#[derive(Debug)]
pub(crate) struct FanOutSend {
    inner: Arc<Mutex<FanOutInner>>,
    mode: FanOutMode,
}

#[derive(Debug)]
struct FanOutInner {
    peers: FxHashMap<u64, FanOutPeer>,
    all_subscribe_all: bool,
    all_targets: SmallVec<[PeerSend; 8]>,
}

#[derive(Debug)]
struct FanOutPeer {
    subscriptions: SubscriptionSet,
    groups: FxHashSet<String>,
    any_groups: bool,
    target: PeerSend,
}

impl FanOutInner {
    fn recompute_subscribe_all(&mut self) {
        self.all_subscribe_all = !self.peers.is_empty()
            && self
                .peers
                .values()
                .all(|p| p.subscriptions.is_subscribe_all());
        if self.all_subscribe_all {
            self.all_targets = self.peers.values().map(|p| p.target.clone()).collect();
        } else {
            self.all_targets.clear();
        }
    }
}

impl FanOutSend {
    pub(crate) fn new(_options: &Options, mode: FanOutMode) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FanOutInner {
                peers: FxHashMap::default(),
                all_subscribe_all: false,
                all_targets: SmallVec::new(),
            })),
            mode,
        }
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            inner: self.inner.clone(),
            mode: self.mode,
        }
    }

    pub(crate) fn connection_added(&mut self, peer_id: u64, handle: DriverHandle) {
        self.add_peer(peer_id, handle, false);
    }

    pub(crate) fn connection_added_any_groups(&mut self, peer_id: u64, handle: DriverHandle) {
        self.add_peer(peer_id, handle, true);
    }

    #[expect(clippy::needless_pass_by_value)]
    fn add_peer(&mut self, peer_id: u64, handle: DriverHandle, any_groups: bool) {
        let target = PeerSend::from_handle(&handle);
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.insert(
            peer_id,
            FanOutPeer {
                subscriptions: SubscriptionSet::new(),
                groups: FxHashSet::default(),
                any_groups,
                target,
            },
        );
        g.recompute_subscribe_all();
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if g.peers.remove(&peer_id).is_some() {
            g.recompute_subscribe_all();
        }
    }

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn peer_subscribe(&self, peer_id: u64, prefix: Bytes) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            p.subscriptions.add(&prefix);
            g.recompute_subscribe_all();
        }
    }

    pub(crate) fn peer_cancel(&self, peer_id: u64, prefix: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            p.subscriptions.remove(prefix);
            g.recompute_subscribe_all();
        }
    }

    pub(crate) fn peer_join(&self, peer_id: u64, group: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id)
            && let Ok(s) = std::str::from_utf8(group)
        {
            p.groups.insert(s.to_string());
        }
    }

    pub(crate) fn peer_leave(&self, peer_id: u64, group: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id)
            && let Ok(s) = std::str::from_utf8(group)
        {
            p.groups.remove(s);
        }
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn shutdown(&self) {}

    pub(crate) fn is_drained(&self) -> bool {
        let g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.values().all(|p| p.target.is_empty())
    }
}

fn dispatch_to_targets(targets: &[PeerSend], msg: &Message) {
    match targets.len() {
        0 => {}
        1 => {
            let _ = targets[0].try_encode(msg);
        }
        _ => {
            let encoded = wire_slot::pre_encode(msg);
            for t in targets {
                match t {
                    PeerSend::Wire { slot, inbox } => {
                        if slot.try_push_encoded(&encoded) == TryEncodeResult::Ineligible {
                            let _ = inbox.try_send(DriverCommand::SendMessage(msg.clone()));
                        }
                    }
                    PeerSend::Inbox(tx) => {
                        let _ = tx.try_send(DriverCommand::SendMessage(msg.clone()));
                    }
                }
            }
        }
    }
}

fn first_frame_bytes(msg: &Message) -> Bytes {
    msg.part_bytes(0).unwrap_or_default()
}

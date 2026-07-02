//! Fan-out send: per-peer `PeerWireSlot`, filtered by subscription.
//!
//! PUB and XPUB filter by SUBSCRIBE-driven prefix set; RADIO filters
//! by joined groups. On every `send`, the message is encoded once
//! into the fan-out arena, then the pre-encoded bytes are pushed into
//! each matching peer's `EncodedQueue`. The driver flushes to the wire.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rustc_hash::{FxHashMap, FxHashSet};

use smallvec::SmallVec;

use bytes::Bytes;

use crate::engine::{DriverCommand, DriverHandle};
use omq_proto::encoded_queue::EncodedQueue;
use omq_proto::error::{Error, Result};
use omq_proto::fan_out_batch::{FanOutBatch, clear_fan_out_batch, finish_fan_out_batch};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::transform::MessageEncoder;

use super::peer_send::PeerSend;
use super::subscription::SubscriptionSet;
use crate::engine::wire_slot::TryEncodeResult;

/// Filter mode for a fan-out send strategy.
#[derive(Debug, Clone, Copy)]
pub(crate) enum FanOutMode {
    /// PUB / XPUB: prefix-match against peer subscriptions.
    SubscriptionPrefix,
    /// RADIO: exact-match against peer joined groups.
    Group,
}

/// Per-peer copy budget for pre-encoded bytes before yielding.
const FAN_OUT_COPY_BUDGET: usize = 128 * 1024;

/// Yield every N peers to keep latency bounded. Scales down with peer
/// count: fewer peers per yield when fan-out is wide (more total work).
/// isqrt gives sub-linear scaling; floor of 16 prevents over-yielding.
fn yield_interval(peer_count: usize) -> u32 {
    let n = (peer_count as u32).max(1);
    (512 / n.isqrt()).max(16)
}

enum CachedResult {
    SoleWire(TryEncodeResult),
    Cached {
        targets: Arc<Vec<PeerSend>>,
        encoder: Option<Arc<Mutex<MessageEncoder>>>,
    },
    Miss,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DispatchStatus {
    Ok,
    Full,
}

#[derive(Debug)]
pub(crate) struct Submitter {
    inner: Arc<Mutex<FanOutInner>>,
    generation: Arc<AtomicU64>,
    mode: FanOutMode,
    send_count: Arc<AtomicU32>,
    xpub_nodrop: bool,
    cached: Mutex<CachedFanOut>,
}

#[derive(Debug, Default)]
struct CachedFanOut {
    generation: u64,
    sole_wire: Option<PeerSend>,
    all_targets: Option<Arc<Vec<PeerSend>>>,
    encoder: Option<Arc<Mutex<MessageEncoder>>>,
    all_wire: bool,
}

impl Clone for Submitter {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            generation: self.generation.clone(),
            mode: self.mode,
            send_count: self.send_count.clone(),
            xpub_nodrop: self.xpub_nodrop,
            cached: Mutex::new(CachedFanOut::default()),
        }
    }
}

impl Submitter {
    pub(crate) fn shutdown(&self) {
        let mut cached = self.cached.lock().unwrap();
        cached.sole_wire = None;
        cached.all_targets = None;
        cached.encoder = None;
        cached.all_wire = false;
        cached.generation = u64::MAX;
    }

    fn validate_group(msg: Message) -> core::result::Result<(Message, Option<String>), Error> {
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
        Ok((msg, Some(group)))
    }

    fn prepare(&self, msg: Message) -> core::result::Result<(Message, Option<String>), Error> {
        match self.mode {
            FanOutMode::SubscriptionPrefix => Ok((msg, None)),
            FanOutMode::Group => Self::validate_group(msg),
        }
    }

    fn try_cached(&self, msg: &Message, group: Option<&str>) -> CachedResult {
        if group.is_some() {
            return CachedResult::Miss;
        }
        let current = self.generation.load(Ordering::Acquire);
        let mut cached = self.cached.lock().unwrap();
        if cached.generation != current {
            let g = self.inner.lock().expect("fanout inner poisoned");
            if g.all_subscribe_all && g.all_targets.len() == 1 && g.fan_out_encoder.is_none() {
                cached.sole_wire = Some(g.all_targets[0].clone());
                cached.all_targets = None;
                cached.encoder = None;
                cached.all_wire = false;
            } else if g.all_subscribe_all && !g.all_targets.is_empty() {
                cached.sole_wire = None;
                let targets: Vec<PeerSend> = g.all_targets.to_vec();
                cached.all_wire = targets.iter().all(|t| matches!(t, PeerSend::Wire { .. }));
                #[cfg(feature = "ws")]
                if cached.all_wire && targets.iter().any(PeerSend::is_ws) {
                    cached.all_wire = false;
                }
                cached.all_targets = Some(Arc::new(targets));
                cached.encoder.clone_from(&g.fan_out_encoder);
            } else {
                cached.sole_wire = None;
                cached.all_targets = None;
                cached.encoder = None;
                cached.all_wire = false;
            }
            cached.generation = current;
        }
        if let Some(ref target) = cached.sole_wire {
            return CachedResult::SoleWire(target.try_encode(msg));
        }
        if let Some(ref targets) = cached.all_targets {
            return CachedResult::Cached {
                targets: targets.clone(),
                encoder: cached.encoder.clone(),
            };
        }
        CachedResult::Miss
    }

    pub(crate) fn try_send(
        &self,
        msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        let (forwarded, group) = self
            .prepare(msg)
            .map_err(omq_proto::error::TrySendError::Error)?;

        match self.try_cached(&forwarded, group.as_deref()) {
            CachedResult::SoleWire(r) => {
                return match r {
                    TryEncodeResult::Ok | TryEncodeResult::Dead | TryEncodeResult::Ineligible => {
                        Ok(())
                    }
                    TryEncodeResult::Full => Err(omq_proto::error::TrySendError::Full(forwarded)),
                };
            }
            CachedResult::Cached { targets, encoder } => {
                if self.xpub_nodrop && !targets_have_space(&targets) {
                    return Err(omq_proto::error::TrySendError::Full(forwarded));
                }
                if dispatch_to_targets(&targets, &forwarded, encoder.as_deref())
                    .map_err(omq_proto::error::TrySendError::Error)?
                    == DispatchStatus::Full
                    && self.xpub_nodrop
                {
                    return Err(omq_proto::error::TrySendError::Full(forwarded));
                }
                return Ok(());
            }
            CachedResult::Miss => {}
        }

        let (targets, encoder) = self.collect_targets(&forwarded, group.as_deref());
        if self.xpub_nodrop && !targets_have_space(&targets) {
            return Err(omq_proto::error::TrySendError::Full(forwarded));
        }
        if dispatch_to_targets(&targets, &forwarded, encoder.as_deref())
            .map_err(omq_proto::error::TrySendError::Error)?
            == DispatchStatus::Full
            && self.xpub_nodrop
        {
            return Err(omq_proto::error::TrySendError::Full(forwarded));
        }
        Ok(())
    }

    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        let (forwarded, group) = self.prepare(msg)?;

        match self.try_cached(&forwarded, group.as_deref()) {
            CachedResult::SoleWire(TryEncodeResult::Ok) => return Ok(()),
            CachedResult::Cached { targets, encoder } => {
                // Encode once and distribute synchronously on this task via
                // the thread-local arena (`dispatch_to_targets`), the same
                // path `try_send` uses. The earlier all-wire fast path
                // handed the message to a single `fan_out_pump` task through
                // `FanOutArena.eq`; under the multi-thread runtime that put
                // the send task and the pump task in a per-message ping-pong
                // on that mutex plus a cross-thread `Notify` wakeup, burning
                // CPU without adding parallelism (distribution stays serial
                // on the one pump). Inline dispatch removes both hops.
                if self.xpub_nodrop {
                    send_to_targets_nodrop(&targets, forwarded.clone()).await?;
                    return Ok(());
                }
                dispatch_to_targets(&targets, &forwarded, encoder.as_deref())?;
                let interval = yield_interval(targets.len());
                if self.send_count.fetch_add(1, Ordering::Relaxed) % interval == interval - 1 {
                    tokio::task::yield_now().await;
                }
                return Ok(());
            }
            _ => {}
        }

        let (targets, encoder) = self.collect_targets(&forwarded, group.as_deref());
        if self.xpub_nodrop {
            send_to_targets_nodrop(&targets, forwarded.clone()).await?;
            return Ok(());
        }
        dispatch_to_targets(&targets, &forwarded, encoder.as_deref())?;
        let interval = yield_interval(targets.len());
        if self.send_count.fetch_add(1, Ordering::Relaxed) % interval == interval - 1 {
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    fn collect_targets(
        &self,
        msg: &Message,
        group: Option<&str>,
    ) -> (SmallVec<[PeerSend; 8]>, Option<Arc<Mutex<MessageEncoder>>>) {
        let g = self.inner.lock().expect("fanout inner poisoned");
        let targets = if g.all_subscribe_all && matches!(self.mode, FanOutMode::SubscriptionPrefix)
        {
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
        };
        let encoder = g.fan_out_encoder.clone();
        (targets, encoder)
    }
}

/// Fan-out send strategy.
#[derive(Debug)]
pub(crate) struct FanOutSend {
    inner: Arc<Mutex<FanOutInner>>,
    generation: Arc<AtomicU64>,
    mode: FanOutMode,
    xpub_nodrop: bool,
}

struct FanOutInner {
    peers: FxHashMap<u64, FanOutPeer>,
    all_subscribe_all: bool,
    all_targets: SmallVec<[PeerSend; 8]>,
    fan_out_encoder: Option<Arc<Mutex<MessageEncoder>>>,
    #[allow(dead_code)]
    options: Options,
}

impl std::fmt::Debug for FanOutInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanOutInner")
            .field("peers", &self.peers.len())
            .field("all_subscribe_all", &self.all_subscribe_all)
            .field("has_fan_out_encoder", &self.fan_out_encoder.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct FanOutPeer {
    subscriptions: SubscriptionSet,
    groups: FxHashSet<String>,
    any_groups: bool,
    target: PeerSend,
}

impl FanOutInner {
    #[allow(clippy::unused_self)]
    fn init_fan_out_encoder(&mut self) {
        #[cfg(feature = "lz4")]
        {
            use omq_proto::endpoint::{Endpoint, Host};
            let dummy = Endpoint::Lz4Tcp {
                host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
                port: 0,
            };
            // Strip the dict: per-connection dict shipping requires the
            // driver's per-peer encoder, which the fan-out path bypasses.
            // Dictless compression still applies the lz4 block transform.
            let mut opts = self.options.clone();
            opts.compression_dict = None;
            if let Some((enc, _dec)) = MessageEncoder::for_endpoint(&dummy, &opts) {
                self.fan_out_encoder = Some(Arc::new(Mutex::new(enc.new_offload())));
            }
        }
    }

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
    pub(crate) fn new(options: &Options, mode: FanOutMode) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FanOutInner {
                peers: FxHashMap::default(),
                all_subscribe_all: false,
                all_targets: SmallVec::new(),
                fan_out_encoder: None,
                options: options.clone(),
            })),
            generation: Arc::new(AtomicU64::new(0)),
            mode,
            xpub_nodrop: options.xpub_nodrop,
        }
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            inner: self.inner.clone(),
            generation: self.generation.clone(),
            mode: self.mode,
            send_count: Arc::new(AtomicU32::new(0)),
            xpub_nodrop: self.xpub_nodrop,
            cached: Mutex::new(CachedFanOut::default()),
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
        let has_transform = handle.wire_slot.as_ref().is_some_and(|s| s.has_transform);
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
        if has_transform && g.fan_out_encoder.is_none() {
            g.init_fan_out_encoder();
        }
        g.recompute_subscribe_all();
        self.bump_generation();
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if g.peers.remove(&peer_id).is_some() {
            g.recompute_subscribe_all();
            drop(g);
            self.bump_generation();
        }
    }

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn peer_subscribe(&self, peer_id: u64, prefix: Bytes) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            p.subscriptions.add(&prefix);
            g.recompute_subscribe_all();
            drop(g);
            self.bump_generation();
        }
    }

    pub(crate) fn peer_cancel(&self, peer_id: u64, prefix: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            p.subscriptions.remove(prefix);
            g.recompute_subscribe_all();
            drop(g);
            self.bump_generation();
        }
    }

    pub(crate) fn peer_join(&self, peer_id: u64, group: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id)
            && let Ok(s) = std::str::from_utf8(group)
        {
            p.groups.insert(s.to_string());
            drop(g);
            self.bump_generation();
        }
    }

    pub(crate) fn peer_leave(&self, peer_id: u64, group: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id)
            && let Ok(s) = std::str::from_utf8(group)
        {
            p.groups.remove(s);
            drop(g);
            self.bump_generation();
        }
    }

    pub(crate) fn shutdown(&self) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.clear();
        g.all_subscribe_all = false;
        g.all_targets.clear();
        g.fan_out_encoder = None;
        drop(g);
        self.bump_generation();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.values().all(|p| p.target.is_empty())
    }
}

fn targets_have_space(targets: &[PeerSend]) -> bool {
    targets.iter().all(|t| match t {
        PeerSend::Wire { slot, .. } => slot.has_space() || slot.dead.load(Ordering::Acquire),
        PeerSend::Inbox(_) => true,
    })
}

async fn send_to_targets_nodrop(targets: &[PeerSend], msg: Message) -> Result<()> {
    for target in targets {
        target.send(msg.clone()).await?;
    }
    Ok(())
}

fn dispatch_encoded(
    eq: &mut EncodedQueue,
    targets: &[PeerSend],
    msg: &Message,
    chunks: &mut Vec<Bytes>,
) -> DispatchStatus {
    let mut full = false;
    match finish_fan_out_batch(eq, chunks, targets.len(), FAN_OUT_COPY_BUDGET) {
        FanOutBatch::Arena(raw) => {
            for t in targets {
                match t {
                    PeerSend::Wire { slot, .. } => {
                        full |= slot.try_push_pre_encoded_no_signal(raw) == TryEncodeResult::Full;
                    }
                    PeerSend::Inbox(tx) => {
                        full |= tx
                            .try_send(DriverCommand::SendMessage(msg.clone()))
                            .is_err();
                    }
                }
            }
            for t in targets {
                if let PeerSend::Wire { slot, .. } = t {
                    slot.signal_encoded();
                }
            }
        }
        FanOutBatch::Chunks(encoded) => {
            for t in targets {
                match t {
                    PeerSend::Wire { slot, .. } => {
                        full |= slot.try_push_encoded(encoded) == TryEncodeResult::Full;
                    }
                    PeerSend::Inbox(tx) => {
                        full |= tx
                            .try_send(DriverCommand::SendMessage(msg.clone()))
                            .is_err();
                    }
                }
            }
        }
    }
    clear_fan_out_batch(eq, chunks);
    if full {
        DispatchStatus::Full
    } else {
        DispatchStatus::Ok
    }
}

fn dispatch_to_targets(
    targets: &[PeerSend],
    msg: &Message,
    encoder: Option<&Mutex<MessageEncoder>>,
) -> Result<DispatchStatus> {
    use std::cell::RefCell;

    match targets.len() {
        0 => Ok(DispatchStatus::Ok),
        1 => match targets[0].try_encode(msg) {
            TryEncodeResult::Full => Ok(DispatchStatus::Full),
            _ => Ok(DispatchStatus::Ok),
        },
        _ => {
            #[cfg(feature = "ws")]
            if targets.iter().any(PeerSend::is_ws) {
                let mut full = false;
                for t in targets {
                    full |= t.try_encode(msg) == TryEncodeResult::Full;
                }
                return Ok(if full {
                    DispatchStatus::Full
                } else {
                    DispatchStatus::Ok
                });
            }

            thread_local! {
                static ARENA: RefCell<EncodedQueue> = RefCell::new(
                    EncodedQueue::one_shot(),
                );
                static CHUNKS: RefCell<Vec<Bytes>> = const { RefCell::new(Vec::new()) };
            }
            ARENA.with(|cell| {
                let eq = &mut *cell.borrow_mut();
                if let Some(enc_mtx) = encoder {
                    let transformed = {
                        let mut enc = enc_mtx.lock().expect("fan_out_encoder poisoned");
                        enc.encode(msg)?
                    };
                    for wire_msg in &transformed {
                        eq.encode_auto(wire_msg);
                    }
                } else {
                    eq.encode_auto(msg);
                }
                CHUNKS.with(|drain| Ok(dispatch_encoded(eq, targets, msg, &mut drain.borrow_mut())))
            })
        }
    }
}

fn first_frame_bytes(msg: &Message) -> Bytes {
    msg.part_bytes(0).unwrap_or_default()
}

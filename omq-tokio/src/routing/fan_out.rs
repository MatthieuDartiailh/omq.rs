//! Fan-out send: caller-side distribution into shard workers.
//!
//! PUB and XPUB filter by SUBSCRIBE-driven prefix set; RADIO filters
//! by joined groups. Normal lossy fan-out sends encode once on the
//! caller, then take one shard mutex and push the encoded dispatch into
//! each nonempty shard's yring input. Each shard owns its peers' yring
//! producers and filters/pushes without a producer mutex. `xpub_nodrop`
//! keeps the direct dispatch path so it can preserve backpressure.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rustc_hash::{FxHashMap, FxHashSet};

use smallvec::SmallVec;

use bytes::Bytes;
use tokio::sync::Notify;

use crate::engine::{DriverCommand, DriverHandle};
use omq_proto::encoded_queue::EncodedQueue;
use omq_proto::error::{Error, Result};
use omq_proto::fan_out_batch::{
    FanOutBatch, clear_fan_out_batch, encode_fan_out_message, finish_fan_out_batch,
};
use omq_proto::message::Message;
use omq_proto::options::{OnMute, Options};
use omq_proto::proto::transform::MessageEncoder;

use super::peer_send::PeerSend;
use super::subscription::SubscriptionSet;
use crate::engine::wire_slot::{PeerWireSlot, TryEncodeResult, WIRE_SLOT_INLINE_CAP, WireSlotItem};

/// Filter mode for a fan-out send strategy.
#[derive(Debug, Clone, Copy)]
pub(crate) enum FanOutMode {
    /// PUB / XPUB: prefix-match against peer subscriptions.
    SubscriptionPrefix,
    /// RADIO: exact-match against peer joined groups.
    Group,
}

/// Total bytes copied into per-peer wire queues before switching to
/// shared `Bytes` chunks. This is fan-out specific. Do not change
/// `EncodedQueue::ARENA_THRESHOLD` for this: PUSH/SCATTER use it too.
const FAN_OUT_TOTAL_COPY_BUDGET: usize = 8 * 1024;
const FAN_OUT_SHARD_THRESHOLD: usize = 4;

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

#[derive(Debug, Default)]
struct DispatchOutcome {
    full_targets: SmallVec<[PeerSend; 8]>,
}

impl DispatchOutcome {
    fn is_full(&self) -> bool {
        !self.full_targets.is_empty()
    }

    fn push_full(&mut self, target: &PeerSend) {
        self.full_targets.push(target.clone());
    }
}

#[derive(Debug)]
enum ShardCommand {
    AddPeer(ShardPeerAdd),
    RemovePeer { peer_id: u64 },
    Subscribe { peer_id: u64, prefix: Bytes },
    Cancel { peer_id: u64, prefix: Bytes },
    Join { peer_id: u64, group: Bytes },
    Leave { peer_id: u64, group: Bytes },
    Dispatch(ShardDispatch),
    Shutdown,
}

#[derive(Debug)]
struct ShardPeerAdd {
    peer_id: u64,
    slot: Arc<PeerWireSlot>,
    producer: yring::Producer<WireSlotItem>,
    any_groups: bool,
}

#[derive(Clone, Debug)]
struct ShardDispatch {
    encoded: EncodedFanOut,
    topic: Bytes,
    group: Option<String>,
}

#[derive(Clone, Debug)]
enum EncodedFanOut {
    Inline {
        buf: [u8; WIRE_SLOT_INLINE_CAP],
        len: u16,
    },
    Shared(Arc<[Bytes]>),
}

impl EncodedFanOut {
    fn inline(data: &[u8]) -> Self {
        debug_assert!(data.len() <= WIRE_SLOT_INLINE_CAP);
        let mut buf = [0; WIRE_SLOT_INLINE_CAP];
        buf[..data.len()].copy_from_slice(data);
        Self::Inline {
            buf,
            len: data.len() as u16,
        }
    }

    fn shared(chunks: Arc<[Bytes]>) -> Self {
        Self::Shared(chunks)
    }

    fn to_wire_item(&self) -> WireSlotItem {
        match self {
            Self::Inline { buf, len } => WireSlotItem::inline(&buf[..*len as usize]),
            Self::Shared(chunks) => WireSlotItem::shared(chunks.clone()),
        }
    }
}

#[derive(Debug)]
struct ShardPeer {
    subscriptions: SubscriptionSet,
    groups: FxHashSet<String>,
    any_groups: bool,
    slot: Arc<PeerWireSlot>,
    producer: yring::Producer<WireSlotItem>,
}

struct ShardEndpoint {
    tx: yring::Producer<ShardCommand>,
    notify: Arc<Notify>,
    load: usize,
}

struct FanOutShards {
    endpoints: Mutex<Vec<ShardEndpoint>>,
}

impl std::fmt::Debug for FanOutShards {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let endpoints = self.endpoints.lock().expect("fanout shards poisoned");
        f.debug_struct("FanOutShards")
            .field("shards", &endpoints.len())
            .field(
                "loads",
                &endpoints.iter().map(|s| s.load).collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[derive(Debug)]
struct ShardWorker {
    rx: yring::Consumer<ShardCommand>,
    notify: Arc<Notify>,
    mode: FanOutMode,
    peers: FxHashMap<u64, ShardPeer>,
}

struct ShardDispatchTargets {
    fallback_targets: SmallVec<[PeerSend; 8]>,
    transform_encoder: Option<Arc<Mutex<MessageEncoder>>>,
    sharded_count: usize,
}

#[derive(Debug)]
pub(crate) struct Submitter {
    shards: Option<Arc<FanOutShards>>,
    inner: Arc<Mutex<FanOutInner>>,
    generation: Arc<AtomicU64>,
    mode: FanOutMode,
    send_count: Arc<AtomicU32>,
    xpub_nodrop: bool,
    lossy: bool,
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
            shards: self.shards.clone(),
            inner: self.inner.clone(),
            generation: self.generation.clone(),
            mode: self.mode,
            send_count: self.send_count.clone(),
            xpub_nodrop: self.xpub_nodrop,
            lossy: self.lossy,
            cached: Mutex::new(CachedFanOut::default()),
        }
    }
}

impl FanOutShards {
    fn spawn(options: &Options, mode: FanOutMode) -> Arc<Self> {
        let pipe_cap = options.send_hwm.unwrap_or(1000).max(1) as usize;
        let shard_count = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        let mut endpoints = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let (shard_tx, shard_rx) = yring::spsc(pipe_cap);
            let notify = Arc::new(Notify::new());
            tokio::spawn(
                ShardWorker {
                    rx: shard_rx,
                    notify: notify.clone(),
                    mode,
                    peers: FxHashMap::default(),
                }
                .run(),
            );
            endpoints.push(ShardEndpoint {
                tx: shard_tx,
                notify,
                load: 0,
            });
        }
        Arc::new(Self {
            endpoints: Mutex::new(endpoints),
        })
    }

    fn least_loaded_shard(endpoints: &[ShardEndpoint]) -> usize {
        let total_load = endpoints.iter().map(|shard| shard.load).sum::<usize>();
        if total_load + 1 < FAN_OUT_SHARD_THRESHOLD {
            return 0;
        }
        endpoints
            .iter()
            .enumerate()
            .min_by_key(|(_, shard)| shard.load)
            .map_or(0, |(idx, _)| idx)
    }

    fn push_control(endpoint: &mut ShardEndpoint, mut cmd: ShardCommand) {
        loop {
            match endpoint.tx.push(cmd) {
                Ok(()) => {
                    endpoint.tx.flush();
                    endpoint.notify.notify_one();
                    return;
                }
                Err(returned) => {
                    cmd = returned;
                    endpoint.tx.flush();
                    endpoint.notify.notify_one();
                    std::thread::yield_now();
                }
            }
        }
    }

    fn add_peer(&self, add: ShardPeerAdd) -> usize {
        let mut endpoints = self.endpoints.lock().expect("fanout shards poisoned");
        let shard = Self::least_loaded_shard(&endpoints);
        endpoints[shard].load += 1;
        Self::push_control(&mut endpoints[shard], ShardCommand::AddPeer(add));
        shard
    }

    fn send_to_shard(&self, shard: usize, cmd: ShardCommand) {
        let mut endpoints = self.endpoints.lock().expect("fanout shards poisoned");
        if let Some(endpoint) = endpoints.get_mut(shard) {
            Self::push_control(endpoint, cmd);
        }
    }

    fn remove_peer(&self, shard: usize, peer_id: u64) {
        let mut endpoints = self.endpoints.lock().expect("fanout shards poisoned");
        if let Some(endpoint) = endpoints.get_mut(shard) {
            endpoint.load = endpoint.load.saturating_sub(1);
            Self::push_control(endpoint, ShardCommand::RemovePeer { peer_id });
        }
    }

    fn dispatch(&self, dispatch: &ShardDispatch) {
        let mut endpoints = self.endpoints.lock().expect("fanout shards poisoned");
        let mut touched = SmallVec::<[usize; 8]>::new();
        for (idx, endpoint) in endpoints.iter_mut().enumerate() {
            if endpoint.load == 0 {
                continue;
            }
            if endpoint
                .tx
                .push(ShardCommand::Dispatch(dispatch.clone()))
                .is_ok()
            {
                touched.push(idx);
            }
        }
        for idx in touched {
            let endpoint = &mut endpoints[idx];
            endpoint.tx.flush();
            endpoint.notify.notify_one();
        }
    }

    fn shutdown(&self) {
        let mut endpoints = self.endpoints.lock().expect("fanout shards poisoned");
        for endpoint in endpoints.iter_mut() {
            Self::push_control(endpoint, ShardCommand::Shutdown);
            endpoint.load = 0;
        }
    }

    fn is_empty(&self) -> bool {
        self.endpoints
            .lock()
            .expect("fanout shards poisoned")
            .iter()
            .all(|endpoint| endpoint.tx.is_empty())
    }
}

impl ShardWorker {
    async fn run(mut self) {
        loop {
            let mut touched: SmallVec<[u64; 32]> = SmallVec::new();
            let mut shutdown = false;

            loop {
                self.rx.prefetch();
                let mut drained = false;
                while let Some(cmd) = self.rx.pop() {
                    drained = true;
                    if self.handle_command(cmd, &mut touched) {
                        shutdown = true;
                    }
                }
                self.rx.release();
                if !drained || shutdown {
                    break;
                }
            }

            self.flush_touched(&mut touched);
            if shutdown {
                self.peers.clear();
                return;
            }
            self.notify.notified().await;
        }
    }

    fn handle_command(&mut self, cmd: ShardCommand, touched: &mut SmallVec<[u64; 32]>) -> bool {
        match cmd {
            ShardCommand::AddPeer(add) => {
                self.peers.insert(
                    add.peer_id,
                    ShardPeer {
                        subscriptions: SubscriptionSet::new(),
                        groups: FxHashSet::default(),
                        any_groups: add.any_groups,
                        slot: add.slot,
                        producer: add.producer,
                    },
                );
            }
            ShardCommand::RemovePeer { peer_id } => {
                self.peers.remove(&peer_id);
            }
            ShardCommand::Subscribe { peer_id, prefix } => {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.subscriptions.add(&prefix);
                }
            }
            ShardCommand::Cancel { peer_id, prefix } => {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.subscriptions.remove(&prefix);
                }
            }
            ShardCommand::Join { peer_id, group } => {
                if let Some(peer) = self.peers.get_mut(&peer_id)
                    && let Ok(s) = std::str::from_utf8(&group)
                {
                    peer.groups.insert(s.to_string());
                }
            }
            ShardCommand::Leave { peer_id, group } => {
                if let Some(peer) = self.peers.get_mut(&peer_id)
                    && let Ok(s) = std::str::from_utf8(&group)
                {
                    peer.groups.remove(s);
                }
            }
            ShardCommand::Dispatch(dispatch) => self.dispatch(&dispatch, touched),
            ShardCommand::Shutdown => return true,
        }
        false
    }

    fn dispatch(&mut self, dispatch: &ShardDispatch, touched: &mut SmallVec<[u64; 32]>) {
        for (&peer_id, peer) in &mut self.peers {
            if !shard_peer_matches(self.mode, peer, dispatch) {
                continue;
            }
            if peer
                .slot
                .try_push_ring_item(&mut peer.producer, dispatch.encoded.to_wire_item())
                == TryEncodeResult::Ok
            {
                touched.push(peer_id);
            }
        }
    }

    fn flush_touched(&mut self, touched: &mut SmallVec<[u64; 32]>) {
        touched.sort_unstable();
        touched.dedup();
        for &peer_id in touched.iter() {
            if let Some(peer) = self.peers.get_mut(&peer_id) {
                peer.slot.flush_ring(&mut peer.producer);
            }
        }
    }
}

fn encode_for_shards(
    msg: &Message,
    target_count: usize,
    transform_encoder: Option<&Mutex<MessageEncoder>>,
) -> Result<EncodedFanOut> {
    let mut eq = EncodedQueue::one_shot();
    let mut chunks = Vec::new();
    if let Some(enc_mtx) = transform_encoder {
        let transformed = {
            let mut enc = enc_mtx.lock().expect("fan_out_encoder poisoned");
            enc.encode(msg)?
        };
        for wire_msg in &transformed {
            encode_fan_out_message(&mut eq, wire_msg, target_count, FAN_OUT_TOTAL_COPY_BUDGET);
        }
    } else {
        encode_fan_out_message(&mut eq, msg, target_count, FAN_OUT_TOTAL_COPY_BUDGET);
    }
    let fanout_encoded = match finish_fan_out_batch(
        &mut eq,
        &mut chunks,
        target_count,
        FAN_OUT_TOTAL_COPY_BUDGET,
    ) {
        FanOutBatch::Arena(raw) if raw.len() <= WIRE_SLOT_INLINE_CAP => EncodedFanOut::inline(raw),
        FanOutBatch::Arena(raw) => {
            EncodedFanOut::shared(Vec::from([Bytes::copy_from_slice(raw)]).into())
        }
        FanOutBatch::Chunks(wire_chunks) => EncodedFanOut::shared(Arc::from(wire_chunks.to_vec())),
    };
    clear_fan_out_batch(&mut eq, &mut chunks);
    Ok(fanout_encoded)
}

fn shard_peer_matches(mode: FanOutMode, peer: &ShardPeer, dispatch: &ShardDispatch) -> bool {
    match (mode, dispatch.group.as_deref()) {
        (FanOutMode::Group, Some(grp)) => peer.any_groups || peer.groups.contains(grp),
        (FanOutMode::SubscriptionPrefix, _) => peer.subscriptions.matches(&dispatch.topic),
        (FanOutMode::Group, None) => false,
    }
}

impl Submitter {
    pub(crate) fn shutdown(&self) {
        if let Some(ref shards) = self.shards {
            shards.shutdown();
        }
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

    fn deactivate_target(&self, target: &PeerSend) {
        let PeerSend::Wire { slot, .. } = target else {
            return;
        };
        let peer_id = slot.peer_id;
        slot.deactivate_fanout();
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if g.deactivate_fanout_peer(peer_id) {
            drop(g);
            self.generation.fetch_add(1, Ordering::Release);
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
            let use_active = self.lossy && !self.xpub_nodrop;
            let all_targets = if use_active {
                &g.active_all_targets
            } else {
                &g.all_targets
            };
            if g.all_subscribe_all && all_targets.len() == 1 && g.fan_out_encoder.is_none() {
                cached.sole_wire = Some(all_targets[0].clone());
                cached.all_targets = None;
                cached.encoder = None;
                cached.all_wire = false;
            } else if g.all_subscribe_all && !all_targets.is_empty() {
                cached.sole_wire = None;
                let targets: Vec<PeerSend> = all_targets.to_vec();
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

    fn dispatch_to_shards_and_fallback(
        &self,
        shards: &FanOutShards,
        msg: &Message,
        group: Option<String>,
    ) -> Result<()> {
        let targets = self.collect_shard_targets(msg, group.as_deref());

        if !targets.fallback_targets.is_empty() {
            let mut deactivate = |target: &PeerSend| self.deactivate_target(target);
            let _ = dispatch_to_targets(
                &targets.fallback_targets,
                msg,
                targets.transform_encoder.as_deref(),
                self.lossy && !self.xpub_nodrop,
                &mut deactivate,
            )?;
        }

        if targets.sharded_count == 0 {
            return Ok(());
        }

        let encoded_dispatch = encode_for_shards(
            msg,
            targets.sharded_count,
            targets.transform_encoder.as_deref(),
        )?;
        let dispatch = ShardDispatch {
            encoded: encoded_dispatch,
            topic: first_frame_bytes(msg),
            group,
        };
        shards.dispatch(&dispatch);
        Ok(())
    }

    pub(crate) fn try_send(
        &self,
        msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        let (forwarded, group) = self
            .prepare(msg)
            .map_err(omq_proto::error::TrySendError::Error)?;

        if let Some(ref shards) = self.shards {
            return self
                .dispatch_to_shards_and_fallback(shards, &forwarded, group)
                .map_err(omq_proto::error::TrySendError::Error);
        }

        match self.try_cached(&forwarded, group.as_deref()) {
            CachedResult::SoleWire(r) => {
                return match r {
                    TryEncodeResult::Ok | TryEncodeResult::Dead | TryEncodeResult::Ineligible => {
                        Ok(())
                    }
                    TryEncodeResult::Full if !self.lossy || self.xpub_nodrop => {
                        Err(omq_proto::error::TrySendError::Full(forwarded))
                    }
                    TryEncodeResult::Full => {
                        let target = {
                            let cached = self.cached.lock().unwrap();
                            cached.sole_wire.clone()
                        };
                        if let Some(target) = target {
                            self.deactivate_target(&target);
                        }
                        Ok(())
                    }
                };
            }
            CachedResult::Cached { targets, encoder } => {
                if (!self.lossy || self.xpub_nodrop) && !targets_have_space(&targets) {
                    return Err(omq_proto::error::TrySendError::Full(forwarded));
                }
                let mut deactivate = |target: &PeerSend| self.deactivate_target(target);
                if dispatch_to_targets(
                    &targets,
                    &forwarded,
                    encoder.as_deref(),
                    self.lossy && !self.xpub_nodrop,
                    &mut deactivate,
                )
                .map_err(omq_proto::error::TrySendError::Error)?
                .is_full()
                    && self.xpub_nodrop
                {
                    return Err(omq_proto::error::TrySendError::Full(forwarded));
                }
                return Ok(());
            }
            CachedResult::Miss => {}
        }

        let (targets, encoder) = self.collect_targets(&forwarded, group.as_deref());
        if (!self.lossy || self.xpub_nodrop) && !targets_have_space(&targets) {
            return Err(omq_proto::error::TrySendError::Full(forwarded));
        }
        let mut deactivate = |target: &PeerSend| self.deactivate_target(target);
        if dispatch_to_targets(
            &targets,
            &forwarded,
            encoder.as_deref(),
            self.lossy && !self.xpub_nodrop,
            &mut deactivate,
        )
        .map_err(omq_proto::error::TrySendError::Error)?
        .is_full()
            && self.xpub_nodrop
        {
            return Err(omq_proto::error::TrySendError::Full(forwarded));
        }
        Ok(())
    }

    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        let (forwarded, group) = self.prepare(msg)?;

        if let Some(ref shards) = self.shards {
            self.dispatch_to_shards_and_fallback(shards, &forwarded, group)?;
            return Ok(());
        }

        match self.try_cached(&forwarded, group.as_deref()) {
            CachedResult::SoleWire(TryEncodeResult::Full) if self.lossy && !self.xpub_nodrop => {
                let target = {
                    let cached = self.cached.lock().unwrap();
                    cached.sole_wire.clone()
                };
                if let Some(target) = target {
                    self.deactivate_target(&target);
                }
                return Ok(());
            }
            CachedResult::SoleWire(TryEncodeResult::Full) => {
                let target = {
                    let cached = self.cached.lock().unwrap();
                    cached.sole_wire.clone()
                };
                if let Some(target) = target {
                    target.send(forwarded).await?;
                    return Ok(());
                }
            }
            CachedResult::SoleWire(TryEncodeResult::Ok) if self.lossy && !self.xpub_nodrop => {
                return Ok(());
            }
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
                let mut deactivate = |target: &PeerSend| self.deactivate_target(target);
                let outcome = dispatch_to_targets(
                    &targets,
                    &forwarded,
                    encoder.as_deref(),
                    self.lossy && !self.xpub_nodrop,
                    &mut deactivate,
                )?;
                if !self.lossy && outcome.is_full() {
                    send_to_targets_nodrop(&outcome.full_targets, forwarded.clone()).await?;
                }
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
        let mut deactivate = |target: &PeerSend| self.deactivate_target(target);
        let outcome = dispatch_to_targets(
            &targets,
            &forwarded,
            encoder.as_deref(),
            self.lossy && !self.xpub_nodrop,
            &mut deactivate,
        )?;
        if !self.lossy && outcome.is_full() {
            send_to_targets_nodrop(&outcome.full_targets, forwarded.clone()).await?;
        }
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
        let use_active = self.lossy && !self.xpub_nodrop;
        let targets = if g.all_subscribe_all && matches!(self.mode, FanOutMode::SubscriptionPrefix)
        {
            if use_active {
                g.active_all_targets.clone()
            } else {
                g.all_targets.clone()
            }
        } else {
            g.peers
                .values()
                .filter(|p| !use_active || p.fanout_active)
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

    fn collect_shard_targets(&self, msg: &Message, group: Option<&str>) -> ShardDispatchTargets {
        let g = self.inner.lock().expect("fanout inner poisoned");
        let fallback_targets = g
            .peers
            .values()
            .filter(|p| p.shard.is_none())
            .filter(|p| match (self.mode, group) {
                (FanOutMode::Group, Some(grp)) => p.any_groups || p.groups.contains(grp),
                (FanOutMode::SubscriptionPrefix, _) => {
                    p.subscriptions.matches(&first_frame_bytes(msg))
                }
                (FanOutMode::Group, None) => false,
            })
            .map(|p| p.target.clone())
            .collect();
        let sharded_count = g.peers.values().filter(|p| p.shard.is_some()).count();
        ShardDispatchTargets {
            fallback_targets,
            transform_encoder: g.fan_out_encoder.clone(),
            sharded_count,
        }
    }
}

/// Fan-out send strategy.
#[derive(Debug)]
pub(crate) struct FanOutSend {
    shards: Option<Arc<FanOutShards>>,
    inner: Arc<Mutex<FanOutInner>>,
    generation: Arc<AtomicU64>,
    mode: FanOutMode,
    xpub_nodrop: bool,
    lossy: bool,
}

struct FanOutInner {
    peers: FxHashMap<u64, FanOutPeer>,
    all_subscribe_all: bool,
    all_targets: SmallVec<[PeerSend; 8]>,
    active_all_targets: SmallVec<[PeerSend; 8]>,
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
    shard: Option<usize>,
    shard_eligible: bool,
    fanout_active: bool,
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
            self.active_all_targets = self
                .peers
                .values()
                .filter(|p| p.fanout_active)
                .map(|p| p.target.clone())
                .collect();
        } else {
            self.all_targets.clear();
            self.active_all_targets.clear();
        }
    }

    fn deactivate_fanout_peer(&mut self, peer_id: u64) -> bool {
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return false;
        };
        if !peer.fanout_active {
            return false;
        }
        peer.fanout_active = false;
        self.recompute_subscribe_all();
        true
    }

    fn reactivate_fanout_peer(&mut self, peer_id: u64) -> bool {
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return false;
        };
        if peer.fanout_active {
            return false;
        }
        peer.fanout_active = true;
        self.recompute_subscribe_all();
        true
    }
}

impl FanOutSend {
    pub(crate) fn new(options: &Options, mode: FanOutMode) -> Self {
        let use_shards = !options.xpub_nodrop
            && matches!(
                tokio::runtime::Handle::current().runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            );
        let shards = if use_shards {
            Some(FanOutShards::spawn(options, mode))
        } else {
            None
        };
        Self {
            shards,
            inner: Arc::new(Mutex::new(FanOutInner {
                peers: FxHashMap::default(),
                all_subscribe_all: false,
                all_targets: SmallVec::new(),
                active_all_targets: SmallVec::new(),
                fan_out_encoder: None,
                options: options.clone(),
            })),
            generation: Arc::new(AtomicU64::new(0)),
            mode,
            xpub_nodrop: options.xpub_nodrop,
            lossy: !matches!(options.on_mute, OnMute::Block),
        }
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            shards: self.shards.clone(),
            inner: self.inner.clone(),
            generation: self.generation.clone(),
            mode: self.mode,
            send_count: Arc::new(AtomicU32::new(0)),
            xpub_nodrop: self.xpub_nodrop,
            lossy: self.lossy,
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

        #[cfg(feature = "ws")]
        let target_is_ws = target.is_ws();
        #[cfg(not(feature = "ws"))]
        let target_is_ws = false;

        let shard_eligible = !target_is_ws
            && self.shards.is_some()
            && matches!(target, PeerSend::Wire { .. })
            && handle.wire_slot_tx.is_some();
        let should_shard = shard_eligible && {
            let g = self.inner.lock().expect("fanout inner poisoned");
            g.peers.values().filter(|p| p.shard_eligible).count() + 1 >= FAN_OUT_SHARD_THRESHOLD
        };

        let shard = if !should_shard {
            None
        } else if let (Some(shards), PeerSend::Wire { slot, .. }) = (self.shards.as_ref(), &target)
        {
            handle
                .wire_slot_tx
                .as_ref()
                .and_then(|tx| tx.lock().expect("wire_slot_tx poisoned").take())
                .map(|producer| {
                    shards.add_peer(ShardPeerAdd {
                        peer_id,
                        slot: slot.clone(),
                        producer,
                        any_groups,
                    })
                })
        } else {
            None
        };

        if has_transform && shard.is_some() {
            let mut g = self.inner.lock().expect("fanout inner poisoned");
            if g.fan_out_encoder.is_none() {
                g.init_fan_out_encoder();
            }
        }
        if shard.is_none() {
            // Fallback/direct peers use `dispatch_to_targets`, which needs
            // the direct fan-out encoder initialized from the socket options.
            let mut g = self.inner.lock().expect("fanout inner poisoned");
            if has_transform && g.fan_out_encoder.is_none() {
                g.init_fan_out_encoder();
            }
        }

        if let PeerSend::Wire { slot, .. } = &target {
            let inner = Arc::downgrade(&self.inner);
            let generation = self.generation.clone();
            slot.set_fanout_reactivation(Arc::new(move |peer_id| {
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                let mut g = inner.lock().expect("fanout inner poisoned");
                if g.reactivate_fanout_peer(peer_id) {
                    drop(g);
                    generation.fetch_add(1, Ordering::Release);
                }
            }));
        }
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.insert(
            peer_id,
            FanOutPeer {
                subscriptions: SubscriptionSet::new(),
                groups: FxHashSet::default(),
                any_groups,
                target,
                shard,
                shard_eligible,
                fanout_active: true,
            },
        );
        g.recompute_subscribe_all();
        self.bump_generation();
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(peer) = g.peers.remove(&peer_id) {
            g.recompute_subscribe_all();
            drop(g);
            if let (Some(shards), Some(shard)) = (self.shards.as_ref(), peer.shard) {
                shards.remove_peer(shard, peer_id);
            }
            self.bump_generation();
        }
    }

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn peer_subscribe(&self, peer_id: u64, prefix: Bytes) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            p.subscriptions.add(&prefix);
            let shard = p.shard;
            g.recompute_subscribe_all();
            drop(g);
            if let (Some(shards), Some(shard)) = (self.shards.as_ref(), shard) {
                shards.send_to_shard(
                    shard,
                    ShardCommand::Subscribe {
                        peer_id,
                        prefix: prefix.clone(),
                    },
                );
            }
            self.bump_generation();
        }
    }

    pub(crate) fn peer_cancel(&self, peer_id: u64, prefix: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            p.subscriptions.remove(prefix);
            let shard = p.shard;
            g.recompute_subscribe_all();
            drop(g);
            if let (Some(shards), Some(shard)) = (self.shards.as_ref(), shard) {
                shards.send_to_shard(
                    shard,
                    ShardCommand::Cancel {
                        peer_id,
                        prefix: Bytes::copy_from_slice(prefix),
                    },
                );
            }
            self.bump_generation();
        }
    }

    pub(crate) fn peer_join(&self, peer_id: u64, group: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id)
            && let Ok(s) = std::str::from_utf8(group)
        {
            p.groups.insert(s.to_string());
            let shard = p.shard;
            drop(g);
            if let (Some(shards), Some(shard)) = (self.shards.as_ref(), shard) {
                shards.send_to_shard(
                    shard,
                    ShardCommand::Join {
                        peer_id,
                        group: Bytes::copy_from_slice(group),
                    },
                );
            }
            self.bump_generation();
        }
    }

    pub(crate) fn peer_leave(&self, peer_id: u64, group: &[u8]) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id)
            && let Ok(s) = std::str::from_utf8(group)
        {
            p.groups.remove(s);
            let shard = p.shard;
            drop(g);
            if let (Some(shards), Some(shard)) = (self.shards.as_ref(), shard) {
                shards.send_to_shard(
                    shard,
                    ShardCommand::Leave {
                        peer_id,
                        group: Bytes::copy_from_slice(group),
                    },
                );
            }
            self.bump_generation();
        }
    }

    pub(crate) fn shutdown(&self) {
        if let Some(ref shards) = self.shards {
            shards.shutdown();
        }
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.clear();
        g.all_subscribe_all = false;
        g.all_targets.clear();
        g.active_all_targets.clear();
        g.fan_out_encoder = None;
        drop(g);
        self.bump_generation();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let shards_empty = self.shards.as_ref().is_none_or(|shards| shards.is_empty());
        let g = self.inner.lock().expect("fanout inner poisoned");
        shards_empty && g.peers.values().all(|p| p.target.is_empty())
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
    drop_on_full: bool,
    deactivate: &mut impl FnMut(&PeerSend),
) -> DispatchOutcome {
    let mut outcome = DispatchOutcome::default();
    match finish_fan_out_batch(eq, chunks, targets.len(), FAN_OUT_TOTAL_COPY_BUDGET) {
        FanOutBatch::Arena(raw) => {
            for t in targets {
                match t {
                    PeerSend::Wire { slot, .. } => {
                        if drop_on_full && !slot.fanout_active() {
                            outcome.push_full(t);
                            continue;
                        }
                        if slot.try_push_pre_encoded_no_signal(raw) == TryEncodeResult::Full {
                            if drop_on_full {
                                deactivate(t);
                            }
                            outcome.push_full(t);
                        }
                    }
                    PeerSend::Inbox(tx) => {
                        if tx
                            .try_send(DriverCommand::SendMessage(msg.clone()))
                            .is_err()
                        {
                            outcome.push_full(t);
                        }
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
                        if drop_on_full && !slot.fanout_active() {
                            outcome.push_full(t);
                            continue;
                        }
                        if slot.try_push_encoded(encoded) == TryEncodeResult::Full {
                            if drop_on_full {
                                deactivate(t);
                            }
                            outcome.push_full(t);
                        }
                    }
                    PeerSend::Inbox(tx) => {
                        if tx
                            .try_send(DriverCommand::SendMessage(msg.clone()))
                            .is_err()
                        {
                            outcome.push_full(t);
                        }
                    }
                }
            }
        }
    }
    clear_fan_out_batch(eq, chunks);
    outcome
}

fn dispatch_to_targets(
    targets: &[PeerSend],
    msg: &Message,
    encoder: Option<&Mutex<MessageEncoder>>,
    drop_on_full: bool,
    deactivate: &mut impl FnMut(&PeerSend),
) -> Result<DispatchOutcome> {
    use std::cell::RefCell;

    match targets.len() {
        0 => Ok(DispatchOutcome::default()),
        1 => match targets[0].try_encode(msg) {
            TryEncodeResult::Full => {
                if drop_on_full {
                    deactivate(&targets[0]);
                }
                let mut outcome = DispatchOutcome::default();
                outcome.push_full(&targets[0]);
                Ok(outcome)
            }
            _ => Ok(DispatchOutcome::default()),
        },
        _ => {
            #[cfg(feature = "ws")]
            if targets.iter().any(PeerSend::is_ws) {
                let mut outcome = DispatchOutcome::default();
                for t in targets {
                    if t.try_encode(msg) == TryEncodeResult::Full {
                        if drop_on_full {
                            deactivate(t);
                        }
                        outcome.push_full(t);
                    }
                }
                return Ok(outcome);
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
                        encode_fan_out_message(
                            eq,
                            wire_msg,
                            targets.len(),
                            FAN_OUT_TOTAL_COPY_BUDGET,
                        );
                    }
                } else {
                    encode_fan_out_message(eq, msg, targets.len(), FAN_OUT_TOTAL_COPY_BUDGET);
                }
                CHUNKS.with(|drain| {
                    Ok(dispatch_encoded(
                        eq,
                        targets,
                        msg,
                        &mut drain.borrow_mut(),
                        drop_on_full,
                        deactivate,
                    ))
                })
            })
        }
    }
}

fn first_frame_bytes(msg: &Message) -> Bytes {
    msg.part_bytes(0).unwrap_or_default()
}

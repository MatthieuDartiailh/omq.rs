//! Fan-out send: caller-side distribution into shard workers.
//!
//! PUB and XPUB filter by SUBSCRIBE-driven prefix set; RADIO filters
//! by joined groups. Normal lossy fan-out sends encode once on the
//! caller, then take one shard mutex and push the encoded dispatch into
//! each nonempty shard's yring input. Each shard owns its peers' yring
//! producers and filters/pushes without a producer mutex. `xpub_nodrop`
//! keeps the direct dispatch path so it can preserve backpressure.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
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

type SharedFanOutEncoder = Arc<Mutex<Option<MessageEncoder>>>;

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
const DIRECT_SHARD_PEER_CAP: usize = 4;
const WORKER_SHARD_PEER_CAP: usize = 4;
const MAX_FAN_OUT_WORKER_SHARDS: usize = 8;

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
        encoder: Option<SharedFanOutEncoder>,
    },
    Miss,
}

#[derive(Debug, Default)]
struct DispatchOutcome {
    full_targets: SmallVec<[PeerSend; 8]>,
    shard_full: bool,
}

impl DispatchOutcome {
    fn is_full(&self) -> bool {
        self.shard_full || !self.full_targets.is_empty()
    }

    fn push_full(&mut self, target: &PeerSend) {
        self.full_targets.push(target.clone());
    }

    fn mark_shard_full(&mut self) {
        self.shard_full = true;
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
    encoded: EncodedFanOutBatch,
    topic: Bytes,
    group: Option<String>,
    peer_ids: Option<Arc<[u64]>>,
}

#[derive(Clone, Debug)]
struct EncodedFanOutBatch {
    dict: Option<EncodedFanOut>,
    payload: EncodedFanOut,
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
    dict_shipped: bool,
}

struct ShardEndpoint {
    tx: yring::Producer<ShardCommand>,
    notify: Arc<Notify>,
    load: usize,
}

struct FanOutShardState {
    direct_load: usize,
    endpoints: Vec<ShardEndpoint>,
    eligible_peers: usize,
    active_limit: usize,
}

struct FanOutShards {
    state: Mutex<FanOutShardState>,
}

impl std::fmt::Debug for FanOutShards {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().expect("fanout shards poisoned");
        f.debug_struct("FanOutShards")
            .field("shards", &(state.endpoints.len() + 1))
            .field("active_limit", &state.active_limit)
            .field("eligible_peers", &state.eligible_peers)
            .field("direct_load", &state.direct_load)
            .field(
                "worker_loads",
                &state.endpoints.iter().map(|s| s.load).collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[derive(Debug)]
struct ShardWorker {
    rx: yring::Consumer<ShardCommand>,
    notify: Arc<Notify>,
    mode: FanOutMode,
    lossy: bool,
    peers: FxHashMap<u64, ShardPeer>,
}

struct ShardDispatchTargets {
    fallback_targets: SmallVec<[PeerSend; 8]>,
    transform_encoder: Option<SharedFanOutEncoder>,
    sharded_count: usize,
}

#[derive(Clone, Copy, Debug)]
enum ShardDispatchMode {
    Lossy,
    TryAll,
    Block,
}

#[derive(Debug)]
struct DeferredFanOut {
    tx: blume::Sender<DeferredFanOutMsg>,
    state: Mutex<DeferredFanOutState>,
    // Fast-path hint only. `DeferredFanOutState::active` owns transitions.
    active_hint: AtomicBool,
    threshold: usize,
}

#[derive(Debug, Default)]
struct DeferredFanOutState {
    active: bool,
    pending_senders: usize,
}

#[derive(Debug)]
struct DeferredFanOutMsg {
    msg: Message,
    topic: Bytes,
    group: Option<String>,
    fallback_targets: SmallVec<[PeerSend; 8]>,
    sharded_peer_ids: Arc<[u64]>,
}

#[derive(Debug)]
enum DeferredEnqueue {
    Direct,
    Enqueued,
    Dropped,
}

#[derive(Debug)]
struct DeferredFanOutWorker {
    deferred: Arc<DeferredFanOut>,
    shards: Arc<FanOutShards>,
    inner: Arc<Mutex<FanOutInner>>,
    generation: Arc<AtomicU64>,
    lossy: bool,
}

#[derive(Debug)]
pub(crate) struct Submitter {
    shards: Option<Arc<FanOutShards>>,
    sharded_peer_count: Arc<AtomicUsize>,
    inner: Arc<Mutex<FanOutInner>>,
    generation: Arc<AtomicU64>,
    mode: FanOutMode,
    send_count: Arc<AtomicU32>,
    xpub_nodrop: bool,
    lossy: bool,
    deferred: Option<Arc<DeferredFanOut>>,
    cached: Mutex<CachedFanOut>,
}

#[derive(Debug, Default)]
struct CachedFanOut {
    generation: u64,
    sole_wire: Option<PeerSend>,
    all_targets: Option<Arc<Vec<PeerSend>>>,
    encoder: Option<SharedFanOutEncoder>,
    all_wire: bool,
}

impl Clone for Submitter {
    fn clone(&self) -> Self {
        Self {
            shards: self.shards.clone(),
            sharded_peer_count: self.sharded_peer_count.clone(),
            inner: self.inner.clone(),
            generation: self.generation.clone(),
            mode: self.mode,
            send_count: self.send_count.clone(),
            xpub_nodrop: self.xpub_nodrop,
            lossy: self.lossy,
            deferred: self.deferred.clone(),
            cached: Mutex::new(CachedFanOut::default()),
        }
    }
}

impl FanOutShards {
    fn spawn(options: &Options, mode: FanOutMode) -> Arc<Self> {
        let pipe_cap = options.send_hwm.unwrap_or(1000).max(16) as usize;
        let runtime_workers = tokio::runtime::Handle::current()
            .metrics()
            .num_workers()
            .max(1);
        let lossy = fan_out_is_lossy(options);
        let worker_shards = Self::worker_shard_count(runtime_workers);
        let mut endpoints = Vec::with_capacity(worker_shards);
        for _ in 0..worker_shards {
            let (shard_tx, shard_rx) = yring::spsc(pipe_cap);
            let notify = Arc::new(Notify::new());
            tokio::spawn(
                ShardWorker {
                    rx: shard_rx,
                    notify: notify.clone(),
                    mode,
                    lossy,
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
            state: Mutex::new(FanOutShardState {
                direct_load: 0,
                endpoints,
                eligible_peers: 0,
                active_limit: 0,
            }),
        })
    }

    fn desired_active_shards(eligible_peers: usize, max_shards: usize) -> usize {
        if eligible_peers == 0 || max_shards == 0 {
            return 0;
        }
        let worker_peers = eligible_peers.saturating_sub(DIRECT_SHARD_PEER_CAP);
        let worker_shards =
            worker_peers.saturating_add(WORKER_SHARD_PEER_CAP - 1) / WORKER_SHARD_PEER_CAP;
        1usize.saturating_add(worker_shards).min(max_shards)
    }

    fn worker_shard_count(runtime_workers: usize) -> usize {
        runtime_workers.clamp(1, MAX_FAN_OUT_WORKER_SHARDS)
    }

    fn max_shards(state: &FanOutShardState) -> usize {
        state.endpoints.len() + 1
    }

    fn logical_shard_load(state: &FanOutShardState, shard: usize) -> usize {
        if shard == 0 {
            state.direct_load
        } else {
            state.endpoints[shard - 1].load
        }
    }

    fn increment_logical_shard_load(state: &mut FanOutShardState, shard: usize) {
        if shard == 0 {
            state.direct_load += 1;
        } else {
            state.endpoints[shard - 1].load += 1;
        }
    }

    fn decrement_logical_shard_load(state: &mut FanOutShardState, shard: usize) {
        if shard == 0 {
            state.direct_load = state.direct_load.saturating_sub(1);
        } else if let Some(endpoint) = state.endpoints.get_mut(shard - 1) {
            endpoint.load = endpoint.load.saturating_sub(1);
        }
    }

    fn least_loaded_shard(state: &FanOutShardState, active_limit: usize) -> usize {
        (0..active_limit)
            .filter(|&shard| shard != 0 || state.direct_load < DIRECT_SHARD_PEER_CAP)
            .min_by_key(|&shard| Self::logical_shard_load(state, shard))
            .unwrap_or(0)
    }

    fn push_control(endpoint: &mut ShardEndpoint, cmd: ShardCommand) {
        #[cfg(feature = "rt-multi-thread")]
        if let Ok(handle) = tokio::runtime::Handle::try_current()
            && matches!(
                handle.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            )
        {
            // A full shard ring is drained by another Tokio task. If a socket
            // actor spins here on a runtime worker, it can starve that drain.
            tokio::task::block_in_place(|| Self::push_control_spinning(endpoint, cmd));
            return;
        }

        Self::push_control_spinning(endpoint, cmd);
    }

    fn push_control_spinning(endpoint: &mut ShardEndpoint, mut cmd: ShardCommand) {
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

    fn assign_peer(&self) -> usize {
        let mut state = self.state.lock().expect("fanout shards poisoned");
        state.eligible_peers += 1;
        let max_shards = Self::max_shards(&state);
        let desired = Self::desired_active_shards(state.eligible_peers, max_shards);
        state.active_limit = state.active_limit.max(desired);
        let active_limit = state.active_limit.max(1).min(max_shards);
        let shard = Self::least_loaded_shard(&state, active_limit);
        Self::increment_logical_shard_load(&mut state, shard);
        shard
    }

    fn add_worker_peer(&self, shard: usize, add: ShardPeerAdd) {
        if shard == 0 {
            return;
        }
        let mut state = self.state.lock().expect("fanout shards poisoned");
        if let Some(endpoint) = state.endpoints.get_mut(shard - 1) {
            Self::push_control(endpoint, ShardCommand::AddPeer(add));
        }
    }

    fn send_to_shard(&self, shard: usize, cmd: ShardCommand) {
        if shard == 0 {
            return;
        }
        let mut state = self.state.lock().expect("fanout shards poisoned");
        if let Some(endpoint) = state.endpoints.get_mut(shard - 1) {
            Self::push_control(endpoint, cmd);
        }
    }

    fn remove_peer(&self, shard: usize, peer_id: u64) {
        let mut state = self.state.lock().expect("fanout shards poisoned");
        state.eligible_peers = state.eligible_peers.saturating_sub(1);
        Self::decrement_logical_shard_load(&mut state, shard);
        if shard > 0
            && let Some(endpoint) = state.endpoints.get_mut(shard - 1)
        {
            Self::push_control(endpoint, ShardCommand::RemovePeer { peer_id });
        }
    }

    fn dispatch(&self, dispatch: &ShardDispatch) {
        let mut state = self.state.lock().expect("fanout shards poisoned");
        let mut touched = SmallVec::<[usize; 8]>::new();
        for (idx, endpoint) in state.endpoints.iter_mut().enumerate() {
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
            let endpoint = &mut state.endpoints[idx];
            endpoint.tx.flush();
            endpoint.notify.notify_one();
        }
    }

    fn try_dispatch_all(&self, dispatch: &ShardDispatch) -> bool {
        let mut state = self.state.lock().expect("fanout shards poisoned");
        for endpoint in state
            .endpoints
            .iter_mut()
            .filter(|endpoint| endpoint.load > 0)
        {
            if endpoint.tx.is_full() {
                endpoint.tx.flush();
                endpoint.notify.notify_one();
                return false;
            }
        }

        let mut touched = SmallVec::<[usize; 8]>::new();
        for (idx, endpoint) in state.endpoints.iter_mut().enumerate() {
            if endpoint.load == 0 {
                continue;
            }
            if endpoint
                .tx
                .push(ShardCommand::Dispatch(dispatch.clone()))
                .is_err()
            {
                endpoint.tx.flush();
                endpoint.notify.notify_one();
                return false;
            }
            touched.push(idx);
        }
        for idx in touched {
            let endpoint = &mut state.endpoints[idx];
            endpoint.tx.flush();
            endpoint.notify.notify_one();
        }
        true
    }

    fn loaded_shards_have_space(&self) -> bool {
        let mut state = self.state.lock().expect("fanout shards poisoned");
        for endpoint in state
            .endpoints
            .iter_mut()
            .filter(|endpoint| endpoint.load > 0)
        {
            if endpoint.tx.is_full() {
                endpoint.tx.flush();
                endpoint.notify.notify_one();
                return false;
            }
        }
        true
    }

    fn dispatch_blocking(&self, dispatch: &ShardDispatch) {
        let mut state = self.state.lock().expect("fanout shards poisoned");
        for endpoint in state
            .endpoints
            .iter_mut()
            .filter(|endpoint| endpoint.load > 0)
        {
            Self::push_control(endpoint, ShardCommand::Dispatch(dispatch.clone()));
        }
    }

    fn shutdown(&self) {
        let mut state = self.state.lock().expect("fanout shards poisoned");
        for endpoint in &mut state.endpoints {
            Self::push_control(endpoint, ShardCommand::Shutdown);
            endpoint.load = 0;
        }
        state.direct_load = 0;
        state.eligible_peers = 0;
        state.active_limit = 0;
    }

    fn is_empty(&self) -> bool {
        self.state
            .lock()
            .expect("fanout shards poisoned")
            .endpoints
            .iter()
            .all(|endpoint| endpoint.tx.is_empty())
    }
}

fn fan_out_is_lossy(options: &Options) -> bool {
    options.conflate || !matches!(options.on_mute, OnMute::Block)
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
                    if self.handle_command(cmd, &mut touched).await {
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

    async fn handle_command(
        &mut self,
        cmd: ShardCommand,
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        match cmd {
            ShardCommand::AddPeer(add) => {
                self.peers.insert(
                    add.peer_id,
                    ShardPeer {
                        subscriptions: SubscriptionSet::new(),
                        groups: FxHashSet::default(),
                        any_groups: add.any_groups,
                        dict_shipped: add.slot.fanout_dict_shipped(),
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
            ShardCommand::Dispatch(dispatch) => self.dispatch(&dispatch, touched).await,
            ShardCommand::Shutdown => return true,
        }
        false
    }

    async fn dispatch(&mut self, dispatch: &ShardDispatch, touched: &mut SmallVec<[u64; 32]>) {
        if let Some(peer_ids) = dispatch.peer_ids.as_ref() {
            for &peer_id in peer_ids.iter() {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    Self::push_dispatch_to_peer(self.lossy, peer_id, peer, dispatch, touched).await;
                }
            }
            return;
        }

        let mut peer_ids = SmallVec::<[u64; 32]>::new();
        for (&peer_id, peer) in &self.peers {
            if shard_peer_matches(self.mode, peer, dispatch) {
                peer_ids.push(peer_id);
            }
        }
        for peer_id in peer_ids {
            if let Some(peer) = self.peers.get_mut(&peer_id) {
                Self::push_dispatch_to_peer(self.lossy, peer_id, peer, dispatch, touched).await;
            }
        }
    }

    async fn push_dispatch_to_peer(
        lossy: bool,
        peer_id: u64,
        peer: &mut ShardPeer,
        dispatch: &ShardDispatch,
        touched: &mut SmallVec<[u64; 32]>,
    ) {
        if let Some(dict) = dispatch.encoded.dict.as_ref()
            && !peer.dict_shipped
        {
            if !Self::push_encoded_to_peer(lossy, peer_id, peer, dict, touched).await {
                return;
            }
            peer.dict_shipped = true;
            peer.slot.mark_fanout_dict_shipped();
        }
        let _ =
            Self::push_encoded_to_peer(lossy, peer_id, peer, &dispatch.encoded.payload, touched)
                .await;
    }

    async fn push_encoded_to_peer(
        lossy: bool,
        peer_id: u64,
        peer: &mut ShardPeer,
        encoded: &EncodedFanOut,
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        loop {
            match peer
                .slot
                .try_push_ring_item(&mut peer.producer, encoded.to_wire_item())
            {
                TryEncodeResult::Ok => {
                    touched.push(peer_id);
                    return true;
                }
                TryEncodeResult::Dead => return false,
                TryEncodeResult::Full | TryEncodeResult::Ineligible if lossy => return false,
                TryEncodeResult::Full => {
                    peer.slot.flush_ring(&mut peer.producer);
                    let notified = peer.slot.space_available.notified();
                    tokio::select! {
                        biased;
                        () = notified => {}
                        () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
                    }
                }
                TryEncodeResult::Ineligible => {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
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

impl DeferredFanOut {
    fn new(tx: blume::Sender<DeferredFanOutMsg>, threshold: usize) -> Self {
        Self {
            tx,
            state: Mutex::new(DeferredFanOutState::default()),
            active_hint: AtomicBool::new(false),
            threshold,
        }
    }

    fn should_defer_fast(&self, msg: &Message) -> bool {
        self.active_hint.load(Ordering::Acquire) || msg.byte_len() >= self.threshold
    }

    fn begin_enqueue(&self, msg: &Message) -> DeferredEnqueue {
        let is_barrier = msg.byte_len() >= self.threshold;
        let mut state = self.state.lock().expect("deferred fanout state poisoned");
        if !state.active && !is_barrier {
            return DeferredEnqueue::Direct;
        }
        if !state.active {
            state.active = true;
            self.active_hint.store(true, Ordering::Release);
        }
        state.pending_senders += 1;
        DeferredEnqueue::Enqueued
    }

    fn finish_enqueue(&self, msg: DeferredFanOutMsg) -> DeferredEnqueue {
        let sent = self.tx.try_send(msg).is_ok();
        let mut state = self.state.lock().expect("deferred fanout state poisoned");
        state.pending_senders = state.pending_senders.saturating_sub(1);
        if sent {
            DeferredEnqueue::Enqueued
        } else {
            if state.pending_senders == 0 && self.tx.is_empty() {
                state.active = false;
                self.active_hint.store(false, Ordering::Release);
            }
            DeferredEnqueue::Dropped
        }
    }

    fn cancel_enqueue(&self) {
        let mut state = self.state.lock().expect("deferred fanout state poisoned");
        state.pending_senders = state.pending_senders.saturating_sub(1);
        if state.pending_senders == 0 && self.tx.is_empty() {
            state.active = false;
            self.active_hint.store(false, Ordering::Release);
        }
    }

    fn complete_if_idle(&self) -> bool {
        let mut state = self.state.lock().expect("deferred fanout state poisoned");
        if state.pending_senders == 0 && self.tx.is_empty() {
            state.active = false;
            self.active_hint.store(false, Ordering::Release);
            true
        } else {
            false
        }
    }

    fn close(&self) {
        self.tx.close();
        let mut state = self.state.lock().expect("deferred fanout state poisoned");
        state.active = false;
        state.pending_senders = 0;
        self.active_hint.store(false, Ordering::Release);
    }

    fn is_empty(&self) -> bool {
        !self.active_hint.load(Ordering::Acquire) && self.tx.is_empty()
    }
}

impl DeferredFanOutWorker {
    async fn run(mut self, mut rx: blume::Receiver<DeferredFanOutMsg>) {
        let mut batch = Vec::new();
        loop {
            batch.clear();
            if rx.recv_batch_mut(&mut batch).await.is_err() {
                return;
            }

            loop {
                for msg in batch.drain(..) {
                    let _ = self.dispatch(msg).await;
                }
                while let Ok(msg) = rx.try_recv() {
                    batch.push(msg);
                }
                if !batch.is_empty() {
                    continue;
                }
                if self.deferred.complete_if_idle() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
    }

    async fn dispatch(&mut self, msg: DeferredFanOutMsg) -> Result<()> {
        let target_count = msg.fallback_targets.len() + msg.sharded_peer_ids.len();
        if target_count == 0 {
            return Ok(());
        }
        let Ok(encoded) = self.encode_deferred_batch(&msg.msg, target_count).await else {
            return Ok(());
        };
        if !msg.fallback_targets.is_empty() {
            let inner = self.inner.clone();
            let generation = self.generation.clone();
            let mut deactivate =
                |target: &PeerSend| deactivate_fanout_target(&inner, &generation, target);
            let _ = dispatch_encoded_batch(
                &encoded,
                &msg.fallback_targets,
                &msg.msg,
                self.lossy,
                &mut deactivate,
            );
        }
        if !msg.sharded_peer_ids.is_empty() {
            let dispatch = ShardDispatch {
                encoded,
                topic: msg.topic,
                group: msg.group,
                peer_ids: Some(msg.sharded_peer_ids),
            };
            if self.lossy {
                self.shards.dispatch(&dispatch);
            } else {
                self.shards.dispatch_blocking(&dispatch);
            }
        }
        Ok(())
    }

    async fn encode_deferred_batch(
        &self,
        msg: &Message,
        target_count: usize,
    ) -> Result<EncodedFanOutBatch> {
        let encoder = {
            let g = self.inner.lock().expect("fanout inner poisoned");
            g.fan_out_encoder.clone()
        };
        let Some(encoder) = encoder else {
            return Ok(EncodedFanOutBatch {
                dict: None,
                payload: encode_message_for_fanout(msg, target_count),
            });
        };
        let mut enc = {
            let mut guard = encoder.lock().expect("fan_out_encoder poisoned");
            guard.take().ok_or_else(|| {
                Error::Protocol("fan-out encoder unavailable during deferred send".into())
            })?
        };
        let msg = msg.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let result = enc.encode(&msg);
            (enc, result)
        });
        let (enc, transformed) = handle
            .await
            .map_err(|_| Error::Protocol("fan-out compression task panicked".into()))?;
        *encoder.lock().expect("fan_out_encoder poisoned") = Some(enc);
        Ok(encode_transformed_for_fanout(
            &self.inner,
            transformed?,
            target_count,
        ))
    }
}

fn encode_messages_for_fanout(messages: &[Message], target_count: usize) -> EncodedFanOut {
    let mut eq = EncodedQueue::one_shot();
    let mut chunks = Vec::new();
    for wire_msg in messages {
        encode_fan_out_message(&mut eq, wire_msg, target_count, FAN_OUT_TOTAL_COPY_BUDGET);
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
    fanout_encoded
}

fn encode_message_for_fanout(msg: &Message, target_count: usize) -> EncodedFanOut {
    encode_messages_for_fanout(std::slice::from_ref(msg), target_count)
}

fn encode_transformed_for_fanout(
    inner: &Arc<Mutex<FanOutInner>>,
    mut transformed: omq_proto::proto::transform::TransformedOut,
    target_count: usize,
) -> EncodedFanOutBatch {
    let dict_msg = MessageEncoder::take_leading_dict_shipment(&mut transformed);
    let dict = dict_msg
        .as_ref()
        .map(|msg| encode_message_for_fanout(msg, target_count));
    let payload = encode_messages_for_fanout(&transformed, target_count);

    let mut g = inner.lock().expect("fanout inner poisoned");
    if let Some(dict) = dict
        && g.fan_out_dict.is_none()
    {
        g.fan_out_dict = Some(dict);
    }
    EncodedFanOutBatch {
        dict: g.fan_out_dict.clone(),
        payload,
    }
}

fn try_push_encoded_fanout(slot: &PeerWireSlot, encoded: &EncodedFanOut) -> TryEncodeResult {
    match encoded {
        EncodedFanOut::Inline { buf, len } => {
            slot.try_push_pre_encoded_no_signal(&buf[..*len as usize])
        }
        EncodedFanOut::Shared(chunks) => slot.try_push_encoded(chunks),
    }
}

fn deactivate_fanout_target(
    inner: &Arc<Mutex<FanOutInner>>,
    generation: &Arc<AtomicU64>,
    target: &PeerSend,
) {
    let PeerSend::Wire { slot, .. } = target else {
        return;
    };
    let peer_id = slot.peer_id;
    slot.deactivate_fanout();
    let mut g = inner.lock().expect("fanout inner poisoned");
    if g.deactivate_fanout_peer(peer_id) {
        drop(g);
        generation.fetch_add(1, Ordering::Release);
    }
}

fn shard_peer_matches(mode: FanOutMode, peer: &ShardPeer, dispatch: &ShardDispatch) -> bool {
    match (mode, dispatch.group.as_deref()) {
        (FanOutMode::Group, Some(grp)) => peer.any_groups || peer.groups.contains(grp),
        (FanOutMode::SubscriptionPrefix, _) => peer.subscriptions.matches(&dispatch.topic),
        (FanOutMode::Group, None) => false,
    }
}

fn fanout_peer_matches(
    mode: FanOutMode,
    peer: &FanOutPeer,
    msg: &Message,
    group: Option<&str>,
) -> bool {
    match (mode, group) {
        (FanOutMode::Group, Some(grp)) => peer.any_groups || peer.groups.contains(grp),
        (FanOutMode::SubscriptionPrefix, _) => peer.subscriptions.matches(&first_frame_bytes(msg)),
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
        deactivate_fanout_target(&self.inner, &self.generation, target);
    }

    fn has_sharded_peers(&self) -> bool {
        self.sharded_peer_count.load(Ordering::Acquire) > 0
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

    fn encode_fanout_batch(
        &self,
        msg: &Message,
        target_count: usize,
        transform_encoder: &Mutex<Option<MessageEncoder>>,
    ) -> Result<EncodedFanOutBatch> {
        let transformed = {
            let mut enc = transform_encoder.lock().expect("fan_out_encoder poisoned");
            let enc = enc.as_mut().ok_or_else(|| {
                Error::Protocol("fan-out encoder unavailable during direct send".into())
            })?;
            enc.encode(msg)?
        };
        Ok(encode_transformed_for_fanout(
            &self.inner,
            transformed,
            target_count,
        ))
    }

    fn dispatch_to_targets(
        &self,
        targets: &[PeerSend],
        msg: &Message,
        encoder: Option<&Mutex<Option<MessageEncoder>>>,
        drop_on_full: bool,
        deactivate: &mut impl FnMut(&PeerSend),
    ) -> Result<DispatchOutcome> {
        let Some(encoder) = encoder else {
            return dispatch_to_targets(targets, msg, None, drop_on_full, deactivate);
        };
        if targets.is_empty() {
            return Ok(DispatchOutcome::default());
        }
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

        let batch = self.encode_fanout_batch(msg, targets.len(), encoder)?;
        Ok(dispatch_encoded_batch(
            &batch,
            targets,
            msg,
            drop_on_full,
            deactivate,
        ))
    }

    fn try_defer_to_shards(&self, msg: &Message, group: Option<String>) -> DeferredEnqueue {
        let Some(deferred) = self.deferred.as_ref() else {
            return DeferredEnqueue::Direct;
        };
        if !deferred.should_defer_fast(msg) {
            return DeferredEnqueue::Direct;
        }
        match deferred.begin_enqueue(msg) {
            DeferredEnqueue::Direct => return DeferredEnqueue::Direct,
            DeferredEnqueue::Dropped => return DeferredEnqueue::Dropped,
            DeferredEnqueue::Enqueued => {}
        }

        let deferred_msg = self.collect_deferred_msg(msg, group);
        if deferred_msg.fallback_targets.is_empty() && deferred_msg.sharded_peer_ids.is_empty() {
            deferred.cancel_enqueue();
            return DeferredEnqueue::Enqueued;
        }
        deferred.finish_enqueue(deferred_msg)
    }

    fn dispatch_to_shards_and_fallback(
        &self,
        shards: &FanOutShards,
        msg: &Message,
        group: Option<String>,
        mode: ShardDispatchMode,
    ) -> Result<DispatchOutcome> {
        match self.try_defer_to_shards(msg, group.clone()) {
            DeferredEnqueue::Direct => {}
            DeferredEnqueue::Enqueued | DeferredEnqueue::Dropped => {
                return Ok(DispatchOutcome::default());
            }
        }

        let targets = self.collect_shard_targets(msg, group.as_deref());
        self.dispatch_shard_targets(shards, msg, group, &targets, mode)
    }

    fn dispatch_shard_targets(
        &self,
        shards: &FanOutShards,
        msg: &Message,
        group: Option<String>,
        targets: &ShardDispatchTargets,
        mode: ShardDispatchMode,
    ) -> Result<DispatchOutcome> {
        let mut outcome = DispatchOutcome::default();
        let target_count = targets.fallback_targets.len() + targets.sharded_count;
        if target_count == 0 {
            return Ok(outcome);
        }
        if matches!(mode, ShardDispatchMode::TryAll)
            && targets.sharded_count > 0
            && !shards.loaded_shards_have_space()
        {
            outcome.mark_shard_full();
            return Ok(outcome);
        }

        let encoded_dispatch = if let Some(encoder) = targets.transform_encoder.as_deref() {
            Some(self.encode_fanout_batch(msg, target_count, encoder)?)
        } else {
            None
        };

        if !targets.fallback_targets.is_empty() {
            let mut deactivate = |target: &PeerSend| self.deactivate_target(target);
            outcome = if let Some(encoded) = encoded_dispatch.as_ref() {
                dispatch_encoded_batch(
                    encoded,
                    &targets.fallback_targets,
                    msg,
                    self.lossy && !self.xpub_nodrop,
                    &mut deactivate,
                )
            } else {
                dispatch_to_targets(
                    &targets.fallback_targets,
                    msg,
                    None,
                    self.lossy && !self.xpub_nodrop,
                    &mut deactivate,
                )?
            };
        }

        if targets.sharded_count == 0 {
            return Ok(outcome);
        }

        let encoded_dispatch = encoded_dispatch.unwrap_or_else(|| EncodedFanOutBatch {
            dict: None,
            payload: encode_message_for_fanout(msg, targets.sharded_count),
        });
        let dispatch = ShardDispatch {
            encoded: encoded_dispatch,
            topic: first_frame_bytes(msg),
            group,
            peer_ids: None,
        };
        match mode {
            ShardDispatchMode::Lossy => shards.dispatch(&dispatch),
            ShardDispatchMode::TryAll => {
                if !shards.try_dispatch_all(&dispatch) {
                    outcome.mark_shard_full();
                }
            }
            ShardDispatchMode::Block => shards.dispatch_blocking(&dispatch),
        }
        Ok(outcome)
    }

    pub(crate) fn try_send(
        &self,
        msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        let (forwarded, group) = self
            .prepare(msg)
            .map_err(omq_proto::error::TrySendError::Error)?;

        if let Some(ref shards) = self.shards
            && self.has_sharded_peers()
        {
            let mode = if self.lossy && !self.xpub_nodrop {
                ShardDispatchMode::Lossy
            } else {
                ShardDispatchMode::TryAll
            };
            let outcome = self
                .dispatch_to_shards_and_fallback(shards, &forwarded, group, mode)
                .map_err(omq_proto::error::TrySendError::Error)?;
            if outcome.is_full() && (!self.lossy || self.xpub_nodrop) {
                return Err(omq_proto::error::TrySendError::Full(forwarded));
            }
            return Ok(());
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
                if self
                    .dispatch_to_targets(
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
        if self
            .dispatch_to_targets(
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

        if let Some(ref shards) = self.shards
            && self.has_sharded_peers()
        {
            match self.try_defer_to_shards(&forwarded, group.clone()) {
                DeferredEnqueue::Direct => {}
                DeferredEnqueue::Enqueued | DeferredEnqueue::Dropped => return Ok(()),
            }

            let targets = self.collect_shard_targets(&forwarded, group.as_deref());
            let target_count = targets.fallback_targets.len() + targets.sharded_count;
            let mode = if self.lossy && !self.xpub_nodrop {
                ShardDispatchMode::Lossy
            } else {
                ShardDispatchMode::Block
            };
            let outcome = self.dispatch_shard_targets(shards, &forwarded, group, &targets, mode)?;
            if !self.lossy && outcome.is_full() {
                send_to_targets_nodrop(&outcome.full_targets, forwarded.clone()).await?;
            }
            let interval = yield_interval(target_count);
            if self.send_count.fetch_add(1, Ordering::Relaxed) % interval == interval - 1 {
                tokio::task::yield_now().await;
            }
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
                let outcome = self.dispatch_to_targets(
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
        let outcome = self.dispatch_to_targets(
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
    ) -> (SmallVec<[PeerSend; 8]>, Option<SharedFanOutEncoder>) {
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
            .filter(|p| p.shard.unwrap_or(0) == 0)
            .filter(|p| match (self.mode, group) {
                (FanOutMode::Group, Some(grp)) => p.any_groups || p.groups.contains(grp),
                (FanOutMode::SubscriptionPrefix, _) => {
                    p.subscriptions.matches(&first_frame_bytes(msg))
                }
                (FanOutMode::Group, None) => false,
            })
            .map(|p| p.target.clone())
            .collect();
        let sharded_count = g
            .peers
            .values()
            .filter(|p| p.shard.is_some_and(|shard| shard > 0))
            .count();
        ShardDispatchTargets {
            fallback_targets,
            transform_encoder: g.fan_out_encoder.clone(),
            sharded_count,
        }
    }

    fn collect_deferred_msg(&self, msg: &Message, group: Option<String>) -> DeferredFanOutMsg {
        let group_ref = group.as_deref();
        let g = self.inner.lock().expect("fanout inner poisoned");
        let fallback_targets = g
            .peers
            .values()
            .filter(|p| p.shard.unwrap_or(0) == 0)
            .filter(|p| fanout_peer_matches(self.mode, p, msg, group_ref))
            .map(|p| p.target.clone())
            .collect();
        let sharded_peer_ids: Arc<[u64]> = g
            .peers
            .iter()
            .filter(|(_, p)| p.shard.is_some_and(|shard| shard > 0))
            .filter(|(_, p)| fanout_peer_matches(self.mode, p, msg, group_ref))
            .map(|(&peer_id, _)| peer_id)
            .collect::<Vec<_>>()
            .into();
        DeferredFanOutMsg {
            msg: msg.clone(),
            topic: first_frame_bytes(msg),
            group,
            fallback_targets,
            sharded_peer_ids,
        }
    }
}

/// Fan-out send strategy.
#[derive(Debug)]
pub(crate) struct FanOutSend {
    shards: Option<Arc<FanOutShards>>,
    sharded_peer_count: Arc<AtomicUsize>,
    inner: Arc<Mutex<FanOutInner>>,
    generation: Arc<AtomicU64>,
    mode: FanOutMode,
    xpub_nodrop: bool,
    lossy: bool,
    deferred: Option<Arc<DeferredFanOut>>,
}

struct FanOutInner {
    peers: FxHashMap<u64, FanOutPeer>,
    all_subscribe_all: bool,
    all_targets: SmallVec<[PeerSend; 8]>,
    active_all_targets: SmallVec<[PeerSend; 8]>,
    fan_out_encoder: Option<SharedFanOutEncoder>,
    fan_out_dict: Option<EncodedFanOut>,
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
            if let Some((enc, _dec)) = MessageEncoder::for_endpoint(&dummy, &self.options) {
                self.fan_out_encoder = Some(Arc::new(Mutex::new(Some(enc))));
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
        let (deferred, deferred_rx) = if use_shards
            && let Some(threshold) = options.compression_offload_threshold
            && threshold > 0
        {
            let cap = options.send_hwm.unwrap_or(1000).max(1) as usize;
            let (tx, rx) = blume::bounded(cap);
            (Some(Arc::new(DeferredFanOut::new(tx, threshold))), Some(rx))
        } else {
            (None, None)
        };
        let inner = Arc::new(Mutex::new(FanOutInner {
            peers: FxHashMap::default(),
            all_subscribe_all: false,
            all_targets: SmallVec::new(),
            active_all_targets: SmallVec::new(),
            fan_out_encoder: None,
            fan_out_dict: None,
            options: options.clone(),
        }));
        let generation = Arc::new(AtomicU64::new(0));
        let sharded_peer_count = Arc::new(AtomicUsize::new(0));
        if let (Some(shards), Some(deferred), Some(rx)) =
            (shards.clone(), deferred.clone(), deferred_rx)
        {
            tokio::spawn(
                DeferredFanOutWorker {
                    deferred,
                    shards,
                    inner: inner.clone(),
                    generation: generation.clone(),
                    lossy: fan_out_is_lossy(options),
                }
                .run(rx),
            );
        }
        Self {
            shards,
            sharded_peer_count,
            inner,
            generation,
            mode,
            xpub_nodrop: options.xpub_nodrop,
            lossy: fan_out_is_lossy(options),
            deferred,
        }
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            shards: self.shards.clone(),
            sharded_peer_count: self.sharded_peer_count.clone(),
            inner: self.inner.clone(),
            generation: self.generation.clone(),
            mode: self.mode,
            send_count: Arc::new(AtomicU32::new(0)),
            xpub_nodrop: self.xpub_nodrop,
            lossy: self.lossy,
            deferred: self.deferred.clone(),
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

        let shard = if !shard_eligible {
            None
        } else if let (Some(shards), PeerSend::Wire { slot, .. }) = (self.shards.as_ref(), &target)
        {
            let shard = shards.assign_peer();
            if shard > 0 {
                let added = handle
                    .wire_slot_tx
                    .as_ref()
                    .and_then(|tx| tx.lock().expect("wire_slot_tx poisoned").take())
                    .map(|producer| {
                        shards.add_worker_peer(
                            shard,
                            ShardPeerAdd {
                                peer_id,
                                slot: slot.clone(),
                                producer,
                                any_groups,
                            },
                        );
                    })
                    .is_some();
                if added {
                    Some(shard)
                } else {
                    shards.remove_peer(shard, peer_id);
                    None
                }
            } else {
                Some(shard)
            }
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
        if shard.is_some_and(|shard| shard > 0) {
            self.sharded_peer_count.fetch_add(1, Ordering::Release);
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
                fanout_active: true,
            },
        );
        g.recompute_subscribe_all();
        self.bump_generation();
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(peer) = g.peers.remove(&peer_id) {
            if peer.shard.is_some_and(|shard| shard > 0) {
                self.sharded_peer_count.fetch_sub(1, Ordering::Release);
            }
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
        if let Some(ref deferred) = self.deferred {
            deferred.close();
        }
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.clear();
        g.all_subscribe_all = false;
        g.all_targets.clear();
        g.active_all_targets.clear();
        g.fan_out_encoder = None;
        g.fan_out_dict = None;
        drop(g);
        self.sharded_peer_count.store(0, Ordering::Release);
        self.bump_generation();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let shards_empty = self.shards.as_ref().is_none_or(|shards| shards.is_empty());
        let deferred_empty = self.deferred.as_ref().is_none_or(|d| d.is_empty());
        let g = self.inner.lock().expect("fanout inner poisoned");
        shards_empty && deferred_empty && g.peers.values().all(|p| p.target.is_empty())
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

fn dispatch_encoded_batch(
    encoded: &EncodedFanOutBatch,
    targets: &[PeerSend],
    msg: &Message,
    drop_on_full: bool,
    deactivate: &mut impl FnMut(&PeerSend),
) -> DispatchOutcome {
    let mut outcome = DispatchOutcome::default();
    for t in targets {
        match t {
            PeerSend::Wire { slot, .. } => {
                if drop_on_full && !slot.fanout_active() {
                    outcome.push_full(t);
                    continue;
                }
                if let Some(dict) = encoded.dict.as_ref()
                    && !slot.fanout_dict_shipped()
                {
                    match try_push_encoded_fanout(slot, dict) {
                        TryEncodeResult::Ok => {
                            slot.mark_fanout_dict_shipped();
                        }
                        TryEncodeResult::Full => {
                            if drop_on_full {
                                deactivate(t);
                            }
                            outcome.push_full(t);
                            continue;
                        }
                        TryEncodeResult::Dead | TryEncodeResult::Ineligible => continue,
                    }
                }
                if try_push_encoded_fanout(slot, &encoded.payload) == TryEncodeResult::Full {
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
    outcome
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Notify;

    use super::{
        DIRECT_SHARD_PEER_CAP, FanOutShardState, FanOutShards, MAX_FAN_OUT_WORKER_SHARDS,
        ShardEndpoint, WORKER_SHARD_PEER_CAP,
    };

    const TEST_MAX_LOGICAL_SHARDS: usize = MAX_FAN_OUT_WORKER_SHARDS + 1;
    const TEST_SINGLE_LOGICAL_SHARD: usize = 1;
    const TEST_LOW_WORKER_COUNT: usize = 2;
    const TEST_HIGH_WORKER_COUNT: usize = 64;
    const TEST_WIDE_PEER_COUNT: usize = 32;

    #[test]
    fn desired_active_shards_ramps_monotonically() {
        assert_eq!(
            FanOutShards::desired_active_shards(0, TEST_MAX_LOGICAL_SHARDS),
            0
        );
        assert_eq!(
            FanOutShards::desired_active_shards(1, TEST_MAX_LOGICAL_SHARDS),
            1
        );
        assert_eq!(
            FanOutShards::desired_active_shards(DIRECT_SHARD_PEER_CAP, TEST_MAX_LOGICAL_SHARDS),
            1
        );
        assert_eq!(
            FanOutShards::desired_active_shards(DIRECT_SHARD_PEER_CAP + 1, TEST_MAX_LOGICAL_SHARDS),
            2
        );
        assert_eq!(
            FanOutShards::desired_active_shards(
                DIRECT_SHARD_PEER_CAP + WORKER_SHARD_PEER_CAP,
                TEST_MAX_LOGICAL_SHARDS
            ),
            2
        );
        assert_eq!(
            FanOutShards::desired_active_shards(
                DIRECT_SHARD_PEER_CAP + WORKER_SHARD_PEER_CAP + 1,
                TEST_MAX_LOGICAL_SHARDS
            ),
            3
        );
        assert_eq!(
            FanOutShards::desired_active_shards(TEST_WIDE_PEER_COUNT, TEST_MAX_LOGICAL_SHARDS),
            8
        );
    }

    #[test]
    fn desired_active_shards_is_capped_by_runtime_workers() {
        assert_eq!(
            FanOutShards::desired_active_shards(0, TEST_LOW_WORKER_COUNT),
            0
        );
        assert_eq!(
            FanOutShards::desired_active_shards(1, TEST_LOW_WORKER_COUNT),
            1
        );
        assert_eq!(
            FanOutShards::desired_active_shards(DIRECT_SHARD_PEER_CAP, TEST_LOW_WORKER_COUNT),
            1
        );
        assert_eq!(
            FanOutShards::desired_active_shards(DIRECT_SHARD_PEER_CAP + 1, TEST_LOW_WORKER_COUNT),
            2
        );
        assert_eq!(
            FanOutShards::desired_active_shards(TEST_HIGH_WORKER_COUNT, TEST_LOW_WORKER_COUNT),
            TEST_LOW_WORKER_COUNT
        );
        assert_eq!(
            FanOutShards::desired_active_shards(TEST_HIGH_WORKER_COUNT, TEST_SINGLE_LOGICAL_SHARD),
            TEST_SINGLE_LOGICAL_SHARD
        );
    }

    #[test]
    fn worker_shard_count_is_capped() {
        assert_eq!(FanOutShards::worker_shard_count(0), 1);
        assert_eq!(
            FanOutShards::worker_shard_count(TEST_LOW_WORKER_COUNT),
            TEST_LOW_WORKER_COUNT
        );
        assert_eq!(
            FanOutShards::worker_shard_count(TEST_HIGH_WORKER_COUNT),
            MAX_FAN_OUT_WORKER_SHARDS
        );
    }

    #[test]
    fn assign_peer_keeps_first_four_on_direct_shard_and_reuses_it() {
        let shards = FanOutShards {
            state: std::sync::Mutex::new(FanOutShardState {
                direct_load: 0,
                endpoints: test_endpoints(TEST_LOW_WORKER_COUNT),
                eligible_peers: 0,
                active_limit: 0,
            }),
        };

        let assigned: Vec<_> = (0..(DIRECT_SHARD_PEER_CAP * 2))
            .map(|_| shards.assign_peer())
            .collect();
        assert_eq!(assigned, vec![0, 0, 0, 0, 1, 1, 1, 1]);

        shards.remove_peer(0, 1);
        assert_eq!(shards.assign_peer(), 0);
    }

    #[test]
    fn assign_peer_caps_direct_shard_at_four_peers() {
        let shards = FanOutShards {
            state: std::sync::Mutex::new(FanOutShardState {
                direct_load: 0,
                endpoints: test_endpoints(WORKER_SHARD_PEER_CAP),
                eligible_peers: 0,
                active_limit: 0,
            }),
        };

        let mut loads = [0usize; WORKER_SHARD_PEER_CAP + 1];
        for _ in 0..TEST_WIDE_PEER_COUNT {
            loads[shards.assign_peer()] += 1;
        }

        assert_eq!(loads, [4, 7, 7, 7, 7]);
    }

    fn test_endpoints(count: usize) -> Vec<ShardEndpoint> {
        (0..count).map(|_| test_endpoint()).collect()
    }

    fn test_endpoint() -> ShardEndpoint {
        let (tx, _rx) = yring::spsc(4);
        ShardEndpoint {
            tx,
            notify: Arc::new(Notify::new()),
            load: 0,
        }
    }
}

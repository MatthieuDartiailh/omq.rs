//! Fan-out send: raw message distribution into shard workers.
//!
//! PUB and XPUB filter by SUBSCRIBE-driven prefix set; RADIO filters
//! by joined groups. The caller pushes raw `Message` values into each
//! active shard's yring. Each shard worker encodes (and optionally
//! compresses) locally, then pushes into its peers' `PeerTransmitSlot`
//! rings.

use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustc_hash::{FxHashMap, FxHashSet};

use smallvec::SmallVec;

use bytes::Bytes;
use tokio::sync::Notify;

use crate::engine::signal::DataSignal;
use crate::engine::{PeerDriverCommand, PeerDriverHandle};
use omq_proto::error::{Error, Result};
use omq_proto::fan_out_frame::{
    FanOutFrame, build_fan_out_frame, clear_fan_out_frame, encode_fan_out_message,
    finish_fan_out_frame,
};
use omq_proto::flow::DrainBudget;
use omq_proto::frame_buffer::FrameBuffer;
use omq_proto::message::Message;
use omq_proto::options::Options;
#[cfg(feature = "lz4")]
use omq_proto::proto::transform::MessageEncoder;

use super::peer_outbound::PeerOutbound;
use super::subscription::SubscriptionSet;
use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};

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
/// `FrameBuffer::ARENA_THRESHOLD` for this: PUSH/SCATTER use it too.
const FAN_OUT_TOTAL_COPY_BUDGET: usize = 8 * 1024;
const WORKER_SHARD_PEER_CAP: usize = 4;
const MAX_FAN_OUT_WORKER_SHARDS: usize = 8;
const SHARD_CTRL_RING_CAP: usize = 64;

/// Yield every N sends to keep latency bounded. Scales down with peer
/// count and message size: fewer sends per yield when one send queues
/// more total work. isqrt gives sub-linear peer scaling; floor of 16
/// prevents over-yielding.
fn yield_interval(peer_count: usize, msg_bytes: usize) -> u32 {
    if peer_count == 0 {
        return 1;
    }
    let n = (peer_count as u32).max(1);
    let peer_interval = (512 / n.isqrt()).max(16);
    let byte_interval = (256 * 1024 / msg_bytes.max(1)).clamp(16, 512) as u32;
    peer_interval.min(byte_interval)
}

#[derive(Debug, Default)]
struct DispatchOutcome {
    full_targets: SmallVec<[PeerOutbound; 8]>,
}

impl DispatchOutcome {
    fn push_full(&mut self, target: &PeerOutbound) {
        self.full_targets.push(target.clone());
    }
}

#[derive(Debug)]
enum ShardControl {
    AddPeer(ShardPeerAdd),
    RemovePeer {
        peer_id: u64,
    },
    Subscribe {
        peer_id: u64,
        prefix: Bytes,
    },
    Cancel {
        peer_id: u64,
        prefix: Bytes,
    },
    Join {
        peer_id: u64,
        group: Bytes,
    },
    Leave {
        peer_id: u64,
        group: Bytes,
    },
    SetCompression {
        options: Box<Options>,
        dict: Option<Bytes>,
    },
    Shutdown,
}

#[derive(Debug)]
struct ShardPeerAdd {
    peer_id: u64,
    slot: Arc<PeerTransmitSlot>,
    any_groups: bool,
}

#[derive(Clone, Debug)]
struct ShardDispatch {
    msg: Message,
    topic: Bytes,
    group: Option<String>,
}

#[derive(Debug)]
struct ShardPeer {
    subscriptions: SubscriptionSet,
    groups: FxHashSet<String>,
    any_groups: bool,
    slot: Arc<PeerTransmitSlot>,
    dict_shipped: bool,
}

struct ShardEndpoint {
    ctrl_tx: yring::Producer<ShardControl>,
    ctrl_notify: Arc<Notify>,
    load: usize,
}

struct DistributorEndpoint {
    data_tx: yring::Producer<ShardDispatch>,
    data_signal: Arc<DataSignal>,
}

struct DistributionTarget {
    data_tx: yring::Producer<ShardDispatch>,
    data_signal: Arc<DataSignal>,
}

struct FanOutShardState {
    endpoints: Vec<ShardEndpoint>,
    eligible_peers: usize,
    active_limit: usize,
}

struct FanOutShards {
    state: Mutex<FanOutShardState>,
    active_mask: Arc<AtomicU8>,
    distributor: Mutex<DistributorEndpoint>,
}

impl std::fmt::Debug for DistributorEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributorEndpoint")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for DistributionTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributionTarget").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for FanOutShards {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().expect("fanout shards poisoned");
        f.debug_struct("FanOutShards")
            .field("shards", &state.endpoints.len())
            .field("active_mask", &self.active_mask.load(Ordering::Relaxed))
            .field("active_limit", &state.active_limit)
            .field("eligible_peers", &state.eligible_peers)
            .field(
                "worker_loads",
                &state.endpoints.iter().map(|s| s.load).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

struct ShardWorker {
    data_rx: yring::Consumer<ShardDispatch>,
    ctrl_rx: yring::Consumer<ShardControl>,
    data_signal: Arc<DataSignal>,
    ctrl_notify: Arc<Notify>,
    mode: FanOutMode,
    lossy: bool,
    peers: FxHashMap<u64, ShardPeer>,
    eq: FrameBuffer,
    chunks: Vec<Bytes>,
    #[cfg(feature = "lz4")]
    encoder: Option<MessageEncoder>,
    distribution_targets: Vec<DistributionTarget>,
    active_mask: Option<Arc<AtomicU8>>,
}

impl std::fmt::Debug for ShardWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardWorker")
            .field("mode", &self.mode)
            .field("lossy", &self.lossy)
            .field("peers", &self.peers.len())
            .field("distribution_targets", &self.distribution_targets.len())
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "lz4")]
struct DictTraining {
    trainer: omq_proto::proto::transform::lz4::DictTrainer,
    msgs_left: usize,
}

#[cfg(feature = "lz4")]
impl std::fmt::Debug for DictTraining {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DictTraining")
            .field("msgs_left", &self.msgs_left)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) struct Submitter {
    shards: Option<Arc<FanOutShards>>,
    sharded_peer_count: Arc<AtomicUsize>,
    fallback_peer_count: Arc<AtomicUsize>,
    inner: Arc<Mutex<FanOutInner>>,
    generation: Arc<AtomicU64>,
    mode: FanOutMode,
    send_count: Arc<AtomicU32>,
    xpub_nodrop: bool,
    lossy: bool,
    #[cfg(feature = "lz4")]
    dict_training: Arc<Mutex<Option<DictTraining>>>,
}

impl Clone for Submitter {
    fn clone(&self) -> Self {
        Self {
            shards: self.shards.clone(),
            sharded_peer_count: self.sharded_peer_count.clone(),
            fallback_peer_count: self.fallback_peer_count.clone(),
            inner: self.inner.clone(),
            generation: self.generation.clone(),
            mode: self.mode,
            send_count: self.send_count.clone(),
            xpub_nodrop: self.xpub_nodrop,
            lossy: self.lossy,
            #[cfg(feature = "lz4")]
            dict_training: self.dict_training.clone(),
        }
    }
}

impl FanOutShards {
    fn spawn(
        options: &Options,
        mode: FanOutMode,
        io_pool: &crate::context::IoPoolHandle,
    ) -> Arc<Self> {
        let pipe_cap = options.send_hwm.unwrap_or(1000).max(16) as usize;
        let worker_shards = Self::worker_shard_count(io_pool.thread_count());
        let lossy = fan_out_is_lossy(options);
        let active_mask = Arc::new(AtomicU8::new(0));

        // Create all channels up front.
        let mut data_channels: Vec<_> = (0..worker_shards)
            .map(|_| {
                let (tx, rx) = yring::spsc(pipe_cap);
                let sig = Arc::new(DataSignal::new());
                (tx, rx, sig)
            })
            .collect();
        let mut ctrl_channels: Vec<_> = (0..worker_shards)
            .map(|_| {
                let (tx, rx) = yring::spsc(SHARD_CTRL_RING_CAP);
                let notify = Arc::new(Notify::new());
                (tx, rx, notify)
            })
            .collect();

        // Channel 0 (shard 1): data Producer goes into the distributor
        // endpoint. Channels 1..N-1 (shards 2..N): data Producers go
        // into distribution targets owned by shard worker 1.
        let (dist_tx, dist_rx, dist_signal) = data_channels.remove(0);
        let distributor = DistributorEndpoint {
            data_tx: dist_tx,
            data_signal: Arc::clone(&dist_signal),
        };

        let mut distribution_targets: Vec<DistributionTarget> =
            Vec::with_capacity(data_channels.len());
        let mut secondary_data: Vec<(yring::Consumer<ShardDispatch>, Arc<DataSignal>)> =
            Vec::with_capacity(data_channels.len());
        for (tx, rx, sig) in data_channels {
            distribution_targets.push(DistributionTarget {
                data_tx: tx,
                data_signal: Arc::clone(&sig),
            });
            secondary_data.push((rx, sig));
        }

        // Build endpoints (ctrl only) and spawn workers.
        let mut dist_rx = Some(dist_rx);
        let mut dist_signal = Some(dist_signal);
        let mut endpoints = Vec::with_capacity(worker_shards);
        for i in 0..worker_shards {
            let (ctrl_tx, ctrl_rx, ctrl_notify) = ctrl_channels.remove(0);

            let (data_rx, data_signal, dist_targets, mask) = if i == 0 {
                (
                    dist_rx.take().expect("shard 0 data_rx"),
                    dist_signal.take().expect("shard 0 data_signal"),
                    std::mem::take(&mut distribution_targets),
                    Some(Arc::clone(&active_mask)),
                )
            } else {
                let (rx, sig) = secondary_data.remove(0);
                (rx, sig, Vec::new(), None)
            };

            io_pool.spawn_on(
                i,
                ShardWorker {
                    data_rx,
                    ctrl_rx,
                    data_signal,
                    ctrl_notify: ctrl_notify.clone(),
                    mode,
                    lossy,
                    peers: FxHashMap::default(),
                    eq: FrameBuffer::one_shot(),
                    chunks: Vec::new(),
                    #[cfg(feature = "lz4")]
                    encoder: None,
                    distribution_targets: dist_targets,
                    active_mask: mask,
                }
                .run(),
            );
            endpoints.push(ShardEndpoint {
                ctrl_tx,
                ctrl_notify,
                load: 0,
            });
        }
        Arc::new(Self {
            state: Mutex::new(FanOutShardState {
                endpoints,
                eligible_peers: 0,
                active_limit: 0,
            }),
            active_mask,
            distributor: Mutex::new(distributor),
        })
    }

    fn desired_active_shards(eligible_peers: usize, max_shards: usize) -> usize {
        if eligible_peers == 0 || max_shards == 0 {
            return 0;
        }
        let worker_shards =
            eligible_peers.saturating_add(WORKER_SHARD_PEER_CAP - 1) / WORKER_SHARD_PEER_CAP;
        1usize.saturating_add(worker_shards).min(max_shards)
    }

    fn worker_shard_count(runtime_workers: usize) -> usize {
        runtime_workers.clamp(1, MAX_FAN_OUT_WORKER_SHARDS)
    }

    fn max_shards(state: &FanOutShardState) -> usize {
        state.endpoints.len() + 1
    }

    fn shard_load(state: &FanOutShardState, shard: usize) -> usize {
        state.endpoints[shard - 1].load
    }

    fn increment_shard_load(state: &mut FanOutShardState, shard: usize) {
        state.endpoints[shard - 1].load += 1;
    }

    fn decrement_shard_load(state: &mut FanOutShardState, shard: usize) {
        if let Some(endpoint) = state.endpoints.get_mut(shard - 1) {
            endpoint.load = endpoint.load.saturating_sub(1);
        }
    }

    fn least_loaded_shard(state: &FanOutShardState, active_limit: usize) -> usize {
        (1..active_limit)
            .min_by_key(|&shard| Self::shard_load(state, shard))
            .unwrap_or(1)
    }

    fn push_control(endpoint: &mut ShardEndpoint, cmd: ShardControl) {
        Self::push_control_spinning(endpoint, cmd);
    }

    /// Spin-loop until the shard worker's control ring has space.
    fn push_control_spinning(endpoint: &mut ShardEndpoint, mut cmd: ShardControl) {
        loop {
            match endpoint.ctrl_tx.push(cmd) {
                Ok(()) => {
                    endpoint.ctrl_tx.flush();
                    endpoint.ctrl_notify.notify_one();
                    return;
                }
                Err(returned) => {
                    cmd = returned;
                    endpoint.ctrl_tx.flush();
                    endpoint.ctrl_notify.notify_one();
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
        Self::increment_shard_load(&mut state, shard);
        self.active_mask
            .fetch_or(1 << (shard - 1), Ordering::Release);
        shard
    }

    fn add_worker_peer(&self, shard: usize, add: ShardPeerAdd) {
        if shard == 0 {
            return;
        }
        let mut state = self.state.lock().expect("fanout shards poisoned");
        if let Some(endpoint) = state.endpoints.get_mut(shard - 1) {
            Self::push_control(endpoint, ShardControl::AddPeer(add));
        }
    }

    fn send_to_shard(&self, shard: usize, cmd: ShardControl) {
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
        Self::decrement_shard_load(&mut state, shard);
        if shard > 0 {
            if state.endpoints.get(shard - 1).is_some_and(|e| e.load == 0) {
                self.active_mask
                    .fetch_and(!(1 << (shard - 1)), Ordering::Release);
            }
            if let Some(endpoint) = state.endpoints.get_mut(shard - 1) {
                Self::push_control(endpoint, ShardControl::RemovePeer { peer_id });
            }
        }
    }

    /// Push a raw message into shard 1's data ring. Shard worker 1
    /// distributes to secondary shards in batches.
    fn dispatch(&self, dispatch: &ShardDispatch) {
        let mut dist = self.distributor.lock().expect("distributor poisoned");
        if dist.data_tx.push(dispatch.clone()).is_ok() {
            dist.data_tx.flush();
            dist.data_signal.mark();
        }
    }

    fn shutdown(&self) {
        let mut state = self.state.lock().expect("fanout shards poisoned");
        for endpoint in &mut state.endpoints {
            Self::push_control(endpoint, ShardControl::Shutdown);
            endpoint.load = 0;
        }
        state.eligible_peers = 0;
        state.active_limit = 0;
        self.active_mask.store(0, Ordering::Release);
    }

    fn is_empty(&self) -> bool {
        let dist = self.distributor.lock().expect("distributor poisoned");
        let dist_empty = dist.data_tx.is_empty();
        drop(dist);
        dist_empty
            && self
                .state
                .lock()
                .expect("fanout shards poisoned")
                .endpoints
                .iter()
                .all(|endpoint| endpoint.ctrl_tx.is_empty())
    }
}

fn fan_out_is_lossy(options: &Options) -> bool {
    // TODO: Fan-out mute currently drops newest. Supporting DropOldest needs
    // per-peer oldest eviction in PeerTransmitSlot or a fan-out-specific queue.
    !options.xpub_nodrop
}

impl ShardWorker {
    async fn run(mut self) {
        let mut budget = DrainBudget::WORKER;
        loop {
            let mut touched: SmallVec<[u64; 32]> = SmallVec::new();
            let mut shutdown = false;

            // 1. ALL control commands, unconditionally.
            self.ctrl_rx.prefetch();
            while let Some(cmd) = self.ctrl_rx.pop() {
                if self.handle_control(cmd) {
                    shutdown = true;
                }
            }
            self.ctrl_rx.release();

            if shutdown {
                self.flush_touched(&mut touched);
                self.peers.clear();
                return;
            }

            // 2. Data up to budget. The distributor shard drains into
            //    a batch, distributes to secondary shards FIRST (so
            //    they can start encoding in parallel), then processes
            //    its own peers.
            budget.reset();
            let mut drained = false;
            let is_distributor = !self.distribution_targets.is_empty();
            if is_distributor {
                let mut batch: SmallVec<[ShardDispatch; 32]> = SmallVec::new();
                self.data_rx.prefetch();
                while let Some(dispatch) = self.data_rx.pop() {
                    drained = true;
                    if !budget.account(dispatch.msg.byte_len()) {
                        batch.push(dispatch);
                        break;
                    }
                    batch.push(dispatch);
                }
                self.data_rx.release();

                if !batch.is_empty() {
                    self.distribute_batch(&batch);
                    for dispatch in &batch {
                        self.dispatch(dispatch, &mut touched).await;
                    }
                }
            } else {
                self.data_rx.prefetch();
                while let Some(dispatch) = self.data_rx.pop() {
                    drained = true;
                    let msg_bytes = dispatch.msg.byte_len();
                    self.dispatch(&dispatch, &mut touched).await;
                    if !budget.account(msg_bytes) {
                        break;
                    }
                }
                self.data_rx.release();
            }

            self.flush_touched(&mut touched);
            if drained {
                self.data_signal.clear();
                self.data_signal.rearm_if_nonempty(self.data_rx.is_empty());
                tokio::task::yield_now().await;
                continue;
            }
            tokio::select! {
                () = self.ctrl_notify.notified() => {}
                () = self.data_signal.notified() => {}
            }
        }
    }

    fn distribute_batch(&mut self, batch: &[ShardDispatch]) {
        let Some(ref active_mask) = self.active_mask else {
            return;
        };
        let mask = active_mask.load(Ordering::Acquire);
        for (i, target) in self.distribution_targets.iter_mut().enumerate() {
            // targets[0] = shard 2 (bit 1), targets[1] = shard 3 (bit 2), ...
            if mask & (1 << (i + 1)) == 0 {
                continue;
            }
            for dispatch in batch {
                let _ = target.data_tx.push(dispatch.clone());
            }
            target.data_tx.flush();
            target.data_signal.mark();
        }
    }

    fn handle_control(&mut self, cmd: ShardControl) -> bool {
        match cmd {
            ShardControl::AddPeer(add) => {
                self.peers.insert(
                    add.peer_id,
                    ShardPeer {
                        subscriptions: SubscriptionSet::new(),
                        groups: FxHashSet::default(),
                        any_groups: add.any_groups,
                        dict_shipped: add.slot.fanout_dict_shipped(),
                        slot: add.slot,
                    },
                );
            }
            ShardControl::RemovePeer { peer_id } => {
                self.peers.remove(&peer_id);
            }
            ShardControl::Subscribe { peer_id, prefix } => {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.subscriptions.add(&prefix);
                }
            }
            ShardControl::Cancel { peer_id, prefix } => {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.subscriptions.remove(&prefix);
                }
            }
            ShardControl::Join { peer_id, group } => {
                if let Some(peer) = self.peers.get_mut(&peer_id)
                    && let Ok(s) = std::str::from_utf8(&group)
                {
                    peer.groups.insert(s.to_string());
                }
            }
            ShardControl::Leave { peer_id, group } => {
                if let Some(peer) = self.peers.get_mut(&peer_id)
                    && let Ok(s) = std::str::from_utf8(&group)
                {
                    peer.groups.remove(s);
                }
            }
            ShardControl::SetCompression { options, dict } => {
                self.init_encoder(&options, dict.as_ref());
            }
            ShardControl::Shutdown => return true,
        }
        false
    }

    #[allow(clippy::unused_self)]
    fn init_encoder(
        &mut self,
        #[allow(unused)] options: &Options,
        #[allow(unused)] dict: Option<&Bytes>,
    ) {
        #[cfg(feature = "lz4")]
        if self.encoder.is_none() {
            use omq_proto::endpoint::{Endpoint, Host};
            let mut opts = options.clone().compression_auto_train(false);
            if let Some(d) = dict {
                opts = opts.compression_dict(d.clone());
            }
            let dummy = Endpoint::Lz4Tcp {
                host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
                port: 0,
            };
            if let Some((enc, _dec)) = MessageEncoder::for_endpoint(&dummy, &opts) {
                self.encoder = Some(enc);
            }
        }
    }

    async fn dispatch(&mut self, dispatch: &ShardDispatch, touched: &mut SmallVec<[u64; 32]>) {
        let mut peer_ids = SmallVec::<[u64; 32]>::new();
        for (&peer_id, peer) in &self.peers {
            if peer.slot.fanout_active() && shard_peer_matches(self.mode, peer, dispatch) {
                peer_ids.push(peer_id);
            }
        }
        if peer_ids.is_empty() {
            return;
        }

        // Compress if an encoder is active (lz4+tcp:// peers present).
        #[cfg(feature = "lz4")]
        let wire_messages: SmallVec<[Message; 2]> = if let Some(ref mut enc) = self.encoder {
            match enc.encode(&dispatch.msg) {
                Ok(transformed) => transformed,
                Err(_) => return,
            }
        } else {
            smallvec::smallvec![dispatch.msg.clone()]
        };
        #[cfg(not(feature = "lz4"))]
        let wire_messages: SmallVec<[Message; 2]> = smallvec::smallvec![dispatch.msg.clone()];

        // Handle dict shipment: the first transformed message may be a dict.
        #[cfg(feature = "lz4")]
        let (dict_msg, payload_start) = {
            if wire_messages
                .first()
                .is_some_and(omq_proto::proto::transform::lz4::is_dict_shipment)
            {
                (Some(&wire_messages[0]), 1)
            } else {
                (None, 0)
            }
        };
        #[cfg(not(feature = "lz4"))]
        let (dict_msg, payload_start): (Option<&Message>, usize) = (None, 0);

        let target_count = peer_ids.len();

        // Encode dict and payload into per-peer slots. Both go through
        // push_frame_to_peer (direct FrameBuffer push) to preserve ordering.
        let has_dict = dict_msg.is_some();
        if let Some(dict) = dict_msg {
            let frame = build_fan_out_frame(
                &mut self.eq,
                dict,
                &mut self.chunks,
                target_count,
                FAN_OUT_TOTAL_COPY_BUDGET,
            );
            for &peer_id in &peer_ids {
                if let Some(peer) = self.peers.get_mut(&peer_id)
                    && !peer.dict_shipped
                    && Self::push_frame_to_peer(self.lossy, peer_id, peer, &frame, touched).await
                {
                    peer.dict_shipped = true;
                    peer.slot.mark_fanout_dict_shipped();
                }
            }
            clear_fan_out_frame(&mut self.eq, &mut self.chunks);
        }

        // Encode the payload messages into the FrameBuffer.
        for wire_msg in &wire_messages[payload_start..] {
            encode_fan_out_message(
                &mut self.eq,
                wire_msg,
                target_count,
                FAN_OUT_TOTAL_COPY_BUDGET,
            );
        }

        let encoded = finish_fan_out_frame(
            &mut self.eq,
            &mut self.chunks,
            target_count,
            FAN_OUT_TOTAL_COPY_BUDGET,
        );

        for peer_id in peer_ids {
            if let Some(peer) = self.peers.get_mut(&peer_id) {
                if has_dict && !peer.dict_shipped {
                    continue;
                }
                Self::push_frame_to_peer(self.lossy, peer_id, peer, &encoded, touched).await;
            }
        }

        clear_fan_out_frame(&mut self.eq, &mut self.chunks);
    }

    async fn push_frame_to_peer(
        lossy: bool,
        peer_id: u64,
        peer: &mut ShardPeer,
        frame: &FanOutFrame<'_>,
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        loop {
            let result = match frame {
                FanOutFrame::Arena(raw) => peer.slot.try_push_pre_framed_no_signal(raw),
                FanOutFrame::Chunks(chunks) => peer.slot.try_push_encoded(chunks),
            };
            match result {
                TryFrameResult::Ok => {
                    touched.push(peer_id);
                    return true;
                }
                TryFrameResult::Dead => return false,
                TryFrameResult::Full if lossy => {
                    peer.slot.deactivate_fanout();
                    return false;
                }
                TryFrameResult::Full => {
                    peer.slot.signal_encoded();
                    let notified = peer.slot.space_available.notified();
                    tokio::select! {
                        biased;
                        () = notified => {}
                        () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
                    }
                }
                TryFrameResult::Ineligible if lossy => return false,
                TryFrameResult::Ineligible => {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            }
        }
    }

    fn flush_touched(&self, touched: &mut SmallVec<[u64; 32]>) {
        touched.sort_unstable();
        touched.dedup();
        for &peer_id in touched.iter() {
            if let Some(peer) = self.peers.get(&peer_id) {
                peer.slot.signal_encoded();
            }
        }
    }
}

fn shard_peer_matches(mode: FanOutMode, peer: &ShardPeer, dispatch: &ShardDispatch) -> bool {
    match (mode, dispatch.group.as_deref()) {
        (FanOutMode::Group, Some(grp)) => peer.any_groups || peer.groups.contains(grp),
        (FanOutMode::SubscriptionPrefix, _) => peer.subscriptions.matches(&dispatch.topic),
        (FanOutMode::Group, None) => false,
    }
}

fn deactivate_fanout_target(
    inner: &Arc<Mutex<FanOutInner>>,
    generation: &Arc<AtomicU64>,
    target: &PeerOutbound,
) {
    let PeerOutbound::Wire { slot, .. } = target else {
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

impl Submitter {
    pub(crate) fn shutdown(&self) {
        if let Some(ref shards) = self.shards {
            shards.shutdown();
        }
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

    fn deactivate_target(&self, target: &PeerOutbound) {
        deactivate_fanout_target(&self.inner, &self.generation, target);
    }

    fn dispatch_raw(
        &self,
        shards: &FanOutShards,
        msg: &Message,
        group: Option<String>,
    ) -> Result<()> {
        // Fast path: no fallback peers, push raw message directly to shards.
        if self.fallback_peer_count.load(Ordering::Relaxed) == 0 {
            let sharded_count = self.sharded_peer_count.load(Ordering::Acquire);
            if sharded_count > 0 {
                let dispatch = ShardDispatch {
                    msg: msg.clone(),
                    topic: first_frame_bytes(msg),
                    group,
                };
                shards.dispatch(&dispatch);
            }
            return Ok(());
        }

        // Slow path: fallback peers exist, acquire inner mutex.
        let g = self.inner.lock().expect("fanout inner poisoned");
        let fallback_targets: SmallVec<[PeerOutbound; 8]> = g
            .peers
            .values()
            .filter(|p| p.shard.unwrap_or(0) == 0)
            .filter(|p| p.fanout_active)
            .filter(|p| match (self.mode, group.as_deref()) {
                (FanOutMode::Group, Some(grp)) => p.any_groups || p.groups.contains(grp),
                (FanOutMode::SubscriptionPrefix, _) => {
                    p.subscriptions.matches(&first_frame_bytes(msg))
                }
                (FanOutMode::Group, None) => false,
            })
            .map(|p| p.target.clone())
            .collect();
        let has_sharded = g
            .peers
            .values()
            .any(|p| p.shard.is_some_and(|shard| shard > 0));
        drop(g);

        if !fallback_targets.is_empty() {
            let mut deactivate = |target: &PeerOutbound| self.deactivate_target(target);
            dispatch_to_targets(&fallback_targets, msg, true, &mut deactivate)?;
        }

        if has_sharded {
            let dispatch = ShardDispatch {
                msg: msg.clone(),
                topic: first_frame_bytes(msg),
                group,
            };
            shards.dispatch(&dispatch);
        }
        Ok(())
    }

    async fn maybe_yield(&self, target_count: usize, msg_bytes: usize) {
        let interval = yield_interval(target_count, msg_bytes);
        if self.send_count.fetch_add(1, Ordering::Relaxed) % interval == interval - 1 {
            tokio::task::yield_now().await;
        }
    }

    pub(crate) fn try_send(
        &self,
        msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        let (forwarded, group) = self
            .prepare(msg)
            .map_err(omq_proto::error::TrySendError::Error)?;

        if let Some(ref shards) = self.shards {
            self.dispatch_raw(shards, &forwarded, group)
                .map_err(omq_proto::error::TrySendError::Error)?;
        }
        Ok(())
    }

    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        let (forwarded, group) = self.prepare(msg)?;
        let msg_bytes = forwarded.byte_len();

        #[cfg(feature = "lz4")]
        self.feed_dict_training(&forwarded);

        if let Some(ref shards) = self.shards {
            self.dispatch_raw(shards, &forwarded, group)?;
            let target_count = self.sharded_peer_count.load(Ordering::Relaxed)
                + self.fallback_peer_count.load(Ordering::Relaxed);
            self.maybe_yield(target_count, msg_bytes).await;
        }
        Ok(())
    }

    #[cfg(feature = "lz4")]
    fn feed_dict_training(&self, msg: &Message) {
        let mut guard = self.dict_training.lock().expect("dict_training poisoned");
        let Some(training) = guard.as_mut() else {
            return;
        };
        let mut idx = 0;
        while let Some(part) = msg.part_bytes(idx) {
            training.trainer.add_sample(&part);
            idx += 1;
        }
        training.msgs_left = training.msgs_left.saturating_sub(1);
        if training.msgs_left > 0 {
            return;
        }
        let training = guard.take().unwrap();
        let dict_bytes = training.trainer.train();
        if dict_bytes.is_empty() {
            return;
        }
        let dict = Bytes::from(dict_bytes);
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        g.compression_dict = Some(dict.clone());
        let options = g.options.clone();
        drop(g);
        if let Some(ref shards) = self.shards {
            let mut state = shards.state.lock().expect("fanout shards poisoned");
            for endpoint in &mut state.endpoints {
                FanOutShards::push_control(
                    endpoint,
                    ShardControl::SetCompression {
                        options: Box::new(options.clone()),
                        dict: Some(dict.clone()),
                    },
                );
            }
        }
    }
}

/// Fan-out send strategy.
#[derive(Debug)]
pub(crate) struct FanOutSend {
    shards: Option<Arc<FanOutShards>>,
    sharded_peer_count: Arc<AtomicUsize>,
    fallback_peer_count: Arc<AtomicUsize>,
    inner: Arc<Mutex<FanOutInner>>,
    generation: Arc<AtomicU64>,
    mode: FanOutMode,
    xpub_nodrop: bool,
    lossy: bool,
    #[cfg(feature = "lz4")]
    dict_training: Arc<Mutex<Option<DictTraining>>>,
}

struct FanOutInner {
    peers: FxHashMap<u64, FanOutPeer>,
    all_subscribe_all: bool,
    all_targets: SmallVec<[PeerOutbound; 8]>,
    active_all_targets: SmallVec<[PeerOutbound; 8]>,
    has_compression: bool,
    compression_dict: Option<Bytes>,
    options: Options,
}

impl std::fmt::Debug for FanOutInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanOutInner")
            .field("peers", &self.peers.len())
            .field("all_subscribe_all", &self.all_subscribe_all)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct FanOutPeer {
    subscriptions: SubscriptionSet,
    groups: FxHashSet<String>,
    any_groups: bool,
    target: PeerOutbound,
    shard: Option<usize>,
    fanout_active: bool,
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
    pub(crate) fn new(
        options: &Options,
        mode: FanOutMode,
        io_pool: &crate::context::IoPoolHandle,
    ) -> Self {
        let shards = Some(FanOutShards::spawn(options, mode, io_pool));
        let inner = Arc::new(Mutex::new(FanOutInner {
            peers: FxHashMap::default(),
            all_subscribe_all: false,
            all_targets: SmallVec::new(),
            active_all_targets: SmallVec::new(),
            has_compression: false,
            compression_dict: options.compression_dict.clone(),
            options: options.clone(),
        }));
        let generation = Arc::new(AtomicU64::new(0));
        let sharded_peer_count = Arc::new(AtomicUsize::new(0));
        let fallback_peer_count = Arc::new(AtomicUsize::new(0));
        Self {
            shards,
            sharded_peer_count,
            fallback_peer_count,
            inner,
            generation,
            mode,
            xpub_nodrop: options.xpub_nodrop,
            lossy: fan_out_is_lossy(options),
            #[cfg(feature = "lz4")]
            dict_training: Arc::new(Mutex::new(
                if options.compression_auto_train && options.compression_dict.is_none() {
                    Some(DictTraining {
                        trainer: omq_proto::proto::transform::lz4::DictTrainer::new(
                            options.compression_dict_capacity.unwrap_or(2048),
                        ),
                        msgs_left: 100,
                    })
                } else {
                    None
                },
            )),
        }
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            shards: self.shards.clone(),
            sharded_peer_count: self.sharded_peer_count.clone(),
            fallback_peer_count: self.fallback_peer_count.clone(),
            inner: self.inner.clone(),
            generation: self.generation.clone(),
            mode: self.mode,
            send_count: Arc::new(AtomicU32::new(0)),
            xpub_nodrop: self.xpub_nodrop,
            lossy: self.lossy,
            #[cfg(feature = "lz4")]
            dict_training: self.dict_training.clone(),
        }
    }

    pub(crate) fn connection_added(&mut self, peer_id: u64, handle: PeerDriverHandle) {
        self.add_peer(peer_id, handle, false);
    }

    pub(crate) fn connection_added_any_groups(&mut self, peer_id: u64, handle: PeerDriverHandle) {
        self.add_peer(peer_id, handle, true);
    }

    #[expect(clippy::needless_pass_by_value)]
    fn add_peer(&mut self, peer_id: u64, handle: PeerDriverHandle, any_groups: bool) {
        let has_transform = handle
            .transmit_slot
            .as_ref()
            .is_some_and(|s| s.has_transform);
        let target = PeerOutbound::from_handle(&handle);

        #[cfg(feature = "ws")]
        let target_is_ws = target.is_ws();
        #[cfg(not(feature = "ws"))]
        let target_is_ws = false;

        let shard_eligible = !target_is_ws
            && matches!(target, PeerOutbound::Wire { .. })
            && handle.transmit_slot.is_some();

        let shard = if !shard_eligible {
            None
        } else if let (Some(shards), PeerOutbound::Wire { slot, .. }) =
            (self.shards.as_ref(), &target)
        {
            let shard = shards.assign_peer();
            shards.add_worker_peer(
                shard,
                ShardPeerAdd {
                    peer_id,
                    slot: slot.clone(),
                    any_groups,
                },
            );
            let mut g = self.inner.lock().expect("fanout inner poisoned");
            if has_transform {
                g.has_compression = true;
            }
            if g.has_compression {
                let options = g.options.clone();
                let dict = g.compression_dict.clone();
                shards.send_to_shard(
                    shard,
                    ShardControl::SetCompression {
                        options: Box::new(options),
                        dict,
                    },
                );
            } else {
                drop(g);
            }
            Some(shard)
        } else {
            None
        };

        if shard.is_none() {
            self.fallback_peer_count.fetch_add(1, Ordering::Release);
        }

        if let PeerOutbound::Wire { slot, .. } = &target {
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
            if peer.shard.is_none() {
                self.fallback_peer_count.fetch_sub(1, Ordering::Release);
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
                    ShardControl::Subscribe {
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
                    ShardControl::Cancel {
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
                    ShardControl::Join {
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
                    ShardControl::Leave {
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
        drop(g);
        self.sharded_peer_count.store(0, Ordering::Release);
        self.bump_generation();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let shards_empty = self.shards.as_ref().is_none_or(|shards| shards.is_empty());
        let g = self.inner.lock().expect("fanout inner poisoned");
        shards_empty && g.peers.values().all(|p| p.target.is_empty())
    }
}

fn dispatch_to_targets(
    targets: &[PeerOutbound],
    msg: &Message,
    drop_on_full: bool,
    deactivate: &mut impl FnMut(&PeerOutbound),
) -> Result<DispatchOutcome> {
    use std::cell::RefCell;

    match targets.len() {
        0 => Ok(DispatchOutcome::default()),
        1 => match targets[0].try_encode(msg) {
            TryFrameResult::Full => {
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
            if targets.iter().any(PeerOutbound::is_ws) {
                let mut outcome = DispatchOutcome::default();
                for t in targets {
                    if t.try_encode(msg) == TryFrameResult::Full {
                        if drop_on_full {
                            deactivate(t);
                        }
                        outcome.push_full(t);
                    }
                }
                return Ok(outcome);
            }

            thread_local! {
                static ARENA: RefCell<FrameBuffer> = RefCell::new(
                    FrameBuffer::one_shot(),
                );
                static CHUNKS: RefCell<Vec<Bytes>> = const { RefCell::new(Vec::new()) };
            }
            ARENA.with(|cell| {
                let eq = &mut *cell.borrow_mut();
                encode_fan_out_message(eq, msg, targets.len(), FAN_OUT_TOTAL_COPY_BUDGET);
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

fn push_to_peers(
    targets: &[PeerOutbound],
    msg: &Message,
    drop_on_full: bool,
    deactivate: &mut impl FnMut(&PeerOutbound),
    outcome: &mut DispatchOutcome,
    push_wire: impl Fn(&PeerTransmitSlot) -> TryFrameResult,
) {
    for t in targets {
        match t {
            PeerOutbound::Wire { slot, .. } => {
                if drop_on_full && !slot.fanout_active() {
                    outcome.push_full(t);
                    continue;
                }
                if push_wire(slot) == TryFrameResult::Full {
                    if drop_on_full {
                        deactivate(t);
                    }
                    outcome.push_full(t);
                }
            }
            PeerOutbound::Inbox(tx) => {
                if tx
                    .try_send(PeerDriverCommand::SendMessage(msg.clone()))
                    .is_err()
                {
                    outcome.push_full(t);
                }
            }
        }
    }
}

fn dispatch_encoded(
    eq: &mut FrameBuffer,
    targets: &[PeerOutbound],
    msg: &Message,
    chunks: &mut Vec<Bytes>,
    drop_on_full: bool,
    deactivate: &mut impl FnMut(&PeerOutbound),
) -> DispatchOutcome {
    let mut outcome = DispatchOutcome::default();
    match finish_fan_out_frame(eq, chunks, targets.len(), FAN_OUT_TOTAL_COPY_BUDGET) {
        FanOutFrame::Arena(raw) => {
            push_to_peers(
                targets,
                msg,
                drop_on_full,
                deactivate,
                &mut outcome,
                |slot| slot.try_push_pre_framed_no_signal(raw),
            );
            for t in targets {
                if let PeerOutbound::Wire { slot, .. } = t {
                    slot.signal_encoded();
                }
            }
        }
        FanOutFrame::Chunks(encoded) => {
            push_to_peers(
                targets,
                msg,
                drop_on_full,
                deactivate,
                &mut outcome,
                |slot| slot.try_push_encoded(encoded),
            );
        }
    }
    clear_fan_out_frame(eq, chunks);
    outcome
}

fn first_frame_bytes(msg: &Message) -> Bytes {
    msg.part_bytes(0).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU8;
    use std::sync::{Arc, Mutex};

    use tokio::sync::Notify;

    use super::{
        DistributorEndpoint, FanOutShardState, FanOutShards, MAX_FAN_OUT_WORKER_SHARDS,
        ShardDispatch, ShardEndpoint, WORKER_SHARD_PEER_CAP, yield_interval,
    };

    const TEST_MAX_LOGICAL_SHARDS: usize = MAX_FAN_OUT_WORKER_SHARDS + 1;
    const TEST_SINGLE_LOGICAL_SHARD: usize = 1;
    const TEST_LOW_WORKER_COUNT: usize = 2;
    const TEST_HIGH_WORKER_COUNT: usize = 64;
    const TEST_WIDE_PEER_COUNT: usize = 32;

    #[test]
    fn yield_interval_scales_with_message_size() {
        assert_eq!(yield_interval(1, 16), 512);
        assert_eq!(yield_interval(1, 256), 512);
        assert_eq!(yield_interval(1, 1024), 256);
        assert_eq!(yield_interval(1, 4096), 64);
        assert_eq!(yield_interval(1, 16 * 1024), 16);
    }

    #[test]
    fn yield_interval_yields_every_send_without_active_targets() {
        assert_eq!(yield_interval(0, 16), 1);
        assert_eq!(yield_interval(0, 16 * 1024), 1);
    }

    #[test]
    fn desired_active_shards_ramps_monotonically() {
        assert_eq!(
            FanOutShards::desired_active_shards(0, TEST_MAX_LOGICAL_SHARDS),
            0
        );
        assert_eq!(
            FanOutShards::desired_active_shards(1, TEST_MAX_LOGICAL_SHARDS),
            2
        );
        assert_eq!(
            FanOutShards::desired_active_shards(WORKER_SHARD_PEER_CAP, TEST_MAX_LOGICAL_SHARDS),
            2
        );
        assert_eq!(
            FanOutShards::desired_active_shards(WORKER_SHARD_PEER_CAP + 1, TEST_MAX_LOGICAL_SHARDS),
            3
        );
        assert_eq!(
            FanOutShards::desired_active_shards(TEST_WIDE_PEER_COUNT, TEST_MAX_LOGICAL_SHARDS),
            9
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
            2
        );
        assert_eq!(
            FanOutShards::desired_active_shards(WORKER_SHARD_PEER_CAP + 1, TEST_LOW_WORKER_COUNT),
            TEST_LOW_WORKER_COUNT
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
    fn assign_peer_distributes_to_workers() {
        let shards = FanOutShards {
            state: std::sync::Mutex::new(FanOutShardState {
                endpoints: test_endpoints(TEST_LOW_WORKER_COUNT),
                eligible_peers: 0,
                active_limit: 0,
            }),
            active_mask: Arc::new(AtomicU8::new(0)),
            distributor: test_distributor(),
        };

        let assigned: Vec<_> = (0..WORKER_SHARD_PEER_CAP)
            .map(|_| shards.assign_peer())
            .collect();
        assert!(
            assigned.iter().all(|&s| s > 0),
            "all peers go to worker shards"
        );
    }

    #[test]
    fn assign_peer_round_robins_across_workers() {
        let shards = FanOutShards {
            state: std::sync::Mutex::new(FanOutShardState {
                endpoints: test_endpoints(WORKER_SHARD_PEER_CAP),
                eligible_peers: 0,
                active_limit: 0,
            }),
            active_mask: Arc::new(AtomicU8::new(0)),
            distributor: test_distributor(),
        };

        let mut loads = [0usize; WORKER_SHARD_PEER_CAP + 1];
        for _ in 0..TEST_WIDE_PEER_COUNT {
            loads[shards.assign_peer()] += 1;
        }

        assert_eq!(loads[0], 0, "shard 0 gets no peers");
        assert!(
            loads[1..].iter().all(|&l| l > 0),
            "all worker shards get peers"
        );
    }

    fn test_endpoints(count: usize) -> Vec<ShardEndpoint> {
        (0..count).map(|_| test_endpoint()).collect()
    }

    fn test_endpoint() -> ShardEndpoint {
        let (ctrl_tx, _ctrl_rx) = yring::spsc(4);
        ShardEndpoint {
            ctrl_tx,
            ctrl_notify: Arc::new(Notify::new()),
            load: 0,
        }
    }

    fn test_distributor() -> Mutex<DistributorEndpoint> {
        let (data_tx, _data_rx) = yring::spsc::<ShardDispatch>(4);
        Mutex::new(DistributorEndpoint {
            data_tx,
            data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
        })
    }
}

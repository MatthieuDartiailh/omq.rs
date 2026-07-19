//! Fan-out send: raw message distribution into IO-lane workers.
//!
//! PUB and XPUB filter by SUBSCRIBE-driven prefix set; RADIO filters
//! by joined groups. The caller pushes raw `Message` values into each
//! active lane's yring. Each lane worker encodes (and optionally
//! compresses) locally, then pushes into its peers' `PeerTransmitSlot`
//! rings.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
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
const LANE_CTRL_RING_CAP: usize = 64;

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

#[derive(Debug)]
enum LaneControl {
    AddPeer(LanePeerAdd),
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
struct LanePeerAdd {
    peer_id: u64,
    slot: Arc<PeerTransmitSlot>,
    any_groups: bool,
}

#[derive(Clone, Debug)]
struct LaneDispatch {
    msg: Message,
    topic: Bytes,
    group: Option<String>,
}

#[derive(Debug)]
struct LanePeer {
    subscriptions: SubscriptionSet,
    groups: FxHashSet<String>,
    any_groups: bool,
    slot: Arc<PeerTransmitSlot>,
    dict_shipped: bool,
}

struct LaneEndpoint {
    ctrl_tx: yring::Producer<LaneControl>,
    ctrl_notify: Arc<Notify>,
    peer_count: usize,
}

struct LaneDistributor {
    data_tx: yring::Producer<LaneDispatch>,
    data_signal: Arc<DataSignal>,
}

struct LaneDistributionTarget {
    lane: usize,
    data_tx: yring::Producer<LaneDispatch>,
    data_signal: Arc<DataSignal>,
}

struct FanOutLaneState {
    endpoints: Vec<LaneEndpoint>,
}

struct FanOutLanes {
    state: Mutex<FanOutLaneState>,
    active_flags: Arc<Vec<AtomicBool>>,
    distributor: Mutex<LaneDistributor>,
}

impl std::fmt::Debug for LaneDistributor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneDistributor").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for LaneDistributionTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneDistributionTarget")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for FanOutLanes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().expect("fanout lanes poisoned");
        f.debug_struct("FanOutLanes")
            .field("lanes", &state.endpoints.len())
            .field(
                "active_lanes",
                &self
                    .active_flags
                    .iter()
                    .filter(|flag| flag.load(Ordering::Relaxed))
                    .count(),
            )
            .field(
                "lane_peer_counts",
                &state
                    .endpoints
                    .iter()
                    .map(|lane| lane.peer_count)
                    .collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

struct LaneWorker {
    data_rx: yring::Consumer<LaneDispatch>,
    ctrl_rx: yring::Consumer<LaneControl>,
    data_signal: Arc<DataSignal>,
    ctrl_notify: Arc<Notify>,
    mode: FanOutMode,
    lossy: bool,
    peers: FxHashMap<u64, LanePeer>,
    eq: FrameBuffer,
    chunks: Vec<Bytes>,
    #[cfg(feature = "lz4")]
    encoder: Option<MessageEncoder>,
    distribution_targets: Vec<LaneDistributionTarget>,
    active_flags: Option<Arc<Vec<AtomicBool>>>,
}

impl std::fmt::Debug for LaneWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneWorker")
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
    lanes: Arc<FanOutLanes>,
    lane_peer_count: Arc<AtomicUsize>,
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
            lanes: self.lanes.clone(),
            lane_peer_count: self.lane_peer_count.clone(),
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

impl FanOutLanes {
    fn spawn(
        options: &Options,
        mode: FanOutMode,
        io_pool: &crate::context::IoPoolHandle,
    ) -> Arc<Self> {
        let pipe_cap = options.send_hwm.max(16) as usize;
        let lane_count = io_pool.thread_count().max(1);
        let lossy = fan_out_is_lossy(options);
        let active_flags = Arc::new(
            (0..lane_count)
                .map(|_| AtomicBool::new(false))
                .collect::<Vec<_>>(),
        );

        // Create all channels up front.
        let mut data_channels: Vec<_> = (0..lane_count)
            .map(|_| {
                let (tx, rx) = yring::spsc(pipe_cap);
                let sig = Arc::new(DataSignal::new());
                (tx, rx, sig)
            })
            .collect();
        let mut ctrl_channels: Vec<_> = (0..lane_count)
            .map(|_| {
                let (tx, rx) = yring::spsc(LANE_CTRL_RING_CAP);
                let notify = Arc::new(Notify::new());
                (tx, rx, notify)
            })
            .collect();

        // Lane 0 receives user sends. It copies each batch to active
        // secondary lanes first, then processes its own peers.
        let (dist_tx, dist_rx, dist_signal) = data_channels.remove(0);
        let distributor = LaneDistributor {
            data_tx: dist_tx,
            data_signal: Arc::clone(&dist_signal),
        };

        let mut distribution_targets: Vec<LaneDistributionTarget> =
            Vec::with_capacity(data_channels.len());
        let mut secondary_data: Vec<(yring::Consumer<LaneDispatch>, Arc<DataSignal>)> =
            Vec::with_capacity(data_channels.len());
        for (i, (tx, rx, sig)) in data_channels.into_iter().enumerate() {
            distribution_targets.push(LaneDistributionTarget {
                lane: i + 1,
                data_tx: tx,
                data_signal: Arc::clone(&sig),
            });
            secondary_data.push((rx, sig));
        }

        // Build endpoints (ctrl only) and spawn workers.
        let mut dist_rx = Some(dist_rx);
        let mut dist_signal = Some(dist_signal);
        let mut endpoints = Vec::with_capacity(lane_count);
        for i in 0..lane_count {
            let (ctrl_tx, ctrl_rx, ctrl_notify) = ctrl_channels.remove(0);

            let (data_rx, data_signal, dist_targets, flags) = if i == 0 {
                (
                    dist_rx.take().expect("lane 0 data_rx"),
                    dist_signal.take().expect("lane 0 data_signal"),
                    std::mem::take(&mut distribution_targets),
                    Some(Arc::clone(&active_flags)),
                )
            } else {
                let (rx, sig) = secondary_data.remove(0);
                (rx, sig, Vec::new(), None)
            };

            io_pool.spawn_on(
                i,
                LaneWorker {
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
                    active_flags: flags,
                }
                .run(),
            );
            endpoints.push(LaneEndpoint {
                ctrl_tx,
                ctrl_notify,
                peer_count: 0,
            });
        }
        Arc::new(Self {
            state: Mutex::new(FanOutLaneState { endpoints }),
            active_flags,
            distributor: Mutex::new(distributor),
        })
    }

    fn lane_count(&self) -> usize {
        self.active_flags.len()
    }

    fn normalize_lane(&self, lane: usize) -> usize {
        let lane_count = self.lane_count();
        debug_assert!(lane < lane_count, "fanout lane out of range");
        lane.min(lane_count.saturating_sub(1))
    }

    fn push_control(endpoint: &mut LaneEndpoint, cmd: LaneControl) {
        Self::push_control_spinning(endpoint, cmd);
    }

    /// Spin-loop until the lane worker's control ring has space.
    fn push_control_spinning(endpoint: &mut LaneEndpoint, mut cmd: LaneControl) {
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

    fn add_lane_peer(&self, lane: usize, add: LanePeerAdd) -> usize {
        let lane = self.normalize_lane(lane);
        let mut state = self.state.lock().expect("fanout lanes poisoned");
        if let Some(endpoint) = state.endpoints.get_mut(lane) {
            endpoint.peer_count += 1;
            self.active_flags[lane].store(true, Ordering::Release);
            Self::push_control(endpoint, LaneControl::AddPeer(add));
        }
        lane
    }

    fn send_to_lane(&self, lane: usize, cmd: LaneControl) {
        let lane = self.normalize_lane(lane);
        let mut state = self.state.lock().expect("fanout lanes poisoned");
        if let Some(endpoint) = state.endpoints.get_mut(lane) {
            Self::push_control(endpoint, cmd);
        }
    }

    fn remove_peer(&self, lane: usize, peer_id: u64) {
        let lane = self.normalize_lane(lane);
        let mut state = self.state.lock().expect("fanout lanes poisoned");
        if let Some(endpoint) = state.endpoints.get_mut(lane) {
            endpoint.peer_count = endpoint.peer_count.saturating_sub(1);
            if endpoint.peer_count == 0 {
                self.active_flags[lane].store(false, Ordering::Release);
            }
            Self::push_control(endpoint, LaneControl::RemovePeer { peer_id });
        }
    }

    /// Push a raw message into lane 0's data ring. Lane 0 distributes
    /// to secondary lanes in batches.
    fn dispatch(&self, dispatch: &LaneDispatch) {
        let mut dist = self.distributor.lock().expect("distributor poisoned");
        if dist.data_tx.push(dispatch.clone()).is_ok() {
            dist.data_tx.flush();
            dist.data_signal.mark();
        }
    }

    fn shutdown(&self) {
        let mut state = self.state.lock().expect("fanout lanes poisoned");
        for endpoint in &mut state.endpoints {
            Self::push_control(endpoint, LaneControl::Shutdown);
            endpoint.peer_count = 0;
        }
        for flag in self.active_flags.iter() {
            flag.store(false, Ordering::Release);
        }
    }

    fn is_empty(&self) -> bool {
        let dist = self.distributor.lock().expect("distributor poisoned");
        let dist_empty = dist.data_tx.is_empty();
        drop(dist);
        dist_empty
            && self
                .state
                .lock()
                .expect("fanout lanes poisoned")
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

impl LaneWorker {
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

            // 2. Data up to budget. Lane 0 drains into
            //    a batch, distributes to secondary lanes FIRST (so
            //    they can start encoding in parallel), then processes
            //    its own peers.
            budget.reset();
            let mut drained = false;
            let is_distributor = !self.distribution_targets.is_empty();
            if is_distributor {
                let mut batch: SmallVec<[LaneDispatch; 32]> = SmallVec::new();
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

    fn distribute_batch(&mut self, batch: &[LaneDispatch]) {
        let Some(ref active_flags) = self.active_flags else {
            return;
        };
        for target in &mut self.distribution_targets {
            if !active_flags[target.lane].load(Ordering::Acquire) {
                continue;
            }
            for dispatch in batch {
                let _ = target.data_tx.push(dispatch.clone());
            }
            target.data_tx.flush();
            target.data_signal.mark();
        }
    }

    fn handle_control(&mut self, cmd: LaneControl) -> bool {
        match cmd {
            LaneControl::AddPeer(add) => {
                self.peers.insert(
                    add.peer_id,
                    LanePeer {
                        subscriptions: SubscriptionSet::new(),
                        groups: FxHashSet::default(),
                        any_groups: add.any_groups,
                        dict_shipped: add.slot.fanout_dict_shipped(),
                        slot: add.slot,
                    },
                );
            }
            LaneControl::RemovePeer { peer_id } => {
                self.peers.remove(&peer_id);
            }
            LaneControl::Subscribe { peer_id, prefix } => {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.subscriptions.add(&prefix);
                }
            }
            LaneControl::Cancel { peer_id, prefix } => {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.subscriptions.remove(&prefix);
                }
            }
            LaneControl::Join { peer_id, group } => {
                if let Some(peer) = self.peers.get_mut(&peer_id)
                    && let Ok(s) = std::str::from_utf8(&group)
                {
                    peer.groups.insert(s.to_string());
                }
            }
            LaneControl::Leave { peer_id, group } => {
                if let Some(peer) = self.peers.get_mut(&peer_id)
                    && let Ok(s) = std::str::from_utf8(&group)
                {
                    peer.groups.remove(s);
                }
            }
            LaneControl::SetCompression { options, dict } => {
                self.init_encoder(&options, dict.as_ref());
            }
            LaneControl::Shutdown => return true,
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
        if self.encoder.is_none() || dict.is_some() {
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

    async fn dispatch(&mut self, dispatch: &LaneDispatch, touched: &mut SmallVec<[u64; 32]>) {
        let mut peer_ids = SmallVec::<[u64; 32]>::new();
        for (&peer_id, peer) in &self.peers {
            if peer.slot.fanout_active() && lane_peer_matches(self.mode, peer, dispatch) {
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
        peer: &mut LanePeer,
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
                    let notified = peer.slot.space_available.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    peer.slot.signal_encoded();
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
                        TryFrameResult::Full => notified.await,
                        TryFrameResult::Ineligible => {
                            unreachable!("pre-framed fanout push cannot be ineligible")
                        }
                    }
                }
                TryFrameResult::Ineligible if lossy => return false,
                TryFrameResult::Ineligible => {
                    unreachable!("pre-framed fanout push cannot be ineligible")
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

fn lane_peer_matches(mode: FanOutMode, peer: &LanePeer, dispatch: &LaneDispatch) -> bool {
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
        self.lanes.shutdown();
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
        lanes: &FanOutLanes,
        msg: &Message,
        group: Option<String>,
    ) -> Result<()> {
        // Fast path: no fallback peers, push raw message directly to lanes.
        if self.fallback_peer_count.load(Ordering::Relaxed) == 0 {
            let lane_count = self.lane_peer_count.load(Ordering::Acquire);
            if lane_count > 0 {
                let dispatch = LaneDispatch {
                    msg: msg.clone(),
                    topic: first_frame_bytes(msg),
                    group,
                };
                lanes.dispatch(&dispatch);
            }
            return Ok(());
        }

        // Slow path: fallback peers exist, acquire inner mutex.
        let g = self.inner.lock().expect("fanout inner poisoned");
        let fallback_targets: SmallVec<[PeerOutbound; 8]> = g
            .peers
            .values()
            .filter(|p| p.lane.is_none())
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
        let has_lane_peers = g.peers.values().any(|p| p.lane.is_some());
        drop(g);

        if !fallback_targets.is_empty() {
            let mut deactivate = |target: &PeerOutbound| self.deactivate_target(target);
            dispatch_to_targets(&fallback_targets, msg, true, &mut deactivate)?;
        }

        if has_lane_peers {
            let dispatch = LaneDispatch {
                msg: msg.clone(),
                topic: first_frame_bytes(msg),
                group,
            };
            lanes.dispatch(&dispatch);
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

        self.dispatch_raw(&self.lanes, &forwarded, group)
            .map_err(omq_proto::error::TrySendError::Error)?;
        Ok(())
    }

    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        let (forwarded, group) = self.prepare(msg)?;
        let msg_bytes = forwarded.byte_len();

        #[cfg(feature = "lz4")]
        self.feed_dict_training(&forwarded);

        self.dispatch_raw(&self.lanes, &forwarded, group)?;
        let target_count = self.lane_peer_count.load(Ordering::Relaxed)
            + self.fallback_peer_count.load(Ordering::Relaxed);
        self.maybe_yield(target_count, msg_bytes).await;
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
        let mut state = self.lanes.state.lock().expect("fanout lanes poisoned");
        for endpoint in &mut state.endpoints {
            FanOutLanes::push_control(
                endpoint,
                LaneControl::SetCompression {
                    options: Box::new(options.clone()),
                    dict: Some(dict.clone()),
                },
            );
        }
    }
}

/// Fan-out send strategy.
#[derive(Debug)]
pub(crate) struct FanOutSend {
    lanes: Arc<FanOutLanes>,
    lane_peer_count: Arc<AtomicUsize>,
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
    has_compression: bool,
    compression_dict: Option<Bytes>,
    options: Options,
}

impl std::fmt::Debug for FanOutInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanOutInner")
            .field("peers", &self.peers.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct FanOutPeer {
    subscriptions: SubscriptionSet,
    groups: FxHashSet<String>,
    any_groups: bool,
    target: PeerOutbound,
    lane: Option<usize>,
    fanout_active: bool,
}

impl FanOutInner {
    fn deactivate_fanout_peer(&mut self, peer_id: u64) -> bool {
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return false;
        };
        if !peer.fanout_active {
            return false;
        }
        peer.fanout_active = false;
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
        true
    }
}

impl FanOutSend {
    pub(crate) fn new(
        options: &Options,
        mode: FanOutMode,
        io_pool: &crate::context::IoPoolHandle,
    ) -> Self {
        let lanes = FanOutLanes::spawn(options, mode, io_pool);
        let inner = Arc::new(Mutex::new(FanOutInner {
            peers: FxHashMap::default(),
            has_compression: false,
            compression_dict: options.compression_dict.clone(),
            options: options.clone(),
        }));
        let generation = Arc::new(AtomicU64::new(0));
        let lane_peer_count = Arc::new(AtomicUsize::new(0));
        let fallback_peer_count = Arc::new(AtomicUsize::new(0));
        Self {
            lanes,
            lane_peer_count,
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
            lanes: self.lanes.clone(),
            lane_peer_count: self.lane_peer_count.clone(),
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

    pub(crate) fn connection_added(
        &mut self,
        peer_id: u64,
        handle: PeerDriverHandle,
        io_thread: usize,
    ) {
        self.add_peer(peer_id, handle, false, io_thread);
    }

    pub(crate) fn connection_added_any_groups(
        &mut self,
        peer_id: u64,
        handle: PeerDriverHandle,
        io_thread: usize,
    ) {
        self.add_peer(peer_id, handle, true, io_thread);
    }

    #[expect(clippy::needless_pass_by_value)]
    fn add_peer(
        &mut self,
        peer_id: u64,
        handle: PeerDriverHandle,
        any_groups: bool,
        io_thread: usize,
    ) {
        let has_transform = handle
            .transmit_slot
            .as_ref()
            .is_some_and(|s| s.has_transform);
        let target = PeerOutbound::from_handle(&handle);

        #[cfg(feature = "ws")]
        let target_is_ws = target.is_ws();
        #[cfg(not(feature = "ws"))]
        let target_is_ws = false;

        let lane_eligible = !target_is_ws
            && matches!(target, PeerOutbound::Wire { .. })
            && handle.transmit_slot.is_some();

        let lane = if !lane_eligible {
            None
        } else if let PeerOutbound::Wire { slot, .. } = &target {
            let lane = self.lanes.add_lane_peer(
                io_thread,
                LanePeerAdd {
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
                self.lanes.send_to_lane(
                    lane,
                    LaneControl::SetCompression {
                        options: Box::new(options),
                        dict,
                    },
                );
            } else {
                drop(g);
            }
            Some(lane)
        } else {
            None
        };

        if lane.is_none() {
            self.fallback_peer_count.fetch_add(1, Ordering::Release);
        } else {
            self.lane_peer_count.fetch_add(1, Ordering::Release);
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
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.insert(
            peer_id,
            FanOutPeer {
                subscriptions: SubscriptionSet::new(),
                groups: FxHashSet::default(),
                any_groups,
                target,
                lane,
                fanout_active: true,
            },
        );
        self.bump_generation();
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(peer) = g.peers.remove(&peer_id) {
            if peer.lane.is_some() {
                self.lane_peer_count.fetch_sub(1, Ordering::Release);
            } else {
                self.fallback_peer_count.fetch_sub(1, Ordering::Release);
            }
            drop(g);
            if let Some(lane) = peer.lane {
                self.lanes.remove_peer(lane, peer_id);
            }
            self.bump_generation();
        }
    }

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn peer_subscribe(&self, peer_id: u64, prefix: Bytes) {
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        if let Some(p) = g.peers.get_mut(&peer_id) {
            p.subscriptions.add(&prefix);
            let lane = p.lane;
            drop(g);
            if let Some(lane) = lane {
                self.lanes.send_to_lane(
                    lane,
                    LaneControl::Subscribe {
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
            let lane = p.lane;
            drop(g);
            if let Some(lane) = lane {
                self.lanes.send_to_lane(
                    lane,
                    LaneControl::Cancel {
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
            let lane = p.lane;
            drop(g);
            if let Some(lane) = lane {
                self.lanes.send_to_lane(
                    lane,
                    LaneControl::Join {
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
            let lane = p.lane;
            drop(g);
            if let Some(lane) = lane {
                self.lanes.send_to_lane(
                    lane,
                    LaneControl::Leave {
                        peer_id,
                        group: Bytes::copy_from_slice(group),
                    },
                );
            }
            self.bump_generation();
        }
    }

    pub(crate) fn shutdown(&self) {
        self.lanes.shutdown();
        let mut g = self.inner.lock().expect("fanout inner poisoned");
        g.peers.clear();
        drop(g);
        self.lane_peer_count.store(0, Ordering::Release);
        self.fallback_peer_count.store(0, Ordering::Release);
        self.bump_generation();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let lanes_empty = self.lanes.is_empty();
        let g = self.inner.lock().expect("fanout inner poisoned");
        lanes_empty && g.peers.values().all(|p| p.target.is_empty())
    }
}

fn dispatch_to_targets(
    targets: &[PeerOutbound],
    msg: &Message,
    drop_on_full: bool,
    deactivate: &mut impl FnMut(&PeerOutbound),
) -> Result<()> {
    use std::cell::RefCell;

    match targets.len() {
        0 => Ok(()),
        1 => match targets[0].try_encode(msg) {
            TryFrameResult::Full => {
                if drop_on_full {
                    deactivate(&targets[0]);
                }
                Ok(())
            }
            _ => Ok(()),
        },
        _ => {
            #[cfg(feature = "ws")]
            if targets.iter().any(PeerOutbound::is_ws) {
                for t in targets {
                    if t.try_encode(msg) == TryFrameResult::Full && drop_on_full {
                        deactivate(t);
                    }
                }
                return Ok(());
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
                    dispatch_encoded(
                        eq,
                        targets,
                        msg,
                        &mut drain.borrow_mut(),
                        drop_on_full,
                        deactivate,
                    );
                    Ok(())
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
    push_wire: impl Fn(&PeerTransmitSlot) -> TryFrameResult,
) {
    for t in targets {
        match t {
            PeerOutbound::Wire { slot, .. } => {
                if drop_on_full && !slot.fanout_active() {
                    continue;
                }
                if push_wire(slot) == TryFrameResult::Full && drop_on_full {
                    deactivate(t);
                }
            }
            PeerOutbound::Inbox(tx) => {
                let _ = tx.try_send(PeerDriverCommand::SendMessage(msg.clone()));
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
) {
    match finish_fan_out_frame(eq, chunks, targets.len(), FAN_OUT_TOTAL_COPY_BUDGET) {
        FanOutFrame::Arena(raw) => {
            push_to_peers(targets, msg, drop_on_full, deactivate, |slot| {
                slot.try_push_pre_framed_no_signal(raw)
            });
            for t in targets {
                if let PeerOutbound::Wire { slot, .. } = t {
                    slot.signal_encoded();
                }
            }
        }
        FanOutFrame::Chunks(encoded) => {
            push_to_peers(targets, msg, drop_on_full, deactivate, |slot| {
                slot.try_push_encoded(encoded)
            });
        }
    }
    clear_fan_out_frame(eq, chunks);
}

fn first_frame_bytes(msg: &Message) -> Bytes {
    msg.part_bytes(0).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    use tokio::sync::Notify;

    use super::{
        FanOutLaneState, FanOutLanes, LaneDispatch, LaneDistributor, LaneEndpoint, LanePeerAdd,
        yield_interval,
    };

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
    fn add_peer_uses_supplied_lane_and_marks_it_active() {
        let lanes = test_lanes(3);

        let assigned = lanes.add_lane_peer(
            2,
            LanePeerAdd {
                peer_id: 7,
                slot: test_slot(7),
                any_groups: false,
            },
        );

        assert_eq!(assigned, 2);
        assert!(!lanes.active_flags[0].load(std::sync::atomic::Ordering::Acquire));
        assert!(!lanes.active_flags[1].load(std::sync::atomic::Ordering::Acquire));
        assert!(lanes.active_flags[2].load(std::sync::atomic::Ordering::Acquire));
        let state = lanes.state.lock().expect("lanes poisoned");
        assert_eq!(state.endpoints[2].peer_count, 1);
    }

    #[test]
    fn remove_peer_clears_lane_when_last_peer_leaves() {
        let lanes = test_lanes(2);
        lanes.add_lane_peer(
            1,
            LanePeerAdd {
                peer_id: 11,
                slot: test_slot(11),
                any_groups: false,
            },
        );
        lanes.add_lane_peer(
            1,
            LanePeerAdd {
                peer_id: 12,
                slot: test_slot(12),
                any_groups: false,
            },
        );

        lanes.remove_peer(1, 11);
        assert!(lanes.active_flags[1].load(std::sync::atomic::Ordering::Acquire));
        lanes.remove_peer(1, 12);
        assert!(!lanes.active_flags[1].load(std::sync::atomic::Ordering::Acquire));
        let state = lanes.state.lock().expect("lanes poisoned");
        assert_eq!(state.endpoints[1].peer_count, 0);
    }

    fn test_lanes(count: usize) -> FanOutLanes {
        FanOutLanes {
            state: std::sync::Mutex::new(FanOutLaneState {
                endpoints: test_endpoints(count),
            }),
            active_flags: Arc::new(
                (0..count)
                    .map(|_| AtomicBool::new(false))
                    .collect::<Vec<_>>(),
            ),
            distributor: test_distributor(),
        }
    }

    fn test_endpoints(count: usize) -> Vec<LaneEndpoint> {
        (0..count).map(|_| test_endpoint()).collect()
    }

    fn test_endpoint() -> LaneEndpoint {
        let (ctrl_tx, _ctrl_rx) = yring::spsc(4);
        LaneEndpoint {
            ctrl_tx,
            ctrl_notify: Arc::new(Notify::new()),
            peer_count: 0,
        }
    }

    fn test_slot(peer_id: u64) -> Arc<crate::engine::transmit_slot::PeerTransmitSlot> {
        crate::engine::transmit_slot::PeerTransmitSlot::new(
            peer_id,
            false,
            None,
            4096,
            16 * 1024,
            64 * 1024,
            16,
            #[cfg(feature = "ws")]
            false,
            #[cfg(feature = "ws")]
            false,
        )
    }

    fn test_distributor() -> Mutex<LaneDistributor> {
        let (data_tx, _data_rx) = yring::spsc::<LaneDispatch>(4);
        Mutex::new(LaneDistributor {
            data_tx,
            data_signal: Arc::new(crate::engine::signal::DataSignal::new()),
        })
    }
}

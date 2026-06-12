//! Per-socket internal state shared via `Arc<SocketInner>`.
//!
//! All mutation lives behind `RwLock` / `Mutex` / atomic - the public
//! [`Socket`] handle is `Clone + Send + Sync` and clones share one
//! `SocketInner`. Wire drivers, dial supervisors, accept loops, and
//! the recv path all hold the same `Arc` and coordinate through these
//! fields.
//!
//! [`Socket`]: super::Socket

use std::cell::{Cell, UnsafeCell};
use std::collections::VecDeque;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

use slab::Slab;

/// A `VecDeque` wrapper that provides `&mut` access without a `Mutex`.
///
/// Sound only when all access is confined to a single thread -- true for
/// compio's cooperative single-threaded runtime. The `Sync` impl is the
/// unsafe contract: callers must never access from multiple threads.
pub(super) struct RecvCache(UnsafeCell<VecDeque<Message>>);

// SAFETY: compio is single-threaded. All recv_cache access happens on
// the runtime thread that created the socket. No concurrent access.
unsafe impl Sync for RecvCache {}

impl RecvCache {
    fn new() -> Self {
        Self(UnsafeCell::new(VecDeque::new()))
    }

    /// Borrow the inner deque mutably. Caller must be on the owning
    /// runtime thread (always true in compio's cooperative model).
    #[inline]
    #[expect(clippy::mut_from_ref)]
    pub(super) fn get(&self) -> &mut VecDeque<Message> {
        unsafe { &mut *self.0.get() }
    }
}
use bytes::{Bytes, BytesMut};
use event_listener::Event;

use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_proto::subscription::SubscriptionSet;
use omq_proto::type_state::TypeState;

use crate::monitor::{DisconnectReason, MonitorEvent, MonitorPublisher};
use crate::transport::driver::DriverCommand;
use crate::transport::inproc::InprocPeerSnapshot;

use omq_proto::inproc::InboundFrame;

/// Compio-specific wrapper: pairs an `InboundFrame` with a
/// `connection_id` so identity-aware socket types (ROUTER, SERVER,
/// REP, STREAM, PEER) can look up the sender without cloning
/// identity bytes into every frame.
#[derive(Debug)]
pub(crate) struct TaggedFrame {
    pub(crate) connection_id: u64,
    pub(crate) frame: InboundFrame,
}
use crate::transport::peer_io::{CancellableRecvStream, SharedPeerIo};

pub(super) use super::direct_io::DirectIoState;
pub(super) use super::peer::{DirectIoHandle, PeerOut, PeerSlot, WirePeerHandle};

/// `Send + Sync`-asserting wrapper around a multi-shot recv stream.
///
/// compio's multi-shot recv stream contains [`compio::driver::SharedFd`]
/// which is `Rc`-based for single-thread efficiency, hence `!Send`. We
/// store such a stream behind an [`async_lock::Mutex`] inside the
/// `Arc<DirectIoState>` so the driver task and the user's
/// `try_direct_recv` task can share it.
///
/// # Safety
///
/// `compio::runtime::Runtime` is thread-pinned: every `compio::runtime::spawn`
/// places the future on the local runtime's thread, and `Socket` is documented
/// as pinned to its creating runtime. Cross-runtime sends in omq-compio go
/// through `flume` mpsc, never moving the `Arc<DirectIoState>` itself.
/// Therefore the inner `Rc` refcount is only ever touched on a single thread,
/// and asserting `Send + Sync` on this wrapper does not introduce a data race
/// in any usage pattern omq-compio supports.
/// Two-state recv mode for a wire peer.
///
/// After a large-frame one-shot, stay in `OneShot` so consecutive large
/// messages pay **zero** cancel+rearm cost. Transition back to `MultiShot`
/// when a small frame arrives.
pub(crate) enum RecvStreamState {
    MultiShot(CancellableRecvStream),
    OneShot,
}

/// Cancel-safe guard for the accumulation buffer. Takes the `BytesMut`
/// out of `DirectIoState::pending_acc` for the duration of an async
/// `read_until`. If the future is dropped (timeout, cancel), the
/// destructor saves the partial buffer back so the next consumer can
/// resume.
pub(crate) struct AccRestore<'a> {
    pub(crate) state: &'a DirectIoState,
    pub(crate) buf: Option<BytesMut>,
}

impl Drop for AccRestore<'_> {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.take() {
            *self.state.pending_acc.lock().expect("pending_acc") = Some(buf);
        }
    }
}

pub(crate) struct LocalStream(pub(crate) async_lock::Mutex<Option<RecvStreamState>>);

// SAFETY: see `LocalStream` doc comment. `CancelToken` is `Rc`-based
// (single-threaded) for the same reason as `SharedFd`, and is only
// touched from the runtime thread. The unsafe `Send + Sync` cover
// both fields of the inner `CancellableRecvStream`.
unsafe impl Send for LocalStream {}
// SAFETY: see `LocalStream` doc comment.
unsafe impl Sync for LocalStream {}

impl LocalStream {
    /// Replace the stream with a freshly-built one. Used to re-arm
    /// after the kernel terminates a multi-shot recv SQE - typically
    /// `ENOBUFS` under sustained delivery on a small `BUF_RING` pool.
    /// The previous stream's lingering op is cancelled when its slot
    /// drops.
    #[expect(clippy::await_holding_lock)]
    pub(crate) async fn rearm(&self, peer_io: &SharedPeerIo) -> std::io::Result<()> {
        let io = peer_io.lock().expect("peer_io");
        if !io.reader.supports_multishot() {
            drop(io);
            *self.0.lock().await = Some(RecvStreamState::OneShot);
            return Ok(());
        }
        let new_stream = io.reader.build_recv_stream();
        drop(io);
        *self.0.lock().await = Some(RecvStreamState::MultiShot(new_stream));
        Ok(())
    }
}

/// Cached single-peer route. Saves `out_peers.read()` + `direct_io.read()`
/// on every send/recv when the peer set hasn't changed.
pub(super) struct CachedPeerRoute {
    pub(super) generation: u64,
    pub(super) out: PeerOut,
    pub(super) direct: Option<Arc<DirectIoState>>,
    pub(super) slot_idx: usize,
}

/// Per-peer SPSC send pipe. Wraps a blume Producer + the remote
/// socket's recv Event. Drop flushes remaining items and wakes
/// the remote recv loop.
pub(super) struct InprocSendPipe {
    pub(super) producer: yring::Producer<Message>,
    pub(super) notify: Arc<Event>,
    /// Set by the remote recv loop when it parks in select.
    /// Cleared when it wakes. Producers skip notify when false.
    pub(super) parked: Arc<AtomicBool>,
    /// Notified by the consumer after `release()` frees ring slots.
    /// The producer listens on this when the ring is full instead of
    /// spinning.
    pub(super) space_event: Arc<Event>,
    /// True when the peer is on a different thread.
    pub(super) cross_thread: bool,
}

impl Drop for InprocSendPipe {
    fn drop(&mut self) {
        self.producer.flush();
        self.notify.notify(usize::MAX);
    }
}

/// Per-socket inproc recv state: per-peer consumers + fair-queue index.
pub(super) struct InprocRecvState {
    pub(super) consumers: Vec<yring::Consumer<Message>>,
    /// Notified after `release()` so the remote producer can wake up
    /// when the ring was full.
    pub(super) space_events: Vec<Arc<Event>>,
    pub(super) fq_index: usize,
}

/// Outbound peer table, routing indices, and round-robin state.
pub(super) struct PeerRouting {
    pub(super) peers: RwLock<Slab<PeerSlot>>,
    /// Bumped on every peer-set write. Lets send/recv skip lock
    /// acquisitions when the peer set is stable.
    pub(super) generation: AtomicU64,
    /// Total outbound peer count (atomically readable without locking
    /// `peers`).
    pub(super) peer_count: Arc<AtomicUsize>,
    /// Count of inproc outbound peers. When zero, multi-peer wire
    /// sends skip `select_peer` entirely.
    pub(super) inproc_count: AtomicUsize,
    /// Cached route for the common single-peer case. Invalidated
    /// when `generation` advances past the stored generation.
    pub(super) cached_route: Mutex<Option<CachedPeerRoute>>,
    /// Identity -> slot index for ROUTER outbound.
    pub(super) identity_to_slot: RwLock<FxHashMap<Bytes, usize>>,
    /// `connection_id` -> peer identity for the recv path.
    pub(super) conn_id_to_identity: RwLock<FxHashMap<u64, Bytes>>,
    /// Round-robin counter for peer selection.
    pub(super) rr_index: AtomicUsize,
    /// Dense list of live slab keys, rebuilt on peer add/remove.
    pub(super) peer_keys: RwLock<Vec<usize>>,
}

/// Inproc transport state: per-peer SPSC pipes, tagged-frame channel,
/// and the recv notification used by cross-thread inproc senders.
pub(super) struct InprocIo {
    /// Per-peer SPSC send pipes, indexed parallel to `routing.peers`.
    pub(super) send_pipes: UnsafeCell<Vec<Option<InprocSendPipe>>>,
    /// Per-peer SPSC recv consumers + fair-queue index.
    pub(super) recv: UnsafeCell<InprocRecvState>,
    /// Single shared recv notification. Remote senders notify this
    /// when `parked` is true.
    pub(super) recv_event: Arc<Event>,
    /// True when recv is parked in select (waiting for data).
    pub(super) parked: Arc<AtomicBool>,
    pub(super) in_tx: blume::Sender<TaggedFrame>,
    pub(super) in_rx: blume::Receiver<TaggedFrame>,
}

/// Cached `DirectIoState` handles for the wire send/recv fast paths.
/// `UnsafeCell` is sound because compio is single-threaded.
pub(super) struct DirectIoCache {
    /// Direct codec access for `try_recv`. Set on first successful
    /// direct recv; cleared on peer disconnect.
    pub(super) recv: UnsafeCell<Option<Arc<DirectIoState>>>,
    /// Cached `DirectIoState` + generation for the wire send fast path.
    pub(super) send: UnsafeCell<Option<(Arc<DirectIoState>, u64)>>,
}

/// PUB/SUB matching caches and subscription tables.
pub(super) struct PubSubState {
    /// Set by subscribe/cancel handlers and peer add/remove.
    pub(super) dirty: Arc<AtomicBool>,
    /// True when every outbound peer has `subscribe_all`.
    pub(super) all_match_all: Cell<bool>,
    /// True when all outbound peers are Wire (not Inproc).
    pub(super) all_wire: Cell<bool>,
    /// Cached `PeerOut`s for the subscribe-all fast path.
    pub(super) all_match_cache: UnsafeCell<SmallVec<[PeerOut; 8]>>,
    /// Cached `DirectIoState` handles for direct-write PUB fan-out.
    pub(super) direct_io_cache: UnsafeCell<SmallVec<[Arc<DirectIoState>; 8]>>,
    pub(super) subscriptions: RwLock<SubscriptionSet>,
    /// Active subscription prefixes (SUB / XSUB). Replayed to new peers.
    pub(super) our_subs: RwLock<Vec<Bytes>>,
}

/// Active listeners and dialers.
pub(super) struct EndpointRegistry {
    pub(super) listeners: RwLock<Vec<ListenerEntry>>,
    pub(super) dialers: RwLock<Vec<DialerEntry>>,
    pub(super) udp_dialers: RwLock<Vec<UdpDialerEntry>>,
}

pub(super) struct SocketInner {
    pub(super) socket_type: SocketType,
    pub(super) simple_recv: bool,
    pub(super) options: Options,
    /// Stable identity for inproc peer tagging.
    pub(super) inproc_identity: Bytes,
    pub(super) routing: PeerRouting,
    pub(super) inproc: InprocIo,
    pub(super) recv_cache: RecvCache,
    pub(super) direct_io: DirectIoCache,
    pub(super) on_peer_ready: Event,
    pub(super) pub_sub: PubSubState,
    /// REQ/REP envelope + alternation state.
    pub(super) type_state: Mutex<TypeState>,
    pub(super) monitor: MonitorPublisher,
    pub(super) next_connection_id: AtomicU64,
    /// Set by `close()` / `Drop` so install paths bail.
    pub(super) closed: AtomicBool,
    /// DISH local-filter group set (UDP RADIO/DISH only).
    pub(super) joined_groups: RwLock<FxHashSet<Bytes>>,
    pub(super) endpoints: EndpointRegistry,
    /// Shared send queue for round-robin socket types.
    pub(super) send_count: Cell<u32>,
    pub(super) shared_send_tx: RwLock<Option<super::shared_queue::SharedQueueSender>>,
    pub(super) shared_send_rx: Option<super::shared_queue::SharedQueueReceiver>,
}

/// Returns `true` for socket types that round-robin their outbound
/// messages across peers: a single shared bounded send queue, fed
/// by `Socket::send`, drained directly by each peer driver (for
/// wire transports) or by a per-peer pump (for inproc, which has
/// no driver).
pub(super) fn is_round_robin_send(t: SocketType) -> bool {
    matches!(
        t,
        SocketType::Push
            | SocketType::Dealer
            | SocketType::Req
            | SocketType::Pair
            | SocketType::Rep
            | SocketType::Client
            | SocketType::Scatter
            | SocketType::Channel
    )
}

impl Drop for SocketInner {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::SeqCst) {
            self.monitor.closed();
        }
    }
}

pub(super) struct ListenerEntry {
    pub(super) endpoint: Endpoint,
    /// Cancels on drop, taking the accept loop with it.
    pub(super) _task: compio::runtime::JoinHandle<()>,
}

pub(super) struct DialerEntry {
    pub(super) endpoint: Endpoint,
    pub(super) _task: compio::runtime::JoinHandle<()>,
}

pub(super) struct UdpDialerEntry {
    pub(super) endpoint: Endpoint,
    pub(super) sock: Arc<compio::net::UdpSocket>,
}

impl SocketInner {
    pub(super) fn new(socket_type: SocketType, options: Options) -> Arc<Self> {
        let (in_tx, in_rx) = match options.recv_hwm {
            None => blume::unbounded::<TaggedFrame>(),
            Some(hwm) => blume::bounded::<TaggedFrame>((hwm as usize).max(16)),
        };
        // Conflate forces cap-1 with drain-before-send semantics so that only
        // the latest message survives in the queue at any point in time.
        // None (unbounded_send) → unbounded shared queue.
        let send_cap: Option<usize> = if options.conflate {
            Some(1)
        } else {
            options.send_hwm.map(|h| (h as usize).max(16))
        };
        let out_peer_count = Arc::new(AtomicUsize::new(0));
        let (shared_send_tx, shared_send_rx) = if is_round_robin_send(socket_type) {
            let (tx, rx) = match send_cap {
                Some(cap) => super::shared_queue::bounded(cap, out_peer_count.clone()),
                None => super::shared_queue::unbounded(out_peer_count.clone()),
            };
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let _ = send_cap;
        let inproc_identity = if options.identity.is_empty() {
            static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
            let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut buf = Vec::with_capacity(9);
            buf.push(0u8); // libzmq auto-identity leading null
            buf.extend_from_slice(&n.to_be_bytes());
            Bytes::from(buf)
        } else {
            options.identity.clone()
        };
        #[expect(clippy::arc_with_non_send_sync)] // compio is single-threaded by design
        Arc::new(Self {
            socket_type,
            simple_recv: matches!(socket_type, SocketType::Pull | SocketType::Pair),
            inproc_identity,
            options,
            routing: PeerRouting {
                peers: RwLock::new(Slab::new()),
                generation: AtomicU64::new(0),
                peer_count: out_peer_count.clone(),
                inproc_count: AtomicUsize::new(0),
                cached_route: Mutex::new(None),
                identity_to_slot: RwLock::new(FxHashMap::default()),
                conn_id_to_identity: RwLock::new(FxHashMap::default()),
                rr_index: AtomicUsize::new(0),
                peer_keys: RwLock::new(Vec::new()),
            },
            inproc: InprocIo {
                send_pipes: UnsafeCell::new(Vec::new()),
                recv: UnsafeCell::new(InprocRecvState {
                    consumers: Vec::new(),
                    space_events: Vec::new(),
                    fq_index: 0,
                }),
                recv_event: Arc::new(Event::new()),
                parked: Arc::new(AtomicBool::new(false)),
                in_tx,
                in_rx,
            },
            recv_cache: RecvCache::new(),
            direct_io: DirectIoCache {
                recv: UnsafeCell::new(None),
                send: UnsafeCell::new(None),
            },
            on_peer_ready: Event::new(),
            pub_sub: PubSubState {
                dirty: Arc::new(AtomicBool::new(true)),
                all_match_all: Cell::new(false),
                all_wire: Cell::new(false),
                all_match_cache: UnsafeCell::new(SmallVec::new()),
                direct_io_cache: UnsafeCell::new(SmallVec::new()),
                subscriptions: RwLock::new(SubscriptionSet::new()),
                our_subs: RwLock::new(Vec::new()),
            },
            type_state: Mutex::new(TypeState::new()),
            monitor: MonitorPublisher::new(),
            next_connection_id: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            joined_groups: RwLock::new(FxHashSet::default()),
            endpoints: EndpointRegistry {
                listeners: RwLock::new(Vec::new()),
                dialers: RwLock::new(Vec::new()),
                udp_dialers: RwLock::new(Vec::new()),
            },
            send_count: Cell::new(0),
            shared_send_tx: RwLock::new(shared_send_tx),
            shared_send_rx,
        })
    }

    pub(super) fn snapshot(&self) -> InprocPeerSnapshot {
        InprocPeerSnapshot {
            socket_type: self.socket_type,
            identity: self.inproc_identity.clone(),
        }
    }

    /// Insert a [`PeerSlot`], resize `inproc_send_pipes`, optionally
    /// register identity (with handover), rebuild peer keys, and
    /// notify waiters. Returns the slab index.
    pub(super) fn insert_peer_slot(&self, slot: PeerSlot, identity: Option<&Bytes>) -> usize {
        let is_inproc = matches!(&slot.out, PeerOut::Inproc { .. });
        let conn_id = slot.connection_id;
        let idx = {
            let mut peers = self.routing.peers.write().expect("peers lock");
            let idx = peers.insert(slot);
            self.routing.generation.fetch_add(1, Ordering::Release);
            idx
        };
        self.routing.peer_count.fetch_add(1, Ordering::Release);
        if is_inproc {
            self.routing.inproc_count.fetch_add(1, Ordering::Release);
        }
        {
            let pipes = unsafe { &mut *self.inproc.send_pipes.get() };
            while pipes.len() <= idx {
                pipes.push(None);
            }
        }
        if let Some(id) = identity {
            if !id.is_empty()
                && let Some(old_idx) = self
                    .routing
                    .identity_to_slot
                    .write()
                    .expect("identity table")
                    .insert(id.clone(), idx)
                && old_idx != idx
            {
                self.evict_peer_for_handover(old_idx);
            }
            self.routing
                .conn_id_to_identity
                .write()
                .expect("conn_id_to_identity lock")
                .insert(conn_id, id.clone());
        }
        self.rebuild_peer_keys();
        self.pub_sub.dirty.store(true, Ordering::Release);
        self.on_peer_ready.notify(usize::MAX);
        idx
    }

    /// Rebuild `peer_keys` from the current `out_peers` (caller
    /// must hold no lock on either; this acquires both reads/writes
    /// internally).
    pub(super) fn rebuild_peer_keys(&self) {
        let peers = self.routing.peers.read().expect("peers lock");
        let keys: Vec<usize> = peers.iter().map(|(key, _)| key).collect();
        drop(peers);
        *self.routing.peer_keys.write().expect("peer_keys lock") = keys;
    }

    pub(super) fn evict_peer_for_handover(&self, slot_idx: usize) {
        let peers = self.routing.peers.read().expect("peers lock");
        let Some(slot) = peers.get(slot_idx) else {
            return;
        };
        match &slot.out {
            PeerOut::Wire(handle) => {
                let (dead_tx, _) = flume::bounded::<DriverCommand>(0);
                *handle.write().expect("wire peer handle lock") = dead_tx;
            }
            PeerOut::Inproc { .. } => {}
        }
        if let Some(dio) = &slot.direct_io {
            *dio.write().expect("direct_io handle lock") = None;
        }
        let info = slot.info.read().expect("info lock").clone();
        if let Some(peer) = info {
            self.monitor.publish(MonitorEvent::Disconnected {
                endpoint: slot.endpoint.clone(),
                peer,
                reason: DisconnectReason::Handover,
            });
        }
        // Suppress the driver's Disconnected on exit.
        *slot.info.write().expect("info lock") = None;
        self.routing.generation.fetch_add(1, Ordering::Release);
    }

    pub(super) fn release_slot(&self, slot_idx: usize) {
        {
            let mut peers = self.routing.peers.write().expect("peers lock");
            if !peers.contains(slot_idx) {
                return;
            }
            self.routing.peer_count.fetch_sub(1, Ordering::Release);
            if matches!(&peers[slot_idx].out, PeerOut::Inproc { .. }) {
                self.routing.inproc_count.fetch_sub(1, Ordering::Release);
            }
            peers.remove(slot_idx);
        }
        {
            let pipes = unsafe { &mut *self.inproc.send_pipes.get() };
            if let Some(entry) = pipes.get_mut(slot_idx) {
                *entry = None;
            }
        }
        {
            let mut table = self
                .routing
                .identity_to_slot
                .write()
                .expect("identity table");
            table.retain(|_, &mut v| v != slot_idx);
        }
        // conn_id_to_identity is NOT cleaned up here: frames
        // referencing this connection_id may still be queued in
        // the blume channel (e.g. STREAM disconnect notifications).
        // Stale entries are harmless since connection_ids are
        // monotonic and never reused.
        self.rebuild_peer_keys();
        self.pub_sub.dirty.store(true, Ordering::Release);
        self.routing.generation.fetch_add(1, Ordering::Release);
    }

    pub(super) fn recompute_pub_all_match_all(&self) {
        let peers = self.routing.peers.read().expect("peers lock");
        let all_match = !peers.is_empty()
            && peers.iter().all(|(_, slot)| {
                slot.peer_sub
                    .as_ref()
                    .is_some_and(|s| s.read().expect("peer_sub lock").is_subscribe_all())
            });
        let all_wire = !peers.is_empty()
            && peers
                .iter()
                .all(|(_, slot)| matches!(&slot.out, PeerOut::Wire(_)));
        self.pub_sub.all_match_all.set(all_match);
        self.pub_sub.all_wire.set(all_wire);
        if all_match {
            let cached: SmallVec<[PeerOut; 8]> = peers.iter().map(|(_, s)| s.out.clone()).collect();
            unsafe { *self.pub_sub.all_match_cache.get() = cached };
            if all_wire {
                let dio: SmallVec<[Arc<DirectIoState>; 8]> = peers
                    .iter()
                    .filter_map(|(_, slot)| {
                        slot.direct_io
                            .as_ref()
                            .and_then(|h| h.read().expect("direct_io handle lock").clone())
                    })
                    .collect();
                unsafe { *self.pub_sub.direct_io_cache.get() = dio };
            } else {
                unsafe { (*self.pub_sub.direct_io_cache.get()).clear() };
            }
        } else {
            unsafe { (*self.pub_sub.direct_io_cache.get()).clear() };
        }
        self.pub_sub.dirty.store(false, Ordering::Release);
    }
}

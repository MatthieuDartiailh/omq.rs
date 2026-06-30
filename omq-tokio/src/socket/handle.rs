//! Public `Socket` handle.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use futures::channel::oneshot;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bytes::Bytes;
use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_proto::type_state::TypeState;

use super::actor::{SocketCommand, SocketDriver, spawn_driver};
use super::monitor::{ConnectionStatus, MonitorPublisher, MonitorStream};
use crate::engine::wire_slot::{PeerWireSlot, TryEncodeResult};
use crate::routing::{SendStrategy, SendSubmitter};
use crate::transport::inproc::InprocSpsc;

pub(crate) type WireSlotHolder = Arc<Mutex<Option<Arc<PeerWireSlot>>>>;

/// Multi-peer round-robin wire slots, shared between the handle (reads)
/// and the actor (writes on peer add/remove). When more than one wire
/// peer is active on a round-robin socket and none are inproc, the actor
/// fills `slots` with every peer's [`PeerWireSlot`]; the handle picks the
/// next one per send via `cursor`, giving strict round-robin distribution
/// over the per-peer direct-encode fast path. Empty for single-peer /
/// inproc-mixed / identity-routed sockets (those fall back to the shared
/// work-stealing queue).
pub(crate) type RrSlots = Arc<RrSlotsInner>;

#[derive(Debug, Default)]
pub(crate) struct RrSlotsInner {
    pub(crate) slots: Mutex<Vec<Arc<PeerWireSlot>>>,
    pub(crate) cursor: std::sync::atomic::AtomicUsize,
}

pub use omq_proto::error::TrySendError;

/// Per-peer SPSC consumers Vec. Actor appends; recv fair-queues.
pub(crate) type SpscConsumers = Arc<RwLock<Vec<Arc<InprocSpsc>>>>;

/// Single-peer send fast path ring. Actor sets/clears.
pub(crate) type SpscSendRing = Arc<RwLock<Option<Arc<InprocSpsc>>>>;

/// Fast-path guard: true when `send_ring` contains `Some`. Lets TCP-only
/// sockets skip the `RwLock` read entirely.
pub(crate) type SpscSendRingActive = Arc<AtomicBool>;

/// Shared recv notification. All inproc producers notify this.
pub(crate) type SpscRecvNotify = Arc<tokio::sync::Notify>;

/// Notified by the actor when the consumers Vec changes. Wakes
/// any `recv()` that's blocked on the normal `async_channel` path.
pub(crate) type SpscActivated = Arc<tokio::sync::Notify>;

/// Grouped handles for per-peer SPSC inproc fast path. Shared between
/// the socket handle (recv side) and the actor (send-ring management,
/// consumer registration).
/// Bumped by the actor whenever the consumers Vec changes. Lets
/// `SpscAwareRecv` skip re-cloning the Vec when nothing changed.
pub(crate) type SpscConsumerGeneration = Arc<AtomicU64>;

/// Per-TCP-peer yring consumer entry. The driver pushes decoded messages
/// into its yring producer; the recv side drains the consumer here.
pub(crate) struct TcpYringConsumer {
    pub consumer: Mutex<yring::Consumer<Message>>,
    pub space: Arc<tokio::sync::Notify>,
    pub peer_id: u64,
}

impl std::fmt::Debug for TcpYringConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpYringConsumer")
            .field("peer_id", &self.peer_id)
            .finish_non_exhaustive()
    }
}

pub(crate) type TcpConsumers = Arc<RwLock<Vec<Arc<TcpYringConsumer>>>>;

#[derive(Debug, Clone)]
pub(crate) struct SpscHandles {
    pub consumers: SpscConsumers,
    pub consumer_generation: SpscConsumerGeneration,
    pub send_ring: SpscSendRing,
    pub send_ring_active: SpscSendRingActive,
    pub recv_notify: SpscRecvNotify,
    pub activated: SpscActivated,
    pub tcp_consumers: TcpConsumers,
}

/// Recv channel that integrates per-peer SPSC awareness. Fair-queues
/// across per-peer yring consumers (inproc + TCP), then falls back to
/// the `async_channel`.
#[derive(Debug)]
struct SpscAwareRecv {
    rx: async_channel::Receiver<Message>,
    /// Per-peer SPSC rings (one per eligible inproc peer). Actor appends.
    consumers: Arc<std::sync::RwLock<Vec<Arc<InprocSpsc>>>>,
    /// Per-TCP-peer yring consumers. Actor appends on handshake.
    tcp_consumers: TcpConsumers,
    /// Generation counter. Bumped by the actor on any consumer add/remove
    /// (inproc or TCP).
    consumer_generation: SpscConsumerGeneration,
    /// Shared recv notification. All drivers/senders notify this.
    recv_notify: Arc<tokio::sync::Notify>,
    /// Notified when consumers Vec changes (new peer added).
    activated: SpscActivated,
    /// Single-peer send fast path ring (None when sender has >1 peer).
    send_ring: Arc<std::sync::RwLock<Option<Arc<InprocSpsc>>>>,
    /// True when `send_ring` is `Some`. Lets the hot path skip the `RwLock`.
    send_ring_active: Arc<AtomicBool>,
    /// Batched messages drained from consumers (inproc + TCP).
    inproc_cache: std::sync::Mutex<std::collections::VecDeque<Message>>,
    /// Cached clone of consumers Vecs, refreshed when generation changes.
    cached_consumers: Mutex<CachedConsumers>,
}

struct CachedConsumers {
    generation: u64,
    inproc: Vec<Arc<InprocSpsc>>,
    tcp: Vec<Arc<TcpYringConsumer>>,
}

impl std::fmt::Debug for CachedConsumers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedConsumers")
            .field("generation", &self.generation)
            .field("inproc_count", &self.inproc.len())
            .field("tcp_count", &self.tcp.len())
            .finish()
    }
}

impl SpscAwareRecv {
    fn try_drain_consumers(&self) -> Option<Message> {
        if self.consumer_generation.load(Ordering::Relaxed) == 0 {
            return None;
        }
        {
            let mut cache = self.inproc_cache.lock().unwrap();
            if let Some(msg) = cache.pop_front() {
                return Some(msg);
            }
        }
        let current_gen = self.consumer_generation.load(Ordering::Acquire);
        let mut cached = self.cached_consumers.lock().unwrap();
        if cached.generation != current_gen {
            cached.inproc.clone_from(&self.consumers.read().unwrap());
            cached.tcp.clone_from(&self.tcp_consumers.read().unwrap());
            cached.generation = current_gen;
        }
        let inproc = cached.inproc.clone();
        let tcp = cached.tcp.clone();
        drop(cached);
        let mut cache = self.inproc_cache.lock().unwrap();
        let mut has_disconnected = false;
        for p in &inproc {
            if let Ok(mut consumer) = p.consumer.try_lock() {
                let got = consumer.prefetch();
                if got > 0 {
                    while let Some(msg) = consumer.pop() {
                        cache.push_back(msg);
                    }
                    consumer.release();
                } else if consumer.is_disconnected() {
                    has_disconnected = true;
                }
            }
        }
        for tc in &tcp {
            if let Ok(mut consumer) = tc.consumer.try_lock() {
                let got = consumer.prefetch();
                if got > 0 {
                    while let Some(msg) = consumer.pop() {
                        cache.push_back(msg);
                    }
                    consumer.release();
                    tc.space.notify_one();
                } else if consumer.is_disconnected() {
                    has_disconnected = true;
                }
            }
        }
        let result = cache.pop_front();
        drop(cache);
        if has_disconnected {
            self.consumers
                .write()
                .unwrap()
                .retain(|p| p.consumer.try_lock().map_or(true, |c| !c.is_disconnected()));
            self.tcp_consumers.write().unwrap().retain(|tc| {
                tc.consumer
                    .try_lock()
                    .map_or(true, |c| !c.is_disconnected())
            });
            self.consumer_generation.fetch_add(1, Ordering::Release);
            self.cached_consumers.lock().unwrap().generation = u64::MAX;
        }
        result
    }

    #[expect(clippy::needless_continue)]
    async fn recv(&self) -> Result<Message> {
        loop {
            if let Some(msg) = self.try_drain_consumers() {
                return Ok(msg);
            }

            if self.consumer_generation.load(Ordering::Acquire) > 0 {
                let notified = self.recv_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Some(msg) = self.try_drain_consumers() {
                    return Ok(msg);
                }
                tokio::select! {
                    biased;
                    () = notified => continue,
                    res = self.rx.recv() => {
                        return res.map_err(|_| Error::Closed);
                    }
                    () = self.activated.notified() => continue,
                }
            } else {
                let activated = self.activated.notified();
                tokio::pin!(activated);
                activated.as_mut().enable();
                tokio::select! {
                    biased;
                    res = self.rx.recv() => {
                        return res.map_err(|_| Error::Closed);
                    }
                    () = activated => continue,
                }
            }
        }
    }

    fn try_recv(&self) -> Result<Message> {
        if let Some(msg) = self.try_drain_consumers() {
            return Ok(msg);
        }
        self.rx.try_recv().map_err(|e| match e {
            async_channel::TryRecvError::Empty => Error::WouldBlock,
            async_channel::TryRecvError::Closed => Error::Closed,
        })
    }
}

/// A ZMQ-style socket. Clone-able; all clones talk to the same underlying
/// driver task. Close happens via the explicit [`Socket::close`] method
/// (the last handle drop cancels the driver without waiting for drain).
///
/// # Concurrency
///
/// The tokio backend is multi-threaded. `recv` reads from an
/// `async_channel` (MPMC), so concurrent `recv` calls from
/// different tasks are safe — each message is delivered to exactly
/// one caller. `send` goes through a per-socket `SendSubmitter`
/// that serializes internally, so concurrent `send` calls are also
/// safe. This is unlike the compio backend, where both `send` and
/// `recv` assume a single caller at a time.
#[derive(Clone, Debug)]
pub struct Socket {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    socket_type: SocketType,
    cmd_tx: mpsc::Sender<SocketCommand>,
    recv_rx: SpscAwareRecv,
    monitor: MonitorPublisher,
    root_cancel: CancellationToken,
    /// Pre-built submitter for socket types that bypass the actor on send.
    /// Cloned from the `SendStrategy` before the driver is spawned.
    send_submitter: SendSubmitter,
    /// Shared with the actor for REP `pre_send` / `post_recv`.
    type_state: Arc<Mutex<TypeState>>,
    /// REQ alternation flag. Avoids Mutex on the REQ hot path.
    /// Shared with the actor for `on_peer_disconnected` reset.
    req_awaiting_reply: Arc<AtomicBool>,
    /// Single-peer encode fast path. When set, the handle encodes
    /// ZMTP frames directly into the peer's `EncodedQueue` and the
    /// driver flushes them to the wire. Set by the actor when exactly
    /// one wire peer is active; cleared on multi-peer or disconnect.
    wire_slot: WireSlotHolder,
    /// Multi-peer round-robin wire slots (see [`RrSlots`]). Used by
    /// `send_via_wire_slot` to dispatch directly to one peer at a time
    /// instead of funneling every message through the shared queue.
    rr_slots: RrSlots,
    /// Cooperative yield counter. Every `SEND_YIELD_INTERVAL` successful
    /// synchronous sends, `send()` yields to the runtime so driver tasks
    /// on the same worker thread can drain and flush.
    send_ops: AtomicU32,
    last_bound_endpoint: RwLock<Option<Endpoint>>,
}

const SEND_YIELD_INTERVAL: u32 = 4096;

impl Socket {
    const STARVATION_THRESHOLD: u32 = 2;

    /// Create a new socket of the given type with the given options. Spawns
    /// the driver task on the current tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if `options` violates ZMTP protocol limits (identity > 255
    /// bytes, heartbeat TTL overflow, etc.) or if `conflate` is set on an
    /// incompatible socket type.
    pub fn new(socket_type: SocketType, options: Options) -> Self {
        Self::new_inner(socket_type, options, None)
    }

    /// Like [`Socket::new`], but installs a [`RecvSinkConfig`] that the
    /// actor will use for the first peer's driver (and refill on
    /// disconnect). Used by omq-libzmq to bypass the recv-pump relay.
    pub fn new_with_recv_sink_config(
        socket_type: SocketType,
        options: Options,
        config: Arc<crate::engine::RecvSinkConfig>,
    ) -> Self {
        Self::new_inner(socket_type, options, Some(config))
    }

    fn new_inner(
        socket_type: SocketType,
        options: Options,
        recv_sink_config: Option<Arc<crate::engine::RecvSinkConfig>>,
    ) -> Self {
        options
            .validate()
            .expect("Options::validate failed in Socket::new");
        assert!(
            !options.conflate || crate::routing::supports_conflate(socket_type),
            "Options::conflate(true) is not valid for socket type {socket_type:?} \
             - only PUSH/PULL/PUB/SUB/XPUB/XSUB/RADIO/DISH/DEALER/SCATTER/GATHER \
             carry queueable single-message-state semantics"
        );
        let cancel = CancellationToken::new();
        let (cmd_tx, cmd_rx) = mpsc::channel(options.send_hwm.unwrap_or(1024).max(16) as usize);
        let (recv_tx, recv_rx) =
            async_channel::bounded::<Message>(options.recv_hwm.unwrap_or(1024).max(16) as usize);
        let monitor = MonitorPublisher::new();
        let send_strategy = SendStrategy::for_socket_type(socket_type, &options);
        let send_submitter = send_strategy.submitter();
        let spsc = SpscHandles {
            consumers: Arc::new(RwLock::new(Vec::new())),
            consumer_generation: Arc::new(AtomicU64::new(0)),
            send_ring: Arc::new(RwLock::new(None)),
            send_ring_active: Arc::new(AtomicBool::new(false)),
            recv_notify: Arc::new(tokio::sync::Notify::new()),
            activated: Arc::new(tokio::sync::Notify::new()),
            tcp_consumers: Arc::new(RwLock::new(Vec::new())),
        };
        let type_state = Arc::new(Mutex::new(TypeState::new()));
        let req_awaiting_reply = Arc::new(AtomicBool::new(false));
        let wire_slot: WireSlotHolder = Arc::new(Mutex::new(None));
        let rr_slots: RrSlots = Arc::new(RrSlotsInner::default());
        let driver = SocketDriver::new(
            socket_type,
            options,
            cmd_rx,
            recv_tx,
            cancel.clone(),
            monitor.clone(),
            send_strategy,
            spsc.clone(),
            type_state.clone(),
            req_awaiting_reply.clone(),
            wire_slot.clone(),
            rr_slots.clone(),
            recv_sink_config,
        );
        spawn_driver(driver);
        Self {
            inner: Arc::new(Inner {
                socket_type,
                cmd_tx,
                recv_rx: SpscAwareRecv {
                    rx: recv_rx,
                    consumers: spsc.consumers,
                    tcp_consumers: spsc.tcp_consumers,
                    consumer_generation: spsc.consumer_generation,
                    recv_notify: spsc.recv_notify,
                    activated: spsc.activated,
                    send_ring: spsc.send_ring,
                    send_ring_active: spsc.send_ring_active,
                    inproc_cache: std::sync::Mutex::new(std::collections::VecDeque::new()),
                    cached_consumers: Mutex::new(CachedConsumers {
                        generation: u64::MAX,
                        inproc: Vec::new(),
                        tcp: Vec::new(),
                    }),
                },
                monitor,
                root_cancel: cancel,
                send_submitter,
                type_state,
                req_awaiting_reply,
                wire_slot,
                rr_slots,
                send_ops: AtomicU32::new(0),
                last_bound_endpoint: RwLock::new(None),
            }),
        }
    }

    /// Subscribe to connection-lifecycle events. Multiple monitors can be
    /// active simultaneously; each sees every event from subscription time
    /// onward. Cheap: backed by a broadcast channel.
    pub fn monitor(&self) -> MonitorStream {
        self.inner.monitor.subscribe()
    }

    /// The socket type.
    pub fn socket_type(&self) -> SocketType {
        self.inner.socket_type
    }

    /// Bind to an endpoint. Returns the resolved endpoint once the
    /// listener is active. For wildcard binds (`tcp://...:0`) the
    /// returned endpoint contains the actual port.
    pub async fn bind(&self, endpoint: Endpoint) -> Result<Endpoint> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Bind { endpoint, ack })
            .await
            .map_err(|_| Error::Closed)?;
        let resolved = rx.await.map_err(|_| Error::Closed)??;
        *self.inner.last_bound_endpoint.write().unwrap() = Some(resolved.clone());
        Ok(resolved)
    }

    /// Return the most recently bound endpoint, if any.
    pub fn last_bound_endpoint(&self) -> Option<Endpoint> {
        self.inner.last_bound_endpoint.read().unwrap().clone()
    }

    /// Queue a connect attempt. Returns immediately; the background reconnect
    /// loop handles retries per the configured `ReconnectPolicy`.
    pub async fn connect(&self, endpoint: Endpoint) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Connect { endpoint, ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Send a message. Awaits until the message has been accepted by a ready
    /// peer's driver inbox (not waited-on-wire).
    pub async fn send(&self, msg: Message) -> Result<()> {
        if self
            .inner
            .send_ops
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(SEND_YIELD_INTERVAL)
        {
            tokio::task::yield_now().await;
        }
        match self.inner.socket_type {
            SocketType::Req => {
                // CAS loop with yield guards against a TOCTOU race: between
                // the CAS failing and the yield returning, the peer holding
                // the reply slot may disconnect (dead slot), which resets the
                // flag. Yielding lets the driver task process the disconnect
                // and clear req_awaiting_reply before we re-check.
                loop {
                    if self
                        .inner
                        .req_awaiting_reply
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                    if self.is_wire_slot_dead() {
                        self.inner
                            .req_awaiting_reply
                            .store(false, Ordering::Release);
                        continue;
                    }
                    return Err(Error::Protocol(
                        "REQ socket must receive a reply before sending again".into(),
                    ));
                }
                let msg = Message::with_prefix(Bytes::new(), msg);
                if self.try_send_wire(&msg) {
                    return Ok(());
                }
                let result = self.send_wire_slow(msg).await;
                if result.is_err() {
                    self.inner
                        .req_awaiting_reply
                        .store(false, Ordering::Release);
                }
                result
            }
            SocketType::Rep => {
                let msg = self
                    .inner
                    .type_state
                    .lock()
                    .expect("type_state")
                    .pre_send(self.inner.socket_type, msg)?;
                return self.send_identity_routed(msg).await;
            }
            SocketType::Router | SocketType::Server | SocketType::Peer | SocketType::Stream => {
                check_pre_send_frame_count(self.inner.socket_type, &msg)?;
                return self.send_identity_routed(msg).await;
            }
            _ => {
                check_pre_send_frame_count(self.inner.socket_type, &msg)?;
                let msg = match self.try_push_spsc(msg) {
                    Ok(()) => return Ok(()),
                    Err(msg) => msg,
                };
                if self.try_send_wire(&msg) {
                    return Ok(());
                }
                if self.inner.wire_slot.lock().expect("wire_slot").is_some() {
                    self.send_wire_slow(msg).await
                } else {
                    self.send_round_robin_wire(msg).await
                }
            }
        }
    }

    /// Non-blocking send. Routes through the `SendSubmitter` directly
    /// (no actor hop), mirroring `send()` but synchronously. Returns
    /// `Full(msg)` when the outbound queue is at HWM so the caller can
    /// retry or fall back to the async `send()`.
    pub fn try_send(&self, msg: Message) -> core::result::Result<(), TrySendError> {
        match self.inner.socket_type {
            SocketType::Req => {
                if self
                    .inner
                    .req_awaiting_reply
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    return Err(TrySendError::Error(Error::Protocol(
                        "REQ socket must receive a reply before sending again".into(),
                    )));
                }
                let msg = Message::with_prefix(Bytes::new(), msg);
                let result = match self.try_send_single_wire(&msg) {
                    Ok(true) => Ok(()),
                    Ok(false) => self.inner.send_submitter.try_send(msg),
                    Err(e) => Err(e),
                };
                if result.is_err() {
                    self.inner
                        .req_awaiting_reply
                        .store(false, Ordering::Release);
                }
                result
            }
            SocketType::Rep => {
                let msg = self
                    .inner
                    .type_state
                    .lock()
                    .expect("type_state")
                    .pre_send(self.inner.socket_type, msg)
                    .map_err(TrySendError::Error)?;
                self.inner.send_submitter.try_send(msg)
            }
            SocketType::Router | SocketType::Server => {
                check_pre_send_frame_count(self.inner.socket_type, &msg)
                    .map_err(TrySendError::Error)?;
                self.inner.send_submitter.try_send(msg)
            }
            _ => {
                check_pre_send_frame_count(self.inner.socket_type, &msg)
                    .map_err(TrySendError::Error)?;
                let msg = match self.try_push_spsc(msg) {
                    Ok(()) => return Ok(()),
                    Err(msg) => msg,
                };
                if self.try_send_single_wire(&msg)? {
                    return Ok(());
                }
                self.inner.send_submitter.try_send(msg)
            }
        }
    }

    /// Receive the next message. Blocks until one is available or the socket
    /// is closed.
    pub async fn recv(&self) -> Result<Message> {
        match self.inner.socket_type {
            SocketType::Req => loop {
                let mut msg = self.inner.recv_rx.recv().await?;
                match msg.pop_front() {
                    Some(delim) if delim.is_empty() => {}
                    _ => continue,
                }
                self.inner
                    .req_awaiting_reply
                    .store(false, Ordering::Release);
                return Ok(msg);
            },
            _ => self.inner.recv_rx.recv().await,
        }
    }

    /// Non-blocking receive. Returns `Err(Error::WouldBlock)` if no message is
    /// currently queued. Does not drive the I/O engine; messages already
    /// delivered by the background driver are visible.
    pub fn try_recv(&self) -> Result<Message> {
        if self.inner.socket_type == SocketType::Req {
            loop {
                let mut msg = self.inner.recv_rx.try_recv()?;
                if let Some(delim) = msg.pop_front()
                    && delim.is_empty()
                {
                    self.inner
                        .req_awaiting_reply
                        .store(false, Ordering::Release);
                    return Ok(msg);
                }
            }
        }
        self.inner.recv_rx.try_recv()
    }

    /// Subscribe to a topic prefix. Only valid on SUB / XSUB sockets; other
    /// types return `Error::Protocol`. An empty prefix subscribes to all
    /// topics. Sends a ZMTP SUBSCRIBE command to every currently-connected
    /// publisher and is replayed to new publishers on connect.
    pub async fn subscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Subscribe {
                prefix: prefix.into(),
                ack,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Cancel a previously-registered subscription prefix. No-op if the
    /// prefix wasn't subscribed.
    pub async fn unsubscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Unsubscribe {
                prefix: prefix.into(),
                ack,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Join a group. Only valid on DISH sockets. Sends a ZMTP JOIN command
    /// to every connected RADIO peer; replayed to new peers on connect.
    pub async fn join(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Join {
                group: group.into(),
                ack,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Leave a previously-joined group. No-op if not joined.
    pub async fn leave(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Leave {
                group: group.into(),
                ack,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Tear down a previously-established bind. Cancels the listener's
    /// accept loop and releases its socket file (filesystem IPC) without
    /// closing already-accepted peers. Returns `Error::Unroutable` if
    /// no listener at `endpoint` is registered.
    pub async fn unbind(&self, endpoint: Endpoint) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Unbind { endpoint, ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Tear down a previously-started connect. Cancels the dial loop
    /// and any in-flight reconnect backoff; existing handshaked peers
    /// from this dialer remain connected. Returns `Error::Unroutable`
    /// if no dialer at `endpoint` is registered.
    pub async fn disconnect(&self, endpoint: Endpoint) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Disconnect { endpoint, ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Snapshot the live status of one connected peer by `connection_id`.
    /// `Ok(None)` means no peer with that id exists (never connected, or
    /// already disconnected). `Err(Error::Closed)` means the socket
    /// driver is gone.
    pub async fn connection_info(&self, connection_id: u64) -> Result<Option<ConnectionStatus>> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::QueryConnection { connection_id, ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)
    }

    /// Snapshot every currently-connected peer. Empty vec when no peers
    /// are live. Useful for introspection / health checks.
    pub async fn connections(&self) -> Result<Vec<ConnectionStatus>> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::QueryConnections { ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)
    }

    /// Graceful close. Stops accepting new work, drains pending sends up to
    /// `options.linger`, then cancels the driver. Consumes the handle; other
    /// clones remain valid until they also drop (subsequent calls on them
    /// return `Error::Closed`).
    pub async fn close(self) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        let _ = self
            .inner
            .cmd_tx
            .send(SocketCommand::Close { ack: Some(ack) })
            .await;
        // Even if the driver is already gone, the channel may be closed; we
        // treat that as "already closed" (success).
        match rx.await {
            Ok(res) => res,
            Err(_) => Ok(()),
        }
    }
}

impl omq_proto::socket_api::SocketApi for Socket {
    fn new(socket_type: SocketType, options: Options) -> Self {
        Socket::new(socket_type, options)
    }
    fn socket_type(&self) -> SocketType {
        self.socket_type()
    }
    async fn bind(&self, endpoint: Endpoint) -> Result<Endpoint> {
        self.bind(endpoint).await
    }
    async fn connect(&self, endpoint: Endpoint) -> Result<()> {
        self.connect(endpoint).await
    }
    async fn send(&self, msg: Message) -> Result<()> {
        self.send(msg).await
    }
    async fn recv(&self) -> Result<Message> {
        self.recv().await
    }
    fn try_send(&self, msg: Message) -> Result<()> {
        self.try_send(msg).map_err(|e| match e {
            TrySendError::Full(_) => Error::WouldBlock,
            TrySendError::Closed => Error::Closed,
            TrySendError::Error(e) => e,
        })
    }
    fn try_recv(&self) -> Result<Message> {
        self.try_recv()
    }
    async fn subscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        self.subscribe(prefix).await
    }
    async fn unsubscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        self.unsubscribe(prefix).await
    }
    async fn join(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        self.join(group).await
    }
    async fn leave(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        self.leave(group).await
    }
    async fn unbind(&self, endpoint: Endpoint) -> Result<()> {
        self.unbind(endpoint).await
    }
    async fn disconnect(&self, endpoint: Endpoint) -> Result<()> {
        self.disconnect(endpoint).await
    }
    async fn close(self) -> Result<()> {
        self.close().await
    }
}

/// Validate frame count for socket types that enforce a fixed count but whose
/// `TypeState::pre_send` has no mutable side effects. This mirrors the check
/// inside `TypeState::pre_send` for the relevant types so the actor-bypass
/// send path still surfaces the same protocol errors.
fn check_pre_send_frame_count(t: SocketType, msg: &Message) -> Result<()> {
    match t {
        SocketType::Client | SocketType::Scatter | SocketType::Gather | SocketType::Channel
            if msg.len() != 1 =>
        {
            Err(Error::Protocol(format!(
                "{t:?} socket requires single-part messages (got {})",
                msg.len()
            )))
        }
        SocketType::Server if msg.len() != 2 => Err(Error::Protocol(
            "SERVER socket requires [routing_id, body] (2 parts)".into(),
        )),
        _ => Ok(()),
    }
}

impl Socket {
    /// SPSC send fast path: push directly into the peer's yring.
    /// Returns `Ok(())` if sent, `Err(msg)` if the fast path is
    /// unavailable or the ring is full.
    fn try_push_spsc(&self, msg: Message) -> core::result::Result<(), Message> {
        if !self.inner.recv_rx.send_ring_active.load(Ordering::Acquire) {
            return Err(msg);
        }
        let spsc = self.inner.recv_rx.send_ring.read().unwrap().clone();
        let Some(ref pair) = spsc else {
            return Err(msg);
        };
        if !pair.recv_ready.load(std::sync::atomic::Ordering::Acquire)
            || pair
                .max_message_size
                .is_some_and(|max| msg.byte_len() > max)
        {
            return Err(msg);
        }
        let Ok(mut producer) = pair.producer.try_lock() else {
            return Err(msg);
        };
        if producer.is_full() {
            return Err(msg);
        }
        let _ = producer.push(msg);
        producer.flush();
        pair.recv_notify.notify_one();
        Ok(())
    }

    /// Synchronous single-peer wire encode fast path. Returns true if
    /// the message was encoded into the peer's `EncodedQueue`.
    #[inline]
    fn try_send_wire(&self, msg: &Message) -> bool {
        if let Some(ref slot) = self.single_wire_slot() {
            return slot.try_encode(msg) == TryEncodeResult::Ok;
        }
        false
    }

    /// Multi-peer round-robin with anti-starvation.
    ///
    /// Normal path: try all slots, skip Full, encode into the first Ok.
    /// Anti-starvation: when any slot has been skipped more than
    /// `STARVATION_THRESHOLD` consecutive times, wait briefly for
    /// that slot's driver to drain (bounded by timeout). This
    /// breaks the TCP flow-control feedback loop that otherwise
    /// causes permanent starvation under MT scheduling.
    async fn send_round_robin_wire(&self, msg: Message) -> Result<()> {
        let starved = {
            let slots = self.inner.rr_slots.slots.lock().expect("rr_slots");
            let n = slots.len();
            let mut starved_slot = None;
            for _ in 0..n {
                let i = self.inner.rr_slots.cursor.fetch_add(1, Ordering::Relaxed) % n;
                match slots[i].try_encode(&msg) {
                    TryEncodeResult::Ok => {
                        slots[i].consecutive_full.store(0, Ordering::Relaxed);
                        return Ok(());
                    }
                    TryEncodeResult::Full => {
                        let prev = slots[i].consecutive_full.fetch_add(1, Ordering::Relaxed);
                        if prev >= Self::STARVATION_THRESHOLD {
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

        self.inner.send_submitter.send(msg).await
    }

    /// Single-peer async slow path: handles backpressure (Full → wait
    /// for space) and falls back to the shared queue for ineligible peers.
    async fn send_wire_slow(&self, msg: Message) -> Result<()> {
        let slot = self.single_wire_slot();
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
        self.inner.send_submitter.send(msg).await
    }

    fn try_send_single_wire(&self, msg: &Message) -> core::result::Result<bool, TrySendError> {
        let Some(slot) = self.single_wire_slot() else {
            return Ok(false);
        };
        match slot.try_encode(msg) {
            TryEncodeResult::Ok => Ok(true),
            TryEncodeResult::Full => Err(TrySendError::Full(msg.clone())),
            TryEncodeResult::Dead | TryEncodeResult::Ineligible => Ok(false),
        }
    }

    fn single_wire_slot(&self) -> Option<Arc<PeerWireSlot>> {
        self.inner.wire_slot.lock().expect("wire_slot").clone()
    }

    async fn send_identity_routed(&self, msg: Message) -> Result<()> {
        self.inner.send_submitter.send(msg).await
    }

    fn is_wire_slot_dead(&self) -> bool {
        self.inner
            .wire_slot
            .lock()
            .expect("wire_slot")
            .as_ref()
            .is_some_and(|s| s.dead.load(Ordering::Acquire))
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Last handle dropped: signal cancellation. The driver tears down
        // without waiting for linger since there's no one to await it.
        self.root_cancel.cancel();
    }
}

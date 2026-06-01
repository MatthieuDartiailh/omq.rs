//! Public `Socket` handle.

use std::sync::atomic::{AtomicBool, Ordering};
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
use crate::engine::direct_io::DirectIo;
use crate::routing::{SendStrategy, SendSubmitter};
use crate::transport::inproc::InprocSpsc;

pub(crate) type DirectIoSlot = Arc<tokio::sync::Mutex<Option<DirectIo>>>;
pub(crate) type DirectIoPending = Arc<AtomicBool>;

/// Per-peer SPSC consumers Vec. Actor appends; recv fair-queues.
pub(crate) type SpscConsumers = Arc<RwLock<Vec<Arc<InprocSpsc>>>>;

/// Single-peer send fast path ring. Actor sets/clears.
pub(crate) type SpscSendRing = Arc<RwLock<Option<Arc<InprocSpsc>>>>;

/// Shared recv notification. All inproc producers notify this.
pub(crate) type SpscRecvNotify = Arc<tokio::sync::Notify>;

/// Notified by the actor when the consumers Vec changes. Wakes
/// any `recv()` that's blocked on the normal `async_channel` path.
pub(crate) type SpscActivated = Arc<tokio::sync::Notify>;

/// Recv channel that integrates per-peer SPSC awareness. Fair-queues
/// across per-peer consumers, then falls back to the `async_channel`.
#[derive(Debug)]
struct SpscAwareRecv {
    rx: async_channel::Receiver<Message>,
    /// Per-peer SPSC rings (one per eligible inproc peer). Actor appends.
    consumers: Arc<std::sync::RwLock<Vec<Arc<InprocSpsc>>>>,
    /// Shared recv notification. All drivers/senders notify this.
    recv_notify: Arc<tokio::sync::Notify>,
    /// Notified when consumers Vec changes (new peer added).
    activated: SpscActivated,
    /// Single-peer send fast path ring (None when sender has >1 peer).
    send_ring: Arc<std::sync::RwLock<Option<Arc<InprocSpsc>>>>,
    /// Batched inproc messages drained from consumers.
    inproc_cache: std::sync::Mutex<std::collections::VecDeque<Message>>,
}

impl SpscAwareRecv {
    fn try_drain_consumers(&self) -> Option<Message> {
        {
            let mut cache = self.inproc_cache.lock().unwrap();
            if let Some(msg) = cache.pop_front() {
                return Some(msg);
            }
        }
        let consumers = self.consumers.read().unwrap().clone();
        let mut cache = self.inproc_cache.lock().unwrap();
        for p in &consumers {
            if let Ok(mut consumer) = p.consumer.try_lock() {
                let got = consumer.prefetch();
                if got > 0 {
                    while let Some(msg) = consumer.pop() {
                        cache.push_back(msg);
                    }
                    consumer.release();
                }
            }
        }
        cache.pop_front()
    }

    #[expect(clippy::needless_continue)]
    async fn recv(&self) -> Result<Message> {
        loop {
            if let Some(msg) = self.try_drain_consumers() {
                return Ok(msg);
            }

            if self.consumers.read().unwrap().is_empty() {
                tokio::select! {
                    biased;
                    res = self.rx.recv() => {
                        return res.map_err(|_| Error::Closed);
                    }
                    () = self.activated.notified() => continue,
                }
            } else {
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
    /// Direct I/O bypass: when set, send does TCP I/O directly on the
    /// user task, eliminating the send-path cross-task wakeup. Recv
    /// stays on the driver (which continues running after handoff).
    direct_io: DirectIoSlot,
    last_bound_endpoint: RwLock<Option<Endpoint>>,
}

impl Socket {
    /// Create a new socket of the given type with the given options. Spawns
    /// the driver task on the current tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if `options` violates ZMTP protocol limits (identity > 255
    /// bytes, heartbeat TTL overflow, etc.) or if `conflate` is set on an
    /// incompatible socket type.
    pub fn new(socket_type: SocketType, options: Options) -> Self {
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
        // Conflate currently affects send-side queues only (per-peer
        // queue cap=1 + DropOldest in fan_out / round_robin). The
        // recv-side adaptation needs a drop-oldest async_channel
        // wrapper that's not yet in place; recv_hwm is honored as
        // before. The headline PUB-conflate use case works.
        let (recv_tx, recv_rx) =
            async_channel::bounded::<Message>(options.recv_hwm.unwrap_or(1024).max(16) as usize);
        let monitor = MonitorPublisher::new();
        // Build the send strategy here so we can hand a submitter clone to
        // `Inner` for the actor-bypass fast path, while the strategy itself
        // moves into the driver.
        let send_strategy = SendStrategy::for_socket_type(socket_type, &options);
        let send_submitter = send_strategy.submitter();
        let consumers: Arc<RwLock<Vec<Arc<InprocSpsc>>>> = Arc::new(RwLock::new(Vec::new()));
        let recv_notify: Arc<tokio::sync::Notify> = Arc::new(tokio::sync::Notify::new());
        let spsc_activated: SpscActivated = Arc::new(tokio::sync::Notify::new());
        let send_ring: Arc<RwLock<Option<Arc<InprocSpsc>>>> = Arc::new(RwLock::new(None));
        let type_state = Arc::new(Mutex::new(TypeState::new()));
        let req_awaiting_reply = Arc::new(AtomicBool::new(false));
        let direct_io: DirectIoSlot = Arc::new(tokio::sync::Mutex::new(None));
        let direct_io_pending: DirectIoPending = Arc::new(AtomicBool::new(false));
        let driver = SocketDriver::new(
            socket_type,
            options,
            cmd_rx,
            recv_tx,
            cancel.clone(),
            monitor.clone(),
            send_strategy,
            send_submitter.clone(),
            consumers.clone(),
            send_ring.clone(),
            recv_notify.clone(),
            spsc_activated.clone(),
            type_state.clone(),
            req_awaiting_reply.clone(),
            direct_io.clone(),
            direct_io_pending.clone(),
        );
        spawn_driver(driver);
        Self {
            inner: Arc::new(Inner {
                socket_type,
                cmd_tx,
                recv_rx: SpscAwareRecv {
                    rx: recv_rx,
                    consumers,
                    recv_notify,
                    activated: spsc_activated,
                    send_ring,
                    inproc_cache: std::sync::Mutex::new(std::collections::VecDeque::new()),
                },
                monitor,
                root_cancel: cancel,
                send_submitter,
                type_state,
                req_awaiting_reply,
                direct_io,
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
        match self.inner.socket_type {
            SocketType::Req => {
                if self.inner.req_awaiting_reply.load(Ordering::Relaxed) {
                    // The driver detects peer disconnect (EOF) and resets
                    // req_awaiting_reply via on_peer_disconnected. Give
                    // the actor a chance to process PeerClosed, then check
                    // if the DirectIo is dead.
                    tokio::task::yield_now().await;
                    if self.inner.req_awaiting_reply.load(Ordering::Relaxed) {
                        let guard = self.inner.direct_io.lock().await;
                        if guard.as_ref().is_some_and(DirectIo::is_dead) {
                            let peer_id = guard.as_ref().unwrap().peer_id;
                            drop(guard);
                            self.clear_direct_io_slot(peer_id).await;
                            self.inner
                                .req_awaiting_reply
                                .store(false, Ordering::Relaxed);
                        } else {
                            drop(guard);
                            if self.inner.req_awaiting_reply.load(Ordering::Relaxed) {
                                return Err(Error::Protocol(
                                    "REQ socket must receive a reply before sending again".into(),
                                ));
                            }
                        }
                    }
                }
                let msg = Message::with_prefix(Bytes::new(), msg);
                self.inner.req_awaiting_reply.store(true, Ordering::Relaxed);
                return self.send_with_direct_io(msg).await;
            }
            SocketType::Rep => {
                let msg = self
                    .inner
                    .type_state
                    .lock()
                    .expect("type_state")
                    .pre_send(self.inner.socket_type, msg)?;
                // pre_send restores the saved envelope which includes the
                // peer identity (added by wrap_for_transform on recv). The
                // identity routing strategy normally strips it; DirectIo
                // bypasses that, so strip it here.
                return self.send_via_direct_io_or_submitter(msg, true).await;
            }
            SocketType::Router | SocketType::Server => {
                check_pre_send_frame_count(self.inner.socket_type, &msg)?;
                return self.send_via_direct_io_or_submitter(msg, true).await;
            }
            _ if is_direct_io_eligible(self.inner.socket_type) => {
                check_pre_send_frame_count(self.inner.socket_type, &msg)?;
                return self.send_with_direct_io(msg).await;
            }
            _ => {
                check_pre_send_frame_count(self.inner.socket_type, &msg)?;
                // SPSC send fast path: push directly to ring.
                // Gated on recv_ready (set by recv-side actor after
                // installing the ring). Single-peer only.
                let spsc = self.inner.recv_rx.send_ring.read().unwrap().clone();
                if let Some(ref pair) = spsc
                    && pair.recv_ready.load(std::sync::atomic::Ordering::Acquire)
                    && pair
                        .max_message_size
                        .is_none_or(|max| msg.byte_len() <= max)
                    && let Ok(mut producer) = pair.producer.try_lock()
                    && !producer.is_full()
                {
                    let _ = producer.push(msg);
                    producer.flush();
                    pair.recv_notify.notify_one();
                    return Ok(());
                }
                self.inner.send_submitter.send(msg).await
            }
        }
    }

    /// Non-blocking send. Returns `Err(Error::WouldBlock)` if the socket's
    /// outbound queue is full (HWM reached). The message is accepted into the
    /// queue and routed asynchronously; delivery confirmation is not awaited.
    pub fn try_send(&self, msg: Message) -> Result<()> {
        use tokio::sync::mpsc::error::TrySendError;
        let (ack, _rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .try_send(SocketCommand::Send { msg, ack })
            .map_err(|e| match e {
                TrySendError::Full(_) => Error::WouldBlock,
                TrySendError::Closed(_) => Error::Closed,
            })
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
                    .store(false, Ordering::Relaxed);
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
            let mut msg = self.inner.recv_rx.try_recv()?;
            match msg.pop_front() {
                Some(delim) if delim.is_empty() => {}
                _ => return Err(Error::WouldBlock),
            }
            self.inner
                .req_awaiting_reply
                .store(false, Ordering::Relaxed);
            return Ok(msg);
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
        self.try_send(msg)
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

use crate::routing::is_direct_io_eligible;

impl Socket {
    /// Try the `DirectIo` fast path, stripping the `routing_id` frame
    /// when `strip_routing_id` is true (Rep / Router / Server). Falls
    /// back to the shared `SendSubmitter` when no `DirectIo` is installed.
    async fn send_via_direct_io_or_submitter(
        &self,
        mut msg: Message,
        strip_routing_id: bool,
    ) -> Result<()> {
        let guard = self.inner.direct_io.lock().await;
        if let Some(dio) = guard.as_ref() {
            let peer_id = dio.peer_id;
            if strip_routing_id {
                let _routing_id = msg.pop_front();
            }
            return match dio.send_msg(&msg).await {
                Ok(()) => Ok(()),
                Err(e) => {
                    drop(guard);
                    self.clear_direct_io_slot(peer_id).await;
                    Err(e)
                }
            };
        }
        drop(guard);
        self.inner.send_submitter.send(msg).await
    }

    async fn send_with_direct_io(&self, msg: Message) -> Result<()> {
        {
            let guard = self.inner.direct_io.lock().await;
            if let Some(dio) = guard.as_ref() {
                if let Ok(()) = dio.send_msg(&msg).await {
                    return Ok(());
                }
                let failed_peer_id = dio.peer_id;
                drop(guard);
                self.clear_direct_io_slot(failed_peer_id).await;
                return self.inner.send_submitter.send(msg).await;
            }
        }
        self.inner.send_submitter.send(msg).await
    }

    async fn clear_direct_io_slot(&self, expected_peer_id: u64) {
        let mut guard = self.inner.direct_io.lock().await;
        if guard
            .as_ref()
            .is_some_and(|dio| dio.peer_id != expected_peer_id)
        {
            return;
        }
        if let Some(dio) = guard.take() {
            let peer_id = dio.peer_id;
            drop(guard);
            let _ = self
                .inner
                .cmd_tx
                .send(SocketCommand::DirectIoDisconnected { peer_id })
                .await;
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Last handle dropped: signal cancellation. The driver tears down
        // without waiting for linger since there's no one to await it.
        self.root_cancel.cancel();
    }
}

//! Socket recv mux for async-channel plus per-peer yring fast paths.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use arc_swap::ArcSwapOption;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;

use crate::transport::inproc::InprocSpsc;

/// Per-peer SPSC consumers Vec. Actor appends; recv fair-queues.
pub(crate) type SpscConsumers = Arc<RwLock<Vec<Arc<InprocSpsc>>>>;

/// Single-peer send fast path ring. Actor sets/clears.
pub(crate) type SpscSendRing = Arc<ArcSwapOption<InprocSpsc>>;

/// Shared recv notification. All inproc producers notify this.
pub(crate) type SpscRecvNotify = Arc<tokio::sync::Notify>;

/// Notified by the actor when the consumers Vec changes. Wakes
/// any `recv()` that's blocked on the normal `async_channel` path.
pub(crate) type SpscActivated = Arc<tokio::sync::Notify>;

/// Bumped by the actor whenever the consumers Vec changes. Lets
/// `SpscAwareRecv` skip re-cloning the Vec when nothing changed.
pub(crate) type SpscConsumerGeneration = Arc<AtomicU64>;

pub(crate) enum SpscPush {
    Sent,
    Unavailable(Message),
    Full {
        msg: Message,
        space: Arc<tokio::sync::Notify>,
    },
}

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
    pub send_ring_available: Arc<AtomicBool>,
    pub recv_notify: SpscRecvNotify,
    pub activated: SpscActivated,
    pub tcp_consumers: TcpConsumers,
}

impl Default for SpscHandles {
    fn default() -> Self {
        Self {
            consumers: Arc::new(RwLock::new(Vec::new())),
            consumer_generation: Arc::new(AtomicU64::new(0)),
            send_ring: Arc::new(ArcSwapOption::empty()),
            send_ring_available: Arc::new(AtomicBool::new(false)),
            recv_notify: Arc::new(tokio::sync::Notify::new()),
            activated: Arc::new(tokio::sync::Notify::new()),
            tcp_consumers: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

/// Recv channel that integrates per-peer SPSC awareness. Fair-queues
/// across per-peer yring consumers (inproc + TCP), then falls back to
/// the `async_channel`.
#[derive(Debug)]
pub(crate) struct SpscAwareRecv {
    rx: async_channel::Receiver<Message>,
    /// Per-peer SPSC rings (one per eligible inproc peer). Actor appends.
    consumers: SpscConsumers,
    /// Per-TCP-peer yring consumers. Actor appends on handshake.
    tcp_consumers: TcpConsumers,
    /// Generation counter. Bumped by the actor on any consumer add/remove
    /// (inproc or TCP).
    consumer_generation: SpscConsumerGeneration,
    /// Shared recv notification. All drivers/senders notify this.
    recv_notify: SpscRecvNotify,
    /// Notified when consumers Vec changes (new peer added).
    activated: SpscActivated,
    /// Single-peer send fast path ring (None when sender has >1 peer).
    send_ring: SpscSendRing,
    /// Cheap guard for the send fast path. Avoids an `ArcSwap` load on the
    /// common TCP/no-inproc path.
    send_ring_available: Arc<AtomicBool>,
    /// Drain state: cached consumer snapshots + message batch buffer.
    /// Single lock covers both to avoid per-drain `Vec::clone`.
    drain_state: Mutex<DrainState>,
}

#[derive(Debug)]
struct DrainState {
    generation: u64,
    inproc: Vec<Arc<InprocSpsc>>,
    tcp: Vec<Arc<TcpYringConsumer>>,
    batch: VecDeque<Message>,
}

impl SpscAwareRecv {
    pub(crate) fn new(rx: async_channel::Receiver<Message>, handles: SpscHandles) -> Self {
        Self {
            rx,
            consumers: handles.consumers,
            tcp_consumers: handles.tcp_consumers,
            consumer_generation: handles.consumer_generation,
            recv_notify: handles.recv_notify,
            activated: handles.activated,
            send_ring: handles.send_ring,
            send_ring_available: handles.send_ring_available,
            drain_state: Mutex::new(DrainState {
                generation: u64::MAX,
                inproc: Vec::new(),
                tcp: Vec::new(),
                batch: VecDeque::new(),
            }),
        }
    }

    fn try_drain_consumers(&self) -> Option<Message> {
        if self.consumer_generation.load(Ordering::Relaxed) == 0 {
            return None;
        }
        let mut guard = self.drain_state.lock().unwrap();
        if let Some(msg) = guard.batch.pop_front() {
            return Some(msg);
        }
        let current_gen = self.consumer_generation.load(Ordering::Acquire);
        if guard.generation != current_gen {
            guard.inproc.clone_from(&self.consumers.read().unwrap());
            guard.tcp.clone_from(&self.tcp_consumers.read().unwrap());
            guard.generation = current_gen;
        }
        let state = &mut *guard;
        let mut has_disconnected = false;
        for p in &state.inproc {
            if let Ok(mut consumer) = p.consumer.try_lock() {
                let got = consumer.prefetch();
                if got > 0 {
                    while let Some(msg) = consumer.pop() {
                        state.batch.push_back(msg);
                    }
                    consumer.release();
                    p.space_notify.notify_waiters();
                } else if consumer.is_disconnected() {
                    has_disconnected = true;
                }
            }
        }
        for tc in &state.tcp {
            if let Ok(mut consumer) = tc.consumer.try_lock() {
                let got = consumer.prefetch();
                if got > 0 {
                    while let Some(msg) = consumer.pop() {
                        state.batch.push_back(msg);
                    }
                    consumer.release();
                    tc.space.notify_one();
                } else if consumer.is_disconnected() {
                    has_disconnected = true;
                }
            }
        }
        let result = state.batch.pop_front();
        drop(guard);
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
            self.drain_state.lock().unwrap().generation = u64::MAX;
        }
        result
    }

    #[expect(clippy::needless_continue)]
    pub(crate) async fn recv(&self) -> Result<Message> {
        let channel_recv = self.rx.recv();
        tokio::pin!(channel_recv);

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
                    res = &mut channel_recv => {
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
                    res = &mut channel_recv => {
                        return res.map_err(|_| Error::Closed);
                    }
                    () = activated => continue,
                }
            }
        }
    }

    pub(crate) fn try_recv(&self) -> Result<Message> {
        if let Some(msg) = self.try_drain_consumers() {
            return Ok(msg);
        }
        self.rx.try_recv().map_err(|e| match e {
            async_channel::TryRecvError::Empty => Error::WouldBlock,
            async_channel::TryRecvError::Closed => Error::Closed,
        })
    }

    pub(crate) fn shutdown(&self) {
        {
            let mut state = self.drain_state.lock().unwrap();
            state.batch.clear();
            state.inproc.clear();
            state.tcp.clear();
            state.generation = u64::MAX;
        }
        self.consumers.write().unwrap().clear();
        self.tcp_consumers.write().unwrap().clear();
        if let Some(pair) = self.send_ring.load_full() {
            pair.space_notify.notify_waiters();
        }
        self.send_ring_available.store(false, Ordering::Release);
        self.send_ring.store(None);
        while self.rx.try_recv().is_ok() {}
    }

    pub(crate) fn try_push_spsc_or_full(&self, msg: Message) -> SpscPush {
        if !self.send_ring_available.load(Ordering::Acquire) {
            return SpscPush::Unavailable(msg);
        }
        let Some(pair) = self.send_ring.load_full() else {
            return SpscPush::Unavailable(msg);
        };
        if !pair.recv_ready.load(Ordering::Acquire)
            || pair
                .max_message_size
                .is_some_and(|max| msg.byte_len() > max)
        {
            return SpscPush::Unavailable(msg);
        }
        let Ok(mut producer) = pair.producer.try_lock() else {
            return SpscPush::Unavailable(msg);
        };
        if producer.is_consumer_dropped() || !pair.recv_ready.load(Ordering::Acquire) {
            return SpscPush::Unavailable(msg);
        }
        if producer.is_full() {
            return SpscPush::Full {
                msg,
                space: pair.space_notify.clone(),
            };
        }
        let _ = producer.push(msg);
        producer.flush();
        pair.recv_notify.notify_one();
        SpscPush::Sent
    }

    /// SPSC send fast path: push directly into the peer's yring.
    /// Returns `Ok(())` if sent, `Err(msg)` if the fast path is
    /// unavailable or the ring is full.
    pub(crate) fn try_push_spsc(&self, msg: Message) -> core::result::Result<(), Message> {
        match self.try_push_spsc_or_full(msg) {
            SpscPush::Sent => Ok(()),
            SpscPush::Unavailable(msg) | SpscPush::Full { msg, .. } => Err(msg),
        }
    }
}

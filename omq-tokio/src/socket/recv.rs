//! Socket recv mux: shared recv pipe (yring + Mutex) plus per-peer
//! yring fast paths. Zero heap allocations per message.

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
/// any `recv()` that's blocked so it re-drains with the updated list.
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

// ---------------------------------------------------------------------------
// SharedRecvPipe: MPSC yring-based recv channel
// ---------------------------------------------------------------------------

/// Shared recv pipe. Replaces `async_channel` for the socket recv path.
///
/// Producers (actor, connection drivers) hold `Arc<SharedRecvPipe>` and
/// call [`send`](Self::send). The single consumer
/// ([`SpscAwareRecv`]) owns the `yring::Consumer` and drains it.
///
/// Zero heap allocations on both sides. The yring is pre-allocated at
/// construction; `tokio::sync::Notify` uses intrusive waiters.
pub(crate) struct SharedRecvPipe {
    producer: Mutex<yring::Producer<Message>>,
    notify: Arc<tokio::sync::Notify>,
    space: Arc<tokio::sync::Notify>,
    closed: AtomicBool,
}

impl std::fmt::Debug for SharedRecvPipe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedRecvPipe")
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl SharedRecvPipe {
    /// Blocking send. Waits for space if the ring is full.
    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        let mut msg = msg;
        loop {
            let space_notified = self.space.notified();
            tokio::pin!(space_notified);
            space_notified.as_mut().enable();

            {
                let mut prod = self.producer.lock().unwrap();
                if self.closed.load(Ordering::Acquire) || prod.is_consumer_dropped() {
                    return Err(Error::Closed);
                }
                match prod.push(msg) {
                    Ok(()) => {
                        prod.flush();
                        drop(prod);
                        self.notify.notify_one();
                        return Ok(());
                    }
                    Err(returned) => {
                        msg = returned;
                    }
                }
            }
            space_notified.await;
        }
    }

    /// Close the pipe. New sends return `Error::Closed`. Existing
    /// messages in the ring can still be drained by the consumer.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        if let Ok(mut prod) = self.producer.lock() {
            prod.close();
        }
        self.notify.notify_waiters();
        self.space.notify_waiters();
    }
}

impl Drop for SharedRecvPipe {
    fn drop(&mut self) {
        if !*self.closed.get_mut() {
            self.producer.get_mut().unwrap().close();
        }
        self.notify.notify_waiters();
        self.space.notify_waiters();
    }
}

/// Create a recv pipe pair.
///
/// Returns `(producer_pipe, consumer, data_notify, space_notify)`.
/// The `data_notify` is fired by producers on push; the consumer
/// awaits it. `space_notify` is fired by the consumer on release;
/// blocked producers await it.
pub(crate) fn recv_pipe(
    capacity: usize,
) -> (
    Arc<SharedRecvPipe>,
    yring::Consumer<Message>,
    Arc<tokio::sync::Notify>,
    Arc<tokio::sync::Notify>,
) {
    let (prod, cons) = yring::spsc(capacity);
    let notify = Arc::new(tokio::sync::Notify::new());
    let space = Arc::new(tokio::sync::Notify::new());
    let pipe = Arc::new(SharedRecvPipe {
        producer: Mutex::new(prod),
        notify: notify.clone(),
        space: space.clone(),
        closed: AtomicBool::new(false),
    });
    (pipe, cons, notify, space)
}

// ---------------------------------------------------------------------------
// SpscHandles / SpscAwareRecv
// ---------------------------------------------------------------------------

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
/// across per-peer yring consumers (inproc + TCP) and the shared recv
/// pipe, returning messages one at a time.
#[derive(Debug)]
pub(crate) struct SpscAwareRecv {
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
    /// Data arrival signal from the shared recv pipe.
    recv_pipe_notify: Arc<tokio::sync::Notify>,
    /// Space-available signal for the shared recv pipe.
    recv_pipe_space: Arc<tokio::sync::Notify>,
    /// Drain state: cached consumer snapshots, message batch buffer,
    /// and the shared recv pipe consumer.
    drain_state: Mutex<DrainState>,
}

#[derive(Debug)]
struct DrainState {
    generation: u64,
    inproc: Vec<Arc<InprocSpsc>>,
    tcp: Vec<Arc<TcpYringConsumer>>,
    batch: VecDeque<Message>,
    recv_consumer: yring::Consumer<Message>,
}

enum DrainResult {
    Message(Message),
    Empty,
    Closed,
}

fn drain_yring(consumer: &mut yring::Consumer<Message>, batch: &mut VecDeque<Message>) -> bool {
    let got = consumer.prefetch();
    if got > 0 {
        while let Some(msg) = consumer.pop() {
            batch.push_back(msg);
        }
        consumer.release();
        true
    } else {
        false
    }
}

impl SpscAwareRecv {
    pub(crate) fn new(
        recv_consumer: yring::Consumer<Message>,
        recv_pipe_notify: Arc<tokio::sync::Notify>,
        recv_pipe_space: Arc<tokio::sync::Notify>,
        handles: SpscHandles,
    ) -> Self {
        Self {
            consumers: handles.consumers,
            tcp_consumers: handles.tcp_consumers,
            consumer_generation: handles.consumer_generation,
            recv_notify: handles.recv_notify,
            activated: handles.activated,
            send_ring: handles.send_ring,
            send_ring_available: handles.send_ring_available,
            recv_pipe_notify,
            recv_pipe_space,
            drain_state: Mutex::new(DrainState {
                generation: u64::MAX,
                inproc: Vec::new(),
                tcp: Vec::new(),
                batch: VecDeque::new(),
                recv_consumer,
            }),
        }
    }

    fn try_drain(&self) -> DrainResult {
        let mut guard = self.drain_state.lock().unwrap();

        if let Some(msg) = guard.batch.pop_front() {
            return DrainResult::Message(msg);
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
                if drain_yring(&mut consumer, &mut state.batch) {
                    p.space_notify.notify_waiters();
                } else if consumer.is_disconnected() {
                    has_disconnected = true;
                }
            }
        }

        for tc in &state.tcp {
            if let Ok(mut consumer) = tc.consumer.try_lock() {
                if drain_yring(&mut consumer, &mut state.batch) {
                    tc.space.notify_one();
                } else if consumer.is_disconnected() {
                    has_disconnected = true;
                }
            }
        }

        if drain_yring(&mut state.recv_consumer, &mut state.batch) {
            self.recv_pipe_space.notify_waiters();
        }

        let result = state.batch.pop_front();
        let pipe_disconnected = state.recv_consumer.is_disconnected();
        let has_peers = !state.inproc.is_empty() || !state.tcp.is_empty();
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

        match result {
            Some(msg) => DrainResult::Message(msg),
            None if pipe_disconnected && !has_peers => DrainResult::Closed,
            None => DrainResult::Empty,
        }
    }

    #[expect(clippy::needless_continue)]
    pub(crate) async fn recv(&self) -> Result<Message> {
        loop {
            match self.try_drain() {
                DrainResult::Message(msg) => return Ok(msg),
                DrainResult::Closed => return Err(Error::Closed),
                DrainResult::Empty => {}
            }

            let pipe = self.recv_pipe_notify.notified();
            tokio::pin!(pipe);
            pipe.as_mut().enable();

            if self.consumer_generation.load(Ordering::Acquire) > 0 {
                let notified = self.recv_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();

                match self.try_drain() {
                    DrainResult::Message(msg) => return Ok(msg),
                    DrainResult::Closed => return Err(Error::Closed),
                    DrainResult::Empty => {}
                }

                tokio::select! {
                    biased;
                    () = notified => continue,
                    () = &mut pipe => continue,
                    () = self.activated.notified() => continue,
                }
            } else {
                match self.try_drain() {
                    DrainResult::Message(msg) => return Ok(msg),
                    DrainResult::Closed => return Err(Error::Closed),
                    DrainResult::Empty => {}
                }

                let activated = self.activated.notified();
                tokio::pin!(activated);
                activated.as_mut().enable();

                tokio::select! {
                    biased;
                    () = &mut pipe => continue,
                    () = activated => continue,
                }
            }
        }
    }

    pub(crate) fn try_recv(&self) -> Result<Message> {
        match self.try_drain() {
            DrainResult::Message(msg) => Ok(msg),
            DrainResult::Closed => Err(Error::Closed),
            DrainResult::Empty => Err(Error::WouldBlock),
        }
    }

    pub(crate) fn shutdown(&self) {
        {
            let mut state = self.drain_state.lock().unwrap();
            while state.recv_consumer.prefetch() > 0 {
                while state.recv_consumer.pop().is_some() {}
                state.recv_consumer.release();
            }
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
        self.recv_pipe_space.notify_waiters();
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

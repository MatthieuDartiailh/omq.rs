//! Lock-free bounded send queue for the multi-peer wire work-stealing path.
//!
//! Backed by [`concurrent_queue::ConcurrentQueue`] (lock-free array ring) and
//! [`event_listener::Event`] for async wakeup. No Mutex in the hot path.

use std::sync::Arc;

use concurrent_queue::{ConcurrentQueue, PushError};
use event_listener::Event;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;

/// Max messages one shared-queue consumer drains before yielding.
/// Scaled down per peer by [`SharedQueueReceiver::batch_limit`].
const MAX_DRAIN: usize = 64;

struct Inner {
    queue: ConcurrentQueue<Message>,
    recv_event: Event,
    send_event: Event,
}

#[derive(Clone)]
pub(crate) struct SharedQueueSender {
    inner: Arc<Inner>,
}

#[derive(Clone)]
pub(crate) struct SharedQueueReceiver {
    inner: Arc<Inner>,
    peer_count: Arc<std::sync::atomic::AtomicUsize>,
}

pub(crate) fn bounded(
    cap: usize,
    peer_count: Arc<std::sync::atomic::AtomicUsize>,
) -> (SharedQueueSender, SharedQueueReceiver) {
    let inner = Arc::new(Inner {
        queue: ConcurrentQueue::bounded(cap),
        recv_event: Event::new(),
        send_event: Event::new(),
    });
    (
        SharedQueueSender {
            inner: inner.clone(),
        },
        SharedQueueReceiver { inner, peer_count },
    )
}

pub(crate) fn unbounded(
    peer_count: Arc<std::sync::atomic::AtomicUsize>,
) -> (SharedQueueSender, SharedQueueReceiver) {
    let inner = Arc::new(Inner {
        queue: ConcurrentQueue::unbounded(),
        recv_event: Event::new(),
        send_event: Event::new(),
    });
    (
        SharedQueueSender {
            inner: inner.clone(),
        },
        SharedQueueReceiver { inner, peer_count },
    )
}

impl SharedQueueSender {
    pub(crate) async fn send_async(&self, mut msg: Message) -> Result<()> {
        loop {
            let listener = self.inner.send_event.listen();
            match self.inner.queue.push(msg) {
                Ok(()) => {
                    self.inner.recv_event.notify(1);
                    return Ok(());
                }
                Err(PushError::Full(returned)) => {
                    msg = returned;
                    listener.await;
                }
                Err(PushError::Closed(_)) => return Err(Error::Closed),
            }
        }
    }

    pub(crate) fn try_send(&self, msg: Message) -> Result<()> {
        match self.inner.queue.push(msg) {
            Ok(()) => {
                self.inner.recv_event.notify(1);
                Ok(())
            }
            Err(PushError::Full(_)) => Err(Error::WouldBlock),
            Err(PushError::Closed(_)) => Err(Error::Closed),
        }
    }
}

impl SharedQueueReceiver {
    pub(crate) async fn recv_async(&self) -> Result<Message> {
        loop {
            let listener = self.inner.recv_event.listen();
            if let Ok(msg) = self.inner.queue.pop() {
                self.inner.send_event.notify(1);
                return Ok(msg);
            }
            if self.inner.queue.is_closed() {
                return Err(Error::Closed);
            }
            listener.await;
        }
    }

    pub(crate) fn try_recv(&self) -> Option<Message> {
        let msg = self.inner.queue.pop().ok()?;
        self.inner.send_event.notify(1);
        Some(msg)
    }

    /// Fair share of the current queue for one driver.
    ///
    /// Single peer: full batch (no competition). Multiple peers: each
    /// driver takes at most `queue_len / peers` to leave work for
    /// others, but always at least 1.
    pub(crate) fn batch_limit(&self) -> usize {
        let peers = self.peer_count.load(std::sync::atomic::Ordering::Relaxed);
        if peers <= 1 {
            return MAX_DRAIN;
        }
        (self.inner.queue.len() / peers).clamp(1, MAX_DRAIN)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.queue.is_empty()
    }

    pub(crate) fn close(&self) {
        self.inner.queue.close();
        self.inner.recv_event.notify(usize::MAX);
        self.inner.send_event.notify(usize::MAX);
    }
}

impl std::fmt::Debug for SharedQueueSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedQueueSender")
            .field("len", &self.inner.queue.len())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for SharedQueueReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedQueueReceiver")
            .field("len", &self.inner.queue.len())
            .finish_non_exhaustive()
    }
}

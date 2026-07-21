//! Lock-free bounded send queue with configurable drop policy.
//!
//! Backed by `concurrent_queue::ConcurrentQueue` (lock-free ring) and
//! [`DataSignal`] for coalesced receiver wakeup (empty-to-non-empty
//! signaling). The `Block` policy additionally uses a
//! `tokio::sync::Semaphore` to track available write slots so blocked
//! senders are woken without spinning when a receiver pops.

use std::sync::Arc;

use concurrent_queue::{ConcurrentQueue, PushError};
use tokio::sync::Semaphore;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::OnMute;

use crate::engine::signal::DataSignal;

struct Inner {
    queue: ConcurrentQueue<Message>,
    data_signal: DataSignal,
    space_available: tokio::sync::Notify,
    slots: Option<Semaphore>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("len", &self.queue.len())
            .field("capacity", &self.queue.capacity())
            .finish_non_exhaustive()
    }
}

/// Bounded, multi-producer multi-consumer send queue with a configurable
/// drop policy. Clone-able; all clones share the same underlying queue.
#[derive(Clone, Debug)]
pub(crate) struct FallbackQueue {
    inner: Arc<Inner>,
    policy: OnMute,
}

/// Cloneable receive handle for a [`FallbackQueue`]. Each clone shares the same
/// underlying queue; any clone can pop the next available message.
#[derive(Clone, Debug)]
pub(crate) struct FallbackReceiver {
    inner: Arc<Inner>,
    peer_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl FallbackQueue {
    /// Create a new queue. Returns `(sender_handle, receiver_handle)`.
    ///
    /// `capacity == usize::MAX` creates an unbounded queue (no `Semaphore`).
    /// Otherwise the queue is bounded to `capacity.max(1)`.
    pub(crate) fn new(capacity: usize, policy: OnMute) -> (Self, FallbackReceiver) {
        let (queue, slots) = if capacity == usize::MAX {
            (ConcurrentQueue::unbounded(), None)
        } else {
            let cap = capacity.max(1);
            (ConcurrentQueue::bounded(cap), Some(Semaphore::new(cap)))
        };
        let inner = Arc::new(Inner {
            queue,
            data_signal: DataSignal::new(),
            space_available: tokio::sync::Notify::new(),
            slots,
        });
        let receiver = FallbackReceiver {
            inner: inner.clone(),
            peer_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        (Self { inner, policy }, receiver)
    }

    fn push_with_permit(&self, msg: Message) -> core::result::Result<(), PushError<Message>> {
        match self.inner.queue.push(msg) {
            Ok(()) => {
                self.inner.data_signal.mark();
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn push_drop_policy(&self, msg: Message) -> core::result::Result<(), PushError<Message>> {
        match self.policy {
            OnMute::DropNewest => {
                match self.inner.queue.push(msg) {
                    Ok(()) => self.inner.data_signal.mark(),
                    Err(PushError::Full(_)) => {}
                    Err(e @ PushError::Closed(_)) => return Err(e),
                }
                Ok(())
            }
            OnMute::DropOldest => {
                let mut item = msg;
                loop {
                    match self.inner.queue.push(item) {
                        Ok(()) => {
                            self.inner.data_signal.mark();
                            return Ok(());
                        }
                        Err(PushError::Full(back)) => {
                            let _ = self.inner.queue.pop();
                            item = back;
                        }
                        Err(e @ PushError::Closed(_)) => return Err(e),
                    }
                }
            }
            OnMute::Block | _ => unreachable!("callers dispatch Block separately"),
        }
    }

    /// Submit a message. Behaviour depends on policy:
    /// - `Block`: await until a slot is available, then push.
    /// - `DropNewest`: if full, discard `msg` silently and return `Ok`.
    /// - `DropOldest`: if full, pop the head to make room, then push.
    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        match self.policy {
            OnMute::Block => {
                if let Some(ref slots) = self.inner.slots {
                    let permit = slots.acquire().await.map_err(|_| Error::Closed)?;
                    permit.forget();
                }
                self.push_with_permit(msg).map_err(|_| Error::Closed)
            }
            _ => self.push_drop_policy(msg).map_err(|_| Error::Closed),
        }
    }

    /// Non-blocking push. Returns `Ok(())` on success, `Err(msg)` if the
    /// queue is full (Block policy) or closed. `DropNewest` and `DropOldest`
    /// never return `Err` for capacity reasons.
    pub(crate) fn try_send(&self, msg: Message) -> core::result::Result<(), Message> {
        match self.policy {
            OnMute::Block => {
                if let Some(ref slots) = self.inner.slots {
                    match slots.try_acquire() {
                        Ok(permit) => permit.forget(),
                        Err(_) => return Err(msg),
                    }
                }
                self.push_with_permit(msg).map_err(PushError::into_inner)
            }
            _ => self.push_drop_policy(msg).map_err(PushError::into_inner),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.queue.len()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.inner.queue.is_closed()
    }

    pub(crate) fn space_notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.inner.space_available.notified()
    }

    pub(crate) async fn wait_space_available(&self) {
        self.inner.space_available.notified().await;
    }

    pub(crate) fn shutdown(&self) {
        self.inner.queue.close();
        let mut drained = 0usize;
        while self.inner.queue.pop().is_ok() {
            drained += 1;
        }
        if drained > 0
            && let Some(ref slots) = self.inner.slots
        {
            slots.add_permits(drained);
        }
        self.inner.data_signal.wake_all();
        self.inner.space_available.notify_waiters();
    }
}

impl FallbackReceiver {
    /// Non-blocking pop. Returns the next message, or `None` if empty.
    ///
    /// Callers that pop a batch must call [`Self::release_permits`] and
    /// [`Self::finish_drain`] when that batch is consumed.
    pub(crate) fn try_pop(&self) -> Option<Message> {
        self.inner.queue.pop().ok()
    }

    pub(crate) fn release_permits(&self, n: usize) {
        if let Some(ref slots) = self.inner.slots {
            slots.add_permits(n);
        }
        if n > 0 {
            self.inner.space_available.notify_waiters();
        }
    }

    /// Complete one consumer drain pass. This clears the coalesced signal
    /// and rearms it if producers filled the queue during the drain.
    pub(crate) fn finish_drain(&self) {
        self.inner.data_signal.clear();
        self.inner
            .data_signal
            .rearm_if_nonempty(self.inner.queue.is_empty());
    }

    /// Fair share of the current queue for one driver.
    ///
    /// Single peer: full batch (no competition). Multiple peers: each
    /// driver takes at most `queue_len / peers` to leave work for
    /// others, but always at least 1.
    pub(crate) fn batch_limit(&self) -> usize {
        let peers = self.peer_count.load(std::sync::atomic::Ordering::Relaxed);
        omq_proto::flow::fair_share(self.inner.queue.len(), peers, super::SHARED_MAX_BATCH_MSGS)
    }

    pub(crate) fn set_peer_count(&self, n: usize) {
        self.peer_count
            .store(n, std::sync::atomic::Ordering::Relaxed);
    }

    /// Async pop. Waits until a message is available or the queue is closed.
    pub(crate) async fn recv(&self) -> Option<Message> {
        loop {
            let notified = self.inner.data_signal.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(msg) = self.try_pop() {
                return Some(msg);
            }
            if self.inner.queue.is_closed() && self.inner.queue.is_empty() {
                return None;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omq_proto::options::OnMute;

    #[tokio::test]
    async fn block_policy_backpressures() {
        let (q, rx) = FallbackQueue::new(1, OnMute::Block);
        q.send(Message::single("a")).await.unwrap();
        let r = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            q.send(Message::single("b")),
        )
        .await;
        assert!(r.is_err(), "second send should block on full queue");
        let _ = rx.try_pop().unwrap();
        rx.release_permits(1);
    }

    #[tokio::test]
    async fn drop_newest_silent() {
        let (q, rx) = FallbackQueue::new(1, OnMute::DropNewest);
        q.send(Message::single("a")).await.unwrap();
        q.send(Message::single("b")).await.unwrap();
        q.send(Message::single("c")).await.unwrap();
        let got = rx.try_pop().unwrap();
        assert_eq!(got.part_bytes(0).unwrap(), &b"a"[..]);
        assert!(rx.try_pop().is_none());
    }

    #[tokio::test]
    async fn drop_oldest_keeps_latest() {
        let (q, rx) = FallbackQueue::new(2, OnMute::DropOldest);
        q.send(Message::single("a")).await.unwrap();
        q.send(Message::single("b")).await.unwrap();
        q.send(Message::single("c")).await.unwrap();
        q.send(Message::single("d")).await.unwrap();
        let got_c = rx.try_pop().unwrap();
        let got_d = rx.try_pop().unwrap();
        assert_eq!(got_c.part_bytes(0).unwrap(), &b"c"[..]);
        assert_eq!(got_d.part_bytes(0).unwrap(), &b"d"[..]);
    }

    #[tokio::test]
    async fn recv_wakes_on_push() {
        let (q, rx) = FallbackQueue::new(4, OnMute::Block);
        let recv_task = tokio::spawn(async move { rx.recv().await });
        tokio::task::yield_now().await;
        q.send(Message::single("hello")).await.unwrap();
        let msg = recv_task.await.unwrap().unwrap();
        assert_eq!(msg.part_bytes(0).unwrap(), &b"hello"[..]);
    }

    #[tokio::test]
    async fn recv_wakes_after_empty_refill() {
        let (q, rx) = FallbackQueue::new(2, OnMute::Block);
        q.send(Message::single("a")).await.unwrap();
        let got = rx.recv().await.unwrap();
        assert_eq!(got.part_bytes(0).unwrap(), &b"a"[..]);
        rx.release_permits(1);
        rx.finish_drain();

        q.send(Message::single("b")).await.unwrap();
        let got = tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv())
            .await
            .expect("refill must wake receiver")
            .unwrap();
        assert_eq!(got.part_bytes(0).unwrap(), &b"b"[..]);
    }

    #[tokio::test]
    async fn shutdown_unblocks_recv() {
        let (q, rx) = FallbackQueue::new(4, OnMute::Block);
        let recv_task = tokio::spawn(async move { rx.recv().await });
        tokio::task::yield_now().await;
        q.shutdown();
        let result = recv_task.await.unwrap();
        assert!(result.is_none(), "closed queue should return None");
    }

    #[test]
    fn try_send_block_succeeds_when_space() {
        let (q, rx) = FallbackQueue::new(2, OnMute::Block);
        q.try_send(Message::single("a")).unwrap();
        q.try_send(Message::single("b")).unwrap();
        let got = rx.try_pop().unwrap();
        assert_eq!(got.part_bytes(0).unwrap(), &b"a"[..]);
        rx.release_permits(1);
    }

    #[test]
    fn try_send_block_returns_err_when_full() {
        let (q, _rx) = FallbackQueue::new(1, OnMute::Block);
        q.try_send(Message::single("a")).unwrap();
        let err = q.try_send(Message::single("b")).unwrap_err();
        assert_eq!(err.part_bytes(0).unwrap(), &b"b"[..]);
    }

    #[test]
    fn try_send_drop_newest_silent() {
        let (q, rx) = FallbackQueue::new(1, OnMute::DropNewest);
        q.try_send(Message::single("a")).unwrap();
        q.try_send(Message::single("b")).unwrap();
        let got = rx.try_pop().unwrap();
        assert_eq!(got.part_bytes(0).unwrap(), &b"a"[..]);
        assert!(rx.try_pop().is_none());
    }

    #[test]
    fn try_send_drop_oldest_keeps_latest() {
        let (q, rx) = FallbackQueue::new(2, OnMute::DropOldest);
        q.try_send(Message::single("a")).unwrap();
        q.try_send(Message::single("b")).unwrap();
        q.try_send(Message::single("c")).unwrap();
        let got_b = rx.try_pop().unwrap();
        let got_c = rx.try_pop().unwrap();
        assert_eq!(got_b.part_bytes(0).unwrap(), &b"b"[..]);
        assert_eq!(got_c.part_bytes(0).unwrap(), &b"c"[..]);
    }

    #[tokio::test]
    async fn try_send_wakes_receiver() {
        let (q, rx) = FallbackQueue::new(4, OnMute::Block);
        let recv_task = tokio::spawn(async move { rx.recv().await });
        tokio::task::yield_now().await;
        q.try_send(Message::single("hello")).unwrap();
        let msg = recv_task.await.unwrap().unwrap();
        assert_eq!(msg.part_bytes(0).unwrap(), &b"hello"[..]);
    }
}

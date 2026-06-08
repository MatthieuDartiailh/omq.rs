//! Lock-free bounded send queue with configurable drop policy.
//!
//! Backed by `concurrent_queue::ConcurrentQueue` (lock-free ring) and
//! `tokio::sync::Notify` for receiver wakeup. The `Block` policy additionally
//! uses a `tokio::sync::Semaphore` to track available write slots so blocked
//! senders are woken without spinning when a receiver pops.

use std::sync::Arc;

use concurrent_queue::{ConcurrentQueue, PushError};
use tokio::sync::{Notify, Semaphore};

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::OnMute;

struct Inner {
    queue: ConcurrentQueue<Message>,
    /// Notified on every successful push; wakes receivers waiting in `recv`.
    recv_notify: Notify,
    /// Tracks available write slots for `Block` policy. `None` for the other
    /// two policies (they never block on full).
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
pub(crate) struct DropQueue {
    inner: Arc<Inner>,
    policy: OnMute,
}

/// Cloneable receive handle for a [`DropQueue`]. Each clone shares the same
/// underlying queue; any clone can pop the next available message.
#[derive(Clone, Debug)]
pub(crate) struct QueueReceiver {
    inner: Arc<Inner>,
}

impl DropQueue {
    /// Create a new queue. Returns `(sender_handle, receiver_handle)`.
    ///
    /// `capacity == usize::MAX` creates an unbounded queue (no `Semaphore`).
    /// Otherwise the queue is bounded to `capacity.max(1)`.
    pub(crate) fn new(capacity: usize, policy: OnMute) -> (Self, QueueReceiver) {
        let (queue, slots) = if capacity == usize::MAX {
            (ConcurrentQueue::unbounded(), None)
        } else {
            let cap = capacity.max(1);
            (ConcurrentQueue::bounded(cap), Some(Semaphore::new(cap)))
        };
        let inner = Arc::new(Inner {
            queue,
            recv_notify: Notify::new(),
            slots,
        });
        let receiver = QueueReceiver {
            inner: inner.clone(),
        };
        (Self { inner, policy }, receiver)
    }

    /// Submit a message. Behaviour depends on policy:
    /// - `Block`: await until a slot is available, then push.
    /// - `DropNewest`: if full, discard `msg` silently and return `Ok`.
    /// - `DropOldest`: if full, pop the head to make room, then push.
    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        match self.policy {
            OnMute::Block => {
                if let Some(ref slots) = self.inner.slots {
                    // Bounded queue: wait for a free slot before pushing.
                    // Consume the permit without returning it on drop; the
                    // corresponding `add_permits(1)` call happens in `try_pop`.
                    let permit = slots.acquire().await.map_err(|_| Error::Closed)?;
                    permit.forget();
                }
                match self.inner.queue.push(msg) {
                    Ok(()) => {
                        self.inner.recv_notify.notify_one();
                        Ok(())
                    }
                    Err(PushError::Closed(_)) => Err(Error::Closed),
                    Err(PushError::Full(_)) => unreachable!("permit guarantees a free slot"),
                }
            }
            OnMute::DropNewest => {
                match self.inner.queue.push(msg) {
                    Ok(()) => self.inner.recv_notify.notify_one(),
                    Err(PushError::Full(_)) => {}
                    Err(PushError::Closed(_)) => return Err(Error::Closed),
                }
                Ok(())
            }
            OnMute::DropOldest => {
                let mut item = msg;
                loop {
                    match self.inner.queue.push(item) {
                        Ok(()) => {
                            self.inner.recv_notify.notify_one();
                            return Ok(());
                        }
                        Err(PushError::Full(back)) => {
                            let _ = self.inner.queue.pop();
                            item = back;
                        }
                        Err(PushError::Closed(_)) => return Err(Error::Closed),
                    }
                }
            }
            _ => unreachable!(),
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
                match self.inner.queue.push(msg) {
                    Ok(()) => {
                        self.inner.recv_notify.notify_one();
                        Ok(())
                    }
                    Err(PushError::Closed(m)) => Err(m),
                    Err(PushError::Full(_)) => unreachable!("permit guarantees a free slot"),
                }
            }
            OnMute::DropNewest => {
                match self.inner.queue.push(msg) {
                    Ok(()) => self.inner.recv_notify.notify_one(),
                    Err(PushError::Full(_)) => {}
                    Err(PushError::Closed(m)) => return Err(m),
                }
                Ok(())
            }
            OnMute::DropOldest => {
                let mut item = msg;
                loop {
                    match self.inner.queue.push(item) {
                        Ok(()) => {
                            self.inner.recv_notify.notify_one();
                            return Ok(());
                        }
                        Err(PushError::Full(back)) => {
                            let _ = self.inner.queue.pop();
                            item = back;
                        }
                        Err(PushError::Closed(m)) => return Err(m),
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.queue.len()
    }

    /// Close the queue and wake all waiting receivers so they can observe
    /// the closed state and return.
    #[allow(dead_code)]
    pub(crate) fn close(&self) {
        self.inner.queue.close();
        self.inner.recv_notify.notify_waiters();
    }
}

impl QueueReceiver {
    /// Non-blocking pop. Returns the next message, or `None` if empty.
    ///
    /// For `Block`-policy queues, also releases one write slot so any sender
    /// waiting in `DropQueue::send` can proceed.
    pub(crate) fn try_pop(&self) -> Option<Message> {
        self.inner.queue.pop().ok()
    }

    pub(crate) fn release_permits(&self, n: usize) {
        if let Some(ref slots) = self.inner.slots {
            slots.add_permits(n);
        }
    }

    /// Async pop. Waits until a message is available or the queue is closed.
    ///
    /// Uses a double-check pattern around `recv_notify.notified()` to avoid
    /// missing a push that arrives between an empty `try_pop` and the future
    /// being polled.
    pub(crate) async fn recv(&self) -> Option<Message> {
        loop {
            let notified = self.inner.recv_notify.notified();
            if let Some(msg) = self.try_pop() {
                return Some(msg);
            }
            if self.inner.queue.is_closed() && self.inner.queue.is_empty() {
                return None;
            }
            tokio::select! {
                biased;
                () = notified => {}
                () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omq_proto::options::OnMute;

    #[tokio::test]
    async fn block_policy_backpressures() {
        let (q, rx) = DropQueue::new(1, OnMute::Block);
        q.send(Message::single("a")).await.unwrap();
        // Second send should block; confirm via short timeout.
        let r = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            q.send(Message::single("b")),
        )
        .await;
        assert!(r.is_err(), "second send should block on full queue");
        // Pop + release unblocks a waiting sender.
        let _ = rx.try_pop().unwrap();
        rx.release_permits(1);
    }

    #[tokio::test]
    async fn drop_newest_silent() {
        let (q, rx) = DropQueue::new(1, OnMute::DropNewest);
        q.send(Message::single("a")).await.unwrap();
        q.send(Message::single("b")).await.unwrap();
        q.send(Message::single("c")).await.unwrap();
        let got = rx.try_pop().unwrap();
        assert_eq!(got.part_bytes(0).unwrap(), &b"a"[..]);
        assert!(rx.try_pop().is_none());
    }

    #[tokio::test]
    async fn drop_oldest_keeps_latest() {
        let (q, rx) = DropQueue::new(2, OnMute::DropOldest);
        q.send(Message::single("a")).await.unwrap();
        q.send(Message::single("b")).await.unwrap();
        q.send(Message::single("c")).await.unwrap(); // drops "a"
        q.send(Message::single("d")).await.unwrap(); // drops "b"
        let got_c = rx.try_pop().unwrap();
        let got_d = rx.try_pop().unwrap();
        assert_eq!(got_c.part_bytes(0).unwrap(), &b"c"[..]);
        assert_eq!(got_d.part_bytes(0).unwrap(), &b"d"[..]);
    }

    #[tokio::test]
    async fn recv_wakes_on_push() {
        let (q, rx) = DropQueue::new(4, OnMute::Block);
        let recv_task = tokio::spawn(async move { rx.recv().await });
        tokio::task::yield_now().await;
        q.send(Message::single("hello")).await.unwrap();
        let msg = recv_task.await.unwrap().unwrap();
        assert_eq!(msg.part_bytes(0).unwrap(), &b"hello"[..]);
    }

    #[tokio::test]
    async fn close_unblocks_recv() {
        let (q, rx) = DropQueue::new(4, OnMute::Block);
        let recv_task = tokio::spawn(async move { rx.recv().await });
        tokio::task::yield_now().await;
        q.close();
        let result = recv_task.await.unwrap();
        assert!(result.is_none(), "closed queue should return None");
    }

    #[test]
    fn try_send_block_succeeds_when_space() {
        let (q, rx) = DropQueue::new(2, OnMute::Block);
        q.try_send(Message::single("a")).unwrap();
        q.try_send(Message::single("b")).unwrap();
        let got = rx.try_pop().unwrap();
        assert_eq!(got.part_bytes(0).unwrap(), &b"a"[..]);
        rx.release_permits(1);
    }

    #[test]
    fn try_send_block_returns_err_when_full() {
        let (q, _rx) = DropQueue::new(1, OnMute::Block);
        q.try_send(Message::single("a")).unwrap();
        let err = q.try_send(Message::single("b")).unwrap_err();
        assert_eq!(err.part_bytes(0).unwrap(), &b"b"[..]);
    }

    #[test]
    fn try_send_drop_newest_silent() {
        let (q, rx) = DropQueue::new(1, OnMute::DropNewest);
        q.try_send(Message::single("a")).unwrap();
        q.try_send(Message::single("b")).unwrap();
        let got = rx.try_pop().unwrap();
        assert_eq!(got.part_bytes(0).unwrap(), &b"a"[..]);
        assert!(rx.try_pop().is_none());
    }

    #[test]
    fn try_send_drop_oldest_keeps_latest() {
        let (q, rx) = DropQueue::new(2, OnMute::DropOldest);
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
        let (q, rx) = DropQueue::new(4, OnMute::Block);
        let recv_task = tokio::spawn(async move { rx.recv().await });
        tokio::task::yield_now().await;
        q.try_send(Message::single("hello")).unwrap();
        let msg = recv_task.await.unwrap().unwrap();
        assert_eq!(msg.part_bytes(0).unwrap(), &b"hello"[..]);
    }
}

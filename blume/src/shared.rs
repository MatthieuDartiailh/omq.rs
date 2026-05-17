use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use event_listener::{Event, Listener};

use crate::error::{RecvError, SendError, TryRecvError, TrySendError};
use crate::receiver::Receiver;
use crate::sender::Sender;

pub(crate) struct Inner<T> {
    pub(crate) queue: VecDeque<T>,
    pub(crate) closed_recv: bool,
}

pub(crate) struct Shared<T> {
    pub(crate) inner: Mutex<Inner<T>>,
    pub(crate) capacity: usize,
    pub(crate) sender_count: AtomicUsize,
    pub(crate) recv_event: Event,
    pub(crate) send_event: Event,
}

impl<T> Shared<T> {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                queue: VecDeque::new(),
                closed_recv: false,
            }),
            capacity,
            sender_count: AtomicUsize::new(1),
            recv_event: Event::new(),
            send_event: Event::new(),
        })
    }

    pub(crate) fn try_send(&self, val: T) -> Result<(), TrySendError<T>> {
        let mut inner = self.inner.lock().expect("blume: poisoned");
        if inner.closed_recv {
            return Err(TrySendError::Disconnected(val));
        }
        if inner.queue.len() >= self.capacity {
            return Err(TrySendError::Full(val));
        }
        let was_empty = inner.queue.is_empty();
        inner.queue.push_back(val);
        drop(inner);
        if was_empty {
            self.recv_event.notify(1);
        }
        Ok(())
    }

    pub(crate) async fn send_async(&self, val: T) -> Result<(), SendError<T>> {
        let mut val = val;
        loop {
            match self.try_send(val) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(v)) => return Err(SendError(v)),
                Err(TrySendError::Full(v)) => val = v,
            }
            let listener = self.send_event.listen();
            // Double-check after registering listener.
            match self.try_send(val) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(v)) => return Err(SendError(v)),
                Err(TrySendError::Full(v)) => val = v,
            }
            listener.await;
        }
    }

    pub(crate) fn send_blocking(&self, val: T) -> Result<(), SendError<T>> {
        let mut val = val;
        loop {
            match self.try_send(val) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(v)) => return Err(SendError(v)),
                Err(TrySendError::Full(v)) => val = v,
            }
            let listener = self.send_event.listen();
            match self.try_send(val) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(v)) => return Err(SendError(v)),
                Err(TrySendError::Full(v)) => val = v,
            }
            listener.wait();
        }
    }

    pub(crate) fn try_drain(&self, cache: &mut VecDeque<T>) -> Result<bool, RecvError> {
        let mut inner = self.inner.lock().expect("blume: poisoned");
        if inner.queue.is_empty() {
            return if self.all_senders_dropped() {
                Err(RecvError)
            } else {
                Ok(false)
            };
        }
        let was_full = inner.queue.len() >= self.capacity;
        std::mem::swap(cache, &mut inner.queue);
        drop(inner);
        if was_full {
            self.send_event.notify(usize::MAX);
        }
        Ok(true)
    }

    pub(crate) fn try_recv_one(&self, cache: &mut VecDeque<T>) -> Result<T, TryRecvError> {
        if let Some(val) = cache.pop_front() {
            return Ok(val);
        }
        match self.try_drain(cache) {
            Ok(true) => cache.pop_front().ok_or(TryRecvError::Empty),
            Ok(false) => Err(TryRecvError::Empty),
            Err(RecvError) => Err(TryRecvError::Disconnected),
        }
    }

    pub(crate) fn all_senders_dropped(&self) -> bool {
        self.sender_count.load(Ordering::Acquire) == 0
    }

    pub(crate) fn is_send_empty(&self) -> bool {
        let inner = self.inner.lock().expect("blume: poisoned");
        inner.queue.is_empty()
    }

    pub(crate) fn is_recv_disconnected(&self) -> bool {
        let inner = self.inner.lock().expect("blume: poisoned");
        inner.closed_recv
    }
}

/// Create a bounded channel with the given capacity.
pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    assert!(cap > 0, "blume: bounded capacity must be > 0");
    let shared = Shared::new(cap);
    (Sender::new(Arc::clone(&shared)), Receiver::new(shared))
}

/// Create an unbounded channel (grows without backpressure).
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Shared::new(usize::MAX);
    (Sender::new(Arc::clone(&shared)), Receiver::new(shared))
}

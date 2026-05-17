use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::error::{SendError, TrySendError};
use crate::shared::Shared;

/// Sending half of a blume channel. Cloneable (MPSC).
pub struct Sender<T> {
    pub(crate) shared: Arc<Shared<T>>,
}

impl<T> Sender<T> {
    pub(crate) fn new(shared: Arc<Shared<T>>) -> Self {
        Self { shared }
    }

    /// Send a value, waiting asynchronously if the channel is full.
    pub async fn send_async(&self, val: T) -> Result<(), SendError<T>> {
        self.shared.send_async(val).await
    }

    /// Try to send without blocking. Fails if full or disconnected.
    pub fn try_send(&self, val: T) -> Result<(), TrySendError<T>> {
        self.shared.try_send(val)
    }

    /// Send a value, blocking the current thread if the channel is full.
    pub fn send(&self, val: T) -> Result<(), SendError<T>> {
        self.shared.send_blocking(val)
    }

    /// Whether the receiver has been dropped.
    pub fn is_disconnected(&self) -> bool {
        self.shared.is_recv_disconnected()
    }

    /// Whether the send-side queue is empty.
    pub fn is_empty(&self) -> bool {
        self.shared.is_send_empty()
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.sender_count.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.shared.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            drop(self.shared.inner.lock().expect("blume: poisoned"));
            self.shared.recv_event.notify(usize::MAX);
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

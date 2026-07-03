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

    /// Current number of values queued on the send side.
    pub fn len(&self) -> usize {
        self.shared.send_len()
    }

    /// Close the channel from the send side and drop queued values.
    ///
    /// Used by socket teardown when the receiver task may still be alive but
    /// its queued payloads are no longer deliverable.
    pub fn close(&self) {
        let queue = self.shared.close_recv();
        self.shared.send_event.notify(usize::MAX);
        self.shared.recv_event.notify(usize::MAX);
        drop(queue);
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared
            .sender_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                count.checked_add(1)
            })
            .expect("blume: sender count overflow");
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.shared.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Lock+drop synchronizes with try_drain: guarantees that any
            // in-flight send() has completed and its items are visible
            // before we notify the receiver of channel closure.
            drop(self.shared.lock_inner());
            self.shared.recv_event.notify(usize::MAX);
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, Mutex};

    use event_listener::Event;

    use super::*;
    use crate::shared::{Inner, Shared};

    #[test]
    #[should_panic(expected = "blume: sender count overflow")]
    fn clone_panics_on_sender_count_overflow() {
        let sender = Sender::<()>::new(Arc::new(Shared {
            inner: Mutex::new(Inner {
                queue: VecDeque::new(),
                closed_recv: false,
            }),
            capacity: 1,
            queued: AtomicUsize::new(0),
            sender_count: AtomicUsize::new(usize::MAX),
            recv_event: Event::new(),
            send_event: Event::new(),
        }));

        let _ = sender.clone();
    }
}

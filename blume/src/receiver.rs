use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

use crate::error::{RecvError, TryRecvError};
use crate::shared::Shared;

/// Receiving half of a blume channel. Not cloneable (single consumer).
pub struct Receiver<T> {
    pub(crate) shared: Arc<Shared<T>>,
    cache: RefCell<VecDeque<T>>,
}

impl<T> Receiver<T> {
    pub(crate) fn new(shared: Arc<Shared<T>>) -> Self {
        Self {
            shared,
            cache: RefCell::new(VecDeque::new()),
        }
    }

    /// Try to receive one value without blocking.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut cache = self.cache.borrow_mut();
        self.shared.try_recv_one(&mut cache)
    }

    /// Receive one value, waiting asynchronously until available.
    pub async fn recv_async(&self) -> Result<T, RecvError> {
        {
            let mut cache = self.cache.borrow_mut();
            match self.shared.try_recv_one(&mut cache) {
                Ok(val) => return Ok(val),
                Err(TryRecvError::Disconnected) => return Err(RecvError),
                Err(TryRecvError::Empty) => {}
            }
        }

        loop {
            let listener = self.shared.recv_event.listen();

            {
                let mut cache = self.cache.borrow_mut();
                match self.shared.try_recv_one(&mut cache) {
                    Ok(val) => return Ok(val),
                    Err(TryRecvError::Disconnected) => return Err(RecvError),
                    Err(TryRecvError::Empty) => {}
                }
            }

            listener.await;
        }
    }

    /// Drain all pending values into `out` in one swap. Waits if empty.
    /// Returns the number of newly drained items.
    pub async fn recv_batch(&self, out: &mut Vec<T>) -> Result<usize, RecvError> {
        let before = out.len();
        {
            let mut cache = self.cache.borrow_mut();
            if Self::drain_cache_into(&mut cache, out) > 0 {
                return Ok(out.len() - before);
            }
            match self.shared.try_drain(&mut cache) {
                Ok(true) => {
                    Self::drain_cache_into(&mut cache, out);
                    return Ok(out.len() - before);
                }
                Ok(false) => {}
                Err(RecvError) => return Err(RecvError),
            }
        }

        loop {
            let listener = self.shared.recv_event.listen();

            {
                let mut cache = self.cache.borrow_mut();
                match self.shared.try_drain(&mut cache) {
                    Ok(true) => {
                        Self::drain_cache_into(&mut cache, out);
                        return Ok(out.len() - before);
                    }
                    Ok(false) => {}
                    Err(RecvError) => return Err(RecvError),
                }
            }

            listener.await;
        }
    }

    /// Whether both the local cache and the shared queue are empty.
    pub fn is_empty(&self) -> bool {
        let cache = self.cache.borrow();
        cache.is_empty() && self.shared.is_send_empty()
    }

    /// Signal senders that the receiver is closed. Subsequent and
    /// in-flight `send_async` calls will return `SendError`.
    pub fn close(&self) {
        let mut inner = self.shared.inner.lock().expect("blume: poisoned");
        inner.closed_recv = true;
        drop(inner);
        self.shared.send_event.notify(usize::MAX);
    }

    fn drain_cache_into(cache: &mut VecDeque<T>, out: &mut Vec<T>) -> usize {
        let n = cache.len();
        out.reserve(n);
        out.extend(cache.drain(..));
        n
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut inner = self.shared.inner.lock().expect("blume: poisoned");
        inner.closed_recv = true;
        drop(inner);
        self.shared.send_event.notify(usize::MAX);
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

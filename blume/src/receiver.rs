use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::error::{RecvError, TryRecvError};
use crate::shared::Shared;

pub struct Receiver<T> {
    pub(crate) shared: Arc<Shared<T>>,
    cache: Mutex<VecDeque<T>>,
}

impl<T> Receiver<T> {
    pub(crate) fn new(shared: Arc<Shared<T>>) -> Self {
        Self {
            shared,
            cache: Mutex::new(VecDeque::new()),
        }
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut cache = self.cache.lock().expect("blume: poisoned");
        self.shared.try_recv_one(&mut cache)
    }

    pub async fn recv_async(&self) -> Result<T, RecvError> {
        {
            let mut cache = self.cache.lock().expect("blume: poisoned");
            match self.shared.try_recv_one(&mut cache) {
                Ok(val) => return Ok(val),
                Err(TryRecvError::Disconnected) => return Err(RecvError),
                Err(TryRecvError::Empty) => {}
            }
        }

        loop {
            let listener = self.shared.recv_event.listen();

            {
                let mut cache = self.cache.lock().expect("blume: poisoned");
                match self.shared.try_recv_one(&mut cache) {
                    Ok(val) => return Ok(val),
                    Err(TryRecvError::Disconnected) => return Err(RecvError),
                    Err(TryRecvError::Empty) => {}
                }
            }

            listener.await;
        }
    }

    pub async fn recv_batch(&self, out: &mut Vec<T>) -> Result<usize, RecvError> {
        {
            let mut cache = self.cache.lock().expect("blume: poisoned");
            if Self::drain_cache_into(&mut cache, out) > 0 {
                return Ok(out.len());
            }
            match self.shared.try_drain(&mut cache) {
                Ok(true) => {
                    Self::drain_cache_into(&mut cache, out);
                    return Ok(out.len());
                }
                Ok(false) => {}
                Err(RecvError) => return Err(RecvError),
            }
        }

        loop {
            let listener = self.shared.recv_event.listen();

            {
                let mut cache = self.cache.lock().expect("blume: poisoned");
                match self.shared.try_drain(&mut cache) {
                    Ok(true) => {
                        Self::drain_cache_into(&mut cache, out);
                        return Ok(out.len());
                    }
                    Ok(false) => {}
                    Err(RecvError) => return Err(RecvError),
                }
            }

            listener.await;
        }
    }

    pub fn is_empty(&self) -> bool {
        let cache = self.cache.lock().expect("blume: poisoned");
        cache.is_empty() && self.shared.is_send_empty()
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

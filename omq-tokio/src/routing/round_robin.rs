//! Round-robin send with fair deactivation.
//!
//! Peers register active per-peer yring pipes. The socket submitter
//! scans active pipes from a moving cursor and sends to the first pipe
//! with capacity. Full pipes move to an inactive list and are reactivated
//! when the consumer drains below LWM. Active order stays stable so a full
//! pipe cannot reorder and bias the cursor toward another peer.

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use crate::engine::{PeerDriverHandle, SendPipeError, SendPipeProducer};
use omq_proto::error::Result;
use omq_proto::message::Message;
use omq_proto::options::Options;

use super::fallback_queue::{FallbackQueue, FallbackReceiver};

#[derive(Debug)]
struct ActivePipe {
    peer_id: u64,
    tx: SendPipeProducer,
}

#[derive(Debug)]
struct ActivePipes {
    active: Vec<ActivePipe>,
    inactive: Vec<ActivePipe>,
    pipe_peers: HashSet<u64>,
    fallback_peers: HashSet<u64>,
    cursor: usize,
    random_state: u64,
    inactive_cursor: usize,
    last_reactivation_probe: Instant,
}

const REACTIVATION_PROBE_INTERVAL: Duration = Duration::from_millis(1);
const REACTIVATION_PROBE_BATCH: usize = 8;

impl Default for ActivePipes {
    fn default() -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(1, |d| d.as_secs() ^ u64::from(d.subsec_nanos()))
            .max(1);
        Self {
            active: Vec::new(),
            inactive: Vec::new(),
            pipe_peers: HashSet::new(),
            fallback_peers: HashSet::new(),
            cursor: 0,
            random_state: seed,
            inactive_cursor: 0,
            last_reactivation_probe: Instant::now(),
        }
    }
}

impl ActivePipes {
    fn clear(&mut self) {
        for pipe in &self.active {
            pipe.tx.space_available().notify_waiters();
        }
        for pipe in &self.inactive {
            pipe.tx.space_available().notify_waiters();
        }
        self.active.clear();
        self.inactive.clear();
        self.pipe_peers.clear();
        self.fallback_peers.clear();
        self.cursor = 0;
        self.inactive_cursor = 0;
        self.last_reactivation_probe = Instant::now();
    }

    fn deactivate(&mut self, pos: usize) {
        let was_empty = self.inactive.is_empty();
        let pipe = self.active.remove(pos);
        self.inactive.push(pipe);
        if was_empty {
            self.inactive_cursor = self.random_index(self.inactive.len());
        }
        if self.active.is_empty() {
            self.cursor = 0;
        } else {
            if pos < self.cursor {
                self.cursor -= 1;
            }
            if self.cursor >= self.active.len() {
                self.cursor = 0;
            }
        }
    }

    fn reactivation_probe_due(&mut self) -> bool {
        if self.inactive.is_empty() {
            return false;
        }
        if self.last_reactivation_probe.elapsed() >= REACTIVATION_PROBE_INTERVAL {
            self.last_reactivation_probe = Instant::now();
            true
        } else {
            false
        }
    }

    fn try_reactivate_one(&mut self) {
        let Some(len) = (!self.inactive.is_empty()).then_some(self.inactive.len()) else {
            return;
        };
        let i = self.inactive_cursor % len;
        self.inactive_cursor = (i + 1) % len;
        if self.inactive[i].tx.above_lwm.load(Ordering::Acquire) {
            return;
        }
        let pipe = self.inactive.remove(i);
        self.active.push(pipe);
        if self.inactive.is_empty() {
            self.inactive_cursor = 0;
        } else if i < self.inactive_cursor {
            self.inactive_cursor -= 1;
        }
    }

    fn try_reactivate_batch(&mut self) {
        for _ in 0..REACTIVATION_PROBE_BATCH {
            if self.inactive.is_empty() {
                break;
            }
            self.try_reactivate_one();
        }
    }

    fn try_reactivate_when_empty(&mut self) {
        let probes = self.inactive.len();
        for _ in 0..probes {
            self.try_reactivate_one();
            if !self.active.is_empty() {
                break;
            }
        }
    }

    fn remove_peer(&mut self, peer_id: u64) {
        self.pipe_peers.remove(&peer_id);
        self.fallback_peers.remove(&peer_id);
        if let Some(pos) = self.active.iter().position(|p| p.peer_id == peer_id) {
            self.active.remove(pos);
            if self.active.is_empty() {
                self.cursor = 0;
            } else {
                if pos < self.cursor {
                    self.cursor -= 1;
                }
                if self.cursor >= self.active.len() {
                    self.cursor = 0;
                }
            }
        } else if let Some(pos) = self.inactive.iter().position(|p| p.peer_id == peer_id) {
            self.inactive.remove(pos);
            if self.inactive.is_empty() {
                self.inactive_cursor = 0;
            } else {
                self.inactive_cursor %= self.inactive.len();
            }
        }
    }

    fn should_use_fallback(&self) -> bool {
        (self.active.is_empty() && self.inactive.is_empty()) || !self.fallback_peers.is_empty()
    }

    fn random_index(&mut self, len: usize) -> usize {
        let mut x = self.random_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.random_state = x.max(1);
        (x as usize) % len
    }
}

/// Cloneable handle for submitting messages into a [`RoundRobinSend`].
#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    queue: FallbackQueue,
    active: Arc<Mutex<ActivePipes>>,
}

impl Submitter {
    pub(crate) fn shutdown(&self) {
        self.queue.shutdown();
        let mut active = self.active.lock().expect("round_robin active");
        active.clear();
    }

    pub(crate) async fn send(&self, mut msg: Message) -> Result<()> {
        loop {
            match self.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(omq_proto::error::TrySendError::Full(returned)) => {
                    msg = returned;
                }
                Err(omq_proto::error::TrySendError::Error(e)) => return Err(e),
                Err(omq_proto::error::TrySendError::Closed) => {
                    return Err(omq_proto::error::Error::Closed);
                }
            }

            let space_available = {
                let mut active = self.active.lock().expect("round_robin active");
                if active.should_use_fallback() {
                    None
                } else {
                    active.next_space_notify_any()
                }
            };

            let Some(space_available) = space_available else {
                return self.queue.send(msg).await;
            };

            let notified = space_available.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(omq_proto::error::TrySendError::Full(returned)) => {
                    msg = returned;
                }
                Err(omq_proto::error::TrySendError::Error(e)) => return Err(e),
                Err(omq_proto::error::TrySendError::Closed) => {
                    return Err(omq_proto::error::Error::Closed);
                }
            }
            notified.await;
        }
    }

    pub(crate) fn try_send(
        &self,
        mut msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        let mut active = self.active.lock().expect("round_robin active");
        if active.should_use_fallback() {
            return self
                .queue
                .try_send(msg)
                .map_err(omq_proto::error::TrySendError::Full);
        }

        if active.active.is_empty() {
            // No pipe can make progress until one inactive pipe crosses LWM.
            // Probe the whole rotating list only in this stalled state.
            active.try_reactivate_when_empty();
        } else if active.reactivation_probe_due() {
            active.try_reactivate_batch();
        }

        let mut scanned = 0usize;
        while scanned < active.active.len() {
            let i = active.cursor;
            active.cursor += 1;
            if active.cursor >= active.active.len() {
                active.cursor = 0;
            }
            scanned += 1;
            match active.active[i].tx.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(SendPipeError::Full(returned)) => {
                    msg = returned;
                    active.deactivate(i);
                    if active.active.is_empty() {
                        break;
                    }
                    // Position i now holds the next stable-order pipe.
                    scanned = scanned.saturating_sub(1);
                }
                Err(SendPipeError::Closed(returned)) => {
                    let peer_id = active.active[i].peer_id;
                    active.pipe_peers.remove(&peer_id);
                    active.active.remove(i);
                    msg = returned;
                    if active.active.is_empty() {
                        active.cursor = 0;
                        break;
                    }
                    if i < active.cursor {
                        active.cursor -= 1;
                    }
                    if active.cursor >= active.active.len() {
                        active.cursor = 0;
                    }
                    scanned = scanned.saturating_sub(1);
                }
            }
        }

        Err(omq_proto::error::TrySendError::Full(msg))
    }
}

impl ActivePipes {
    fn next_space_notify_any(&mut self) -> Option<Arc<Notify>> {
        // Prefer inactive pipes: they are the ones we're waiting on.
        for pipe in &self.inactive {
            if pipe.tx.is_alive() {
                return Some(pipe.tx.space_available());
            }
        }
        // Fallback: scan active pipes (rare: all active hit Full this
        // call but haven't been deactivated yet).
        let mut scanned = 0usize;
        while scanned < self.active.len() {
            let i = self.cursor % self.active.len();
            self.cursor = (i + 1) % self.active.len();
            scanned += 1;
            if self.active[i].tx.is_alive() {
                return Some(self.active[i].tx.space_available());
            }
            let peer_id = self.active[i].peer_id;
            self.pipe_peers.remove(&peer_id);
            self.active.remove(i);
            if self.active.is_empty() {
                self.cursor = 0;
                return None;
            }
            if i < self.cursor {
                self.cursor -= 1;
            }
            if self.cursor >= self.active.len() {
                self.cursor = 0;
            }
            scanned = scanned.saturating_sub(1);
        }
        None
    }
}

/// Round-robin send strategy.
#[derive(Debug)]
pub(crate) struct RoundRobinSend {
    queue: FallbackQueue,
    shared_rx: FallbackReceiver,
    active: Arc<Mutex<ActivePipes>>,
    peer_count: usize,
}

impl RoundRobinSend {
    pub(crate) fn new(options: &Options) -> Self {
        let (cap, policy) = super::effective_queue_params(options);
        let (queue, shared_rx) = FallbackQueue::new(cap, policy);
        Self {
            queue,
            shared_rx,
            active: Arc::new(Mutex::new(ActivePipes::default())),
            peer_count: 0,
        }
    }

    /// Returns a clone of the shared receive end. Each connection driver
    /// calls this once and holds the clone for the lifetime of the connection.
    pub(crate) fn shared_rx(&self) -> FallbackReceiver {
        self.shared_rx.clone()
    }

    pub(crate) fn connection_added(
        &mut self,
        peer_id: u64,
        handle: &PeerDriverHandle,
        _is_inproc: bool,
    ) {
        self.peer_count += 1;
        self.shared_rx.set_peer_count(self.peer_count);

        let send_pipe = handle
            .send_pipe
            .as_ref()
            .and_then(|pipe| pipe.lock().expect("round_robin send pipe").take());

        let mut active = self.active.lock().expect("round_robin active");
        if let Some(tx) = send_pipe {
            active.remove_peer(peer_id);
            active.pipe_peers.insert(peer_id);
            let pos = {
                let len = active.active.len() + 1;
                active.random_index(len)
            };
            active.active.insert(pos, ActivePipe { peer_id, tx });
            return;
        }
        active.fallback_peers.insert(peer_id);
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        self.peer_count = self.peer_count.saturating_sub(1);
        self.shared_rx.set_peer_count(self.peer_count);
        self.active
            .lock()
            .expect("round_robin active")
            .remove_peer(peer_id);
    }

    /// Cloneable handle for enqueuing from a spawned task. Lets the socket
    /// driver hand off `Send` command handling so the actor loop never
    /// blocks on HWM backpressure.
    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            queue: self.queue.clone(),
            active: self.active.clone(),
        }
    }

    pub(crate) fn shutdown(&self) {
        self.queue.shutdown();
        let mut active = self.active.lock().expect("round_robin active");
        active.clear();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let guard = self.active.lock().expect("round_robin active");
        let active_empty = guard.active.iter().all(|pipe| pipe.tx.is_empty());
        let inactive_empty = guard.inactive.iter().all(|pipe| pipe.tx.is_empty());
        self.queue.len() == 0 && active_empty && inactive_empty
    }
}

#[cfg(test)]
mod tests {
    use super::{ActivePipe, ActivePipes};
    use crate::engine::send_pipe::send_pipe;

    #[test]
    fn deactivation_preserves_active_peer_order() {
        let mut pipes = ActivePipes::default();
        for peer_id in 0..4 {
            pipes.active.push(ActivePipe {
                peer_id,
                tx: send_pipe(4).0,
            });
        }
        pipes.cursor = 2;

        pipes.deactivate(1);

        assert_eq!(
            pipes
                .active
                .iter()
                .map(|pipe| pipe.peer_id)
                .collect::<Vec<_>>(),
            vec![0, 2, 3]
        );
        assert_eq!(pipes.cursor, 1);
    }
}

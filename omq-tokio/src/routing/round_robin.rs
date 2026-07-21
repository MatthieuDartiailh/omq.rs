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
}

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
    active_changed: Arc<Notify>,
}

impl Submitter {
    pub(crate) fn shutdown(&self) {
        self.queue.shutdown();
        let mut active = self.active.lock().expect("round_robin active");
        active.clear();
        self.active_changed.notify_waiters();
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
                if self.queue.is_closed() {
                    return Err(omq_proto::error::Error::Closed);
                }
                let queue_space = self.queue.space_notified();
                let active_changed = self.active_changed.notified();
                tokio::pin!(queue_space);
                tokio::pin!(active_changed);
                queue_space.as_mut().enable();
                active_changed.as_mut().enable();

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

                tokio::select! {
                    () = queue_space => {}
                    () = active_changed => {}
                }
                continue;
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

    pub(crate) async fn wait_send_progress(&self) {
        let space_available = {
            let mut active = self.active.lock().expect("round_robin active");
            if active.should_use_fallback() {
                None
            } else {
                active.next_space_notify_any()
            }
        };

        let Some(space_available) = space_available else {
            if self.queue.is_closed() {
                return;
            }
            let queue_space = self.queue.space_notified();
            let active_changed = self.active_changed.notified();
            tokio::pin!(queue_space);
            tokio::pin!(active_changed);
            queue_space.as_mut().enable();
            active_changed.as_mut().enable();
            tokio::select! {
                () = queue_space => {}
                () = active_changed => {}
            }
            return;
        };

        space_available.notified().await;
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
        } else if !active.inactive.is_empty() {
            active.try_reactivate_one();
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
    active_changed: Arc<Notify>,
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
            active_changed: Arc::new(Notify::new()),
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
            self.active_changed.notify_waiters();
            return;
        }
        active.fallback_peers.insert(peer_id);
        self.active_changed.notify_waiters();
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        self.peer_count = self.peer_count.saturating_sub(1);
        self.shared_rx.set_peer_count(self.peer_count);
        self.active
            .lock()
            .expect("round_robin active")
            .remove_peer(peer_id);
        self.active_changed.notify_waiters();
    }

    /// Cloneable handle for enqueuing from a spawned task. Lets the socket
    /// driver hand off `Send` command handling so the actor loop never
    /// blocks on HWM backpressure.
    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            queue: self.queue.clone(),
            active: self.active.clone(),
            active_changed: self.active_changed.clone(),
        }
    }

    pub(crate) fn shutdown(&self) {
        self.queue.shutdown();
        let mut active = self.active.lock().expect("round_robin active");
        active.clear();
        self.active_changed.notify_waiters();
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
    use super::{ActivePipe, ActivePipes, RoundRobinSend};
    use crate::engine::PeerDriverHandle;
    use crate::engine::send_pipe::send_pipe;
    use omq_proto::message::Message;
    use omq_proto::options::Options;
    use tokio_util::sync::CancellationToken;

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

    #[test]
    fn inactive_pipe_reactivates_without_timer() {
        let (tx0, mut rx0) = send_pipe(2);
        let (tx1, _rx1) = send_pipe(2);
        let mut pipes = ActivePipes::default();
        pipes.active.push(ActivePipe {
            peer_id: 0,
            tx: tx0,
        });
        pipes.active.push(ActivePipe {
            peer_id: 1,
            tx: tx1,
        });

        pipes.active[0].tx.try_send(Message::single("a")).unwrap();
        pipes.active[0].tx.try_send(Message::single("b")).unwrap();
        assert!(matches!(
            pipes.active[0].tx.try_send(Message::single("c")),
            Err(crate::engine::SendPipeError::Full(_))
        ));
        pipes.deactivate(0);
        assert_eq!(pipes.inactive.len(), 1);

        let mut batch = Vec::new();
        assert_eq!(rx0.drain_into(&mut batch, 1, usize::MAX), 1);
        pipes.try_reactivate_one();

        assert_eq!(pipes.inactive.len(), 0);
        assert!(pipes.active.iter().any(|pipe| pipe.peer_id == 0));
    }

    #[tokio::test]
    async fn blocked_fallback_send_retries_pipe_on_activation() {
        let mut send = RoundRobinSend::new(&Options::default().send_hwm(1));
        let submitter = send.submitter();
        submitter.send(Message::single("fallback")).await.unwrap();

        let mut blocked = {
            let submitter = submitter.clone();
            tokio::spawn(async move { submitter.send(Message::single("pipe")).await })
        };
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut blocked)
                .await
                .is_err(),
            "second send should wait while fallback queue is full"
        );

        let (send_pipe, mut send_pipe_rx) = send_pipe(1);
        let (inbox, _inbox_rx) = tokio::sync::mpsc::channel(1);
        let handle = PeerDriverHandle {
            inbox,
            cancel: CancellationToken::new(),
            transmit_slot: None,
            direct_tcp_writer: None,
            send_pipe: Some(std::sync::Arc::new(std::sync::Mutex::new(Some(send_pipe)))),
        };
        send.connection_added(7, &handle, false);

        blocked.await.unwrap().unwrap();

        let shared_rx = send.shared_rx();
        let first = shared_rx.try_pop().unwrap();
        assert_eq!(first.part_bytes(0).unwrap(), &b"fallback"[..]);
        assert!(shared_rx.try_pop().is_none(), "retry must skip fallback");

        let mut batch = Vec::new();
        assert_eq!(send_pipe_rx.drain_into(&mut batch, 1, usize::MAX), 1);
        assert_eq!(batch[0].part_bytes(0).unwrap(), &b"pipe"[..]);
    }
}

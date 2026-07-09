//! Round-robin send.
//!
//! Peers register active per-peer yring pipes. The socket submitter scans
//! those pipes from a moving cursor, sends to the first pipe with capacity,
//! and skips full peers. If every pipe is full, async `send` waits for one
//! selected pipe while `try_send` reports backpressure.

use std::collections::HashSet;
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

#[derive(Debug, Default)]
struct ActivePipes {
    pipes: Vec<ActivePipe>,
    pipe_peers: HashSet<u64>,
    fallback_peers: HashSet<u64>,
    cursor: usize,
}

impl ActivePipes {
    fn clear(&mut self) {
        for pipe in &self.pipes {
            pipe.tx.space_available().notify_waiters();
        }
        self.pipes.clear();
        self.pipe_peers.clear();
        self.fallback_peers.clear();
        self.cursor = 0;
    }

    fn remove_at(&mut self, pos: usize) {
        let peer_id = self.pipes[pos].peer_id;
        self.pipe_peers.remove(&peer_id);
        self.pipes.swap_remove(pos);
        if self.pipes.is_empty() {
            self.cursor = 0;
        } else {
            self.cursor %= self.pipes.len();
        }
    }

    fn remove_peer(&mut self, peer_id: u64) {
        self.pipe_peers.remove(&peer_id);
        self.fallback_peers.remove(&peer_id);
        if let Some(pos) = self.pipes.iter().position(|pipe| pipe.peer_id == peer_id) {
            self.remove_at(pos);
        }
    }

    fn should_use_fallback(&self) -> bool {
        self.pipes.is_empty() || !self.fallback_peers.is_empty()
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

        let mut scanned = 0usize;
        while scanned < active.pipes.len() {
            let i = active.cursor % active.pipes.len();
            active.cursor = (i + 1) % active.pipes.len();
            scanned += 1;
            match active.pipes[i].tx.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(SendPipeError::Full(returned)) => {
                    msg = returned;
                }
                Err(SendPipeError::Closed(returned)) => {
                    active.remove_at(i);
                    msg = returned;
                }
            }
        }

        Err(omq_proto::error::TrySendError::Full(msg))
    }
}

impl ActivePipes {
    fn next_space_notify_any(&mut self) -> Option<Arc<Notify>> {
        if self.pipes.is_empty() {
            return None;
        }

        let mut scanned = 0usize;
        while scanned < self.pipes.len() {
            let i = self.cursor % self.pipes.len();
            self.cursor = (i + 1) % self.pipes.len();
            scanned += 1;
            if self.pipes[i].tx.is_alive() {
                return Some(self.pipes[i].tx.space_available());
            }
            self.remove_at(i);
            if self.pipes.is_empty() {
                return None;
            }
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
            active.pipes.push(ActivePipe { peer_id, tx });
            return;
        }
        active.fallback_peers.insert(peer_id);
        // Handles without a pipe fall back to shared_rx directly. Normal
        // PUSH TCP/IPC/inproc peers all install a yring pipe.
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        self.peer_count = self.peer_count.saturating_sub(1);
        self.shared_rx.set_peer_count(self.peer_count);
        self.active
            .lock()
            .expect("round_robin active")
            .remove_peer(peer_id);
        // Peer tasks self-cancel via their CancellationToken.
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
        let active_empty = self
            .active
            .lock()
            .expect("round_robin active")
            .pipes
            .iter()
            .all(|pipe| pipe.tx.is_empty());
        self.queue.len() == 0 && active_empty
    }
}

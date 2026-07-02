//! Round-robin send.
//!
//! Byte-stream peers register active per-peer pipes. The socket submitter
//! scans those pipes from a moving cursor, sends to the first pipe with
//! capacity, and skips full peers. If every pipe is full, async `send`
//! waits for one selected pipe while `try_send` reports backpressure.
//!
//! Inproc and mixed peer sets use the shared fallback queue so peers without
//! byte-stream driver pipes stay visible to the existing pump path.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

use crate::engine::DriverHandle;
use omq_proto::error::Result;
use omq_proto::message::Message;
use omq_proto::options::Options;

use super::drop_queue::{DropQueue, QueueReceiver};

#[derive(Clone, Debug)]
struct ActivePipe {
    peer_id: u64,
    tx: blume::Sender<Message>,
}

#[derive(Debug, Default)]
struct ActivePipes {
    pipes: Vec<ActivePipe>,
    pipe_peers: HashSet<u64>,
    fallback_peers: HashSet<u64>,
    cursor: usize,
}

impl ActivePipes {
    fn remove_peer(&mut self, peer_id: u64) {
        self.pipe_peers.remove(&peer_id);
        self.fallback_peers.remove(&peer_id);
        if let Some(pos) = self.pipes.iter().position(|pipe| pipe.peer_id == peer_id) {
            self.pipes.swap_remove(pos);
            if self.pipes.is_empty() {
                self.cursor = 0;
            } else {
                self.cursor %= self.pipes.len();
            }
        }
    }

    fn should_use_fallback(&self) -> bool {
        self.pipes.is_empty() || !self.fallback_peers.is_empty()
    }
}

/// Cloneable handle for submitting messages into a [`RoundRobinSend`].
#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    queue: DropQueue,
    active: Arc<Mutex<ActivePipes>>,
}

impl Submitter {
    pub(crate) fn shutdown(&self) {
        self.queue.shutdown();
        let mut active = self.active.lock().expect("round_robin active");
        active.pipes.clear();
        active.pipe_peers.clear();
        active.fallback_peers.clear();
        active.cursor = 0;
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

            let tx = {
                let mut active = self.active.lock().expect("round_robin active");
                if active.should_use_fallback() {
                    None
                } else {
                    active.next_pipe_any()
                }
            };

            let Some(tx) = tx else {
                return self.queue.send(msg).await;
            };

            match tx.send_async(msg).await {
                Ok(()) => return Ok(()),
                Err(blume::SendError(returned)) => {
                    msg = returned;
                }
            }
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
                Err(blume::TrySendError::Full(returned)) => {
                    msg = returned;
                }
                Err(blume::TrySendError::Disconnected(returned)) => {
                    let peer_id = active.pipes[i].peer_id;
                    active.pipe_peers.remove(&peer_id);
                    active.pipes.swap_remove(i);
                    if active.pipes.is_empty() {
                        active.cursor = 0;
                    } else {
                        active.cursor %= active.pipes.len();
                    }
                    msg = returned;
                }
            }
        }

        Err(omq_proto::error::TrySendError::Full(msg))
    }
}

impl ActivePipes {
    fn next_pipe_any(&mut self) -> Option<blume::Sender<Message>> {
        if self.pipes.is_empty() {
            return None;
        }

        let mut scanned = 0usize;
        while scanned < self.pipes.len() {
            let i = self.cursor % self.pipes.len();
            self.cursor = (i + 1) % self.pipes.len();
            scanned += 1;
            if !self.pipes[i].tx.is_disconnected() {
                return Some(self.pipes[i].tx.clone());
            }
            let peer_id = self.pipes[i].peer_id;
            self.pipe_peers.remove(&peer_id);
            self.pipes.swap_remove(i);
            if self.pipes.is_empty() {
                self.cursor = 0;
                return None;
            }
            self.cursor %= self.pipes.len();
        }
        None
    }
}

/// Round-robin send strategy.
#[derive(Debug)]
pub(crate) struct RoundRobinSend {
    queue: DropQueue,
    shared_rx: QueueReceiver,
    active: Arc<Mutex<ActivePipes>>,
    root_cancel: CancellationToken,
    peer_count: usize,
}

impl RoundRobinSend {
    pub(crate) fn new(options: &Options) -> Self {
        let (cap, policy) = super::effective_queue_params(options);
        let (queue, shared_rx) = DropQueue::new(cap, policy);
        Self {
            queue,
            shared_rx,
            active: Arc::new(Mutex::new(ActivePipes::default())),
            root_cancel: CancellationToken::new(),
            peer_count: 0,
        }
    }

    /// Returns a clone of the shared receive end. Each connection driver
    /// calls this once and holds the clone for the lifetime of the connection.
    pub(crate) fn shared_rx(&self) -> QueueReceiver {
        self.shared_rx.clone()
    }

    pub(crate) fn connection_added(&mut self, peer_id: u64, handle: DriverHandle, is_inproc: bool) {
        self.peer_count += 1;
        self.shared_rx.set_peer_count(self.peer_count);

        let mut active = self.active.lock().expect("round_robin active");
        if !is_inproc && let Some(tx) = handle.send_pipe.clone() {
            active.remove_peer(peer_id);
            active.pipe_peers.insert(peer_id);
            active.pipes.push(ActivePipe { peer_id, tx });
            return;
        }
        active.fallback_peers.insert(peer_id);
        drop(active);

        // inproc_peer_driver reads from inbox (mpsc), not from shared_rx.
        // Spawn a forwarding pump. The pump self-cancels when the peer's
        // inbox closes (driver exits) or root_cancel fires (shutdown).
        if is_inproc {
            let rx = self.shared_rx.clone();
            let cancel = self.root_cancel.child_token();
            tokio::spawn(super::pump::drain_one(rx, handle, cancel));
        }
        // Byte-stream without a pipe also falls back to shared_rx directly.
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        self.peer_count = self.peer_count.saturating_sub(1);
        self.shared_rx.set_peer_count(self.peer_count);
        self.active
            .lock()
            .expect("round_robin active")
            .remove_peer(peer_id);
        // Byte-stream drivers self-cancel via their CancellationToken.
        // Inproc pumps self-cancel when peer inbox closes (driver exits).
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
        self.root_cancel.cancel();
        self.queue.shutdown();
        let mut active = self.active.lock().expect("round_robin active");
        active.pipes.clear();
        active.pipe_peers.clear();
        active.fallback_peers.clear();
        active.cursor = 0;
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

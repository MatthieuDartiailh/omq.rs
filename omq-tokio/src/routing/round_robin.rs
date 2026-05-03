//! Round-robin send.
//!
//! **Default mode** (no `priority` feature): a single shared send queue
//! feeds N per-peer pumps. Each pump is a tokio task that races its
//! peers for the next message. Fast peers naturally pull more; slow
//! peers pull what they can. Load-balancing semantics for PUSH /
//! DEALER / REQ / PAIR / CLIENT / CHANNEL / SCATTER.
//!
//! Per-batch fairness: each pump wakes, pulls one message, then opportun-
//! istically drains up to 256 more or 512 KiB (whichever first), then
//! `tokio::task::yield_now()`s so the tokio worker can schedule peers.
//!
//! **Priority mode** (`feature = "priority"` on): no shared queue, no
//! pumps. Each peer's `DriverHandle.inbox` IS its outbound queue. The
//! submitter walks peers in strict priority order, `try_send`s on each
//! peer's inbox, falls through `Full`/`Closed` to the next priority,
//! and back-pressures only when all peers at every priority are
//! `Full` (await `send` on the highest-priority alive). `Disconnected`
//! / `Closed` peers are skipped instantly - no HWM-stall on a dead
//! high-priority pipe. Mirrors the omq-compio implementation.

#[cfg(feature = "priority")]
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicUsize, Ordering},
};

use tokio_util::sync::CancellationToken;

use crate::engine::DriverHandle;
#[cfg(feature = "priority")]
use crate::engine::DriverCommand;
#[cfg(feature = "priority")]
use omq_proto::error::Error;
use omq_proto::error::Result;
use omq_proto::message::Message;
use omq_proto::options::Options;

#[cfg(not(feature = "priority"))]
use super::drop_queue::DropQueue;

/// Cloneable handle for submitting messages into a [`RoundRobinSend`].
#[cfg(not(feature = "priority"))]
#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    queue: DropQueue,
}

#[cfg(not(feature = "priority"))]
impl Submitter {
    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        self.queue.send(msg).await
    }
}

/// Round-robin send strategy.
///
/// A single shared flume queue feeds all connection drivers directly.
/// Each driver polls `shared_rx` inside its own select! loop after the
/// ZMTP handshake completes, eliminating the pump-task intermediary and
/// the per-message inbox hop that it implied.
#[cfg(not(feature = "priority"))]
#[derive(Debug)]
pub(crate) struct RoundRobinSend {
    queue: DropQueue,
    shared_rx: flume::Receiver<Message>,
    root_cancel: CancellationToken,
}

#[cfg(not(feature = "priority"))]
impl RoundRobinSend {
    pub(crate) fn new(options: &Options) -> Self {
        let (cap, policy) = super::effective_queue_params(options);
        let (queue, shared_rx) = DropQueue::new(cap, policy);
        Self {
            queue,
            shared_rx,
            root_cancel: CancellationToken::new(),
        }
    }

    /// Returns a clone of the shared receive end. Each connection driver
    /// calls this once and holds the clone for the lifetime of the connection.
    pub(crate) fn shared_rx(&self) -> flume::Receiver<Message> {
        self.shared_rx.clone()
    }

    pub(crate) fn connection_added(&mut self, _peer_id: u64, handle: DriverHandle, is_inproc: bool) {
        if is_inproc {
            // inproc_peer_driver reads from inbox (mpsc), not from shared_rx
            // (flume). Spawn a forwarding pump. The pump self-cancels when the
            // peer's inbox closes (driver exits) or root_cancel fires (shutdown).
            let rx = self.shared_rx.clone();
            let cancel = self.root_cancel.child_token();
            tokio::spawn(super::pump::drain(rx, handle, cancel));
        }
        // Byte-stream: driver reads from shared_rx directly; no pump needed.
    }

    #[allow(clippy::unused_self)]
    pub(crate) fn connection_removed(&mut self, _peer_id: u64) {
        // Byte-stream drivers self-cancel via their CancellationToken.
        // Inproc pumps self-cancel when peer inbox closes (driver exits).
    }

    /// Cloneable handle for enqueuing from a spawned task. Lets the socket
    /// driver hand off `Send` command handling so the actor loop never
    /// blocks on HWM backpressure.
    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            queue: self.queue.clone(),
        }
    }

    pub(crate) fn shutdown(&self) {
        self.root_cancel.cancel();
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.queue.len() == 0
    }
}

// ============================================================================
// Priority mode - strict per-pipe priority via per-peer driver inboxes.
// ============================================================================

#[cfg(feature = "priority")]
#[derive(Clone, Debug)]
struct PriorityPeer {
    peer_id: u64,
    priority: u8,
    handle: DriverHandle,
}

#[cfg(feature = "priority")]
#[derive(Clone)]
pub(crate) struct Submitter {
    peers: Arc<RwLock<Vec<PriorityPeer>>>,
    rr_index: Arc<AtomicUsize>,
    on_change: Arc<tokio::sync::Notify>,
}

#[cfg(feature = "priority")]
impl std::fmt::Debug for Submitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Submitter").finish_non_exhaustive()
    }
}

#[cfg(feature = "priority")]
impl Submitter {
    /// Walk peers in priority order; `try_send` on each peer's driver
    /// inbox. Returns Ok on first success. Falls through Full / Closed
    /// to lower priorities. If every alive peer's queue is Full,
    /// `send().await`s on the highest-priority alive (back-pressure).
    /// All peers Closed or no peers at all → wait for a peer-set
    /// change notification, then retry.
    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        loop {
            let snapshot: Vec<PriorityPeer> = {
                let peers = self.peers.read().expect("peers lock");
                peers.clone()
            };
            if snapshot.is_empty() {
                let waiter = self.on_change.notified();
                if !self.peers.read().expect("peers lock").is_empty() {
                    continue;
                }
                waiter.await;
                continue;
            }
            let rr = self.rr_index.fetch_add(1, Ordering::Relaxed);
            let mut highest_alive: Option<DriverHandle> = None;
            let mut i = 0;
            while i < snapshot.len() {
                let prio = snapshot[i].priority;
                let mut j = i;
                while j < snapshot.len() && snapshot[j].priority == prio {
                    j += 1;
                }
                let tier_size = j - i;
                let offset = rr % tier_size;
                for k in 0..tier_size {
                    let peer = &snapshot[i + (offset + k) % tier_size];
                    match peer
                        .handle
                        .inbox
                        .try_send(DriverCommand::SendMessage(msg.clone()))
                    {
                        Ok(()) => return Ok(()),
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            if highest_alive.is_none() {
                                highest_alive = Some(peer.handle.clone());
                            }
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
                    }
                }
                i = j;
            }
            if let Some(h) = highest_alive {
                return h
                    .inbox
                    .send(DriverCommand::SendMessage(msg))
                    .await
                    .map_err(|_| Error::Closed);
            }
            let waiter = self.on_change.notified();
            if self.has_live_peer() {
                continue;
            }
            waiter.await;
        }
    }

    fn has_live_peer(&self) -> bool {
        let peers = self.peers.read().expect("peers lock");
        peers.iter().any(|p| !p.handle.inbox.is_closed())
    }
}

#[cfg(feature = "priority")]
#[derive(Debug)]
pub(crate) struct RoundRobinSend {
    peers: Arc<RwLock<Vec<PriorityPeer>>>,
    rr_index: Arc<AtomicUsize>,
    on_change: Arc<tokio::sync::Notify>,
    root_cancel: CancellationToken,
}

#[cfg(feature = "priority")]
impl RoundRobinSend {
    pub(crate) fn new(_options: &Options) -> Self {
        Self {
            peers: Arc::new(RwLock::new(Vec::new())),
            rr_index: Arc::new(AtomicUsize::new(0)),
            on_change: Arc::new(tokio::sync::Notify::new()),
            root_cancel: CancellationToken::new(),
        }
    }

    #[allow(dead_code)] // kept for parity with the non-priority impl's API
    pub(crate) fn connection_added(&mut self, peer_id: u64, handle: DriverHandle) {
        self.connection_added_with_priority(peer_id, handle, omq_proto::DEFAULT_PRIORITY);
    }

    pub(crate) fn connection_added_with_priority(
        &mut self,
        peer_id: u64,
        handle: DriverHandle,
        priority: u8,
    ) {
        let mut peers = self.peers.write().expect("peers lock");
        peers.push(PriorityPeer {
            peer_id,
            priority,
            handle,
        });
        peers.sort_by_key(|p| p.priority);
        drop(peers);
        // Wake any submitter awaiting a peer-set change (notify_waiters
        // wakes all current waiters; new waiters after this call see a
        // fresh state on next read).
        self.on_change.notify_waiters();
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        let mut peers = self.peers.write().expect("peers lock");
        peers.retain(|p| p.peer_id != peer_id);
        drop(peers);
        self.on_change.notify_waiters();
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            peers: self.peers.clone(),
            rr_index: self.rr_index.clone(),
            on_change: self.on_change.clone(),
        }
    }

    pub(crate) fn shutdown(&self) {
        self.root_cancel.cancel();
    }

    /// In priority mode the "queue" lives in the per-peer driver
    /// inboxes - `is_drained` tells the socket driver "all sends have
    /// been dispatched"; once we've handed each `SendMessage` off to
    /// an inbox via `try_send/send`, it's the connection driver's job
    /// to flush, not ours.
    #[allow(clippy::unused_self)]
    pub(crate) fn is_drained(&self) -> bool {
        true
    }
}


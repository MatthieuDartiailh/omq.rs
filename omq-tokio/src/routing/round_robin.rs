//! Round-robin send.
//!
//! A single shared send queue feeds N per-peer pumps. Each pump is a
//! tokio task that races its peers for the next message. Fast peers
//! naturally pull more; slow peers pull what they can. Load-balancing
//! semantics for PUSH / DEALER / REQ / PAIR / CLIENT / CHANNEL / SCATTER.
//!
//! Per-batch fairness: each pump wakes, pulls one message, then opportun-
//! istically drains up to 256 more or 512 KiB (whichever first), then
//! `tokio::task::yield_now()`s so the tokio worker can schedule peers.

use tokio_util::sync::CancellationToken;

use crate::engine::DriverHandle;
use omq_proto::error::Result;
use omq_proto::message::Message;
use omq_proto::options::Options;

use super::drop_queue::{DropQueue, QueueReceiver};

/// Cloneable handle for submitting messages into a [`RoundRobinSend`].
#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    queue: DropQueue,
}

impl Submitter {
    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        self.queue.send(msg).await
    }

    pub(crate) fn try_send(
        &self,
        msg: Message,
    ) -> core::result::Result<(), crate::socket::handle::TrySendError> {
        self.queue
            .try_send(msg)
            .map_err(crate::socket::handle::TrySendError::Full)
    }
}

/// Round-robin send strategy.
///
/// A single shared queue feeds all connection drivers directly.
/// Each driver polls `shared_rx` inside its own select! loop after the
/// ZMTP handshake completes, eliminating the pump-task intermediary and
/// the per-message inbox hop that it implied.
#[derive(Debug)]
pub(crate) struct RoundRobinSend {
    queue: DropQueue,
    shared_rx: QueueReceiver,
    root_cancel: CancellationToken,
}

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
    pub(crate) fn shared_rx(&self) -> QueueReceiver {
        self.shared_rx.clone()
    }

    pub(crate) fn connection_added(
        &mut self,
        _peer_id: u64,
        handle: DriverHandle,
        is_inproc: bool,
    ) {
        if is_inproc {
            // inproc_peer_driver reads from inbox (mpsc), not from shared_rx.
            // Spawn a forwarding pump. The pump self-cancels when the peer's
            // inbox closes (driver exits) or root_cancel fires (shutdown).
            let rx = self.shared_rx.clone();
            let cancel = self.root_cancel.child_token();
            tokio::spawn(super::pump::drain_one(rx, handle, cancel));
        }
        // Byte-stream: driver reads from shared_rx directly; no pump needed.
    }

    #[expect(clippy::unused_self)]
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

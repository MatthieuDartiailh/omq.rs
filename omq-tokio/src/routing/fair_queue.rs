//! Fair-queue recv: one shared channel, all peers push in.
//!
//! Every incoming message goes to the socket's MPMC recv channel.
//! Fairness comes naturally from tokio's scheduler — each peer's
//! `ConnectionDriver` is a separate task and yields between events.
//! Subscription filtering (SUB/XSUB) and group filtering (DISH) sit
//! on top of this as recv-side filters.

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;

/// Fair-queue recv strategy: forward every incoming message to a shared
/// [`async_channel`] that the public `Socket::recv` reads from.
#[derive(Debug)]
pub(crate) struct FairQueueRecv {
    recv_tx: async_channel::Sender<Message>,
}

impl FairQueueRecv {
    pub(crate) fn new(recv_tx: async_channel::Sender<Message>) -> Self {
        Self { recv_tx }
    }

    #[allow(clippy::unused_self)]
    pub(crate) fn connection_added(&mut self, _peer_id: u64) {}

    #[allow(clippy::unused_self)]
    pub(crate) fn connection_removed(&mut self, _peer_id: u64) {}

    pub(crate) async fn deliver(&self, _peer_id: u64, msg: Message) -> Result<()> {
        self.recv_tx.send(msg).await.map_err(|_| Error::Closed)
    }
}

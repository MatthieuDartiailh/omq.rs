//! Fair-queue recv: one shared pipe, all peers push in.
//!
//! Every incoming message goes to the socket's shared recv pipe.
//! Fairness comes naturally from tokio's scheduler — each peer's
//! `ConnectionDriver` is a separate task and yields between events.
//! Subscription filtering (SUB/XSUB) and group filtering (DISH) sit
//! on top of this as recv-side filters.

use std::sync::Arc;

use omq_proto::error::Result;
use omq_proto::message::Message;

use crate::socket::recv::SharedRecvPipe;

/// Fair-queue recv strategy: forward every incoming message to the
/// shared [`SharedRecvPipe`] that the public `Socket::recv` reads from.
#[derive(Debug)]
pub(crate) struct FairQueueRecv {
    recv_tx: Arc<SharedRecvPipe>,
}

impl FairQueueRecv {
    pub(crate) fn new(recv_tx: Arc<SharedRecvPipe>) -> Self {
        Self { recv_tx }
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn connection_added(&mut self, _peer_id: u64) {}

    #[expect(clippy::unused_self)]
    pub(crate) fn connection_removed(&mut self, _peer_id: u64) {}

    pub(crate) async fn deliver(&self, _peer_id: u64, msg: Message) -> Result<()> {
        self.recv_tx.send(msg).await
    }
}

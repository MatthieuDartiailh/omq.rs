use omq_proto::error::{Error, Result};
use omq_proto::message::Message;

use crate::socket::handle::Socket;

impl Socket {
    /// Identity-routed send: ROUTER, SERVER, PEER. Message must be
    /// `[routing_id, body...]`; the first frame names the target peer
    /// in `identity_to_slot`. Unknown identity is dropped silently
    /// unless `router_mandatory` is set, which surfaces `Unroutable`.
    pub(super) async fn send_identity_routed(&self, mut msg: Message) -> Result<()> {
        if msg.is_empty() {
            return Err(Error::Unroutable);
        }
        let identity = msg.part_bytes(0).unwrap_or_default();
        let target = {
            let table = self
                .inner()
                .routing
                .identity_to_slot
                .read()
                .expect("identity table");
            let idx = table.get(&identity).copied();
            drop(table);
            idx.and_then(|idx| {
                let peers = self.inner().routing.peers.read().expect("peers lock");
                peers.get(idx).map(|p| p.out.clone())
            })
        };
        let Some(out) = target else {
            if self.inner().options.router_mandatory {
                return Err(Error::Unroutable);
            }
            return Ok(());
        };
        msg.pop_front();
        out.send(msg).await
    }

    pub(super) fn try_send_identity_routed(&self, msg: &Message) -> Result<()> {
        if msg.is_empty() {
            return Err(Error::Unroutable);
        }
        let identity = msg.part_bytes(0).unwrap_or_default();
        let target = {
            let table = self
                .inner()
                .routing
                .identity_to_slot
                .read()
                .expect("identity table");
            let idx = table.get(&identity).copied();
            drop(table);
            idx.and_then(|idx| {
                let peers = self.inner().routing.peers.read().expect("peers lock");
                peers.get(idx).map(|p| p.out.clone())
            })
        };
        let Some(out) = target else {
            if self.inner().options.router_mandatory {
                return Err(Error::Unroutable);
            }
            return Ok(());
        };
        let mut body = msg.clone();
        body.pop_front();
        out.try_send_immediate(body)
    }
}

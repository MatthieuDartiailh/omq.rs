//! Identity-based routing for ROUTER, REP, SERVER, PEER.
//!
//! Each peer is keyed by `(identity, connection_id)`; the identity-to-peer
//! map holds the LATEST `peer_id` for a given identity, so a reconnect
//! replaces the stale entry without leaking the old peer state.
//!
//! Send: first frame of the user message is the routing identity. Look up
//! the matching peer; forward the rest. If no match:
//! - `router_mandatory = true` -> `Error::Unroutable`.
//! - otherwise silently drop (libzmq default).
//!
//! Recv: we prepend the peer's identity as the first frame of the message
//! before delivering to the socket's recv channel.

use std::sync::{Arc, Mutex};

use rustc_hash::FxHashMap;

use bytes::Bytes;

use crate::engine::DriverHandle;
use crate::engine::wire_slot::TryEncodeResult;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;

use super::peer_send::PeerSend;

#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    inner: Arc<Mutex<IdentityInner>>,
    router_mandatory: bool,
}

impl Submitter {
    pub(crate) fn shutdown(&self) {
        let mut g = self.inner.lock().expect("identity inner poisoned");
        g.peers.clear();
        g.identity_to_peer.clear();
    }

    fn resolve_target(&self, msg: &mut Message) -> Result<Option<PeerSend>> {
        if msg.is_empty() {
            return Err(Error::Unroutable);
        }
        let identity = msg.pop_front().unwrap();

        let target: Option<PeerSend> = {
            let g = self.inner.lock().expect("identity inner poisoned");
            g.identity_to_peer
                .get(&identity)
                .and_then(|peer_id| g.peers.get(peer_id))
                .map(|p| p.target.clone())
        };

        if target.is_none() && self.router_mandatory {
            return Err(Error::Unroutable);
        }
        Ok(target)
    }

    pub(crate) fn try_send(
        &self,
        mut msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        let retry = msg.clone();
        let target = self
            .resolve_target(&mut msg)
            .map_err(omq_proto::error::TrySendError::Error)?;

        if let Some(t) = target {
            match t.try_encode(&msg) {
                TryEncodeResult::Ok => {}
                TryEncodeResult::Full | TryEncodeResult::Ineligible => {
                    return Err(omq_proto::error::TrySendError::Full(retry));
                }
                TryEncodeResult::Dead => {
                    return Err(omq_proto::error::TrySendError::Closed);
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn send(&self, mut msg: Message) -> Result<()> {
        let Some(t) = self.resolve_target(&mut msg)? else {
            return Ok(());
        };
        t.send(msg).await
    }
}

#[derive(Debug)]
pub(crate) struct IdentitySend {
    inner: Arc<Mutex<IdentityInner>>,
    router_mandatory: bool,
}

#[derive(Debug)]
struct IdentityInner {
    peers: FxHashMap<u64, IdentityPeer>,
    identity_to_peer: FxHashMap<Bytes, u64>,
}

#[derive(Debug)]
struct IdentityPeer {
    identity: Bytes,
    target: PeerSend,
}

impl IdentitySend {
    pub(crate) fn new(options: &Options) -> Self {
        Self {
            inner: Arc::new(Mutex::new(IdentityInner {
                peers: FxHashMap::default(),
                identity_to_peer: FxHashMap::default(),
            })),
            router_mandatory: options.router_mandatory,
        }
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            inner: self.inner.clone(),
            router_mandatory: self.router_mandatory,
        }
    }

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn connection_added(&mut self, peer_id: u64, handle: DriverHandle, identity: Bytes) {
        let target = PeerSend::from_handle(&handle);
        let mut g = self.inner.lock().expect("identity inner poisoned");
        g.peers.insert(
            peer_id,
            IdentityPeer {
                identity: identity.clone(),
                target,
            },
        );
        g.identity_to_peer.insert(identity, peer_id);
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        let mut g = self.inner.lock().expect("identity inner poisoned");
        if let Some(p) = g.peers.remove(&peer_id)
            && g.identity_to_peer.get(&p.identity) == Some(&peer_id)
        {
            g.identity_to_peer.remove(&p.identity);
        }
    }

    pub(crate) fn peer_for_identity(&self, identity: &Bytes) -> Option<u64> {
        let g = self.inner.lock().expect("identity inner poisoned");
        g.identity_to_peer.get(identity).copied()
    }

    pub(crate) fn shutdown(&self) {
        let mut g = self.inner.lock().expect("identity inner poisoned");
        g.peers.clear();
        g.identity_to_peer.clear();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let g = self.inner.lock().expect("identity inner poisoned");
        g.peers.values().all(|p| p.target.is_empty())
    }
}

/// Recv strategy that prepends each peer's identity as the first frame.
#[derive(Debug)]
pub(crate) struct IdentityRecv {
    peers: Arc<Mutex<FxHashMap<u64, Bytes>>>,
    recv_tx: async_channel::Sender<Message>,
}

impl IdentityRecv {
    pub(crate) fn new(recv_tx: async_channel::Sender<Message>) -> Self {
        Self {
            peers: Arc::new(Mutex::new(FxHashMap::default())),
            recv_tx,
        }
    }

    pub(crate) fn connection_added(&mut self, peer_id: u64, identity: Bytes) {
        let mut g = self.peers.lock().expect("identity recv poisoned");
        g.insert(peer_id, identity);
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        let mut g = self.peers.lock().expect("identity recv poisoned");
        g.remove(&peer_id);
    }

    pub(crate) async fn deliver(&self, peer_id: u64, msg: Message) -> Result<()> {
        let wrapped = self.wrap(peer_id, msg);
        self.recv_tx.send(wrapped).await.map_err(|_| Error::Closed)
    }

    pub(crate) fn wrap(&self, peer_id: u64, msg: Message) -> Message {
        let identity = {
            let g = self.peers.lock().expect("identity recv poisoned");
            g.get(&peer_id).cloned().unwrap_or_default()
        };
        Message::with_prefix(identity, msg)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::engine::DriverCommand;

    #[test]
    fn try_send_reports_full_and_preserves_routing_frame() {
        let mut send = IdentitySend::new(&Options::default());
        let submitter = send.submitter();
        let (tx, mut rx) = mpsc::channel(1);
        send.connection_added(
            1,
            DriverHandle {
                inbox: tx,
                cancel: CancellationToken::new(),
                wire_slot: None,
                wire_slot_tx: None,
                send_pipe: None,
            },
            Bytes::from_static(b"id"),
        );

        submitter
            .try_send(Message::multipart([
                Bytes::from_static(b"id"),
                Bytes::from_static(b"one"),
            ]))
            .unwrap();

        let returned = match submitter.try_send(Message::multipart([
            Bytes::from_static(b"id"),
            Bytes::from_static(b"two"),
        ])) {
            Err(omq_proto::error::TrySendError::Full(msg)) => msg,
            other => panic!("expected Full, got {other:?}"),
        };

        assert_eq!(returned.part_bytes(0).unwrap(), &b"id"[..]);
        assert_eq!(returned.part_bytes(1).unwrap(), &b"two"[..]);

        match rx.try_recv().unwrap() {
            DriverCommand::SendMessage(msg) => {
                assert_eq!(msg.part_bytes(0).unwrap(), &b"one"[..]);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }
}

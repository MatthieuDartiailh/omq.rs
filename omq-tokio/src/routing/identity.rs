//! Identity-based routing for ROUTER, REP, SERVER, PEER, STREAM.
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

use tokio::sync::Notify;

use rustc_hash::FxHashMap;

use bytes::Bytes;

use crate::engine::{PeerDriverCommand, PeerDriverHandle, SendPipeError, SendPipeProducer};
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;

enum SendRetry {
    Full(Message, Arc<Notify>),
}

/// Per-peer send target. Prefers `SendPipe` (zero-copy yring) when available;
/// falls back to the driver inbox for peers without a pipe (STREAM raw TCP).
#[derive(Debug)]
enum PeerTarget {
    Pipe(SendPipeProducer),
    Inbox(tokio::sync::mpsc::Sender<PeerDriverCommand>),
}

impl PeerTarget {
    fn try_send(&mut self, msg: Message) -> core::result::Result<(), SendPipeError> {
        match self {
            Self::Pipe(p) => p.try_send(msg),
            Self::Inbox(tx) => match tx.try_send(PeerDriverCommand::SendMessage(msg)) {
                Ok(()) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(
                    PeerDriverCommand::SendMessage(m),
                )) => Err(SendPipeError::Full(m)),
                Err(_) => Err(SendPipeError::Closed(Message::default())),
            },
        }
    }

    fn space_available(&self) -> Option<Arc<Notify>> {
        match self {
            Self::Pipe(p) => Some(p.space_available()),
            Self::Inbox(_) => None,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Pipe(p) => p.is_empty(),
            Self::Inbox(_) => true,
        }
    }
}

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

    pub(crate) fn try_send(
        &self,
        mut msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        let retry = msg.clone();
        if msg.is_empty() {
            return Err(omq_proto::error::TrySendError::Error(Error::Unroutable));
        }
        let identity = msg.pop_front().unwrap();
        let mut g = self.inner.lock().expect("identity inner poisoned");
        let Some(&id) = g.identity_to_peer.get(&identity) else {
            if self.router_mandatory {
                return Err(omq_proto::error::TrySendError::Error(Error::Unroutable));
            }
            return Ok(());
        };
        let Some(peer) = g.peers.get_mut(&id) else {
            if self.router_mandatory {
                return Err(omq_proto::error::TrySendError::Error(Error::Unroutable));
            }
            return Ok(());
        };
        match peer.target.try_send(msg) {
            Ok(()) => Ok(()),
            Err(SendPipeError::Full(_)) => Err(omq_proto::error::TrySendError::Full(retry)),
            Err(SendPipeError::Closed(_)) => Err(omq_proto::error::TrySendError::Closed),
        }
    }

    pub(crate) async fn send(&self, mut msg: Message) -> Result<()> {
        if msg.is_empty() {
            return Err(Error::Unroutable);
        }
        let identity = msg.pop_front().unwrap();

        loop {
            let retry = self.try_send_to(&identity, msg)?;
            match retry {
                Ok(()) => return Ok(()),
                Err(SendRetry::Full(returned, space)) => {
                    msg = returned;
                    let notified = space.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    match self.try_send_to(&identity, msg)? {
                        Ok(()) => return Ok(()),
                        Err(SendRetry::Full(returned, _)) => msg = returned,
                    }
                    notified.await;
                }
            }
        }
    }

    fn try_send_to(
        &self,
        identity: &Bytes,
        msg: Message,
    ) -> Result<core::result::Result<(), SendRetry>> {
        let mut g = self.inner.lock().expect("identity inner poisoned");
        let Some(&id) = g.identity_to_peer.get(identity) else {
            if self.router_mandatory {
                return Err(Error::Unroutable);
            }
            return Ok(Ok(()));
        };
        let Some(peer) = g.peers.get_mut(&id) else {
            if self.router_mandatory {
                return Err(Error::Unroutable);
            }
            return Ok(Ok(()));
        };
        match peer.target.try_send(msg) {
            Ok(()) => Ok(Ok(())),
            Err(SendPipeError::Closed(_)) => Err(Error::Closed),
            Err(SendPipeError::Full(returned)) => {
                let space = peer
                    .target
                    .space_available()
                    .unwrap_or_else(|| Arc::new(Notify::new()));
                Ok(Err(SendRetry::Full(returned, space)))
            }
        }
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
    target: PeerTarget,
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
    pub(crate) fn connection_added(
        &mut self,
        peer_id: u64,
        handle: PeerDriverHandle,
        identity: Bytes,
    ) {
        let target = if let Some(ref pipe_handle) = handle.send_pipe {
            if let Some(pipe) = pipe_handle.lock().expect("identity send pipe").take() {
                PeerTarget::Pipe(pipe)
            } else {
                PeerTarget::Inbox(handle.inbox.clone())
            }
        } else {
            PeerTarget::Inbox(handle.inbox.clone())
        };

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
    recv_tx: Arc<crate::socket::recv::SharedRecvPipe>,
}

impl IdentityRecv {
    pub(crate) fn new(recv_tx: Arc<crate::socket::recv::SharedRecvPipe>) -> Self {
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
        self.recv_tx.send(wrapped).await
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

    use super::*;
    use crate::engine::send_pipe;

    #[test]
    fn try_send_reports_full_and_preserves_routing_frame() {
        let mut send = IdentitySend::new(&Options::default());
        let submitter = send.submitter();

        let (pipe_tx, _pipe_rx) = send_pipe(1);
        let handle = PeerDriverHandle {
            inbox: tokio::sync::mpsc::channel(1).0,
            cancel: tokio_util::sync::CancellationToken::new(),
            transmit_slot: None,
            transmit_slot_tx: None,
            send_pipe: Some(std::sync::Arc::new(std::sync::Mutex::new(Some(pipe_tx)))),
        };
        send.connection_added(1, handle, Bytes::from_static(b"id"));

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
    }
}

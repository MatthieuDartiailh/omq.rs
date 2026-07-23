//! Low-latency send routing for request/reply ping-pong.
//!
//! This route encodes directly into each peer's transmit slot. It avoids the
//! generic yring send pipe, which is the right tradeoff for one-message-at-a-
//! time REQ/REP but not for throughput-oriented routing.

use std::sync::{Arc, Mutex};

use crate::engine::PeerDriverHandle;
use crate::engine::transmit_slot::TryFrameResult;
use crate::routing::fallback_queue::{FallbackQueue, FallbackReceiver};
use crate::routing::peer_outbound::PeerOutbound;
use omq_proto::error::{Error, Result, TrySendError};
use omq_proto::message::Message;
use omq_proto::options::Options;

#[derive(Debug)]
struct Peer {
    id: u64,
    target: PeerOutbound,
}

#[derive(Debug, Default)]
struct State {
    peers: Vec<Peer>,
    cursor: usize,
}

#[derive(Debug)]
pub(crate) struct LatencySend {
    queue: FallbackQueue,
    shared_rx: FallbackReceiver,
    state: Arc<Mutex<State>>,
    peer_count: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    queue: FallbackQueue,
    state: Arc<Mutex<State>>,
}

impl LatencySend {
    pub(crate) fn new(options: &Options) -> Self {
        let (cap, policy) = super::effective_queue_params(options);
        let (queue, shared_rx) = FallbackQueue::new(cap, policy);
        Self {
            queue,
            shared_rx,
            state: Arc::new(Mutex::new(State::default())),
            peer_count: 0,
        }
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            queue: self.queue.clone(),
            state: self.state.clone(),
        }
    }

    pub(crate) fn shared_rx(&self) -> FallbackReceiver {
        self.shared_rx.clone()
    }

    pub(crate) fn connection_added(&mut self, peer_id: u64, handle: &PeerDriverHandle) {
        self.peer_count += 1;
        self.shared_rx.set_peer_count(self.peer_count);
        let mut state = self.state.lock().expect("latency send state");
        state.peers.retain(|peer| peer.id != peer_id);
        state.peers.push(Peer {
            id: peer_id,
            target: PeerOutbound::from_handle(handle),
        });
        state.cursor %= state.peers.len();
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        self.peer_count = self.peer_count.saturating_sub(1);
        self.shared_rx.set_peer_count(self.peer_count);
        let mut state = self.state.lock().expect("latency send state");
        state.peers.retain(|peer| peer.id != peer_id);
        if state.peers.is_empty() {
            state.cursor = 0;
        } else {
            state.cursor %= state.peers.len();
        }
    }

    pub(crate) fn shutdown(&self) {
        self.queue.shutdown();
        self.state.lock().expect("latency send state").peers.clear();
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.queue.len() == 0
            && self
                .state
                .lock()
                .expect("latency send state")
                .peers
                .iter()
                .all(|peer| peer.target.is_empty())
    }
}

impl Submitter {
    pub(crate) fn shutdown(&self) {
        self.queue.shutdown();
        self.state.lock().expect("latency send state").peers.clear();
    }

    pub(crate) async fn send(&self, mut msg: Message) -> Result<()> {
        loop {
            match self.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(returned)) => msg = returned,
                Err(TrySendError::Error(error)) => return Err(error),
                Err(TrySendError::Closed) => return Err(Error::Closed),
            }

            let notified = {
                let state = self.state.lock().expect("latency send state");
                if state
                    .peers
                    .iter()
                    .any(|peer| peer.target.has_direct_writer())
                {
                    None
                } else {
                    state
                        .peers
                        .iter()
                        .find_map(|peer| peer.target.space_available())
                }
            };
            let Some(notified) = notified else {
                tokio::task::yield_now().await;
                match self.try_send(msg) {
                    Ok(()) => return Ok(()),
                    Err(TrySendError::Full(msg)) => return self.queue.send(msg).await,
                    Err(TrySendError::Error(error)) => return Err(error),
                    Err(TrySendError::Closed) => return Err(Error::Closed),
                }
            };
            let seen = notified.generation();
            let notified = notified.changed_after(seen);
            tokio::pin!(notified);
            match self.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(returned)) => msg = returned,
                Err(TrySendError::Error(error)) => return Err(error),
                Err(TrySendError::Closed) => return Err(Error::Closed),
            }
            notified.await;
        }
    }

    pub(crate) async fn wait_send_progress(&self) {
        let notified = {
            let state = self.state.lock().expect("latency send state");
            if state
                .peers
                .iter()
                .any(|peer| peer.target.has_direct_writer())
            {
                None
            } else {
                state
                    .peers
                    .iter()
                    .find_map(|peer| peer.target.space_available())
            }
        };
        if let Some(notified) = notified {
            let seen = notified.generation();
            notified.changed_after(seen).await;
        } else if self.queue.len() != 0 {
            self.queue.wait_space_available().await;
        } else {
            tokio::task::yield_now().await;
        }
    }

    pub(crate) fn try_send(&self, msg: Message) -> core::result::Result<(), TrySendError> {
        if self.queue.len() != 0 {
            return self.queue.try_send(msg).map_err(TrySendError::Full);
        }

        let mut state = self.state.lock().expect("latency send state");
        if state.peers.is_empty() {
            return self.queue.try_send(msg).map_err(TrySendError::Full);
        }

        let mut full = false;
        let count = state.peers.len();
        for _ in 0..count {
            let index = state.cursor % count;
            state.cursor = (index + 1) % count;
            match state.peers[index].target.try_encode(&msg) {
                TryFrameResult::Ok => return Ok(()),
                TryFrameResult::Full => full = true,
                TryFrameResult::Dead => return Err(TrySendError::Closed),
                TryFrameResult::Ineligible => unreachable!("inbox fallback handles ineligible"),
            }
        }
        if full {
            Err(TrySendError::Full(msg))
        } else {
            Err(TrySendError::Closed)
        }
    }
}

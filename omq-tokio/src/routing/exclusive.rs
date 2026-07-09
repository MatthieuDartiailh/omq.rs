use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::engine::{PeerDriverCommand, PeerDriverHandle};
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;

#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    peer: Arc<Mutex<Option<PeerDriverHandle>>>,
    peer_ready: Arc<Notify>,
    closed: Arc<AtomicBool>,
}

impl Submitter {
    pub(crate) fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        *self.peer.lock().expect("exclusive peer") = None;
        self.peer_ready.notify_waiters();
    }

    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            let handle = self.peer.lock().expect("exclusive peer").clone();
            match handle {
                Some(h) => {
                    return h
                        .inbox
                        .send(PeerDriverCommand::SendMessage(msg))
                        .await
                        .map_err(|_| Error::Closed);
                }
                None => {
                    tokio::select! {
                        biased;
                        () = self.peer_ready.notified() => {}
                        () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
                    }
                }
            }
        }
    }

    pub(crate) fn try_send(
        &self,
        msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(omq_proto::error::TrySendError::Closed);
        }
        let handle = self.peer.lock().expect("exclusive peer").clone();
        match handle {
            Some(h) => h
                .inbox
                .try_send(PeerDriverCommand::SendMessage(msg))
                .map_err(|e| match e {
                    tokio::sync::mpsc::error::TrySendError::Full(
                        PeerDriverCommand::SendMessage(m),
                    ) => omq_proto::error::TrySendError::Full(m),
                    _ => omq_proto::error::TrySendError::Closed,
                }),
            None => Err(omq_proto::error::TrySendError::Full(msg)),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ExclusiveSend {
    peer: Arc<Mutex<Option<PeerDriverHandle>>>,
    peer_ready: Arc<Notify>,
    closed: Arc<AtomicBool>,
}

impl ExclusiveSend {
    pub(crate) fn new() -> Self {
        Self {
            peer: Arc::new(Mutex::new(None)),
            peer_ready: Arc::new(Notify::new()),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            peer: self.peer.clone(),
            peer_ready: self.peer_ready.clone(),
            closed: self.closed.clone(),
        }
    }

    pub(crate) fn connection_added(&mut self, _peer_id: u64, handle: PeerDriverHandle) {
        *self.peer.lock().expect("exclusive peer") = Some(handle);
        self.peer_ready.notify_waiters();
    }

    pub(crate) fn connection_removed(&mut self, _peer_id: u64) {
        *self.peer.lock().expect("exclusive peer") = None;
    }

    pub(crate) fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        *self.peer.lock().expect("exclusive peer") = None;
        self.peer_ready.notify_waiters();
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn is_drained(&self) -> bool {
        true
    }
}

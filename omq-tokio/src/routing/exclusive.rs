use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::engine::{DriverCommand, DriverHandle};
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;

#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    peer: Arc<Mutex<Option<DriverHandle>>>,
    peer_ready: Arc<Notify>,
}

impl Submitter {
    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        loop {
            let handle = self.peer.lock().expect("exclusive peer").clone();
            match handle {
                Some(h) => {
                    return h
                        .inbox
                        .send(DriverCommand::SendMessage(msg))
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
    ) -> core::result::Result<(), crate::socket::handle::TrySendError> {
        let handle = self.peer.lock().expect("exclusive peer").clone();
        match handle {
            Some(h) => h
                .inbox
                .try_send(DriverCommand::SendMessage(msg))
                .map_err(|e| match e {
                    tokio::sync::mpsc::error::TrySendError::Full(DriverCommand::SendMessage(m)) => {
                        crate::socket::handle::TrySendError::Full(m)
                    }
                    _ => crate::socket::handle::TrySendError::Closed,
                }),
            None => Err(crate::socket::handle::TrySendError::Full(msg)),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ExclusiveSend {
    peer: Arc<Mutex<Option<DriverHandle>>>,
    peer_ready: Arc<Notify>,
}

impl ExclusiveSend {
    pub(crate) fn new() -> Self {
        Self {
            peer: Arc::new(Mutex::new(None)),
            peer_ready: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            peer: self.peer.clone(),
            peer_ready: self.peer_ready.clone(),
        }
    }

    pub(crate) fn connection_added(&mut self, _peer_id: u64, handle: DriverHandle) {
        *self.peer.lock().expect("exclusive peer") = Some(handle);
        self.peer_ready.notify_waiters();
    }

    pub(crate) fn connection_removed(&mut self, _peer_id: u64) {
        *self.peer.lock().expect("exclusive peer") = None;
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn shutdown(&self) {}

    #[expect(clippy::unused_self)]
    pub(crate) fn is_drained(&self) -> bool {
        true
    }
}

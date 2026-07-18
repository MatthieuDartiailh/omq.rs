use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::engine::{PeerDriverHandle, SendPipeError, SendPipeProducer};
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;

#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    pipe: Arc<Mutex<Option<SendPipeProducer>>>,
    peer_ready: Arc<Notify>,
    closed: Arc<AtomicBool>,
}

impl Submitter {
    pub(crate) fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        *self.pipe.lock().expect("exclusive pipe") = None;
        self.peer_ready.notify_waiters();
    }

    pub(crate) async fn send(&self, mut msg: Message) -> Result<()> {
        loop {
            match self.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(omq_proto::error::TrySendError::Full(returned)) => msg = returned,
                Err(omq_proto::error::TrySendError::Error(e)) => return Err(e),
                Err(omq_proto::error::TrySendError::Closed) => return Err(Error::Closed),
            }

            let space = {
                let guard = self.pipe.lock().expect("exclusive pipe");
                guard.as_ref().map(SendPipeProducer::space_available)
            };

            if let Some(space) = space {
                let notified = space.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                match self.try_send(msg) {
                    Ok(()) => return Ok(()),
                    Err(omq_proto::error::TrySendError::Full(returned)) => msg = returned,
                    Err(omq_proto::error::TrySendError::Error(e)) => return Err(e),
                    Err(omq_proto::error::TrySendError::Closed) => return Err(Error::Closed),
                }
                notified.await;
                continue;
            }

            let notified = self.peer_ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(omq_proto::error::TrySendError::Full(returned)) => msg = returned,
                Err(omq_proto::error::TrySendError::Error(e)) => return Err(e),
                Err(omq_proto::error::TrySendError::Closed) => return Err(Error::Closed),
            }
            notified.await;
        }
    }

    pub(crate) fn try_send(
        &self,
        msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(omq_proto::error::TrySendError::Closed);
        }
        let mut guard = self.pipe.lock().expect("exclusive pipe");
        match guard.as_mut() {
            Some(producer) => match producer.try_send(msg) {
                Ok(()) => Ok(()),
                Err(SendPipeError::Full(m)) => Err(omq_proto::error::TrySendError::Full(m)),
                Err(SendPipeError::Closed(_)) => Err(omq_proto::error::TrySendError::Closed),
            },
            None => Err(omq_proto::error::TrySendError::Full(msg)),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ExclusiveSend {
    pipe: Arc<Mutex<Option<SendPipeProducer>>>,
    peer_ready: Arc<Notify>,
    closed: Arc<AtomicBool>,
}

impl ExclusiveSend {
    pub(crate) fn new() -> Self {
        Self {
            pipe: Arc::new(Mutex::new(None)),
            peer_ready: Arc::new(Notify::new()),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            pipe: self.pipe.clone(),
            peer_ready: self.peer_ready.clone(),
            closed: self.closed.clone(),
        }
    }

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn connection_added(&mut self, _peer_id: u64, handle: PeerDriverHandle) {
        let send_pipe = handle
            .send_pipe
            .as_ref()
            .and_then(|pipe| pipe.lock().expect("exclusive send pipe").take());
        *self.pipe.lock().expect("exclusive pipe") = send_pipe;
        self.peer_ready.notify_waiters();
    }

    pub(crate) fn connection_removed(&mut self, _peer_id: u64) {
        *self.pipe.lock().expect("exclusive pipe") = None;
    }

    pub(crate) fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        *self.pipe.lock().expect("exclusive pipe") = None;
        self.peer_ready.notify_waiters();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let guard = self.pipe.lock().expect("exclusive pipe");
        guard.as_ref().is_none_or(SendPipeProducer::is_empty)
    }
}

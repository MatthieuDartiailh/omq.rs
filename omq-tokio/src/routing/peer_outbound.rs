use std::sync::Arc;

use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};
use crate::engine::{PeerDriverCommand, PeerDriverHandle};
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;

#[derive(Debug, Clone)]
pub(crate) enum PeerOutbound {
    Wire {
        slot: Arc<PeerTransmitSlot>,
        inbox: tokio::sync::mpsc::Sender<PeerDriverCommand>,
    },
    Inbox(tokio::sync::mpsc::Sender<PeerDriverCommand>),
}

impl PeerOutbound {
    pub(crate) fn from_handle(handle: &PeerDriverHandle) -> Self {
        match handle.transmit_slot {
            Some(ref slot) => Self::Wire {
                slot: slot.clone(),
                inbox: handle.inbox.clone(),
            },
            None => Self::Inbox(handle.inbox.clone()),
        }
    }

    pub(crate) fn try_encode(&self, msg: &Message) -> TryFrameResult {
        match self {
            Self::Wire { slot, inbox } => match slot.try_encode(msg) {
                TryFrameResult::Ineligible => {
                    match inbox.try_send(PeerDriverCommand::SendMessage(msg.clone())) {
                        Ok(()) => TryFrameResult::Ok,
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            TryFrameResult::Full
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            TryFrameResult::Dead
                        }
                    }
                }
                other => other,
            },
            Self::Inbox(tx) => match tx.try_send(PeerDriverCommand::SendMessage(msg.clone())) {
                Ok(()) => TryFrameResult::Ok,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => TryFrameResult::Full,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => TryFrameResult::Dead,
            },
        }
    }

    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        match self {
            Self::Wire { slot, inbox } => match slot.try_encode(&msg) {
                TryFrameResult::Ok => Ok(()),
                // HWM backpressure: retry until space is available or
                // the slot dies. Termination: `mark_dead()` fires on
                // peer disconnect, connection error, heartbeat timeout,
                // or socket close, causing `try_encode` to return `Dead`.
                TryFrameResult::Full => loop {
                    let notified = slot.space_available.notified();
                    match slot.try_encode(&msg) {
                        TryFrameResult::Ok => return Ok(()),
                        TryFrameResult::Dead => return Err(Error::Closed),
                        TryFrameResult::Full => {
                            tokio::select! {
                                biased;
                                () = notified => {}
                                () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
                            }
                        }
                        TryFrameResult::Ineligible => {
                            return inbox
                                .send(PeerDriverCommand::SendMessage(msg))
                                .await
                                .map_err(|_| Error::Closed);
                        }
                    }
                },
                TryFrameResult::Ineligible => inbox
                    .send(PeerDriverCommand::SendMessage(msg))
                    .await
                    .map_err(|_| Error::Closed),
                TryFrameResult::Dead => Err(Error::Closed),
            },
            Self::Inbox(tx) => tx
                .send(PeerDriverCommand::SendMessage(msg))
                .await
                .map_err(|_| Error::Closed),
        }
    }

    #[cfg(feature = "ws")]
    pub(crate) fn is_ws(&self) -> bool {
        match self {
            Self::Wire { slot, .. } => slot.is_ws(),
            Self::Inbox(_) => false,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Wire { slot, .. } => slot.is_empty(),
            Self::Inbox(_) => true,
        }
    }
}

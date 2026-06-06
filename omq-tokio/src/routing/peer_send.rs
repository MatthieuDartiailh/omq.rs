use std::sync::Arc;

use crate::engine::encode_slot::{PeerEncodeSlot, TryEncodeResult};
use crate::engine::{DriverCommand, DriverHandle};
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;

#[derive(Debug, Clone)]
pub(crate) enum PeerSend {
    Wire {
        slot: Arc<PeerEncodeSlot>,
        inbox: tokio::sync::mpsc::Sender<DriverCommand>,
    },
    Inbox(tokio::sync::mpsc::Sender<DriverCommand>),
}

impl PeerSend {
    pub(crate) fn from_handle(handle: &DriverHandle) -> Self {
        match handle.encode_slot {
            Some(ref slot) => Self::Wire {
                slot: slot.clone(),
                inbox: handle.inbox.clone(),
            },
            None => Self::Inbox(handle.inbox.clone()),
        }
    }

    pub(crate) fn try_encode(&self, msg: &Message) -> TryEncodeResult {
        match self {
            Self::Wire { slot, inbox } => match slot.try_encode(msg) {
                TryEncodeResult::Ineligible => {
                    match inbox.try_send(DriverCommand::SendMessage(msg.clone())) {
                        Ok(()) => TryEncodeResult::Ok,
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            TryEncodeResult::Full
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            TryEncodeResult::Dead
                        }
                    }
                }
                other => other,
            },
            Self::Inbox(tx) => match tx.try_send(DriverCommand::SendMessage(msg.clone())) {
                Ok(()) => TryEncodeResult::Ok,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => TryEncodeResult::Full,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => TryEncodeResult::Dead,
            },
        }
    }

    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        match self {
            Self::Wire { slot, inbox } => match slot.try_encode(&msg) {
                TryEncodeResult::Ok => Ok(()),
                TryEncodeResult::Ineligible | TryEncodeResult::Full => inbox
                    .send(DriverCommand::SendMessage(msg))
                    .await
                    .map_err(|_| Error::Closed),
                TryEncodeResult::Dead => Err(Error::Closed),
            },
            Self::Inbox(tx) => tx
                .send(DriverCommand::SendMessage(msg))
                .await
                .map_err(|_| Error::Closed),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Wire { slot, .. } => slot.is_empty(),
            Self::Inbox(_) => true,
        }
    }
}

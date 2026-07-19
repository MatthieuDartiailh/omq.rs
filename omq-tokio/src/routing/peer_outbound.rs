use std::sync::Arc;
use tokio::sync::Notify;

use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};
use crate::engine::{PeerDriverCommand, PeerDriverHandle};
use omq_proto::message::Message;

#[derive(Debug, Clone)]
pub(crate) enum PeerOutbound {
    Wire {
        slot: Arc<PeerTransmitSlot>,
        inbox: tokio::sync::mpsc::Sender<PeerDriverCommand>,
        direct: Option<Arc<crate::socket::dispatch::DirectTcpWriter>>,
    },
    Inbox(tokio::sync::mpsc::Sender<PeerDriverCommand>),
}

impl PeerOutbound {
    pub(crate) fn from_handle(handle: &PeerDriverHandle) -> Self {
        match handle.transmit_slot {
            Some(ref slot) => Self::Wire {
                slot: slot.clone(),
                inbox: handle.inbox.clone(),
                direct: handle.direct_tcp_writer.clone(),
            },
            None => Self::Inbox(handle.inbox.clone()),
        }
    }

    pub(crate) fn try_encode(&self, msg: &Message) -> TryFrameResult {
        match self {
            Self::Wire {
                slot,
                inbox,
                direct,
            } => {
                if direct.is_none() {
                    return match inbox.try_send(PeerDriverCommand::SendMessage(msg.clone())) {
                        Ok(()) => TryFrameResult::Ok,
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            TryFrameResult::Full
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            TryFrameResult::Dead
                        }
                    };
                }
                match slot.try_encode(msg) {
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
                    TryFrameResult::Ok if direct.is_some() => {
                        let direct = direct.as_ref().unwrap();
                        match direct
                            .try_write_buffer(|buf| slot.try_drain_arena_only(buf).is_some())
                        {
                            // External frame entries remain queued for the IO
                            // driver; false does not mean the slot was full.
                            Ok(true | false) => TryFrameResult::Ok,
                            Err(_) => TryFrameResult::Dead,
                        }
                    }
                    other => other,
                }
            }
            Self::Inbox(tx) => match tx.try_send(PeerDriverCommand::SendMessage(msg.clone())) {
                Ok(()) => TryFrameResult::Ok,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => TryFrameResult::Full,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => TryFrameResult::Dead,
            },
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

    pub(crate) fn space_available(&self) -> Option<Arc<Notify>> {
        match self {
            Self::Wire { slot, .. } => Some(slot.space_available.clone()),
            Self::Inbox(_) => None,
        }
    }

    pub(crate) fn has_direct_writer(&self) -> bool {
        matches!(
            self,
            Self::Wire {
                direct: Some(_),
                ..
            }
        )
    }
}

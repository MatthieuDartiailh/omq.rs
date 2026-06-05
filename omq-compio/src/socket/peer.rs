use std::sync::{Arc, RwLock};

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::inproc::InboundFrame;
use omq_proto::message::Message;
use omq_proto::subscription::SubscriptionSet;

use crate::monitor::PeerInfo;
use crate::transport::driver::DriverCommand;
use crate::transport::inproc::InprocPeerSnapshot;

use super::inner::TaggedFrame;

pub(super) enum PeerOut {
    Inproc {
        sender: blume::Sender<TaggedFrame>,
        connection_id: u64,
    },
    Wire(WirePeerHandle),
}

pub(super) type WirePeerHandle = Arc<RwLock<flume::Sender<DriverCommand>>>;

pub(super) type DirectIoHandle = Arc<RwLock<Option<Arc<super::direct_io::DirectIoState>>>>;

pub(super) struct PeerSlot {
    pub(super) out: PeerOut,
    pub(super) direct_io: Option<DirectIoHandle>,
    pub(super) peer: Arc<RwLock<Option<InprocPeerSnapshot>>>,
    pub(super) connection_id: u64,
    pub(super) endpoint: Endpoint,
    pub(super) info: Arc<RwLock<Option<PeerInfo>>>,
    pub(super) peer_sub: Option<Arc<RwLock<SubscriptionSet>>>,
    pub(super) peer_groups: Option<Arc<RwLock<rustc_hash::FxHashSet<bytes::Bytes>>>>,
}

impl PeerOut {
    fn current_wire_sender(handle: &WirePeerHandle) -> flume::Sender<DriverCommand> {
        handle.read().expect("wire peer handle lock").clone()
    }

    pub(super) async fn send(&self, msg: Message) -> Result<()> {
        match self {
            Self::Inproc {
                sender,
                connection_id,
            } => sender
                .send_async(TaggedFrame {
                    connection_id: *connection_id,
                    frame: InboundFrame::Message(msg),
                })
                .await
                .map_err(|_| Error::Closed),
            Self::Wire(handle) => Self::current_wire_sender(handle)
                .send_async(DriverCommand::SendMessage(msg))
                .await
                .map_err(|_| Error::Closed),
        }
    }

    pub(super) fn try_send_immediate(&self, msg: Message) -> Result<()> {
        match self {
            Self::Inproc {
                sender,
                connection_id,
            } => {
                let frame = TaggedFrame {
                    connection_id: *connection_id,
                    frame: InboundFrame::Message(msg),
                };
                sender.try_send(frame).map_err(|e| match e {
                    blume::TrySendError::Full(_) => Error::WouldBlock,
                    blume::TrySendError::Disconnected(_) => Error::Closed,
                })
            }
            Self::Wire(handle) => {
                let tx = handle.read().expect("wire peer handle lock").clone();
                tx.try_send(DriverCommand::SendMessage(msg))
                    .map_err(|e| match e {
                        flume::TrySendError::Full(_) => Error::WouldBlock,
                        flume::TrySendError::Disconnected(_) => Error::Closed,
                    })
            }
        }
    }

    pub(super) async fn send_command(&self, c: omq_proto::proto::Command) -> Result<()> {
        match self {
            Self::Inproc {
                sender,
                connection_id,
            } => sender
                .send_async(TaggedFrame {
                    connection_id: *connection_id,
                    frame: InboundFrame::Command(Box::new(c)),
                })
                .await
                .map_err(|_| Error::Closed),
            Self::Wire(handle) => Self::current_wire_sender(handle)
                .send_async(DriverCommand::SendCommand(c))
                .await
                .map_err(|_| Error::Closed),
        }
    }
}

impl Clone for PeerOut {
    fn clone(&self) -> Self {
        match self {
            Self::Inproc {
                sender,
                connection_id,
            } => Self::Inproc {
                sender: sender.clone(),
                connection_id: *connection_id,
            },
            Self::Wire(h) => Self::Wire(h.clone()),
        }
    }
}

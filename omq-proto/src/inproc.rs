//! Shared inproc types used by both backends.

use bytes::Bytes;

use crate::message::Message;
use crate::proto::SocketType;
use crate::proto::command::Command;

/// Frame exchanged between two inproc peers. Either a fully-assembled
/// application `Message` or a ZMTP `Command` (SUBSCRIBE, CANCEL, JOIN,
/// LEAVE, etc.). No frame headers, no greeting, no codec: both ends
/// are in the same process.
#[derive(Debug)]
pub enum InboundFrame {
    Message(InboundMessage),
    Command(Box<Command>),
}

/// Application message with optional sender identity. The identity is
/// used by identity-aware socket types (ROUTER, SERVER, REP, STREAM,
/// PEER) to identify which peer sent the message.
#[derive(Debug)]
pub struct InboundMessage {
    pub peer_identity: Option<Bytes>,
    pub msg: Message,
}

impl InboundFrame {
    /// Construct a `Message` frame with no sender identity.
    pub fn message(msg: Message) -> Self {
        Self::Message(InboundMessage {
            peer_identity: None,
            msg,
        })
    }

    /// Construct a `Message` frame tagged with the sender's identity.
    /// Empty identity collapses to `None`.
    pub fn message_from(identity: Bytes, msg: Message) -> Self {
        let peer_identity = if identity.is_empty() {
            None
        } else {
            Some(identity)
        };
        Self::Message(InboundMessage { peer_identity, msg })
    }
}

/// Pre-computed peer info known at connect/accept time for inproc
/// peers. Stands in for the `READY` properties that real ZMTP
/// exchanges over the wire.
#[derive(Clone, Debug)]
pub struct InprocPeerSnapshot {
    pub socket_type: SocketType,
    pub identity: Bytes,
}

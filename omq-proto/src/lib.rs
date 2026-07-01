//! Sans-I/O core for omq.
//!
//! ZMTP codec, message + payload types, frame parsing, mechanism
//! handshakes (NULL / CURVE / BLAKE3ZMQ), compression transforms
//! (lz4), endpoint parsing, options, and the prefix-
//! subscription matcher. None of this depends on a runtime -
//! `omq-tokio` and `omq-compio` (and any future backend) embed it
//! directly.
#![forbid(unsafe_code)]

pub mod backoff;
pub mod direct_encode;
pub mod encoded_queue;
pub mod endpoint;
pub mod error;
pub mod fan_out_batch;
pub mod flow;
pub mod inproc;
pub mod message;
pub mod monitor;
pub mod options;
pub mod proto;
pub mod routing;
pub mod socket_api;
pub mod socket_ref;
pub mod subscription;
pub mod type_state;

#[cfg(unix)]
pub use endpoint::IpcPath;
pub use endpoint::{Endpoint, EndpointRole, EndpointSpec};
pub use error::{Error, Result, TrySendError};
pub use message::{Frame, FrameFlags, Message, MessageIter, PartCountError, generated_identity};
pub use monitor::{
    ConnectionStatus, DisconnectReason, MonitorEvent, MonitorRecvError, MonitorTryRecvError,
    PeerCommandKind, PeerIdent, PeerInfo,
};
pub use options::{KeepAlive, MechanismConfig, OnMute, Options, ReconnectPolicy};
pub use proto::mechanism::MechanismSetup;
#[cfg(any(feature = "curve", feature = "blake3zmq", feature = "plain"))]
pub use proto::mechanism::{Authenticator, MechanismPeerInfo};
#[cfg(feature = "blake3zmq")]
pub use proto::mechanism::{Blake3ZmqKeypair, Blake3ZmqPublicKey, Blake3ZmqSecretKey};
#[cfg(feature = "curve")]
pub use proto::mechanism::{CurveCookieKeyring, CurveKeypair, CurvePublicKey, CurveSecretKey};
pub use proto::{SocketType, is_compatible};

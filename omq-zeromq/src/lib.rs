//! Compatibility layer for migrating from zmq.rs to omq.
//!
//! Provides zmq.rs-compatible types (`ZmqMessage`, `Socket`, `SocketRecv`,
//! `SocketSend`, `ZmqError`) backed by omq under the hood. Drop-in
//! replacement for most zmq.rs usage with minimal code changes.

pub mod endpoint;
pub mod error;
pub mod message;
pub mod monitor;
pub mod options;
pub mod prelude;
pub mod proxy;
pub mod socket;
pub mod util;

pub use endpoint::{Endpoint, TryIntoEndpoint};
pub use error::{ZmqError, ZmqResult};
pub use message::ZmqMessage;
pub use monitor::{MonitorStream, SocketEvent};
pub use options::{DEFAULT_CONNECT_TIMEOUT, PeerIdentity, SocketOptions};
pub use proxy::proxy;
pub use socket::{
    CaptureSocket, ChannelSocket, ClientSocket, DealerRecvHalf, DealerSendHalf, DealerSocket,
    DishSocket, GatherSocket, PairSocket, PeerSocket, PubSocket, PullSocket, PushSocket,
    RadioSocket, RepSocket, ReqSocket, RouterRecvHalf, RouterSendHalf, RouterSocket, ScatterSocket,
    ServerSocket, Socket, SocketRecv, SocketSend, SubSocket, XPubSocket, XSubSocket,
};

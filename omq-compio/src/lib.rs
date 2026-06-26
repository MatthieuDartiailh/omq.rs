// TODO: remove once clippy::unused_async_trait_impl exists on stable.
#![allow(unknown_lints, clippy::unused_async_trait_impl)]

//! omq-compio - compio-runtime backend for omq.
//!
//! Built on compio's thread-per-core executor with io_uring (Linux),
//! IOCP (Windows), and kqueue (macOS) drivers. Each `Socket`
//! is pinned to the executor it was created on; cross-executor sends
//! use a runtime-agnostic mpsc (flume).
//!
//! The codec, message types, mechanism handshakes, and routing
//! algorithms come from the runtime-agnostic `omq-proto` crate.
//! This crate provides only the runtime glue.
//!
//! Supports all ZMTP socket types, TCP/IPC/inproc/UDP/WebSocket
//! transports, CURVE/BLAKE3ZMQ/PLAIN mechanisms, and lz4 compression.

#[cfg(any(feature = "curve", feature = "blake3zmq", feature = "plain"))]
pub use omq_proto::{Authenticator, MechanismPeerInfo};
#[cfg(feature = "blake3zmq")]
pub use omq_proto::{Blake3ZmqKeypair, Blake3ZmqPublicKey, Blake3ZmqSecretKey};
#[cfg(feature = "curve")]
pub use omq_proto::{CurveCookieKeyring, CurveKeypair, CurvePublicKey, CurveSecretKey};
pub use omq_proto::{
    Endpoint, EndpointRole, EndpointSpec, Error, Frame, FrameFlags, IpcPath, KeepAlive,
    MechanismConfig, MechanismSetup, Message, MessageIter, OnMute, Options, PartCountError,
    ReconnectPolicy, Result, SocketType, TrySendError, is_compatible,
};

pub use omq_proto::endpoint;
pub use omq_proto::error;
pub use omq_proto::message;
pub use omq_proto::options;
pub use omq_proto::proto;

pub(crate) mod local_cell;
pub mod monitor;
pub mod runtime;
pub mod socket;
pub mod transport;

pub use monitor::{
    ConnectionStatus, DisconnectReason, MonitorEvent, MonitorRecvError, MonitorStream,
    MonitorTryRecvError, PeerCommandKind, PeerIdent, PeerInfo,
};
pub use runtime::{
    DEFAULT_BUFFER_POOL_COUNT, DEFAULT_BUFFER_POOL_LEN, ProactorBuilderExt, build_default_runtime,
};
pub use socket::Socket;

/// Yield to the runtime once, allowing other spawned tasks to make
/// progress. Used in fan-out send paths that may return `Ok(())`
/// without awaiting anything (e.g. PUB with no matching subscribers),
/// which would otherwise starve the single-threaded compio executor.
pub(crate) async fn yield_now() {
    let mut ready = false;
    std::future::poll_fn(|cx| {
        if ready {
            std::task::Poll::Ready(())
        } else {
            ready = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await;
}

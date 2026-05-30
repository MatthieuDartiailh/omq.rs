//! Socket actor for omq-compio.
//!
//! Clone-able [`Socket`] that owns `Arc<SocketInner>`. Inproc peers
//! route directly through flume; wire peers (TCP, IPC) go through a
//! per-connection driver task that runs the ZMTP codec, with a dial
//! supervisor swapping the per-peer Sender in place when the
//! underlying driver dies.

use std::sync::{Arc, RwLock};

use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_proto::subscription::SubscriptionSet;

// Helpers shared with omq-tokio live in `omq-proto`:
//   omq_proto::endpoint::reject_encrypted_inproc
//   Endpoint::underlying_tcp / .rewrap_tcp / .scheme / .is_tcp_family
//   omq_proto::proto::transform::MessageEncoder::for_endpoint
pub(crate) use omq_proto::endpoint::reject_encrypted_inproc;

mod bind;
mod connect;
mod dial;
mod direct_io;
mod handle;
mod inner;
mod install;
mod peer;
mod recv;
mod send;
#[cfg(not(feature = "priority"))]
pub(crate) mod shared_queue;

pub use handle::Socket;

pub(crate) use direct_io::{
    DirectIoState, OneShotLargeRecvOutcome, one_shot_recv_and_feed, try_one_shot_large_recv,
};
pub(crate) const FLAT_THRESHOLD: usize = 32 * 1024;
pub(crate) use inner::{AccRestore, RecvStreamState};

/// Per-peer cmd channel capacity, sized off `Options::send_hwm`.
/// When conflate is enabled the shared send queue is cap-1 (drain-before-send),
/// so the per-peer channel only needs to hold the single forwarded message.
fn cmd_channel_capacity(options: &Options) -> usize {
    if options.conflate {
        1
    } else {
        options.send_hwm.unwrap_or(1024).max(16) as usize
    }
}

pub(super) use omq_proto::routing::supports_conflate;

/// Build a fresh empty subscription set for this socket's PUB-side
/// fan-out filter, or `None` if the socket type doesn't filter.
fn pub_side_peer_sub(st: SocketType) -> Option<Arc<RwLock<SubscriptionSet>>> {
    if matches!(st, SocketType::Pub | SocketType::XPub) {
        Some(Arc::new(RwLock::new(SubscriptionSet::new())))
    } else {
        None
    }
}

/// Build a fresh empty joined-group set for this socket's RADIO-side
/// fan-out filter, or `None` if the socket type doesn't filter.
fn radio_side_peer_groups(
    st: SocketType,
) -> Option<Arc<RwLock<rustc_hash::FxHashSet<bytes::Bytes>>>> {
    if matches!(st, SocketType::Radio) {
        Some(Arc::new(RwLock::new(rustc_hash::FxHashSet::default())))
    } else {
        None
    }
}

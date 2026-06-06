//! Connection driver: tokio glue between a `Transport`'s stream and the
//! sans-I/O ZMTP [`Connection`].
//!
//! The driver owns the stream and the codec and runs a `tokio::select!`
//! loop over (socket read, socket write, command inbox, cancellation).
//! Events produced by the codec are forwarded on a `mpsc::Sender<Event>`.
//!
//! The socket actor composes one of these per peer.

pub mod compression_pool;
pub mod direct_io;
pub mod driver;
pub(crate) mod encode_slot;

pub use driver::{
    ConnectionDriver, DriverCommand, DriverConfig, DriverHandle, PeerOut, RecvSink, RecvSinkConfig,
    YringSink,
};
#[expect(unused_imports)]
pub(crate) use encode_slot::PeerEncodeSlot;

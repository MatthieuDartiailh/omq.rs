//! Batching MPSC channel.
//!
//! The consumer side uses swap-drain: N sends produce one wake, then the
//! receiver swaps out the entire queue in O(1). Designed for high-throughput
//! one-consumer message delivery.
#![forbid(unsafe_code)]

mod error;
mod receiver;
mod sender;
mod shared;

pub use error::{RecvError, SendError, TryRecvError, TrySendError};
pub use receiver::Receiver;
pub use sender::Sender;
pub use shared::{bounded, unbounded};

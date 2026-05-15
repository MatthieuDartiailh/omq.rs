mod error;
mod receiver;
mod sender;
mod shared;
pub mod spsc;

pub use error::{RecvError, SendError, TryRecvError, TrySendError};
pub use receiver::Receiver;
pub use sender::Sender;
pub use shared::{bounded, unbounded};

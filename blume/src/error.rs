use std::fmt;

/// The receiver was dropped; the unsent value is returned.
#[derive(PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SendError(..)")
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("channel disconnected")
    }
}

impl<T> std::error::Error for SendError<T> {}

/// Non-blocking send failed: channel full or disconnected.
#[derive(PartialEq, Eq)]
pub enum TrySendError<T> {
    /// Bounded capacity reached; the unsent value is returned.
    Full(T),
    /// The receiver was dropped; the unsent value is returned.
    Disconnected(T),
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("Full(..)"),
            Self::Disconnected(_) => f.write_str("Disconnected(..)"),
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("channel full"),
            Self::Disconnected(_) => f.write_str("channel disconnected"),
        }
    }
}

impl<T> std::error::Error for TrySendError<T> {}

/// All senders were dropped and the channel is empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("channel disconnected")
    }
}

impl std::error::Error for RecvError {}

/// Non-blocking receive failed: channel empty or disconnected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// No messages available right now.
    Empty,
    /// All senders were dropped and the channel is drained.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("channel empty"),
            Self::Disconnected => f.write_str("channel disconnected"),
        }
    }
}

impl std::error::Error for TryRecvError {}

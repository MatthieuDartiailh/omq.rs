//! Error and Result types.

use std::fmt;

use thiserror::Error;

use crate::message::Message;

/// Convenience alias with `Error` as the default error type.
pub type Result<T, E = Error> = core::result::Result<T, E>;

/// Every expected failure mode in omq.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),

    #[error("unsupported transport scheme: {0}")]
    UnsupportedScheme(String),

    #[error("unsupported ZMTP version: {major}.{minor}")]
    UnsupportedZmtpVersion { major: u8, minor: u8 },

    #[error("protocol violation: {0}")]
    Protocol(String),

    #[error("handshake failed: {0}")]
    HandshakeFailed(String),

    #[error("socket closed")]
    Closed,

    #[error("operation timed out")]
    Timeout,

    #[error("message too large: {size} bytes exceeds max {max}")]
    MessageTooLarge { size: usize, max: usize },

    #[error("no route to peer")]
    Unroutable,

    #[error("operation would block")]
    WouldBlock,

    #[error("invalid configuration: {0}")]
    Config(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    pub fn is_connection_refused(&self) -> bool {
        matches!(self, Self::Io(e) if e.kind() == std::io::ErrorKind::ConnectionRefused)
    }
}

/// Error returned by `Socket::try_send`.
#[derive(Debug)]
pub enum TrySendError {
    /// Channel full (HWM reached). Contains the message for retry.
    Full(Message),
    /// Socket closed.
    Closed,
    /// Protocol or framing error.
    Error(Error),
}

impl fmt::Display for TrySendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => write!(f, "send queue full"),
            Self::Closed => write!(f, "socket closed"),
            Self::Error(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for TrySendError {}

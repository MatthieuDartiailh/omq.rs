use std::fmt;
use std::time::Duration;

use crate::endpoint::Endpoint;
use crate::message::ZmqMessage;

/// All zmq.rs-compatible error variants.
#[derive(Debug)]
pub enum ZmqError {
    Endpoint(String),
    Network(std::io::Error),
    NoSuchBind(Endpoint),
    Codec(String),
    Socket(&'static str),
    BufferFull(&'static str),
    ReturnToSender {
        reason: &'static str,
        message: ZmqMessage,
    },
    ReturnToSenderMultipart {
        reason: &'static str,
        message: ZmqMessage,
    },
    Task(String),
    Other(&'static str),
    NoMessage,
    PeerIdentity,
    UnsupportedVersion((u8, u8)),
    ConnectTimeout(Duration),
}

impl fmt::Display for ZmqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Endpoint(s) => write!(f, "endpoint error: {s}"),
            Self::Network(e) => write!(f, "network error: {e}"),
            Self::NoSuchBind(ep) => write!(f, "no such bind: {ep}"),
            Self::Codec(s) => write!(f, "codec error: {s}"),
            Self::Socket(s) => write!(f, "socket error: {s}"),
            Self::BufferFull(s) => write!(f, "buffer full: {s}"),
            Self::ReturnToSender { reason, .. } => write!(f, "return to sender: {reason}"),
            Self::ReturnToSenderMultipart { reason, .. } => {
                write!(f, "return to sender (multipart): {reason}")
            }
            Self::Task(s) => write!(f, "task error: {s}"),
            Self::Other(s) => write!(f, "{s}"),
            Self::NoMessage => write!(f, "no message available"),
            Self::PeerIdentity => write!(f, "peer identity error"),
            Self::UnsupportedVersion((maj, min)) => {
                write!(f, "unsupported ZMTP version: {maj}.{min}")
            }
            Self::ConnectTimeout(d) => write!(f, "connection timeout after {d:?}"),
        }
    }
}

impl std::error::Error for ZmqError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Network(e) => Some(e),
            _ => None,
        }
    }
}

/// Convenience alias matching the zmq.rs API.
pub type ZmqResult<T> = Result<T, ZmqError>;

impl From<omq_tokio::TrySendError> for ZmqError {
    fn from(e: omq_tokio::TrySendError) -> Self {
        match e {
            omq_tokio::TrySendError::Full(_) => Self::BufferFull("send buffer full"),
            omq_tokio::TrySendError::Closed => Self::Socket("socket closed"),
            omq_tokio::TrySendError::Error(e) => Self::from(e),
        }
    }
}

impl From<omq_proto::Error> for ZmqError {
    fn from(e: omq_proto::Error) -> Self {
        match e {
            omq_proto::Error::InvalidEndpoint(s) | omq_proto::Error::UnsupportedScheme(s) => {
                Self::Endpoint(s)
            }
            omq_proto::Error::UnsupportedZmtpVersion { major, minor } => {
                Self::UnsupportedVersion((major, minor))
            }
            omq_proto::Error::Protocol(s) | omq_proto::Error::HandshakeFailed(s) => Self::Codec(s),
            omq_proto::Error::Closed => Self::Socket("socket closed"),
            omq_proto::Error::Timeout => Self::ConnectTimeout(Duration::ZERO),
            omq_proto::Error::MessageTooLarge { size, max } => {
                Self::Codec(format!("message too large: {size} bytes exceeds max {max}"))
            }
            omq_proto::Error::Unroutable => Self::ReturnToSender {
                reason: "no route to peer",
                message: ZmqMessage::new(),
            },
            omq_proto::Error::WouldBlock => Self::BufferFull("send buffer full"),
            omq_proto::Error::Io(io) => Self::Network(io),
            _ => Self::Other("unknown error"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_invalid_endpoint() {
        let e: ZmqError = omq_proto::Error::InvalidEndpoint("bad://x".into()).into();
        assert!(matches!(e, ZmqError::Endpoint(_)));
        assert!(e.to_string().contains("bad://x"));
    }

    #[test]
    fn from_unsupported_scheme() {
        let e: ZmqError = omq_proto::Error::UnsupportedScheme("pgm".into()).into();
        assert!(matches!(e, ZmqError::Endpoint(_)));
    }

    #[test]
    fn from_protocol() {
        let e: ZmqError = omq_proto::Error::Protocol("bad frame".into()).into();
        assert!(matches!(e, ZmqError::Codec(_)));
    }

    #[test]
    fn from_closed() {
        let e: ZmqError = omq_proto::Error::Closed.into();
        assert!(matches!(e, ZmqError::Socket("socket closed")));
    }

    #[test]
    fn from_timeout() {
        let e: ZmqError = omq_proto::Error::Timeout.into();
        assert!(matches!(e, ZmqError::ConnectTimeout(_)));
    }

    #[test]
    fn from_unroutable() {
        let e: ZmqError = omq_proto::Error::Unroutable.into();
        assert!(matches!(e, ZmqError::ReturnToSender { .. }));
    }

    #[test]
    fn from_would_block() {
        let e: ZmqError = omq_proto::Error::WouldBlock.into();
        assert!(matches!(e, ZmqError::BufferFull(_)));
    }

    #[test]
    fn from_io() {
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let e: ZmqError = omq_proto::Error::Io(io).into();
        assert!(matches!(e, ZmqError::Network(_)));
    }

    #[test]
    fn from_version() {
        let e: ZmqError = omq_proto::Error::UnsupportedZmtpVersion { major: 2, minor: 0 }.into();
        assert!(matches!(e, ZmqError::UnsupportedVersion((2, 0))));
    }

    #[test]
    fn display_all_variants() {
        let variants: Vec<ZmqError> = vec![
            ZmqError::Endpoint("test".into()),
            ZmqError::Network(std::io::Error::other("x")),
            ZmqError::NoSuchBind(Endpoint::Tcp("127.0.0.1:0".parse().unwrap())),
            ZmqError::Codec("bad".into()),
            ZmqError::Socket("closed"),
            ZmqError::BufferFull("full"),
            ZmqError::ReturnToSender {
                reason: "r",
                message: ZmqMessage::new(),
            },
            ZmqError::ReturnToSenderMultipart {
                reason: "r",
                message: ZmqMessage::new(),
            },
            ZmqError::Task("failed".into()),
            ZmqError::Other("other"),
            ZmqError::NoMessage,
            ZmqError::PeerIdentity,
            ZmqError::UnsupportedVersion((3, 1)),
            ZmqError::ConnectTimeout(Duration::from_secs(5)),
        ];
        for v in variants {
            let s = v.to_string();
            assert!(!s.is_empty());
        }
    }
}

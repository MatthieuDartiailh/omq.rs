use std::time::Duration;

use bytes::Bytes;
use omq_proto::Options;

/// Socket identity for routing-aware patterns (ROUTER, DEALER).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerIdentity(Vec<u8>);

impl PeerIdentity {
    pub fn new(id: Vec<u8>) -> Self {
        Self(id)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for PeerIdentity {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl From<&[u8]> for PeerIdentity {
    fn from(s: &[u8]) -> Self {
        Self(s.to_vec())
    }
}

impl From<&str> for PeerIdentity {
    fn from(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }
}

/// Socket configuration options (zmq.rs-compatible subset).
#[derive(Debug, Clone, Default)]
pub struct SocketOptions {
    pub(crate) identity: Option<PeerIdentity>,
    pub(crate) connect_timeout: Option<Duration>,
}

impl SocketOptions {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn peer_identity(mut self, identity: PeerIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    #[must_use]
    pub fn no_connect_timeout(mut self) -> Self {
        self.connect_timeout = None;
        self
    }

    pub(crate) fn to_omq_options(&self) -> Options {
        let mut opts = Options::default();
        if let Some(ref id) = self.identity {
            opts = opts.identity(Bytes::from(id.0.clone()));
        }
        if let Some(timeout) = self.connect_timeout {
            opts = opts.handshake_timeout(timeout);
        }
        opts
    }
}

/// Default connection timeout used by zmq.rs.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options() {
        let opts = SocketOptions::new();
        assert!(opts.identity.is_none());
        assert!(opts.connect_timeout.is_none());
    }

    #[test]
    fn builder_identity() {
        let opts = SocketOptions::new().peer_identity(PeerIdentity::from("my-id"));
        assert_eq!(opts.identity.unwrap().as_bytes(), b"my-id");
    }

    #[test]
    fn builder_timeout() {
        let opts = SocketOptions::new().connect_timeout(Duration::from_secs(5));
        assert_eq!(opts.connect_timeout.unwrap(), Duration::from_secs(5));
    }

    #[test]
    fn no_connect_timeout() {
        let opts = SocketOptions::new()
            .connect_timeout(Duration::from_secs(5))
            .no_connect_timeout();
        assert!(opts.connect_timeout.is_none());
    }

    #[test]
    fn to_omq_options_identity() {
        let opts = SocketOptions::new().peer_identity(PeerIdentity::from("test"));
        let omq = opts.to_omq_options();
        assert_eq!(omq.identity.as_ref(), b"test");
    }

    #[test]
    fn to_omq_options_default() {
        let opts = SocketOptions::new();
        let omq = opts.to_omq_options();
        assert!(omq.identity.is_empty());
    }
}

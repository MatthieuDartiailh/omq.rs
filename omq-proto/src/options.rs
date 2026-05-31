//! Socket options: typed builder.
//!
//! Defaults differ from libzmq in two places: per-socket HWM semantics
//! and conflate restricted to `FanOut` patterns.

use std::time::Duration;

use bytes::Bytes;

use crate::proto::mechanism::MechanismSetup;
#[cfg(any(feature = "curve", feature = "blake3zmq", feature = "plain"))]
use crate::proto::mechanism::{Authenticator, MechanismPeerInfo};
#[cfg(feature = "blake3zmq")]
use crate::proto::mechanism::{Blake3ZmqKeypair, Blake3ZmqPublicKey};
#[cfg(feature = "curve")]
use crate::proto::mechanism::{CurveKeypair, CurvePublicKey};
/// Upper bound for `Options::compression_dict`. Both transports
/// cap at 8 KiB. Inlined as a const so the `compression_dict`
/// setter works regardless of which compression features are enabled.
const COMPRESSION_DICT_MAX: usize = 8 * 1024;

/// Per-socket configuration.
#[derive(Clone, Debug)]
pub struct Options {
    /// Send-side high-water mark, total for the socket. `None` = unbounded.
    pub send_hwm: Option<u32>,

    /// Receive-side high-water mark, total for the socket. `None` = unbounded.
    pub recv_hwm: Option<u32>,

    /// Time to wait on close for the send queue to drain.
    /// `None` = wait forever. `Some(Duration::ZERO)` = drop immediately.
    pub linger: Option<Duration>,

    /// Identity used for ROUTER / DEALER / SERVER / PEER routing. Empty = auto.
    pub identity: Bytes,

    /// Reconnection policy after a lost connection.
    pub reconnect: ReconnectPolicy,

    /// ZMTP PING interval. `None` = heartbeats disabled.
    pub heartbeat_interval: Option<Duration>,

    /// TTL announced in PING (peer's how-long-to-wait hint). `None` = omit.
    pub heartbeat_ttl: Option<Duration>,

    /// Close the connection if no traffic received within this window.
    /// Defaults to `heartbeat_interval` when unset.
    pub heartbeat_timeout: Option<Duration>,

    /// Max time allowed to complete the ZMTP handshake.
    pub handshake_timeout: Option<Duration>,

    /// Reject incoming messages larger than this. `None` = no limit.
    pub max_message_size: Option<usize>,

    /// Conflate: keep only the latest message per subscriber. Applies to
    /// `FanOut` patterns only (PUB/XPUB/RADIO). Ignored elsewhere.
    pub conflate: bool,

    /// ROUTER: fail `send` with `Error::Unroutable` for unknown identities.
    pub router_mandatory: bool,

    /// Behaviour when the socket's send HWM is reached.
    pub on_mute: OnMute,

    /// TCP keepalive policy. Applied to every accepted / dialed TCP
    /// stream after connect. Ignored on non-TCP transports
    /// (`inproc://`, `ipc://`, `udp://`).
    pub tcp_keepalive: KeepAlive,

    /// `SO_RCVBUF` size in bytes. Applied to every TCP/IPC stream after
    /// connect/accept. `None` leaves the OS default. Larger values
    /// reduce the number of kernel-to-userspace round-trips for large
    /// messages.
    pub recv_buffer_size: Option<usize>,

    /// `SO_SNDBUF` size in bytes. Applied to every TCP/IPC stream after
    /// connect/accept. `None` leaves the OS default.
    pub send_buffer_size: Option<usize>,

    /// Active security mechanism. Defaults to `Null` (no encryption).
    pub mechanism: MechanismSetup,

    /// Outbound compression dictionary. Used by `lz4+tcp://` (and, when it
    /// lands, `zstd+tcp://`); ignored on plain transports. The dict is
    /// shipped to the peer once per connection; subsequent parts are
    /// compressed against it. Must be 1..=8192 bytes.
    pub compression_dict: Option<Bytes>,

    /// Zstd auto-trained dictionaries (RFC §6.5). Defaults to **on**.
    /// When neither `compression_dict` nor any other dict source
    /// is configured on a `zstd+tcp://` connection, the encoder
    /// samples the first 1000 outbound messages or 100 KiB total
    /// plaintext (whichever fires first), trains a dict (capacity
    /// controlled by `compression_dict_capacity`, default 2 KiB),
    /// and ships it. After that the per-frame compression threshold
    /// drops from 512 B to 64 B and small messages start riding the
    /// dict. Setting `compression_dict` overrides: auto-train is
    /// silently disabled when a static dict is supplied.
    /// Ignored by `lz4+tcp://` (LZ4 has no standard trainer).
    /// Set to `false` to suppress training (e.g. tests that need a
    /// deterministic wire shape).
    pub compression_auto_train: bool,

    /// Zstd compression level. Negative values select the "fast" strategy
    /// (lower ratio, higher speed); 0 maps to zstd's default (level 3);
    /// positive values trade speed for ratio. Ignored by `lz4+tcp://`.
    /// Default: -3.
    pub compression_level: i32,

    /// Minimum payload size (bytes) before compression is attempted.
    /// Messages smaller than this are sent uncompressed regardless of
    /// dict presence. `None` uses the built-in defaults (which vary by
    /// transport and dict presence). Useful on high-bandwidth links
    /// where compressing tiny messages wastes CPU.
    pub compression_threshold: Option<usize>,

    /// Zstd auto-train dict capacity in bytes. Controls the maximum
    /// size of the dictionary produced by auto-training. Default: 2048.
    /// Ignored by `lz4+tcp://` and when `compression_dict` is set.
    pub compression_dict_capacity: Option<usize>,

    /// Maximum dictionary size (bytes) accepted from a peer. Dicts
    /// larger than this are rejected. Default: 8192 for both
    /// transports.
    pub max_recv_dict_size: Option<usize>,

    /// Minimum message size (bytes) before compression is offloaded to
    /// a background thread (tokio backend only). Messages smaller than
    /// this are compressed inline on the driver task. `None` disables
    /// offloading entirely. Default: `Some(8192)`.
    pub compression_offload_threshold: Option<usize>,

    /// Switch the recv path to a sized one-shot read for any inbound
    /// frame whose wire payload is at least this many bytes.
    ///
    /// On `omq-compio` the default recv path uses an io_uring multi-shot
    /// SQE with a `BUF_RING` pool; each CQE delivers a borrowed pool
    /// slot that the driver memcpys into an owned `Bytes` so the slot
    /// can be returned to the pool immediately. For frames much larger
    /// than the slot size that memcpy dominates the recv cost. With
    /// `Some(n)` set, the driver detects a large frame from its header
    /// and recvs the payload directly into one sized `BytesMut` of
    /// exactly `payload_len` bytes — no pool, no userspace copy.
    ///
    /// `None` disables the optimization (multi-shot path always).
    /// Default: `Some(128 * 1024)` — small enough to skip the memcpy on
    /// any frame past four 32 KiB pool slots, large enough that the
    /// per-frame SQE-rebuild cost is amortised.
    ///
    /// On `omq-tokio` the same threshold triggers a `read_exact` fast
    /// path that reads large payloads into a single pre-sized buffer
    /// instead of accumulating fixed-size reads through the codec.
    pub large_message_threshold: Option<usize>,

    /// TLS configuration for `wss://` endpoints. Ignored for non-WSS
    /// transports. Requires the `ws` feature.
    #[cfg(feature = "ws")]
    pub wss_tls: WssTls,
}

/// TLS configuration for WSS endpoints.
#[cfg(feature = "ws")]
#[derive(Clone, Debug, Default)]
pub struct WssTls {
    /// PEM-encoded server certificate chain for WSS bind.
    pub server_cert_pem: Option<Vec<u8>>,
    /// PEM-encoded server private key for WSS bind.
    pub server_key_pem: Option<Vec<u8>>,
    /// Accept invalid server certificates on connect (for testing).
    pub accept_invalid_certs: bool,
}

/// Backward-compatible alias. [`MechanismSetup`] is the canonical type.
pub type MechanismConfig = MechanismSetup;

impl Default for Options {
    fn default() -> Self {
        Self {
            send_hwm: Some(1000),
            recv_hwm: Some(1000),
            linger: Some(Duration::ZERO),
            identity: Bytes::new(),
            reconnect: ReconnectPolicy::default(),
            heartbeat_interval: None,
            heartbeat_ttl: None,
            heartbeat_timeout: None,
            handshake_timeout: Some(Duration::from_secs(30)),
            max_message_size: None,
            conflate: false,
            router_mandatory: false,
            on_mute: OnMute::Block,
            tcp_keepalive: KeepAlive::default(),
            recv_buffer_size: None,
            send_buffer_size: None,
            mechanism: MechanismSetup::Null,
            compression_dict: None,
            compression_auto_train: true,
            compression_level: -3,
            compression_threshold: None,
            compression_dict_capacity: None,
            max_recv_dict_size: None,
            compression_offload_threshold: Some(8192),
            large_message_threshold: Some(128 * 1024),
            #[cfg(feature = "ws")]
            wss_tls: WssTls::default(),
        }
    }
}

/// ZMTP PING encodes TTL as tenths of a second in a `u16`.
const MAX_HEARTBEAT_TTL_MS: u128 = 6_553_500;

impl Options {
    /// Create options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check ZMTP protocol limits that would cause hard-to-debug wire
    /// failures if violated. Called from `Socket::new` in both backends.
    pub fn validate(&self) -> crate::error::Result<()> {
        let id_len = self.identity.len();
        if id_len > 255 {
            return Err(crate::error::Error::Config(format!(
                "identity length {id_len} exceeds ZMTP limit of 255 bytes"
            )));
        }
        if let Some(ttl) = self.heartbeat_ttl
            && ttl.as_millis() > MAX_HEARTBEAT_TTL_MS
        {
            return Err(crate::error::Error::Config(format!(
                "heartbeat_ttl {ttl:?} exceeds ZMTP maximum of 6553.5s"
            )));
        }
        #[cfg(feature = "plain")]
        if let MechanismSetup::PlainClient {
            ref username,
            ref password,
        } = self.mechanism
        {
            if username.len() > 255 {
                return Err(crate::error::Error::Config(format!(
                    "PLAIN username length {} exceeds 255-byte limit",
                    username.len()
                )));
            }
            if password.len() > 255 {
                return Err(crate::error::Error::Config(format!(
                    "PLAIN password length {} exceeds 255-byte limit",
                    password.len()
                )));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn send_hwm(mut self, hwm: u32) -> Self {
        self.send_hwm = Some(hwm);
        self
    }

    #[must_use]
    pub fn recv_hwm(mut self, hwm: u32) -> Self {
        self.recv_hwm = Some(hwm);
        self
    }

    #[must_use]
    pub fn unbounded_send(mut self) -> Self {
        self.send_hwm = None;
        self
    }

    #[must_use]
    pub fn unbounded_recv(mut self) -> Self {
        self.recv_hwm = None;
        self
    }

    #[must_use]
    pub fn linger(mut self, d: Duration) -> Self {
        self.linger = Some(d);
        self
    }

    #[must_use]
    pub fn linger_forever(mut self) -> Self {
        self.linger = None;
        self
    }

    #[must_use]
    pub fn identity(mut self, id: impl Into<Bytes>) -> Self {
        self.identity = id.into();
        self
    }

    #[must_use]
    pub fn reconnect(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    #[must_use]
    pub fn heartbeat_interval(mut self, d: Duration) -> Self {
        self.heartbeat_interval = Some(d);
        self
    }

    #[must_use]
    pub fn heartbeat_ttl(mut self, d: Duration) -> Self {
        self.heartbeat_ttl = Some(d);
        self
    }

    #[must_use]
    pub fn heartbeat_timeout(mut self, d: Duration) -> Self {
        self.heartbeat_timeout = Some(d);
        self
    }

    #[must_use]
    pub fn handshake_timeout(mut self, d: Duration) -> Self {
        self.handshake_timeout = Some(d);
        self
    }

    #[must_use]
    pub fn max_message_size(mut self, n: usize) -> Self {
        self.max_message_size = Some(n);
        self
    }

    #[must_use]
    pub fn conflate(mut self, c: bool) -> Self {
        self.conflate = c;
        self
    }

    #[must_use]
    pub fn router_mandatory(mut self, m: bool) -> Self {
        self.router_mandatory = m;
        self
    }

    #[must_use]
    pub fn on_mute(mut self, m: OnMute) -> Self {
        self.on_mute = m;
        self
    }

    #[must_use]
    pub fn tcp_keepalive(mut self, k: KeepAlive) -> Self {
        self.tcp_keepalive = k;
        self
    }

    #[must_use]
    pub fn recv_buffer_size(mut self, bytes: usize) -> Self {
        self.recv_buffer_size = Some(bytes);
        self
    }

    #[must_use]
    pub fn send_buffer_size(mut self, bytes: usize) -> Self {
        self.send_buffer_size = Some(bytes);
        self
    }

    /// Set the wire-payload size at which the recv path switches to a
    /// sized one-shot read. See the field-level docs on
    /// [`large_message_threshold`](Self::large_message_threshold) for
    /// the trade-offs. Pass `0` to fall back to the multi-shot path
    /// for every frame; the threshold is treated as `usize::MAX` in
    /// that case.
    #[must_use]
    pub fn large_message_threshold(mut self, n: usize) -> Self {
        self.large_message_threshold = if n == 0 { None } else { Some(n) };
        self
    }

    /// Disable the one-shot recv switch entirely; the multi-shot path
    /// is used for every inbound frame regardless of size.
    #[must_use]
    pub fn disable_large_message_path(mut self) -> Self {
        self.large_message_threshold = None;
        self
    }

    /// Configure this socket as a CURVE server with the given long-term
    /// keypair. Incoming clients must present the matching server public
    /// key during their handshake. A fresh cookie keyring with the
    /// default rotation interval (~30 s) is created. Reach in via
    /// [`MechanismSetup::curve_cookie_keyring`] to configure or share
    /// it. Use [`Self::authenticator`] to add a per-client admission
    /// callback.
    #[cfg(feature = "curve")]
    #[must_use]
    pub fn curve_server(mut self, our_keypair: CurveKeypair) -> Self {
        self.mechanism = MechanismSetup::CurveServer {
            our_keypair,
            cookie_keyring: std::sync::Arc::new(crate::proto::mechanism::CurveCookieKeyring::new()),
            authenticator: None,
        };
        self
    }

    /// Configure this socket as a CURVE client targeting `server_public`.
    #[cfg(feature = "curve")]
    #[must_use]
    pub fn curve_client(
        mut self,
        our_keypair: CurveKeypair,
        server_public: CurvePublicKey,
    ) -> Self {
        self.mechanism = MechanismSetup::CurveClient {
            our_keypair,
            server_public,
        };
        self
    }

    /// Configure this socket as a BLAKE3ZMQ server. Non-standard,
    /// omq-to-omq only - peers must also be `blake3zmq`-built.
    /// A fresh cookie keyring with the default rotation interval
    /// (~30 s) is created. Reach in via
    /// [`MechanismSetup::blake3zmq_cookie_keyring`] to configure or
    /// share it. Use [`Self::blake3zmq_authenticator`] to add a
    /// per-client admission callback.
    #[cfg(feature = "blake3zmq")]
    #[must_use]
    pub fn blake3zmq_server(mut self, our_keypair: Blake3ZmqKeypair) -> Self {
        self.mechanism = MechanismSetup::Blake3ZmqServer {
            our_keypair,
            cookie_keyring: std::sync::Arc::new(
                crate::proto::mechanism::blake3zmq::CookieKeyring::new(),
            ),
            authenticator: None,
        };
        self
    }

    /// Install a server-side authenticator. Called once per handshake
    /// after the underlying mechanism has cryptographically verified
    /// the peer (CURVE: vouch decrypt; BLAKE3ZMQ: vouch decrypt).
    /// The callback receives the peer's long-term public key plus a
    /// tag identifying which mechanism produced it. Return `false` to
    /// reject the client; the handshake aborts.
    ///
    /// Works for both CURVE and BLAKE3ZMQ server configurations.
    /// Panics if the current mechanism is not a server configuration
    /// of an encrypting mechanism (i.e., `curve_server` or `blake3zmq_server`
    /// must be called before this method).
    #[cfg(any(feature = "curve", feature = "blake3zmq"))]
    #[must_use]
    #[track_caller]
    pub fn authenticator<F>(mut self, f: F) -> Self
    where
        F: Fn(&MechanismPeerInfo) -> bool + Send + Sync + 'static,
    {
        let auth = Authenticator::new(f);
        match &mut self.mechanism {
            #[cfg(feature = "curve")]
            MechanismSetup::CurveServer { authenticator, .. } => {
                *authenticator = Some(auth);
            }
            #[cfg(feature = "blake3zmq")]
            MechanismSetup::Blake3ZmqServer { authenticator, .. } => {
                *authenticator = Some(auth);
            }
            _ => panic!("authenticator requires a server-side encrypting mechanism"),
        }
        self
    }

    /// Configure this socket as a PLAIN server (RFC 24). The
    /// authenticator receives [`MechanismPeerInfo`] with `username`
    /// and `password` populated; return `true` to admit the client.
    /// No encryption is applied — use on trusted networks only.
    #[cfg(feature = "plain")]
    #[must_use]
    pub fn plain_server<F>(mut self, f: F) -> Self
    where
        F: Fn(&MechanismPeerInfo) -> bool + Send + Sync + 'static,
    {
        self.mechanism = MechanismSetup::PlainServer {
            authenticator: Authenticator::new(f),
        };
        self
    }

    /// Configure this socket as a PLAIN client with the given
    /// credentials. The server's authenticator decides admission.
    #[cfg(feature = "plain")]
    #[must_use]
    pub fn plain_client(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.mechanism = MechanismSetup::PlainClient {
            username: username.into(),
            password: password.into(),
        };
        self
    }

    /// Configure this socket as a BLAKE3ZMQ client targeting
    /// `server_public`. Non-standard, omq-to-omq only.
    #[cfg(feature = "blake3zmq")]
    #[must_use]
    pub fn blake3zmq_client(
        mut self,
        our_keypair: Blake3ZmqKeypair,
        server_public: Blake3ZmqPublicKey,
    ) -> Self {
        self.mechanism = MechanismSetup::Blake3ZmqClient {
            our_keypair,
            server_public,
        };
        self
    }

    /// Set the outbound compression dictionary. Used by compression
    /// transports (`lz4+tcp://`, future `zstd+tcp://`). Panics if the dict
    /// is empty or larger than 8192 bytes (`omq-lz4` RFC §6.2).
    #[must_use]
    pub fn compression_dict(mut self, dict: impl Into<Bytes>) -> Self {
        let dict = dict.into();
        assert!(
            !dict.is_empty() && dict.len() <= COMPRESSION_DICT_MAX,
            "compression dict must be 1..={COMPRESSION_DICT_MAX} bytes, got {}",
            dict.len()
        );
        self.compression_dict = Some(dict);
        self
    }

    /// Toggle Zstd auto-trained dictionaries (`zstd+tcp://` only).
    /// On by default; pass `false` to suppress training. See
    /// [`Options::compression_auto_train`] for semantics.
    #[must_use]
    pub fn compression_auto_train(mut self, enabled: bool) -> Self {
        self.compression_auto_train = enabled;
        self
    }

    /// Set the zstd compression level (default -3). Negative = fast
    /// strategy, 0 = zstd default (3), positive = higher ratio.
    /// Ignored by `lz4+tcp://`.
    #[must_use]
    pub fn compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    /// Override the minimum payload size for compression. Messages
    /// smaller than `threshold` bytes are sent uncompressed. Useful
    /// on high-bandwidth links where compressing tiny messages wastes
    /// CPU without meaningful wire savings.
    #[must_use]
    pub fn compression_threshold(mut self, threshold: usize) -> Self {
        self.compression_threshold = Some(threshold);
        self
    }

    /// Set the zstd auto-train dictionary capacity in bytes
    /// (default 2048). Ignored by `lz4+tcp://` and when
    /// `compression_dict` is set.
    #[must_use]
    pub fn compression_dict_capacity(mut self, capacity: usize) -> Self {
        self.compression_dict_capacity = Some(capacity);
        self
    }

    /// Set the maximum dictionary size accepted from a peer.
    /// Dicts larger than this are rejected at decode time.
    #[must_use]
    pub fn max_recv_dict_size(mut self, max: usize) -> Self {
        self.max_recv_dict_size = Some(max);
        self
    }

    /// Minimum message size before compression is offloaded to a
    /// background thread (tokio backend only). `None` disables offloading.
    #[must_use]
    pub fn compression_offload_threshold(mut self, threshold: Option<usize>) -> Self {
        self.compression_offload_threshold = threshold;
        self
    }
}

impl From<Bytes> for Options {
    /// Convenience: build options with a given identity, defaults for the rest.
    fn from(identity: Bytes) -> Self {
        Self::default().identity(identity)
    }
}

/// Reconnection policy applied after a lost connection on `connect()` sockets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReconnectPolicy {
    /// No reconnect; the connection is dropped permanently on failure.
    Disabled,
    /// Retry at a constant interval.
    Fixed(Duration),
    /// Exponential backoff between `min` and `max`, doubling on each retry.
    Exponential { min: Duration, max: Duration },
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        // Constant 100ms matches libzmq's `ZMQ_RECONNECT_IVL` default.
        // Users who want exponential backoff opt in via
        // `Options::reconnect(ReconnectPolicy::Exponential { .. })`.
        Self::Fixed(Duration::from_millis(100))
    }
}

/// What to do when the send HWM is reached and a new message arrives.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum OnMute {
    /// Block the sender until room is available.
    #[default]
    Block,
    /// Drop the incoming message silently.
    DropNewest,
    /// Drop the oldest queued message, then enqueue the new one.
    DropOldest,
}

/// TCP keepalive policy. `Default` leaves the OS defaults alone (matches
/// libzmq's `ZMQ_TCP_KEEPALIVE = -1`); `Disabled` clears `SO_KEEPALIVE`;
/// `Enabled` sets `SO_KEEPALIVE` and pins the three timing knobs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum KeepAlive {
    /// OS defaults; nothing applied to the socket.
    #[default]
    Default,
    /// Explicitly disable `SO_KEEPALIVE`.
    Disabled,
    /// Enable `SO_KEEPALIVE` and set the timing triplet.
    Enabled {
        /// Idle time before the first probe is sent (`TCP_KEEPIDLE`).
        idle: Duration,
        /// Interval between probes (`TCP_KEEPINTVL`).
        intvl: Duration,
        /// Failed probes before declaring the connection dead (`TCP_KEEPCNT`).
        cnt: u32,
    },
}

impl Options {
    /// Apply `SO_RCVBUF` and `SO_SNDBUF` to a connected socket.
    pub fn apply_socket_buffers<S: std::os::fd::AsFd>(&self, sock: &S) -> std::io::Result<()> {
        let sref = socket2::SockRef::from(sock);
        if let Some(n) = self.recv_buffer_size {
            sref.set_recv_buffer_size(n)?;
        }
        if let Some(n) = self.send_buffer_size {
            sref.set_send_buffer_size(n)?;
        }
        Ok(())
    }
}

impl KeepAlive {
    /// Apply this keepalive policy to a connected TCP socket. Used by
    /// both `omq-tokio` and `omq-compio` after `connect`/`accept` so the
    /// option is in effect for the connection's lifetime.
    pub fn apply<S: std::os::fd::AsFd>(&self, sock: &S) -> std::io::Result<()> {
        let sref = socket2::SockRef::from(sock);
        match self {
            KeepAlive::Default => Ok(()),
            KeepAlive::Disabled => sref.set_keepalive(false),
            KeepAlive::Enabled { idle, intvl, cnt } => {
                let ka = socket2::TcpKeepalive::new()
                    .with_time(*idle)
                    .with_interval(*intvl)
                    .with_retries(*cnt);
                sref.set_tcp_keepalive(&ka)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_per_socket_hwm_block() {
        let o = Options::default();
        assert_eq!(o.send_hwm, Some(1000));
        assert_eq!(o.recv_hwm, Some(1000));
        assert_eq!(o.linger, Some(Duration::ZERO));
        assert_eq!(o.handshake_timeout, Some(Duration::from_secs(30)));
        assert_eq!(o.heartbeat_interval, None);
        assert_eq!(o.max_message_size, None);
        assert_eq!(o.tcp_keepalive, KeepAlive::Default);
        assert!(!o.conflate);
        assert!(!o.router_mandatory);
        assert_eq!(o.on_mute, OnMute::Block);
        assert_eq!(o.large_message_threshold, Some(128 * 1024));
    }

    #[test]
    fn large_message_threshold_setters() {
        assert_eq!(
            Options::new()
                .large_message_threshold(64 * 1024)
                .large_message_threshold,
            Some(64 * 1024),
        );
        assert_eq!(
            Options::new()
                .large_message_threshold(0)
                .large_message_threshold,
            None,
        );
        assert_eq!(
            Options::new()
                .disable_large_message_path()
                .large_message_threshold,
            None,
        );
    }

    #[test]
    fn tcp_keepalive_builder() {
        let o = Options::new().tcp_keepalive(KeepAlive::Disabled);
        assert_eq!(o.tcp_keepalive, KeepAlive::Disabled);
        let o = Options::new().tcp_keepalive(KeepAlive::Enabled {
            idle: Duration::from_secs(30),
            intvl: Duration::from_secs(5),
            cnt: 3,
        });
        match o.tcp_keepalive {
            KeepAlive::Enabled { idle, intvl, cnt } => {
                assert_eq!(idle, Duration::from_secs(30));
                assert_eq!(intvl, Duration::from_secs(5));
                assert_eq!(cnt, 3);
            }
            _ => panic!("expected Enabled"),
        }
    }

    #[test]
    fn reconnect_default_fixed_100ms() {
        assert_eq!(
            ReconnectPolicy::default(),
            ReconnectPolicy::Fixed(Duration::from_millis(100))
        );
    }

    #[test]
    fn builder_chaining() {
        let o = Options::new()
            .send_hwm(42)
            .recv_hwm(99)
            .linger(Duration::from_secs(5))
            .identity("router-id")
            .heartbeat_interval(Duration::from_secs(1))
            .max_message_size(1024)
            .conflate(true)
            .router_mandatory(true)
            .on_mute(OnMute::DropNewest);
        assert_eq!(o.send_hwm, Some(42));
        assert_eq!(o.recv_hwm, Some(99));
        assert_eq!(o.linger, Some(Duration::from_secs(5)));
        assert_eq!(o.identity, &b"router-id"[..]);
        assert_eq!(o.heartbeat_interval, Some(Duration::from_secs(1)));
        assert_eq!(o.max_message_size, Some(1024));
        assert!(o.conflate);
        assert!(o.router_mandatory);
        assert_eq!(o.on_mute, OnMute::DropNewest);
    }

    #[test]
    fn unbounded_queues() {
        let o = Options::new().unbounded_send().unbounded_recv();
        assert_eq!(o.send_hwm, None);
        assert_eq!(o.recv_hwm, None);
    }

    #[test]
    fn linger_forever() {
        let o = Options::new().linger_forever();
        assert_eq!(o.linger, None);
    }

    #[test]
    fn from_bytes_sets_identity() {
        let o: Options = Bytes::from_static(b"id").into();
        assert_eq!(o.identity, &b"id"[..]);
        assert_eq!(o.send_hwm, Some(1000));
    }
}

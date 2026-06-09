//! Socket options overlay and `zmq_setsockopt` / `zmq_getsockopt`.
#![expect(clippy::cast_possible_wrap)]

use std::ffi::c_int;
use std::time::Duration;

use bytes::Bytes;
use omq_tokio::MechanismSetup;
use omq_tokio::options::{KeepAlive, ReconnectPolicy};

use crate::socket::DEFAULT_HWM;

macro_rules! lock_overlay {
    ($sock:expr) => {
        match $sock.overlay.lock() {
            Ok(g) => g,
            Err(_) => return crate::error::fail(crate::error::ETERM),
        }
    };
}

#[derive(Clone, Debug, Default)]
#[expect(clippy::struct_excessive_bools)]
pub(crate) struct SocketOverlay {
    pub send_hwm: Option<u32>,
    pub recv_hwm: Option<u32>,
    pub linger: Option<Duration>,
    pub identity: Bytes,
    pub router_mandatory: bool,
    pub reconnect_ivl: Option<Duration>,
    pub reconnect_ivl_max: Option<Duration>,
    pub heartbeat_ivl: Option<Duration>,
    pub heartbeat_ttl: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
    pub handshake_ivl: Option<Duration>,
    pub max_message_size: Option<usize>,
    pub conflate: bool,
    pub tcp_keepalive: i32,
    pub tcp_keepalive_cnt: Option<u32>,
    pub tcp_keepalive_idle: Option<Duration>,
    pub tcp_keepalive_intvl: Option<Duration>,
    pub mechanism: MechanismOverlay,
    pub sndbuf: Option<usize>,
    pub rcvbuf: Option<usize>,
    pub xpub_verbose: bool,
    pub ipv6: bool,
    pub backlog: i32,
    pub immediate: bool,
    pub connect_timeout: i32,
    pub probe_router: bool,
    pub req_correlate: bool,
    pub req_relaxed: bool,
    pub xpub_nodrop: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) enum MechanismOverlay {
    #[default]
    Null,
    PlainServer,
    PlainClient {
        username: String,
        password: String,
    },
    CurveServer {
        secret_key: [u8; 32],
    },
    CurveClient {
        public_key: [u8; 32],
        secret_key: [u8; 32],
        server_key: [u8; 32],
    },
}

impl SocketOverlay {
    pub(crate) fn to_options(&self) -> omq_tokio::Options {
        let keepalive = match self.tcp_keepalive {
            1 => KeepAlive::Enabled {
                idle: self.tcp_keepalive_idle.unwrap_or(Duration::from_mins(1)),
                intvl: self.tcp_keepalive_intvl.unwrap_or(Duration::from_secs(10)),
                cnt: self.tcp_keepalive_cnt.unwrap_or(6),
            },
            0 => KeepAlive::Disabled,
            _ => KeepAlive::Default,
        };
        let reconnect = match (self.reconnect_ivl, self.reconnect_ivl_max) {
            (None, _) => ReconnectPolicy::Disabled,
            (Some(min), None) => ReconnectPolicy::Fixed(min),
            (Some(min), Some(max)) => ReconnectPolicy::Exponential { min, max },
        };
        let mechanism = match &self.mechanism {
            MechanismOverlay::Null => MechanismSetup::Null,
            MechanismOverlay::PlainServer => MechanismSetup::PlainServer {
                authenticator: omq_tokio::Authenticator::new(|_| true),
            },
            MechanismOverlay::PlainClient { username, password } => MechanismSetup::PlainClient {
                username: username.clone(),
                password: password.clone(),
            },
            MechanismOverlay::CurveServer { secret_key } => {
                let sec = omq_tokio::CurveSecretKey::from_bytes(*secret_key);
                let crypto_sec = crypto_box::SecretKey::from(*secret_key);
                let crypto_pub = crypto_sec.public_key();
                let pubk = omq_tokio::CurvePublicKey::from_bytes(*crypto_pub.as_bytes());
                MechanismSetup::CurveServer {
                    our_keypair: omq_tokio::CurveKeypair {
                        secret: sec,
                        public: pubk,
                    },
                    cookie_keyring: std::sync::Arc::new(omq_tokio::CurveCookieKeyring::new()),
                    authenticator: None,
                }
            }
            MechanismOverlay::CurveClient {
                public_key,
                secret_key,
                server_key,
            } => MechanismSetup::CurveClient {
                our_keypair: omq_tokio::CurveKeypair {
                    secret: omq_tokio::CurveSecretKey::from_bytes(*secret_key),
                    public: omq_tokio::CurvePublicKey::from_bytes(*public_key),
                },
                server_public: omq_tokio::CurvePublicKey::from_bytes(*server_key),
            },
        };
        omq_tokio::Options {
            send_hwm: self.send_hwm.or(Some(DEFAULT_HWM as u32)),
            recv_hwm: self.recv_hwm.or(Some(DEFAULT_HWM as u32)),
            linger: self.linger,
            identity: self.identity.clone(),
            max_message_size: self.max_message_size,
            router_mandatory: self.router_mandatory,
            heartbeat_interval: self.heartbeat_ivl,
            heartbeat_ttl: self.heartbeat_ttl,
            heartbeat_timeout: self.heartbeat_timeout,
            handshake_timeout: match (self.handshake_ivl, self.connect_timeout) {
                (Some(h), t) if t > 0 => Some(h.min(Duration::from_millis(t as u64))),
                (None, t) if t > 0 => Some(Duration::from_millis(t as u64)),
                (h, _) => h,
            },
            conflate: self.conflate,
            tcp_keepalive: keepalive,
            reconnect,
            send_buffer_size: self.sndbuf,
            recv_buffer_size: self.rcvbuf,
            mechanism,
            xpub_nodrop: self.xpub_nodrop,
            ..Default::default()
        }
    }
}

// ZMQ socket option constants
const ZMQ_SNDHWM: c_int = 23;
const ZMQ_RCVHWM: c_int = 24;
const ZMQ_SNDTIMEO: c_int = 28;
const ZMQ_RCVTIMEO: c_int = 27;
const ZMQ_SUBSCRIBE: c_int = 6;
const ZMQ_UNSUBSCRIBE: c_int = 7;
const ZMQ_LINGER: c_int = 17;
const ZMQ_IDENTITY: c_int = 5;
const ZMQ_RCVMORE: c_int = 13;
const ZMQ_TYPE: c_int = 16;
const ZMQ_FD: c_int = 14;
const ZMQ_EVENTS: c_int = 15;
const ZMQ_RECONNECT_IVL: c_int = 18;
const ZMQ_RECONNECT_IVL_MAX: c_int = 21;
const ZMQ_HEARTBEAT_IVL: c_int = 75;
const ZMQ_HEARTBEAT_TTL: c_int = 76;
const ZMQ_HEARTBEAT_TIMEOUT: c_int = 77;
const ZMQ_HANDSHAKE_IVL: c_int = 66;
const ZMQ_MAXMSGSIZE: c_int = 22;
const ZMQ_ROUTER_MANDATORY: c_int = 33;
const ZMQ_CONFLATE: c_int = 54;
const ZMQ_TCP_KEEPALIVE: c_int = 34;
const ZMQ_TCP_KEEPALIVE_CNT: c_int = 35;
const ZMQ_TCP_KEEPALIVE_IDLE: c_int = 36;
const ZMQ_TCP_KEEPALIVE_INTVL: c_int = 37;
const ZMQ_SNDBUF: c_int = 11;
const ZMQ_RCVBUF: c_int = 12;
const ZMQ_XPUB_VERBOSE: c_int = 40;
const ZMQ_LAST_ENDPOINT: c_int = 32;
const ZMQ_MECHANISM: c_int = 43;
const ZMQ_PLAIN_SERVER: c_int = 44;
const ZMQ_PLAIN_USERNAME: c_int = 45;
const ZMQ_PLAIN_PASSWORD: c_int = 46;
const ZMQ_CURVE_SERVER: c_int = 47;
const ZMQ_CURVE_PUBLICKEY: c_int = 48;
const ZMQ_CURVE_SECRETKEY: c_int = 49;
const ZMQ_CURVE_SERVERKEY: c_int = 50;

const ZMQ_BACKLOG: c_int = 19;
const ZMQ_IMMEDIATE: c_int = 39;
const ZMQ_IPV6: c_int = 42;
const ZMQ_PROBE_ROUTER: c_int = 51;
const ZMQ_REQ_CORRELATE: c_int = 52;
const ZMQ_REQ_RELAXED: c_int = 53;
const ZMQ_ROUTER_HANDOVER: c_int = 56;
const ZMQ_XPUB_NODROP: c_int = 69;
const ZMQ_CONNECT_TIMEOUT: c_int = 79;

const ZMQ_AFFINITY: c_int = 4;
const ZMQ_RATE: c_int = 8;
const ZMQ_RECOVERY_IVL: c_int = 9;
const ZMQ_MULTICAST_HOPS: c_int = 25;
const ZMQ_ZAP_DOMAIN: c_int = 55;
const ZMQ_TOS: c_int = 57;
const ZMQ_CONNECT_ROUTING_ID: c_int = 61;
const ZMQ_SOCKS_PROXY: c_int = 68;
const ZMQ_INVERT_MATCHING: c_int = 74;
const ZMQ_TCP_MAXRT: c_int = 80;
const ZMQ_BINDTODEVICE: c_int = 92;
const ZMQ_MULTICAST_LOOP: c_int = 96;
const ZMQ_ROUTER_NOTIFY: c_int = 97;

const ZMQ_POLLIN: c_int = crate::consts::ZMQ_POLLIN;
const ZMQ_POLLOUT: c_int = crate::consts::ZMQ_POLLOUT;

const ZMQ_NULL: c_int = 0;
const ZMQ_PLAIN: c_int = 1;
const ZMQ_CURVE: c_int = 2;

#[expect(clippy::too_many_lines)]
#[unsafe(no_mangle)]
pub extern "C" fn zmq_setsockopt(
    sock: *mut libc::c_void,
    option: c_int,
    optval: *const libc::c_void,
    optvallen: usize,
) -> c_int {
    if sock.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: caller guarantees sock is a valid socket pointer from zmq_socket.
    let sock_arc = unsafe { &*(sock.cast::<std::sync::Arc<crate::socket::OmqSocket>>()) };

    match option {
        ZMQ_SNDTIMEO => {
            let v = read_i32(optval, optvallen);
            sock_arc
                .sndtimeo_ms
                .store(i64::from(v), std::sync::atomic::Ordering::Relaxed);
        }
        ZMQ_RCVTIMEO => {
            let v = read_i32(optval, optvallen);
            sock_arc
                .rcvtimeo_ms
                .store(i64::from(v), std::sync::atomic::Ordering::Relaxed);
        }
        ZMQ_SNDHWM => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).send_hwm = if v <= 0 { None } else { Some(v as u32) };
        }
        ZMQ_RCVHWM => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).recv_hwm = if v <= 0 { None } else { Some(v as u32) };
        }
        ZMQ_LINGER => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).linger = if v < 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_IDENTITY => {
            if optval.is_null() {
                return crate::error::fail(libc::EFAULT);
            }
            // SAFETY: optval is non-null (checked above); optvallen bytes are readable.
            let bytes = unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), optvallen) };
            lock_overlay!(sock_arc).identity = Bytes::copy_from_slice(bytes);
        }
        ZMQ_RECONNECT_IVL => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).reconnect_ivl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_RECONNECT_IVL_MAX => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).reconnect_ivl_max = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_HEARTBEAT_IVL => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).heartbeat_ivl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_HEARTBEAT_TTL => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).heartbeat_ttl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_HEARTBEAT_TIMEOUT => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).heartbeat_timeout = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_HANDSHAKE_IVL => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).handshake_ivl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64 * 1000))
            };
        }
        ZMQ_MAXMSGSIZE => {
            let v = read_i64(optval, optvallen);
            lock_overlay!(sock_arc).max_message_size = if v < 0 { None } else { Some(v as usize) };
        }
        ZMQ_ROUTER_MANDATORY => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).router_mandatory = v != 0;
        }
        ZMQ_CONFLATE => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).conflate = v != 0;
        }
        ZMQ_TCP_KEEPALIVE => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).tcp_keepalive = v;
        }
        ZMQ_TCP_KEEPALIVE_CNT => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).tcp_keepalive_cnt = if v <= 0 { None } else { Some(v as u32) };
        }
        ZMQ_TCP_KEEPALIVE_IDLE => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).tcp_keepalive_idle = if v <= 0 {
                None
            } else {
                Some(Duration::from_secs(v as u64))
            };
        }
        ZMQ_TCP_KEEPALIVE_INTVL => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).tcp_keepalive_intvl = if v <= 0 {
                None
            } else {
                Some(Duration::from_secs(v as u64))
            };
        }
        ZMQ_SNDBUF => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).sndbuf = if v <= 0 { None } else { Some(v as usize) };
        }
        ZMQ_RCVBUF => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).rcvbuf = if v <= 0 { None } else { Some(v as usize) };
        }
        ZMQ_XPUB_VERBOSE => {
            let v = read_i32(optval, optvallen);
            lock_overlay!(sock_arc).xpub_verbose = v != 0;
        }
        ZMQ_SUBSCRIBE => {
            return do_subscribe(sock_arc, optval, optvallen, true);
        }
        ZMQ_UNSUBSCRIBE => {
            return do_subscribe(sock_arc, optval, optvallen, false);
        }
        ZMQ_PLAIN_SERVER => {
            let v = read_i32(optval, optvallen);
            let mut ov = lock_overlay!(sock_arc);
            if v != 0 {
                ov.mechanism = MechanismOverlay::PlainServer;
            } else if matches!(ov.mechanism, MechanismOverlay::PlainServer) {
                ov.mechanism = MechanismOverlay::Null;
            }
        }
        ZMQ_PLAIN_USERNAME => {
            let s = read_string(optval, optvallen);
            let mut ov = lock_overlay!(sock_arc);
            match &mut ov.mechanism {
                MechanismOverlay::PlainClient { username, .. } => *username = s,
                _ => {
                    ov.mechanism = MechanismOverlay::PlainClient {
                        username: s,
                        password: String::new(),
                    };
                }
            }
        }
        ZMQ_PLAIN_PASSWORD => {
            let s = read_string(optval, optvallen);
            let mut ov = lock_overlay!(sock_arc);
            match &mut ov.mechanism {
                MechanismOverlay::PlainClient { password, .. } => *password = s,
                _ => {
                    ov.mechanism = MechanismOverlay::PlainClient {
                        username: String::new(),
                        password: s,
                    };
                }
            }
        }
        ZMQ_CURVE_SERVER => {
            let v = read_i32(optval, optvallen);
            let mut ov = lock_overlay!(sock_arc);
            if v != 0 {
                if !matches!(ov.mechanism, MechanismOverlay::CurveServer { .. }) {
                    ov.mechanism = MechanismOverlay::CurveServer {
                        secret_key: [0; 32],
                    };
                }
            } else if matches!(ov.mechanism, MechanismOverlay::CurveServer { .. }) {
                ov.mechanism = MechanismOverlay::Null;
            }
        }
        ZMQ_CURVE_PUBLICKEY => {
            let key = read_key(optval, optvallen);
            let mut ov = lock_overlay!(sock_arc);
            match &mut ov.mechanism {
                MechanismOverlay::CurveClient { public_key, .. } => *public_key = key,
                _ => {
                    ov.mechanism = MechanismOverlay::CurveClient {
                        public_key: key,
                        secret_key: [0; 32],
                        server_key: [0; 32],
                    };
                }
            }
        }
        ZMQ_CURVE_SECRETKEY => {
            let key = read_key(optval, optvallen);
            let mut ov = lock_overlay!(sock_arc);
            match &mut ov.mechanism {
                MechanismOverlay::CurveServer { secret_key, .. }
                | MechanismOverlay::CurveClient { secret_key, .. } => *secret_key = key,
                _ => {
                    ov.mechanism = MechanismOverlay::CurveClient {
                        public_key: [0; 32],
                        secret_key: key,
                        server_key: [0; 32],
                    };
                }
            }
        }
        ZMQ_CURVE_SERVERKEY => {
            let key = read_key(optval, optvallen);
            let mut ov = lock_overlay!(sock_arc);
            match &mut ov.mechanism {
                MechanismOverlay::CurveClient { server_key, .. } => *server_key = key,
                _ => {
                    ov.mechanism = MechanismOverlay::CurveClient {
                        public_key: [0; 32],
                        secret_key: [0; 32],
                        server_key: key,
                    };
                }
            }
        }
        ZMQ_IPV6 => {
            lock_overlay!(sock_arc).ipv6 = read_i32(optval, optvallen) != 0;
        }
        // Always-on in omq; accept silently.
        #[expect(clippy::match_same_arms)]
        ZMQ_ROUTER_HANDOVER => {}
        ZMQ_BACKLOG => {
            lock_overlay!(sock_arc).backlog = read_i32(optval, optvallen);
        }
        ZMQ_IMMEDIATE => {
            lock_overlay!(sock_arc).immediate = read_i32(optval, optvallen) != 0;
        }
        ZMQ_CONNECT_TIMEOUT => {
            lock_overlay!(sock_arc).connect_timeout = read_i32(optval, optvallen);
        }
        ZMQ_PROBE_ROUTER => {
            lock_overlay!(sock_arc).probe_router = read_i32(optval, optvallen) != 0;
        }
        ZMQ_REQ_CORRELATE => {
            lock_overlay!(sock_arc).req_correlate = read_i32(optval, optvallen) != 0;
        }
        ZMQ_REQ_RELAXED => {
            lock_overlay!(sock_arc).req_relaxed = read_i32(optval, optvallen) != 0;
        }
        ZMQ_XPUB_NODROP => {
            lock_overlay!(sock_arc).xpub_nodrop = read_i32(optval, optvallen) != 0;
        }
        #[expect(clippy::match_same_arms)]
        ZMQ_AFFINITY
        | ZMQ_RATE
        | ZMQ_RECOVERY_IVL
        | ZMQ_MULTICAST_HOPS
        | ZMQ_TOS
        | ZMQ_CONNECT_ROUTING_ID
        | ZMQ_ZAP_DOMAIN
        | ZMQ_SOCKS_PROXY
        | ZMQ_INVERT_MATCHING
        | ZMQ_TCP_MAXRT
        | ZMQ_ROUTER_NOTIFY
        | ZMQ_MULTICAST_LOOP
        | ZMQ_BINDTODEVICE => {}
        _ => {}
    }
    0
}

fn do_subscribe(
    sock_arc: &std::sync::Arc<crate::socket::OmqSocket>,
    optval: *const libc::c_void,
    optvallen: usize,
    subscribe: bool,
) -> c_int {
    if optval.is_null() && optvallen > 0 {
        return crate::error::fail(libc::EFAULT);
    }
    let prefix = if optval.is_null() {
        Bytes::new()
    } else {
        // SAFETY: optval is non-null (checked above) with optvallen readable bytes.
        Bytes::copy_from_slice(unsafe {
            std::slice::from_raw_parts(optval.cast::<u8>(), optvallen)
        })
    };
    crate::socket::ensure_materialized(sock_arc);
    let Some(inner) = sock_arc.inner.get() else {
        return crate::error::fail(crate::error::ETERM);
    };
    let result = if subscribe {
        crate::socket::with_socket(
            &sock_arc.ctx,
            sock_arc.thread_idx,
            inner,
            move |s| async move { s.subscribe(prefix).await },
        )
    } else {
        crate::socket::with_socket(
            &sock_arc.ctx,
            sock_arc.thread_idx,
            inner,
            move |s| async move { s.unsubscribe(prefix).await },
        )
    };
    match result {
        Ok(Ok(())) => 0,
        Ok(Err(ref e)) => crate::error::fail(crate::error::map_omq_err(e)),
        Err(()) => crate::error::fail(crate::error::ETERM),
    }
}

#[expect(clippy::too_many_lines)]
#[unsafe(no_mangle)]
pub extern "C" fn zmq_getsockopt(
    sock: *mut libc::c_void,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> c_int {
    if sock.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: caller guarantees sock is a valid socket pointer from zmq_socket.
    let sock_arc = unsafe { &*(sock.cast::<std::sync::Arc<crate::socket::OmqSocket>>()) };

    match option {
        ZMQ_SNDTIMEO => write_i32(
            optval,
            optvallen,
            sock_arc
                .sndtimeo_ms
                .load(std::sync::atomic::Ordering::Relaxed) as i32,
        ),
        ZMQ_RCVTIMEO => write_i32(
            optval,
            optvallen,
            sock_arc
                .rcvtimeo_ms
                .load(std::sync::atomic::Ordering::Relaxed) as i32,
        ),
        ZMQ_SNDHWM => {
            let v = lock_overlay!(sock_arc).send_hwm.map_or(0, |n| n as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_RCVHWM => {
            let v = lock_overlay!(sock_arc).recv_hwm.map_or(0, |n| n as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_LINGER => {
            let v = lock_overlay!(sock_arc)
                .linger
                .map_or(-1, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_IDENTITY => {
            let ov = lock_overlay!(sock_arc);
            write_bytes(optval, optvallen, &ov.identity)
        }
        ZMQ_RCVMORE => {
            let more = sock_arc
                .drain_nonempty
                .load(std::sync::atomic::Ordering::Relaxed);
            write_i32(optval, optvallen, i32::from(more))
        }
        ZMQ_TYPE => {
            use omq_tokio::SocketType;
            let v: i32 = match sock_arc.socket_type {
                SocketType::Pair => 0,
                SocketType::Pub => 1,
                SocketType::Sub => 2,
                SocketType::Req => 3,
                SocketType::Rep => 4,
                SocketType::Dealer => 5,
                SocketType::Router => 6,
                SocketType::Pull => 7,
                SocketType::Push => 8,
                SocketType::XPub => 9,
                SocketType::XSub => 10,
                SocketType::Server => 12,
                SocketType::Client => 13,
                SocketType::Radio => 14,
                SocketType::Dish => 15,
                SocketType::Gather => 16,
                SocketType::Scatter => 17,
                SocketType::Peer => 19,
                SocketType::Channel => 20,
                SocketType::Stream => 11,
                _ => return crate::error::fail(libc::EINVAL),
            };
            write_i32(optval, optvallen, v)
        }
        ZMQ_FD => {
            #[cfg(target_os = "linux")]
            let fd = sock_arc.notify.recv_fd;
            #[cfg(not(target_os = "linux"))]
            let fd = sock_arc.notify.recv_read;
            write_i32(optval, optvallen, fd)
        }
        ZMQ_EVENTS => {
            let mut events = ZMQ_POLLOUT; // optimistic: always writable
            let has_data = sock_arc
                .drain_nonempty
                .load(std::sync::atomic::Ordering::Relaxed)
                // SAFETY: zmq contract guarantees single-threaded access per socket.
                || unsafe { &*sock_arc.recv_cons.get() }
                    .as_ref()
                    .is_some_and(|c| !c.fast.is_empty() || !c.pump.is_empty())
                // SAFETY: zmq contract guarantees single-threaded access per socket.
                || unsafe { &*sock_arc.bypass_recv.get() }
                    .as_ref()
                    .is_some_and(|br| !br.is_empty());
            if has_data {
                events |= ZMQ_POLLIN;
            }
            write_i32(optval, optvallen, events)
        }
        ZMQ_RECONNECT_IVL => {
            let v = lock_overlay!(sock_arc)
                .reconnect_ivl
                .map_or(-1, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_RECONNECT_IVL_MAX => {
            let v = lock_overlay!(sock_arc)
                .reconnect_ivl_max
                .map_or(0, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_HEARTBEAT_IVL => {
            let v = lock_overlay!(sock_arc)
                .heartbeat_ivl
                .map_or(0, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_HEARTBEAT_TTL => {
            let v = lock_overlay!(sock_arc)
                .heartbeat_ttl
                .map_or(0, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_HEARTBEAT_TIMEOUT => {
            let v = lock_overlay!(sock_arc)
                .heartbeat_timeout
                .map_or(0, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_HANDSHAKE_IVL => {
            let v = lock_overlay!(sock_arc)
                .handshake_ivl
                .map_or(30, |d| d.as_secs() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_MAXMSGSIZE => {
            let v = lock_overlay!(sock_arc)
                .max_message_size
                .map_or(-1i64, |n| n as i64);
            write_i64(optval, optvallen, v)
        }
        ZMQ_ROUTER_MANDATORY => {
            let v = lock_overlay!(sock_arc).router_mandatory;
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_CONFLATE => {
            let v = lock_overlay!(sock_arc).conflate;
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_TCP_KEEPALIVE => {
            let v = lock_overlay!(sock_arc).tcp_keepalive;
            write_i32(optval, optvallen, v)
        }
        ZMQ_TCP_KEEPALIVE_CNT => {
            let v = lock_overlay!(sock_arc)
                .tcp_keepalive_cnt
                .map_or(-1, |n| n as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_TCP_KEEPALIVE_IDLE => {
            let v = lock_overlay!(sock_arc)
                .tcp_keepalive_idle
                .map_or(-1, |d| d.as_secs() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_TCP_KEEPALIVE_INTVL => {
            let v = lock_overlay!(sock_arc)
                .tcp_keepalive_intvl
                .map_or(-1, |d| d.as_secs() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_SNDBUF => {
            let v = lock_overlay!(sock_arc).sndbuf.map_or(0, |n| n as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_RCVBUF => {
            let v = lock_overlay!(sock_arc).rcvbuf.map_or(0, |n| n as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_XPUB_VERBOSE => {
            let v = lock_overlay!(sock_arc).xpub_verbose;
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_LAST_ENDPOINT => {
            let Ok(ep) = sock_arc.last_endpoint.lock() else {
                return crate::error::fail(crate::error::ETERM);
            };
            let s = ep.as_deref().unwrap_or("");
            write_string(optval, optvallen, s.as_bytes())
        }
        ZMQ_MECHANISM => {
            let v = match lock_overlay!(sock_arc).mechanism {
                MechanismOverlay::Null => ZMQ_NULL,
                MechanismOverlay::PlainServer | MechanismOverlay::PlainClient { .. } => ZMQ_PLAIN,
                MechanismOverlay::CurveServer { .. } | MechanismOverlay::CurveClient { .. } => {
                    ZMQ_CURVE
                }
            };
            write_i32(optval, optvallen, v)
        }
        ZMQ_PLAIN_SERVER => {
            let v = matches!(
                lock_overlay!(sock_arc).mechanism,
                MechanismOverlay::PlainServer
            );
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_PLAIN_USERNAME => {
            let ov = lock_overlay!(sock_arc);
            if let MechanismOverlay::PlainClient { ref username, .. } = ov.mechanism {
                write_string(optval, optvallen, username.as_bytes())
            } else {
                write_string(optval, optvallen, b"")
            }
        }
        ZMQ_PLAIN_PASSWORD => {
            let ov = lock_overlay!(sock_arc);
            if let MechanismOverlay::PlainClient { ref password, .. } = ov.mechanism {
                write_string(optval, optvallen, password.as_bytes())
            } else {
                write_string(optval, optvallen, b"")
            }
        }
        ZMQ_CURVE_SERVER => {
            let v = matches!(
                lock_overlay!(sock_arc).mechanism,
                MechanismOverlay::CurveServer { .. }
            );
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_CURVE_PUBLICKEY => {
            let ov = lock_overlay!(sock_arc);
            if let MechanismOverlay::CurveClient { ref public_key, .. } = ov.mechanism {
                write_key(optval, optvallen, public_key)
            } else {
                write_key(optval, optvallen, &[0; 32])
            }
        }
        ZMQ_CURVE_SECRETKEY => {
            let ov = lock_overlay!(sock_arc);
            let key = match ov.mechanism {
                MechanismOverlay::CurveServer { ref secret_key, .. }
                | MechanismOverlay::CurveClient { ref secret_key, .. } => secret_key,
                _ => &[0; 32],
            };
            write_key(optval, optvallen, key)
        }
        ZMQ_CURVE_SERVERKEY => {
            let ov = lock_overlay!(sock_arc);
            if let MechanismOverlay::CurveClient { ref server_key, .. } = ov.mechanism {
                write_key(optval, optvallen, server_key)
            } else {
                write_key(optval, optvallen, &[0; 32])
            }
        }
        ZMQ_IPV6 => {
            let v = lock_overlay!(sock_arc).ipv6;
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_ROUTER_HANDOVER => write_i32(optval, optvallen, 1),
        ZMQ_BACKLOG => write_i32(optval, optvallen, lock_overlay!(sock_arc).backlog),
        ZMQ_IMMEDIATE => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).immediate),
        ),
        ZMQ_CONNECT_TIMEOUT => {
            write_i32(optval, optvallen, lock_overlay!(sock_arc).connect_timeout)
        }
        ZMQ_PROBE_ROUTER => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).probe_router),
        ),
        ZMQ_REQ_CORRELATE => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).req_correlate),
        ),
        ZMQ_REQ_RELAXED => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).req_relaxed),
        ),
        ZMQ_XPUB_NODROP => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).xpub_nodrop),
        ),
        ZMQ_AFFINITY => write_i64(optval, optvallen, 0),
        ZMQ_RATE | ZMQ_RECOVERY_IVL | ZMQ_MULTICAST_HOPS | ZMQ_TOS | ZMQ_TCP_MAXRT
        | ZMQ_ROUTER_NOTIFY | ZMQ_MULTICAST_LOOP | ZMQ_INVERT_MATCHING => {
            write_i32(optval, optvallen, 0)
        }
        ZMQ_ZAP_DOMAIN | ZMQ_SOCKS_PROXY | ZMQ_CONNECT_ROUTING_ID | ZMQ_BINDTODEVICE => {
            write_string(optval, optvallen, b"")
        }
        _ => 0,
    }
}

fn read_i32(optval: *const libc::c_void, optvallen: usize) -> i32 {
    if optval.is_null() || optvallen < 4 {
        return 0;
    }
    // SAFETY: optval is non-null (checked above) and points to at least 4 readable bytes.
    unsafe { std::ptr::read_unaligned(optval.cast::<i32>()) }
}

fn read_i64(optval: *const libc::c_void, optvallen: usize) -> i64 {
    if optval.is_null() || optvallen < 8 {
        return 0;
    }
    // SAFETY: optval is non-null (checked above) and points to at least 8 readable bytes.
    unsafe { std::ptr::read_unaligned(optval.cast::<i64>()) }
}

fn read_string(optval: *const libc::c_void, optvallen: usize) -> String {
    if optval.is_null() || optvallen == 0 {
        return String::new();
    }
    // SAFETY: optval is non-null with optvallen > 0 (checked above).
    let slice = unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), optvallen) };
    String::from_utf8_lossy(slice).into_owned()
}

fn read_key(optval: *const libc::c_void, optvallen: usize) -> [u8; 32] {
    if optval.is_null() {
        return [0; 32];
    }
    let mut key = [0u8; 32];
    if optvallen == 32 {
        // SAFETY: optval is non-null (checked above) and optvallen == 32.
        let slice = unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), 32) };
        key.copy_from_slice(slice);
    } else if optvallen >= 40 {
        // SAFETY: optval is non-null (checked above) and optvallen >= 40.
        let slice = unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), 40) };
        let Ok(s) = std::str::from_utf8(slice) else {
            return key;
        };
        if let Ok(decoded) = omq_tokio::proto::z85::decode(s)
            && decoded.len() == 32
        {
            key.copy_from_slice(&decoded);
        }
    }
    key
}

fn write_i32(optval: *mut libc::c_void, optvallen: *mut usize, val: i32) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return 0;
    }
    // SAFETY: optvallen is non-null (checked above); reading the available size.
    let avail = unsafe { *optvallen };
    if avail < 4 {
        return crate::error::fail(libc::EINVAL);
    }
    // SAFETY: optval is non-null with at least 4 bytes available (checked above).
    unsafe {
        std::ptr::write_unaligned(optval.cast::<i32>(), val);
        *optvallen = 4;
    }
    0
}

fn write_i64(optval: *mut libc::c_void, optvallen: *mut usize, val: i64) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return 0;
    }
    // SAFETY: optvallen is non-null (checked above).
    let avail = unsafe { *optvallen };
    if avail < 8 {
        return crate::error::fail(libc::EINVAL);
    }
    // SAFETY: optval is non-null with at least 8 bytes available (checked above).
    unsafe {
        std::ptr::write_unaligned(optval.cast::<i64>(), val);
        *optvallen = 8;
    }
    0
}

fn write_bytes(optval: *mut libc::c_void, optvallen: *mut usize, data: &[u8]) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return 0;
    }
    // SAFETY: optvallen is non-null (checked above).
    let avail = unsafe { *optvallen };
    let copy_len = data.len().min(avail);
    // SAFETY: optval is non-null with at least copy_len bytes available.
    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), optval.cast::<u8>(), copy_len);
        *optvallen = data.len();
    }
    0
}

fn write_string(optval: *mut libc::c_void, optvallen: *mut usize, data: &[u8]) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return 0;
    }
    // SAFETY: optvallen is non-null (checked above).
    let avail = unsafe { *optvallen };
    let copy_len = data.len().min(avail);
    // SAFETY: optval is non-null with at least copy_len bytes; null-terminator
    // only written when buffer has room.
    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), optval.cast::<u8>(), copy_len);
        if copy_len < avail {
            *optval.cast::<u8>().add(copy_len) = 0;
        }
        *optvallen = data.len() + 1;
    }
    0
}

fn write_key(optval: *mut libc::c_void, optvallen: *mut usize, key: &[u8; 32]) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return 0;
    }
    // SAFETY: optvallen is non-null (checked above).
    let avail = unsafe { *optvallen };
    if avail >= 41
        && let Ok(z85) = omq_tokio::proto::z85::encode(key)
    {
        // SAFETY: optval has at least 41 bytes available (checked above).
        unsafe {
            std::ptr::copy_nonoverlapping(z85.as_ptr(), optval.cast::<u8>(), 40);
            *(optval.cast::<u8>()).add(40) = 0;
            *optvallen = 41;
        }
        return 0;
    }
    if avail >= 32 {
        // SAFETY: optval has at least 32 bytes available (checked above).
        unsafe {
            std::ptr::copy_nonoverlapping(key.as_ptr(), optval.cast::<u8>(), 32);
            *optvallen = 32;
        }
        return 0;
    }
    crate::error::fail(libc::EINVAL)
}

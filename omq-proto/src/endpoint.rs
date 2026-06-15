//! Endpoint parsing and formatting.
//!
//! An [`Endpoint`] is a transport address like `tcp://host:port` or
//! `inproc://name`. An [`EndpointSpec`] is an endpoint with an optional
//! role prefix (`@` for bind, `>` for connect), useful for CLI-style
//! single-string specifications.

use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;

#[cfg(unix)]
use std::path::PathBuf;

use crate::error::{Error, Result};

/// A transport endpoint.
///
/// The scheme picks the transport; the rest of the string carries transport-
/// specific addressing.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Endpoint {
    /// `tcp://host:port` (IPv4, IPv6, or DNS name).
    Tcp { host: Host, port: u16 },
    /// `ipc://path` filesystem socket, or `ipc://@name` Linux abstract namespace.
    /// Only available on Unix platforms.
    #[cfg(unix)]
    Ipc(IpcPath),
    /// `inproc://name` in-process transport.
    Inproc { name: String },
    /// `udp://[group@]host:port` for RADIO/DISH (group optional).
    Udp {
        group: Option<String>,
        host: Host,
        port: u16,
    },
    /// `lz4+tcp://host:port` LZ4-compressed TCP. Requires the `lz4` feature.
    #[cfg(feature = "lz4")]
    Lz4Tcp { host: Host, port: u16 },
    /// `ws://host:port/path` ZeroMQ over WebSocket (RFC 45). Requires the
    /// `ws` feature.
    #[cfg(feature = "ws")]
    Ws { host: Host, port: u16, path: String },
    /// `wss://host:port/path` ZeroMQ over WebSocket with TLS. Requires the
    /// `ws` feature.
    #[cfg(feature = "ws")]
    Wss { host: Host, port: u16, path: String },
}

/// TCP / UDP host specification: either an IP address or a DNS name.
///
/// Kept as a distinct variant so resolution can be deferred until bind/connect
/// time without forcing callers to reparse.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Host {
    Ip(IpAddr),
    Name(String),
    /// Wildcard (`0.0.0.0` / `::` / `*`) -- bind-only.
    Wildcard,
}

/// IPC path, possibly in the Linux abstract namespace (leading `@`).
/// Only available on Unix platforms.
#[cfg(unix)]
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum IpcPath {
    Filesystem(PathBuf),
    /// Linux abstract namespace (no filesystem entry).
    Abstract(String),
}

/// Role for an endpoint in a single-string spec: bind, connect, or default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EndpointRole {
    /// `@endpoint` -- explicit bind.
    Bind,
    /// `>endpoint` -- explicit connect.
    Connect,
    /// No prefix -- socket type picks (PUSH connects, PULL binds, etc.).
    Default,
}

/// An endpoint plus an optional role prefix. Parsed from strings like
/// `"@tcp://*:5555"` or `">tcp://host:5555"` or just `"tcp://host:5555"`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndpointSpec {
    /// Bind, connect, or socket-type default.
    pub role: EndpointRole,
    /// The transport address.
    pub endpoint: Endpoint,
}

impl FromStr for Endpoint {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (scheme, rest) = s
            .split_once("://")
            .ok_or_else(|| Error::InvalidEndpoint(s.to_string()))?;

        match scheme {
            "tcp" => parse_host_port(rest).map(|(host, port)| Endpoint::Tcp { host, port }),
            #[cfg(unix)]
            "ipc" => Ok(Endpoint::Ipc(parse_ipc(rest)?)),
            "inproc" => {
                if rest.is_empty() {
                    return Err(Error::InvalidEndpoint(s.to_string()));
                }
                Ok(Endpoint::Inproc {
                    name: rest.to_string(),
                })
            }
            "udp" => parse_udp(rest),
            #[cfg(feature = "lz4")]
            "lz4+tcp" => parse_host_port(rest).map(|(host, port)| Endpoint::Lz4Tcp { host, port }),
            #[cfg(feature = "ws")]
            "ws" => parse_ws(rest, false),
            #[cfg(feature = "ws")]
            "wss" => parse_ws(rest, true),
            _ => Err(Error::UnsupportedScheme(scheme.to_string())),
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp { host, port } => write!(f, "tcp://{host}:{port}"),
            #[cfg(unix)]
            Self::Ipc(path) => write!(f, "ipc://{path}"),
            Self::Inproc { name } => write!(f, "inproc://{name}"),
            Self::Udp { group, host, port } => match group {
                Some(g) => write!(f, "udp://{g}@{host}:{port}"),
                None => write!(f, "udp://{host}:{port}"),
            },
            #[cfg(feature = "lz4")]
            Self::Lz4Tcp { host, port } => write!(f, "lz4+tcp://{host}:{port}"),
            #[cfg(feature = "ws")]
            Self::Ws { host, port, path } => write!(f, "ws://{host}:{port}{path}"),
            #[cfg(feature = "ws")]
            Self::Wss { host, port, path } => write!(f, "wss://{host}:{port}{path}"),
        }
    }
}

impl Endpoint {
    /// Strip the compression scheme prefix so the underlying TCP
    /// transport sees a plain `tcp://` endpoint. Identity for plain
    /// `tcp://`. Returns the endpoint unchanged for `ipc://` /
    /// `inproc://` / `udp://`.
    #[must_use]
    pub fn underlying_tcp(&self) -> Endpoint {
        match self {
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Tcp { host, port } => Endpoint::Tcp {
                host: host.clone(),
                port: *port,
            },
            _ => self.clone(),
        }
    }

    /// Re-attach the original endpoint's scheme to a resolved address.
    /// Used after binding through the underlying TCP transport so the
    /// bound endpoint surfaced to the user still says `lz4+tcp://...`.
    #[must_use]
    pub fn rewrap_tcp(&self, resolved: Endpoint) -> Endpoint {
        match (self, resolved) {
            #[cfg(feature = "lz4")]
            (Endpoint::Lz4Tcp { .. }, Endpoint::Tcp { host, port }) => {
                Endpoint::Lz4Tcp { host, port }
            }
            (_, resolved) => resolved,
        }
    }

    /// Whether this endpoint rides on the TCP byte-stream transport.
    /// Includes the compression-wrapped variants.
    pub fn is_tcp_family(&self) -> bool {
        match self {
            Endpoint::Tcp { .. } => true,
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Tcp { .. } => true,
            _ => false,
        }
    }

    /// Whether this endpoint uses the WebSocket transport.
    #[cfg(feature = "ws")]
    pub fn is_ws_family(&self) -> bool {
        matches!(self, Endpoint::Ws { .. } | Endpoint::Wss { .. })
    }

    /// Short scheme tag suitable for monitor / log output.
    pub fn scheme(&self) -> &'static str {
        match self {
            Endpoint::Tcp { .. } => "tcp",
            #[cfg(unix)]
            Endpoint::Ipc(_) => "ipc",
            Endpoint::Inproc { .. } => "inproc",
            Endpoint::Udp { .. } => "udp",
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Tcp { .. } => "lz4+tcp",
            #[cfg(feature = "ws")]
            Endpoint::Ws { .. } => "ws",
            #[cfg(feature = "ws")]
            Endpoint::Wss { .. } => "wss",
        }
    }
}

/// Inproc + an encrypting mechanism makes no sense - both ends are
/// in the same process, the fast path skips the codec, and there's
/// no wire to attack. Reject explicitly so the user notices their
/// config doesn't do what they think it does.
pub fn reject_encrypted_inproc(
    endpoint: &Endpoint,
    mechanism: &crate::proto::mechanism::MechanismSetup,
) -> crate::error::Result<()> {
    if matches!(endpoint, Endpoint::Inproc { .. })
        && !matches!(mechanism, crate::proto::mechanism::MechanismSetup::Null)
    {
        return Err(crate::error::Error::InvalidEndpoint(
            "encrypting mechanisms (CURVE / BLAKE3ZMQ) are not supported on \
             inproc - use ipc:// or tcp:// for encrypted in-host channels"
                .into(),
        ));
    }
    Ok(())
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ip(IpAddr::V4(v4)) => write!(f, "{v4}"),
            Self::Ip(IpAddr::V6(v6)) => write!(f, "[{v6}]"),
            Self::Name(n) => write!(f, "{n}"),
            Self::Wildcard => write!(f, "*"),
        }
    }
}

#[cfg(unix)]
impl fmt::Display for IpcPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Filesystem(p) => write!(f, "{}", p.display()),
            Self::Abstract(name) => write!(f, "@{name}"),
        }
    }
}

impl FromStr for EndpointSpec {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (role, rest) = match s.as_bytes().first() {
            Some(b'@') => (EndpointRole::Bind, &s[1..]),
            Some(b'>') => (EndpointRole::Connect, &s[1..]),
            _ => (EndpointRole::Default, s),
        };
        Ok(Self {
            role,
            endpoint: rest.parse()?,
        })
    }
}

impl fmt::Display for EndpointSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.role {
            EndpointRole::Bind => write!(f, "@{}", self.endpoint),
            EndpointRole::Connect => write!(f, ">{}", self.endpoint),
            EndpointRole::Default => write!(f, "{}", self.endpoint),
        }
    }
}

fn parse_host_port(rest: &str) -> Result<(Host, u16)> {
    // IPv6 must be bracketed: `[::1]:5555`.
    if let Some(stripped) = rest.strip_prefix('[') {
        let close = stripped
            .find(']')
            .ok_or_else(|| Error::InvalidEndpoint(format!("unmatched '[' in {rest}")))?;
        let ip_str = &stripped[..close];
        let after = &stripped[close + 1..];
        let port = after
            .strip_prefix(':')
            .and_then(|p| p.parse::<u16>().ok())
            .ok_or_else(|| Error::InvalidEndpoint(format!("missing port in {rest}")))?;
        let ip: IpAddr = ip_str
            .parse()
            .map_err(|_| Error::InvalidEndpoint(format!("invalid IPv6 {ip_str}")))?;
        return Ok((Host::Ip(ip), port));
    }

    let (host_str, port_str) = rest
        .rsplit_once(':')
        .ok_or_else(|| Error::InvalidEndpoint(format!("missing port in {rest}")))?;
    let port: u16 = if port_str == "*" {
        0
    } else {
        port_str
            .parse()
            .map_err(|_| Error::InvalidEndpoint(format!("invalid port {port_str}")))?
    };
    let host = parse_host(host_str)?;
    Ok((host, port))
}

fn parse_host(s: &str) -> Result<Host> {
    if s == "*" || s.is_empty() {
        return Ok(Host::Wildcard);
    }
    // Bare IPv6 is ambiguous with host:port -- require bracketed form.
    if s.contains(':') {
        return Err(Error::InvalidEndpoint(format!(
            "IPv6 must be bracketed: [{s}]"
        )));
    }
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Ok(Host::Ip(ip));
    }
    if !s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
    {
        return Err(Error::InvalidEndpoint(format!("invalid host {s}")));
    }
    Ok(Host::Name(s.to_string()))
}

#[cfg(unix)]
fn parse_ipc(rest: &str) -> Result<IpcPath> {
    if rest.is_empty() {
        return Err(Error::InvalidEndpoint("empty ipc path".to_string()));
    }
    if let Some(name) = rest.strip_prefix('@') {
        if name.is_empty() {
            return Err(Error::InvalidEndpoint(
                "empty abstract ipc name".to_string(),
            ));
        }
        return Ok(IpcPath::Abstract(name.to_string()));
    }
    Ok(IpcPath::Filesystem(PathBuf::from(rest)))
}

#[cfg(feature = "ws")]
fn parse_ws(rest: &str, tls: bool) -> Result<Endpoint> {
    let (hp, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = parse_host_port(hp)?;
    let path = path.to_string();
    if tls {
        Ok(Endpoint::Wss { host, port, path })
    } else {
        Ok(Endpoint::Ws { host, port, path })
    }
}

fn parse_udp(rest: &str) -> Result<Endpoint> {
    let (group, hp) = match rest.split_once('@') {
        Some((g, hp)) if !g.is_empty() => (Some(g.to_string()), hp),
        _ => (None, rest),
    };
    let (host, port) = parse_host_port(hp)?;
    Ok(Endpoint::Udp { group, host, port })
}

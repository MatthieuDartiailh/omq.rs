use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use crate::error::{ZmqError, ZmqResult};

/// Transport endpoint address.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Endpoint {
    Tcp(SocketAddr),
    Ipc(String),
    Inproc(String),
    Udp(SocketAddr),
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(addr) => write!(f, "tcp://{addr}"),
            Self::Ipc(path) => write!(f, "ipc://{path}"),
            Self::Inproc(name) => write!(f, "inproc://{name}"),
            Self::Udp(addr) => write!(f, "udp://{addr}"),
        }
    }
}

/// Transport type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Transport {
    Tcp,
    Ipc,
    Inproc,
    Udp,
}

/// Host portion of a TCP endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Host {
    Localhost,
    Any,
    Specific(String),
}

/// Trait for types that can be converted to an omq `Endpoint`.
pub trait TryIntoEndpoint {
    fn try_into(&self) -> ZmqResult<omq_proto::Endpoint>;
}

impl TryIntoEndpoint for &str {
    fn try_into(&self) -> ZmqResult<omq_proto::Endpoint> {
        omq_proto::Endpoint::from_str(self).map_err(|e| ZmqError::Endpoint(e.to_string()))
    }
}

impl TryIntoEndpoint for String {
    fn try_into(&self) -> ZmqResult<omq_proto::Endpoint> {
        omq_proto::Endpoint::from_str(self).map_err(|e| ZmqError::Endpoint(e.to_string()))
    }
}

impl TryIntoEndpoint for Endpoint {
    fn try_into(&self) -> ZmqResult<omq_proto::Endpoint> {
        match self {
            Endpoint::Tcp(addr) => Ok(omq_proto::Endpoint::Tcp {
                host: omq_proto::endpoint::Host::Ip(addr.ip()),
                port: addr.port(),
            }),
            Endpoint::Ipc(path) => {
                let ipc_path = omq_proto::IpcPath::Filesystem(path.into());
                Ok(omq_proto::Endpoint::Ipc(ipc_path))
            }
            Endpoint::Inproc(name) => Ok(omq_proto::Endpoint::Inproc { name: name.clone() }),
            Endpoint::Udp(addr) => Ok(omq_proto::Endpoint::Udp {
                group: None,
                host: omq_proto::endpoint::Host::Ip(addr.ip()),
                port: addr.port(),
            }),
        }
    }
}

/// Convert an omq `Endpoint` to the compat `Endpoint`.
pub(crate) fn from_omq_endpoint(ep: &omq_proto::Endpoint) -> ZmqResult<Endpoint> {
    match ep {
        omq_proto::Endpoint::Tcp { host, port } => {
            let ip = match host {
                omq_proto::endpoint::Host::Ip(ip) => *ip,
                omq_proto::endpoint::Host::Wildcard => "0.0.0.0".parse().unwrap(),
                omq_proto::endpoint::Host::Name(name) => {
                    return Err(ZmqError::Endpoint(format!(
                        "unresolved hostname in endpoint: {name}"
                    )));
                }
            };
            Ok(Endpoint::Tcp(SocketAddr::new(ip, *port)))
        }
        omq_proto::Endpoint::Ipc(path) => Ok(Endpoint::Ipc(path.to_string())),
        omq_proto::Endpoint::Inproc { name } => Ok(Endpoint::Inproc(name.clone())),
        omq_proto::Endpoint::Udp { host, port, .. } => {
            let ip = match host {
                omq_proto::endpoint::Host::Ip(ip) => *ip,
                omq_proto::endpoint::Host::Wildcard => "0.0.0.0".parse().unwrap(),
                omq_proto::endpoint::Host::Name(name) => {
                    return Err(ZmqError::Endpoint(format!(
                        "unresolved hostname in endpoint: {name}"
                    )));
                }
            };
            Ok(Endpoint::Udp(SocketAddr::new(ip, *port)))
        }
        #[allow(unreachable_patterns)]
        _ => {
            // Compression transports (Lz4Tcp, ZstdTcp) resolve to TCP addresses.
            // Extract via Display and re-parse the underlying TCP portion.
            let s = ep.to_string();
            Err(ZmqError::Endpoint(format!(
                "cannot represent endpoint as compat type: {s}"
            )))
        }
    }
}

/// Parse an endpoint string into an omq `Endpoint`. All transports accepted.
pub(crate) fn parse_endpoint(s: &str) -> ZmqResult<omq_proto::Endpoint> {
    omq_proto::Endpoint::from_str(s).map_err(|e| ZmqError::Endpoint(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tcp_ipv4() {
        let ep = parse_endpoint("tcp://127.0.0.1:5555").unwrap();
        assert!(matches!(ep, omq_proto::Endpoint::Tcp { .. }));
        let compat = from_omq_endpoint(&ep).unwrap();
        assert_eq!(compat, Endpoint::Tcp("127.0.0.1:5555".parse().unwrap()));
    }

    #[test]
    fn parse_tcp_ipv6() {
        let ep = parse_endpoint("tcp://[::1]:5555").unwrap();
        let compat = from_omq_endpoint(&ep).unwrap();
        assert_eq!(compat, Endpoint::Tcp("[::1]:5555".parse().unwrap()));
    }

    #[test]
    fn parse_tcp_wildcard() {
        let ep = parse_endpoint("tcp://*:0").unwrap();
        let compat = from_omq_endpoint(&ep).unwrap();
        assert_eq!(compat, Endpoint::Tcp("0.0.0.0:0".parse().unwrap()));
    }

    #[test]
    fn parse_ipc() {
        let ep = parse_endpoint("ipc:///tmp/test.sock").unwrap();
        let compat = from_omq_endpoint(&ep).unwrap();
        assert!(matches!(compat, Endpoint::Ipc(ref p) if p.contains("test.sock")));
    }

    #[test]
    fn parse_inproc() {
        let ep = parse_endpoint("inproc://test").unwrap();
        let compat = from_omq_endpoint(&ep).unwrap();
        assert_eq!(compat, Endpoint::Inproc("test".into()));
    }

    #[test]
    fn parse_invalid() {
        assert!(parse_endpoint("garbage").is_err());
    }

    #[test]
    fn endpoint_display() {
        let tcp = Endpoint::Tcp("127.0.0.1:5555".parse().unwrap());
        assert_eq!(tcp.to_string(), "tcp://127.0.0.1:5555");
        let ipc = Endpoint::Ipc("/tmp/x.sock".into());
        assert_eq!(ipc.to_string(), "ipc:///tmp/x.sock");
        let inproc = Endpoint::Inproc("test".into());
        assert_eq!(inproc.to_string(), "inproc://test");
    }

    #[test]
    fn try_into_from_str() {
        let ep: ZmqResult<omq_proto::Endpoint> = TryIntoEndpoint::try_into(&"tcp://127.0.0.1:1234");
        assert!(ep.is_ok());
    }

    #[test]
    fn try_into_from_endpoint() {
        let compat = Endpoint::Tcp("10.0.0.1:9999".parse().unwrap());
        let ep: ZmqResult<omq_proto::Endpoint> = TryIntoEndpoint::try_into(&compat);
        assert!(ep.is_ok());
    }

    #[test]
    fn try_into_inproc() {
        let compat = Endpoint::Inproc("hello".into());
        let ep: ZmqResult<omq_proto::Endpoint> = TryIntoEndpoint::try_into(&compat);
        let ep = ep.unwrap();
        assert!(matches!(ep, omq_proto::Endpoint::Inproc { ref name } if name == "hello"));
    }
}

//! WebSocket bind/connect glue (ZWS/2.0, RFC 45).
//!
//! Performs the HTTP upgrade handshake and returns a raw TCP stream.
//! The Connection codec in omq-proto handles WS framing internally.

use std::net::SocketAddr;

use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::{TcpListener, TcpStream};

use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::error::{Error, Result};
use omq_proto::proto::ws_handshake;

const SUBPROTOCOL_NULL: &str = "ZWS2.0/NULL";

fn resolve_bind(host: &Host, port: u16) -> Result<SocketAddr> {
    use std::net::{IpAddr, Ipv4Addr};
    match host {
        Host::Wildcard => Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)),
        Host::Ip(ip) => Ok(SocketAddr::new(*ip, port)),
        Host::Name(_) => Err(Error::InvalidEndpoint(
            "DNS resolution not yet supported on omq-compio WS".into(),
        )),
    }
}

fn resolve_connect(host: &Host, port: u16) -> Result<SocketAddr> {
    match host {
        Host::Wildcard => Err(Error::InvalidEndpoint(
            "cannot connect to wildcard host".into(),
        )),
        Host::Ip(ip) => Ok(SocketAddr::new(*ip, port)),
        Host::Name(_) => Err(Error::InvalidEndpoint(
            "DNS resolution not yet supported on omq-compio WS".into(),
        )),
    }
}

pub(crate) struct WsListener {
    pub(crate) inner: TcpListener,
    pub(crate) local_addr: SocketAddr,
}

pub(crate) async fn bind(endpoint: &Endpoint) -> Result<WsListener> {
    let (host, port) = match endpoint {
        Endpoint::Ws { host, port, .. } | Endpoint::Wss { host, port, .. } => (host, *port),
        _ => {
            return Err(Error::InvalidEndpoint(format!(
                "ws transport got non-ws endpoint: {endpoint}"
            )));
        }
    };
    let addr = resolve_bind(host, port)?;
    let listener = TcpListener::bind(addr).await.map_err(Error::Io)?;
    let local = listener.local_addr().map_err(Error::Io)?;
    Ok(WsListener {
        inner: listener,
        local_addr: local,
    })
}

#[allow(clippy::result_large_err)]
pub(crate) async fn accept(stream: TcpStream) -> Result<TcpStream> {
    let _ = stream.set_nodelay(true);

    let buf = vec![0u8; 4096];
    let mut stream = stream;
    let compio::buf::BufResult(n, buf) = stream.read(buf).await;
    let n = n.map_err(Error::Io)?;
    if n == 0 {
        return Err(Error::HandshakeFailed("empty HTTP upgrade request".into()));
    }

    let upgrade = ws_handshake::parse_client_upgrade(&buf[..n])?;
    let chosen = upgrade
        .subprotocols
        .iter()
        .find(|p| p.as_str() == SUBPROTOCOL_NULL || p.as_str() == "ZWS2.0")
        .cloned()
        .unwrap_or_else(|| SUBPROTOCOL_NULL.to_string());

    let accept_value = ws_handshake::compute_ws_accept(&upgrade.key);
    let response = ws_handshake::format_server_upgrade(&accept_value, &chosen);
    stream.write_all(response).await.0.map_err(Error::Io)?;

    Ok(stream)
}

pub(crate) async fn connect(endpoint: &Endpoint) -> Result<TcpStream> {
    let (host, port, path) = match endpoint {
        Endpoint::Ws {
            host, port, path, ..
        }
        | Endpoint::Wss {
            host, port, path, ..
        } => (host, *port, path.as_str()),
        _ => {
            return Err(Error::InvalidEndpoint(format!(
                "ws transport got non-ws endpoint: {endpoint}"
            )));
        }
    };
    let addr = resolve_connect(host, port)?;
    let mut stream = TcpStream::connect(addr).await.map_err(Error::Io)?;
    let _ = stream.set_nodelay(true);

    let host_header = format!("{host}:{port}");
    let key = ws_handshake::generate_ws_key();
    let request = ws_handshake::format_client_upgrade(&host_header, path, &key, SUBPROTOCOL_NULL);
    stream.write_all(request).await.0.map_err(Error::Io)?;

    let buf = vec![0u8; 4096];
    let compio::buf::BufResult(n, buf) = stream.read(buf).await;
    let n = n.map_err(Error::Io)?;
    if n == 0 {
        return Err(Error::HandshakeFailed("empty HTTP upgrade response".into()));
    }

    ws_handshake::parse_server_upgrade(&buf[..n], &key)?;

    Ok(stream)
}

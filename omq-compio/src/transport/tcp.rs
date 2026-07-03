//! TCP bind/connect glue. Driver lives in `transport::driver`.

use std::net::SocketAddr;

use compio::net::{TcpListener, TcpStream};

use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::error::{Error, Result};

pub use crate::transport::driver::DriverCommand as TcpDriverCommand;

pub(crate) fn resolve_name(name: &str, port: u16) -> Result<SocketAddr> {
    resolve_name_all(name, port)?
        .into_iter()
        .next()
        .ok_or_else(|| Error::InvalidEndpoint(format!("no addresses for {name}")))
}

pub(crate) fn resolve_name_all(name: &str, port: u16) -> Result<Vec<SocketAddr>> {
    use std::net::ToSocketAddrs;
    let addrs: Vec<_> = (name, port)
        .to_socket_addrs()
        .map_err(|e| Error::InvalidEndpoint(format!("{name}: {e}")))?
        .collect();
    if addrs.is_empty() {
        return Err(Error::InvalidEndpoint(format!("no addresses for {name}")));
    }
    Ok(addrs)
}

fn resolve_bind(host: &Host, port: u16) -> Result<SocketAddr> {
    use std::net::{IpAddr, Ipv4Addr};
    match host {
        Host::Wildcard => Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)),
        Host::Ip(ip) => Ok(SocketAddr::new(*ip, port)),
        Host::Name(name) => resolve_name(name, port),
        _ => unreachable!(),
    }
}

fn resolve_connect(host: &Host, port: u16) -> Result<Vec<SocketAddr>> {
    match host {
        Host::Wildcard => Err(Error::InvalidEndpoint(
            "cannot connect to wildcard host".into(),
        )),
        Host::Ip(ip) => Ok(vec![SocketAddr::new(*ip, port)]),
        Host::Name(name) => resolve_name_all(name, port),
        _ => unreachable!(),
    }
}

#[expect(clippy::unused_async)]
pub async fn bind(endpoint: &Endpoint) -> Result<(TcpListener, SocketAddr)> {
    let Endpoint::Tcp { host, port } = endpoint else {
        return Err(Error::InvalidEndpoint(format!(
            "tcp transport got non-tcp endpoint: {endpoint}"
        )));
    };
    let addr = resolve_bind(host, *port)?;
    let listener = reuse_addr_bind(addr)?;
    let local = listener.local_addr().map_err(Error::Io)?;
    Ok((listener, local))
}

pub(crate) fn reuse_addr_bind(addr: SocketAddr) -> Result<TcpListener> {
    let domain = if addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let sock = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))
        .map_err(Error::Io)?;
    sock.set_reuse_address(true).map_err(Error::Io)?;
    sock.set_nonblocking(true).map_err(Error::Io)?;
    sock.bind(&addr.into()).map_err(Error::Io)?;
    sock.listen(1024).map_err(Error::Io)?;
    TcpListener::from_std(std::net::TcpListener::from(sock)).map_err(Error::Io)
}

pub async fn connect(endpoint: &Endpoint) -> Result<TcpStream> {
    let Endpoint::Tcp { host, port } = endpoint else {
        return Err(Error::InvalidEndpoint(format!(
            "tcp transport got non-tcp endpoint: {endpoint}"
        )));
    };
    connect_any_resolved(resolve_connect(host, *port)?).await
}

pub(crate) async fn connect_any_resolved(addrs: Vec<SocketAddr>) -> Result<TcpStream> {
    let mut last_err = None;
    for addr in addrs {
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                stream.set_nodelay(true).map_err(Error::Io)?;
                return Ok(stream);
            }
            Err(e) => last_err = Some(e),
        }
    }
    Err(Error::Io(last_err.unwrap_or_else(|| {
        std::io::Error::other("no addresses to connect")
    })))
}

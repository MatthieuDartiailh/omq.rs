//! WebSocket bind/connect glue (ZWS/2.0, RFC 45).
//!
//! Performs the HTTP upgrade handshake and returns a raw TCP stream
//! (plain WS) or a TLS-wrapped stream (WSS). The Connection codec in
//! omq-proto handles WS framing internally.

use std::net::SocketAddr;

use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::{TcpListener, TcpStream};

use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::error::{Error, Result};
use omq_proto::options::MechanismConfig;
use omq_proto::proto::ws_handshake;

fn mechanism_subprotocol(
    #[cfg_attr(not(feature = "plain"), allow(unused_variables))] mechanism: &MechanismConfig,
) -> &'static str {
    #[cfg(feature = "plain")]
    match mechanism {
        MechanismConfig::PlainClient { .. } | MechanismConfig::PlainServer { .. } => {
            return "ZWS2.0/PLAIN";
        }
        _ => {}
    }
    "ZWS2.0/NULL"
}

fn is_known_subprotocol(s: &str) -> bool {
    matches!(s, "ZWS2.0" | "ZWS2.0/NULL" | "ZWS2.0/PLAIN")
}

fn resolve_bind(host: &Host, port: u16) -> Result<SocketAddr> {
    use std::net::{IpAddr, Ipv4Addr};
    match host {
        Host::Wildcard => Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)),
        Host::Ip(ip) => Ok(SocketAddr::new(*ip, port)),
        Host::Name(name) => super::tcp::resolve_name(name, port),
    }
}

fn resolve_connect(host: &Host, port: u16) -> Result<SocketAddr> {
    match host {
        Host::Wildcard => Err(Error::InvalidEndpoint(
            "cannot connect to wildcard host".into(),
        )),
        Host::Ip(ip) => Ok(SocketAddr::new(*ip, port)),
        Host::Name(name) => super::tcp::resolve_name(name, port),
    }
}

// ---- TLS helpers ----

pub(crate) type TlsStream = compio_tls::TlsStream<TcpStream>;
pub(crate) type SharedTls = std::sync::Arc<async_lock::Mutex<TlsStream>>;

pub(crate) fn build_tls_connector(accept_invalid_certs: bool) -> Result<compio_tls::TlsConnector> {
    use std::sync::Arc;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let config = if accept_invalid_certs {
        let mut cfg = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        cfg.dangerous().set_certificate_verifier(Arc::new(NoVerify));
        cfg
    } else {
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_native_certs::load_native_certs().expect("load system certs") {
            let _ = roots.add(cert);
        }
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    };
    Ok(Arc::new(config).into())
}

pub(crate) fn build_tls_acceptor(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<compio_tls::TlsAcceptor> {
    use std::sync::Arc;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certs: Vec<_> = rustls_pemfile::certs(&mut &*cert_pem)
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| Error::Protocol(format!("invalid cert PEM: {e}")))?;
    let key = rustls_pemfile::private_key(&mut &*key_pem)
        .map_err(|e| Error::Protocol(format!("invalid key PEM: {e}")))?
        .ok_or_else(|| Error::Protocol("no private key in PEM".into()))?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| Error::Protocol(format!("TLS config: {e}")))?;
    Ok(Arc::new(config).into())
}

#[derive(Debug)]
struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _: &rustls_pki_types::CertificateDer<'_>,
        _: &[rustls_pki_types::CertificateDer<'_>],
        _: &rustls_pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls_pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &rustls_pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &rustls_pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::CryptoProvider::get_default()
            .map(|p| p.signature_verification_algorithms.supported_schemes())
            .unwrap_or_default()
    }
}

// ---- Bind / Accept / Connect ----

pub(crate) struct WsListener {
    pub(crate) inner: TcpListener,
    pub(crate) local_addr: SocketAddr,
    pub(crate) tls_acceptor: Option<compio_tls::TlsAcceptor>,
}

pub(crate) enum WsTransport {
    Plain(TcpStream),
    Tls(TlsStream),
}

pub(crate) struct WsUpgraded {
    pub transport: WsTransport,
    pub leftover: bytes::Bytes,
}

pub(crate) async fn bind(
    endpoint: &Endpoint,
    tls_acceptor: Option<compio_tls::TlsAcceptor>,
) -> Result<WsListener> {
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
        tls_acceptor,
    })
}

struct HttpRead {
    buf: Vec<u8>,
    header_end: usize,
    total: usize,
}

async fn read_until_header_end_tcp(stream: &mut TcpStream) -> Result<HttpRead> {
    let mut buf = vec![0u8; 4096];
    let mut total = 0;
    loop {
        let slice = buf[total..].to_vec();
        let compio::buf::BufResult(n, returned) = stream.read(slice).await;
        let n = n.map_err(Error::Io)?;
        if n == 0 {
            return Err(Error::HandshakeFailed(
                "connection closed during HTTP upgrade".into(),
            ));
        }
        buf[total..total + n].copy_from_slice(&returned[..n]);
        total += n;
        if let Some(pos) = buf[..total].windows(4).position(|w| w == b"\r\n\r\n") {
            return Ok(HttpRead {
                buf,
                header_end: pos + 4,
                total,
            });
        }
        if total >= buf.len() {
            return Err(Error::HandshakeFailed("HTTP headers too large".into()));
        }
    }
}

async fn read_until_header_end_tls(stream: &mut TlsStream) -> Result<HttpRead> {
    let mut buf = vec![0u8; 4096];
    let mut total = 0;
    loop {
        let slice = buf[total..].to_vec();
        let compio::buf::BufResult(n, returned) = stream.read(slice).await;
        let n = n.map_err(Error::Io)?;
        if n == 0 {
            return Err(Error::HandshakeFailed(
                "connection closed during HTTP upgrade".into(),
            ));
        }
        buf[total..total + n].copy_from_slice(&returned[..n]);
        total += n;
        if let Some(pos) = buf[..total].windows(4).position(|w| w == b"\r\n\r\n") {
            return Ok(HttpRead {
                buf,
                header_end: pos + 4,
                total,
            });
        }
        if total >= buf.len() {
            return Err(Error::HandshakeFailed("HTTP headers too large".into()));
        }
    }
}

fn extract_leftover(http: &HttpRead) -> bytes::Bytes {
    if http.header_end < http.total {
        bytes::Bytes::copy_from_slice(&http.buf[http.header_end..http.total])
    } else {
        bytes::Bytes::new()
    }
}

#[allow(clippy::result_large_err)]
pub(crate) async fn accept(
    stream: TcpStream,
    tls_acceptor: Option<&compio_tls::TlsAcceptor>,
) -> Result<WsUpgraded> {
    let _ = stream.set_nodelay(true);

    if let Some(acc) = tls_acceptor {
        let mut tls = acc.accept(stream).await.map_err(Error::Io)?;
        let http = read_until_header_end_tls(&mut tls).await?;
        let upgrade = ws_handshake::parse_client_upgrade(&http.buf[..http.header_end])?;
        let chosen = upgrade
            .subprotocols
            .iter()
            .find(|p| is_known_subprotocol(p))
            .cloned()
            .unwrap_or_else(|| "ZWS2.0/NULL".to_string());
        let accept_value = ws_handshake::compute_ws_accept(&upgrade.key);
        let response = ws_handshake::format_server_upgrade(&accept_value, &chosen);
        tls.write_all(response).await.0.map_err(Error::Io)?;
        compio::io::AsyncWrite::flush(&mut tls)
            .await
            .map_err(Error::Io)?;
        let leftover = extract_leftover(&http);
        return Ok(WsUpgraded {
            transport: WsTransport::Tls(tls),
            leftover,
        });
    }

    let mut stream = stream;
    let http = read_until_header_end_tcp(&mut stream).await?;
    let upgrade = ws_handshake::parse_client_upgrade(&http.buf[..http.header_end])?;
    let chosen = upgrade
        .subprotocols
        .iter()
        .find(|p| is_known_subprotocol(p))
        .cloned()
        .unwrap_or_else(|| "ZWS2.0/NULL".to_string());
    let accept_value = ws_handshake::compute_ws_accept(&upgrade.key);
    let response = ws_handshake::format_server_upgrade(&accept_value, &chosen);
    stream.write_all(response).await.0.map_err(Error::Io)?;
    let leftover = extract_leftover(&http);
    Ok(WsUpgraded {
        transport: WsTransport::Plain(stream),
        leftover,
    })
}

pub(crate) async fn connect(
    endpoint: &Endpoint,
    mechanism: &MechanismConfig,
    accept_invalid_certs: bool,
) -> Result<WsUpgraded> {
    let (host, port, path, tls) = match endpoint {
        Endpoint::Ws {
            host, port, path, ..
        } => (host, *port, path.as_str(), false),
        Endpoint::Wss {
            host, port, path, ..
        } => (host, *port, path.as_str(), true),
        _ => {
            return Err(Error::InvalidEndpoint(format!(
                "ws transport got non-ws endpoint: {endpoint}"
            )));
        }
    };
    let addr = resolve_connect(host, port)?;
    let stream = TcpStream::connect(addr).await.map_err(Error::Io)?;
    let _ = stream.set_nodelay(true);

    let subprotocol = mechanism_subprotocol(mechanism);

    if tls {
        let connector = build_tls_connector(accept_invalid_certs)?;
        let domain = match host {
            Host::Name(n) => n.as_str(),
            Host::Ip(ip) => {
                return connect_tls_ip(&connector, stream, ip, port, path, subprotocol).await;
            }
            Host::Wildcard => "localhost",
        };
        let mut tls = connector.connect(domain, stream).await.map_err(Error::Io)?;
        let host_header = format!("{host}:{port}");
        let key = ws_handshake::generate_ws_key();
        let request = ws_handshake::format_client_upgrade(&host_header, path, &key, subprotocol);
        tls.write_all(request).await.0.map_err(Error::Io)?;
        compio::io::AsyncWrite::flush(&mut tls)
            .await
            .map_err(Error::Io)?;
        let http = read_until_header_end_tls(&mut tls).await?;
        ws_handshake::parse_server_upgrade(&http.buf[..http.header_end], &key)?;
        let leftover = extract_leftover(&http);
        return Ok(WsUpgraded {
            transport: WsTransport::Tls(tls),
            leftover,
        });
    }

    let mut stream = stream;
    let host_header = format!("{host}:{port}");
    let key = ws_handshake::generate_ws_key();
    let request = ws_handshake::format_client_upgrade(&host_header, path, &key, subprotocol);
    stream.write_all(request).await.0.map_err(Error::Io)?;
    let http = read_until_header_end_tcp(&mut stream).await?;
    ws_handshake::parse_server_upgrade(&http.buf[..http.header_end], &key)?;
    let leftover = extract_leftover(&http);
    Ok(WsUpgraded {
        transport: WsTransport::Plain(stream),
        leftover,
    })
}

async fn connect_tls_ip(
    connector: &compio_tls::TlsConnector,
    stream: TcpStream,
    ip: &std::net::IpAddr,
    port: u16,
    path: &str,
    subprotocol: &str,
) -> Result<WsUpgraded> {
    let domain = ip.to_string();
    let mut tls = connector
        .connect(&domain, stream)
        .await
        .map_err(Error::Io)?;
    let host_header = format!("{ip}:{port}");
    let key = ws_handshake::generate_ws_key();
    let request = ws_handshake::format_client_upgrade(&host_header, path, &key, subprotocol);
    tls.write_all(request).await.0.map_err(Error::Io)?;
    compio::io::AsyncWrite::flush(&mut tls)
        .await
        .map_err(Error::Io)?;
    let http = read_until_header_end_tls(&mut tls).await?;
    ws_handshake::parse_server_upgrade(&http.buf[..http.header_end], &key)?;
    let leftover = extract_leftover(&http);
    Ok(WsUpgraded {
        transport: WsTransport::Tls(tls),
        leftover,
    })
}

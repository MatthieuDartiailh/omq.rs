//! WebSocket bind/connect glue and WS connection driver (ZWS/2.0, RFC 45).

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderValue, Request, Response, StatusCode};
use tokio_util::sync::CancellationToken;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::connection::{Connection, ConnectionConfig, Role, TransportMode};
use omq_proto::proto::{Command, Event, SocketType};

use crate::engine::driver::{DriverCommand, DriverConfig, PeerOut};
use crate::routing::drop_queue::QueueReceiver;

pub(crate) type WsStream = tokio_tungstenite::WebSocketStream<WsTransport>;

pub(crate) enum WsTransport {
    Plain(TcpStream),
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
    TlsServer(tokio_rustls::server::TlsStream<TcpStream>),
}

impl tokio::io::AsyncRead for WsTransport {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            Self::TlsServer(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for WsTransport {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            Self::TlsServer(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            Self::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
            Self::TlsServer(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            Self::TlsServer(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

const SUBPROTOCOL: &str = "ZWS2.0";
const SUBPROTOCOL_NULL: &str = "ZWS2.0/NULL";

fn ws_err(e: impl std::fmt::Display) -> Error {
    Error::Io(std::io::Error::other(e.to_string()))
}

// ---- Bind / Accept / Connect ----

pub(crate) struct WsListener {
    pub(crate) inner: TcpListener,
    pub(crate) local_addr: SocketAddr,
    pub(crate) tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
}

pub(crate) async fn bind(
    host: &omq_proto::endpoint::Host,
    port: u16,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
) -> Result<WsListener> {
    use std::net::{IpAddr, Ipv4Addr};
    let addr = match host {
        omq_proto::endpoint::Host::Wildcard => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)
        }
        omq_proto::endpoint::Host::Ip(ip) => SocketAddr::new(*ip, port),
        omq_proto::endpoint::Host::Name(name) => tokio::net::lookup_host(format!("{name}:{port}"))
            .await
            .map_err(Error::Io)?
            .next()
            .ok_or_else(|| Error::InvalidEndpoint(format!("DNS lookup failed for {name}")))?,
    };
    let listener = TcpListener::bind(addr).await.map_err(Error::Io)?;
    let local = listener.local_addr().map_err(Error::Io)?;
    Ok(WsListener {
        inner: listener,
        local_addr: local,
        tls_acceptor,
    })
}

#[allow(clippy::result_large_err)]
pub(crate) async fn accept(
    stream: TcpStream,
    tls_acceptor: Option<&tokio_rustls::TlsAcceptor>,
) -> Result<WsStream> {
    let _ = stream.set_nodelay(true);
    let transport = if let Some(acc) = tls_acceptor {
        let tls = acc.accept(stream).await.map_err(Error::Io)?;
        WsTransport::TlsServer(tls)
    } else {
        WsTransport::Plain(stream)
    };
    let ws = tokio_tungstenite::accept_hdr_async(
        transport,
        |req: &Request<()>,
         mut resp: Response<()>|
         -> std::result::Result<Response<()>, Response<Option<String>>> {
            let protocols = req
                .headers()
                .get("Sec-WebSocket-Protocol")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let chosen = protocols
                .split(',')
                .map(str::trim)
                .find(|p| *p == SUBPROTOCOL_NULL || *p == SUBPROTOCOL);
            if let Some(proto) = chosen {
                resp.headers_mut().insert(
                    "Sec-WebSocket-Protocol",
                    HeaderValue::from_str(proto).expect("valid header"),
                );
                Ok(resp)
            } else {
                let mut err_resp: Response<Option<String>> =
                    Response::new(Some("no supported ZWS subprotocol".into()));
                *err_resp.status_mut() = StatusCode::BAD_REQUEST;
                Err(err_resp)
            }
        },
    )
    .await
    .map_err(ws_err)?;
    Ok(ws)
}

pub(crate) async fn connect(
    host: &omq_proto::endpoint::Host,
    port: u16,
    path: &str,
    tls: bool,
    accept_invalid_certs: bool,
) -> Result<WsStream> {
    let addr = match host {
        omq_proto::endpoint::Host::Wildcard => {
            return Err(Error::InvalidEndpoint(
                "cannot connect to wildcard host".into(),
            ));
        }
        omq_proto::endpoint::Host::Ip(ip) => SocketAddr::new(*ip, port),
        omq_proto::endpoint::Host::Name(name) => tokio::net::lookup_host(format!("{name}:{port}"))
            .await
            .map_err(Error::Io)?
            .next()
            .ok_or_else(|| Error::InvalidEndpoint(format!("DNS lookup failed for {name}")))?,
    };
    let stream = TcpStream::connect(addr).await.map_err(Error::Io)?;
    let _ = stream.set_nodelay(true);

    let scheme = if tls { "wss" } else { "ws" };
    let url = format!("{scheme}://{host}:{port}{path}");
    let mut request = url.into_client_request().map_err(ws_err)?;
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_static(SUBPROTOCOL_NULL),
    );

    let transport = if tls {
        let connector = build_tls_connector(accept_invalid_certs)?;
        let host_str = match host {
            omq_proto::endpoint::Host::Name(n) => n.clone(),
            omq_proto::endpoint::Host::Ip(ip) => ip.to_string(),
            omq_proto::endpoint::Host::Wildcard => "localhost".into(),
        };
        let domain = rustls_pki_types::ServerName::try_from(host_str).map_err(ws_err)?;
        let tls_stream = connector.connect(domain, stream).await.map_err(Error::Io)?;
        WsTransport::Tls(tls_stream)
    } else {
        WsTransport::Plain(stream)
    };

    let (ws, _response) = tokio_tungstenite::client_async(request, transport)
        .await
        .map_err(ws_err)?;
    Ok(ws)
}

#[allow(clippy::unnecessary_wraps)]
fn build_tls_connector(accept_invalid_certs: bool) -> Result<tokio_rustls::TlsConnector> {
    use std::sync::Arc;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    if accept_invalid_certs {
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(NoVerify));
    } else {
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_native_certs::load_native_certs().expect("load system certs") {
            let _ = roots.add(cert);
        }
        config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
    }
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

pub(crate) fn build_tls_acceptor(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<tokio_rustls::TlsAcceptor> {
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
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
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

// ---- WS Connection Driver ----

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_ws_driver(
    mut ws: WsStream,
    role: Role,
    socket_type: SocketType,
    options: &Options,
    mut inbox: mpsc::Receiver<DriverCommand>,
    peer_out: mpsc::Sender<(u64, PeerOut)>,
    peer_id: u64,
    cancel: CancellationToken,
    config: DriverConfig,
    shared_rx: Option<QueueReceiver>,
    recv_direct: Option<async_channel::Sender<Message>>,
) {
    let res = run_ws_inner(
        &mut ws,
        role,
        socket_type,
        options,
        &mut inbox,
        &peer_out,
        peer_id,
        &cancel,
        &config,
        shared_rx.as_ref(),
        recv_direct.as_ref(),
    )
    .await;
    if let Err(e) = &res {
        let ev = Event::Command(Command::Error {
            reason: format!("{e}"),
        });
        let _ = peer_out.send((peer_id, PeerOut::Event(ev))).await;
    }
    let _ = peer_out.send((peer_id, PeerOut::Closed)).await;
    let _ = ws.close(None).await;
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_ws_inner(
    ws: &mut WsStream,
    role: Role,
    socket_type: SocketType,
    options: &Options,
    inbox: &mut mpsc::Receiver<DriverCommand>,
    peer_out: &mpsc::Sender<(u64, PeerOut)>,
    peer_id: u64,
    cancel: &CancellationToken,
    config: &DriverConfig,
    shared_rx: Option<&QueueReceiver>,
    recv_direct: Option<&async_channel::Sender<Message>>,
) -> Result<()> {
    let mut cfg = ConnectionConfig::new(role, socket_type)
        .identity(options.identity.clone())
        .mechanism(options.mechanism.to_setup())
        .transport_mode(TransportMode::WebSocket);
    if let Some(n) = options.max_message_size {
        cfg = cfg.max_message_size(n);
    }
    let mut codec = Connection::new(cfg);

    let hb_interval = config.heartbeat_interval;
    let hb_timeout = config
        .heartbeat_timeout
        .or(config.heartbeat_interval)
        .unwrap_or(Duration::MAX);
    let hb_ttl_deciseconds = config
        .heartbeat_ttl
        .and_then(|d| u16::try_from(d.as_millis() / 100).ok())
        .unwrap_or(0);

    let mut handshake_done = false;
    let mut deadline: Option<Instant> = config.handshake_timeout.map(|t| Instant::now() + t);
    let mut hb_next: Option<Instant> = None;
    let mut hb_last_input = Instant::now();

    flush_ws_frames(&mut codec, ws).await?;

    loop {
        drain_events(
            &mut codec,
            &mut handshake_done,
            &mut deadline,
            &mut hb_next,
            hb_interval,
            peer_out,
            peer_id,
            recv_direct,
        )
        .await?;
        flush_ws_frames(&mut codec, ws).await?;

        let sleep_deadline = match (deadline, hb_next) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let timeout = async {
            match sleep_deadline {
                Some(t) => tokio::time::sleep_until(t.into()).await,
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            () = timeout => {
                if let Some(dl) = deadline
                    && Instant::now() >= dl
                {
                    return Err(Error::HandshakeFailed("handshake timeout".into()));
                }
                if hb_next.is_some() {
                    if hb_last_input.elapsed() > hb_timeout {
                        return Err(Error::Timeout);
                    }
                    let ping = Command::Ping {
                        ttl_deciseconds: hb_ttl_deciseconds,
                        context: Bytes::new(),
                    };
                    codec.send_command(&ping)?;
                    if let Some(iv) = hb_interval {
                        hb_next = Some(Instant::now() + iv);
                    }
                }
            }
            msg = ws.next() => {
                let Some(msg) = msg else { return Ok(()); };
                let msg = msg.map_err(ws_err)?;
                match msg {
                    tokio_tungstenite::tungstenite::Message::Binary(data) => {
                        hb_last_input = Instant::now();
                        codec.handle_ws_message(data)?;
                    }
                    tokio_tungstenite::tungstenite::Message::Close(_) => return Ok(()),
                    _ => {}
                }
            }
            cmd = inbox.recv() => {
                let Some(cmd) = cmd else { return Ok(()); };
                match cmd {
                    DriverCommand::SendMessage(m) => {
                        codec.send_message(&m)?;
                        flush_ws_frames(&mut codec, ws).await?;
                    }
                    DriverCommand::SendCommand(c) => {
                        codec.send_command(&c)?;
                        flush_ws_frames(&mut codec, ws).await?;
                    }
                    DriverCommand::Close => return Ok(()),
                }
            }
            msg = async {
                match shared_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending::<Option<Message>>().await,
                }
            } => {
                if let Some(m) = msg {
                    codec.send_message(&m)?;
                    let mut drained = 1usize;
                    if let Some(rx) = shared_rx {
                        while let Some(extra) = rx.try_pop() {
                            codec.send_message(&extra)?;
                            drained += 1;
                        }
                        rx.release_permits(drained);
                    }
                    flush_ws_frames(&mut codec, ws).await?;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn drain_events(
    codec: &mut Connection,
    handshake_done: &mut bool,
    deadline: &mut Option<Instant>,
    hb_next: &mut Option<Instant>,
    hb_interval: Option<Duration>,
    peer_out: &mpsc::Sender<(u64, PeerOut)>,
    peer_id: u64,
    recv_direct: Option<&async_channel::Sender<Message>>,
) -> Result<()> {
    while let Some(ev) = codec.poll_event() {
        match ev {
            Event::HandshakeSucceeded { .. } if !*handshake_done => {
                *handshake_done = true;
                *deadline = None;
                if let Some(iv) = hb_interval {
                    *hb_next = Some(Instant::now() + iv);
                }
            }
            _ => {}
        }
        let _ = peer_out.send((peer_id, PeerOut::Event(ev))).await;
    }
    while let Some(m) = codec.poll_message() {
        if let Some(direct) = recv_direct {
            let _ = direct.send(m).await;
            continue;
        }
        let ev = Event::Message(m);
        let _ = peer_out.send((peer_id, PeerOut::Event(ev))).await;
    }
    Ok(())
}

async fn flush_ws_frames(codec: &mut Connection, ws: &mut WsStream) -> Result<()> {
    let mut any = false;
    while let Some(frame) = codec.poll_ws_frame() {
        ws.feed(tokio_tungstenite::tungstenite::Message::Binary(frame))
            .await
            .map_err(ws_err)?;
        any = true;
    }
    if any {
        ws.flush().await.map_err(ws_err)?;
    }
    Ok(())
}

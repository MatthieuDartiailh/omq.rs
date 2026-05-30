//! Transport dispatch types and bind / connect helpers.
//!
//! `AnyStream` is the common byte-stream half (TCP or IPC); inproc
//! has its own non-byte-stream Message-channel pair carried inside
//! `AnyConn::Inproc`. `AnyListener` wraps the same three transports
//! on the bind side. `bind_any` / `connect_any` are the dispatch
//! entry points the socket actor calls from its bind / dial paths.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpStream, UnixStream};

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};

use crate::transport::{
    InprocConn, InprocPeerSnapshot, IpcTransport, Listener as _, PeerIdent, TcpTransport,
    Transport as _, inproc as inproc_transport,
};

/// Byte-stream dispatch across TCP-shaped transports (TCP, IPC, WS).
/// Inproc does NOT go through this - it skips the ZMTP codec entirely
/// and uses its own Message-typed channel pair (see `AnyConn`).
#[derive(Debug)]
pub(crate) enum AnyStream {
    Tcp(TcpStream),
    Ipc(UnixStream),
    #[cfg(feature = "ws")]
    Ws(Box<crate::transport::ws::WsTransport>),
}

impl AnyStream {
    /// Apply per-socket TCP options (currently just keepalive). No-op
    /// for non-TCP variants. Called from the actor on every accepted /
    /// connected stream so the option lives for the connection's
    /// lifetime.
    pub(crate) fn apply_tcp_options(&self, options: &omq_proto::Options) -> std::io::Result<()> {
        match self {
            Self::Tcp(s) => {
                options.tcp_keepalive.apply(s)?;
                options.apply_socket_buffers(s)?;
                Ok(())
            }
            Self::Ipc(s) => options.apply_socket_buffers(s),
            #[cfg(feature = "ws")]
            Self::Ws(_) => Ok(()),
        }
    }
}

impl AsyncRead for AnyStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            Self::Ipc(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "ws")]
            Self::Ws(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AnyStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            Self::Ipc(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "ws")]
            Self::Ws(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_flush(cx),
            Self::Ipc(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "ws")]
            Self::Ws(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            Self::Ipc(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "ws")]
            Self::Ws(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// What `bind_any` / `connect_any` hand back. Either a byte-stream
/// (TCP / IPC - runs the ZMTP codec via `ConnectionDriver`) or a
/// pre-paired Message channel (inproc - runs the codec-less
/// `InprocPeerDriver`).
pub(crate) enum AnyConn {
    ByteStream {
        stream: AnyStream,
        peer_ident: PeerIdent,
        leftover: bytes::Bytes,
    },
    Inproc {
        conn: InprocConn,
        peer_ident: PeerIdent,
    },
}

impl std::fmt::Debug for AnyConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ByteStream { peer_ident, .. } => f
                .debug_struct("AnyConn::ByteStream")
                .field("peer_ident", peer_ident)
                .finish(),
            Self::Inproc { peer_ident, .. } => f
                .debug_struct("AnyConn::Inproc")
                .field("peer_ident", peer_ident)
                .finish(),
        }
    }
}

impl AnyConn {
    pub(crate) fn peer_ident(&self) -> &PeerIdent {
        match self {
            Self::ByteStream { peer_ident, .. } | Self::Inproc { peer_ident, .. } => peer_ident,
        }
    }
}

pub(super) enum AnyListener {
    Tcp(crate::transport::tcp::TcpListener),
    Inproc(crate::transport::InprocListener),
    Ipc(crate::transport::ipc::IpcListener),
    #[cfg(feature = "ws")]
    Ws(crate::transport::ws::WsListener),
}

impl AnyListener {
    pub(super) fn local_endpoint(&self) -> &Endpoint {
        match self {
            Self::Tcp(l) => l.local_endpoint(),
            Self::Inproc(l) => l.local_endpoint(),
            Self::Ipc(l) => l.local_endpoint(),
            #[cfg(feature = "ws")]
            Self::Ws(_) => {
                static DUMMY: Endpoint = Endpoint::Tcp {
                    host: omq_proto::endpoint::Host::Wildcard,
                    port: 0,
                };
                &DUMMY
            }
        }
    }

    pub(super) async fn accept(&mut self) -> Result<AnyConn> {
        match self {
            Self::Tcp(l) => l.accept().await.map(|(s, peer_ident)| AnyConn::ByteStream {
                stream: AnyStream::Tcp(s),
                peer_ident,
                leftover: bytes::Bytes::new(),
            }),
            Self::Inproc(l) => {
                let peer_ident = PeerIdent::Inproc(l.name().to_string());
                let conn = l.accept().await?;
                Ok(AnyConn::Inproc { conn, peer_ident })
            }
            Self::Ipc(l) => l.accept().await.map(|(s, peer_ident)| AnyConn::ByteStream {
                stream: AnyStream::Ipc(s),
                peer_ident,
                leftover: bytes::Bytes::new(),
            }),
            #[cfg(feature = "ws")]
            Self::Ws(l) => {
                let (stream, addr) = l.inner.accept().await.map_err(Error::Io)?;
                let accepted =
                    crate::transport::ws::accept(stream, l.tls_acceptor.as_ref()).await?;
                Ok(AnyConn::ByteStream {
                    stream: AnyStream::Ws(Box::new(accepted.transport)),
                    peer_ident: PeerIdent::Socket(addr),
                    leftover: accepted.leftover,
                })
            }
        }
    }
}

/// Bind dispatch: route an endpoint to its transport's listener and wrap it.
///
/// `lz4+tcp://` and `zstd+tcp://` reuse the TCP listener; the per-connection
/// transform is installed by the actor based on the endpoint scheme.
pub(super) async fn bind_any(
    endpoint: &Endpoint,
    snapshot: &InprocPeerSnapshot,
    recv_notify: &std::sync::Arc<tokio::sync::Notify>,
    max_message_size: Option<usize>,
    #[cfg(feature = "ws")] wss_tls: &omq_proto::options::WssTls,
) -> Result<AnyListener> {
    if endpoint.is_tcp_family() {
        return Ok(AnyListener::Tcp(
            TcpTransport::bind(&endpoint.underlying_tcp()).await?,
        ));
    }
    #[cfg(feature = "ws")]
    if endpoint.is_ws_family() {
        let (host, port) = match endpoint {
            Endpoint::Ws { host, port, .. } | Endpoint::Wss { host, port, .. } => (host, *port),
            _ => unreachable!(),
        };
        let tls_acc = if matches!(endpoint, Endpoint::Wss { .. }) {
            let cert = wss_tls.server_cert_pem.as_deref().ok_or_else(|| {
                Error::Protocol("wss:// bind requires server_cert_pem in WssTls options".into())
            })?;
            let key = wss_tls.server_key_pem.as_deref().ok_or_else(|| {
                Error::Protocol("wss:// bind requires server_key_pem in WssTls options".into())
            })?;
            Some(crate::transport::ws::build_tls_acceptor(cert, key)?)
        } else {
            None
        };
        let l = crate::transport::ws::bind(host, port, tls_acc).await?;
        return Ok(AnyListener::Ws(l));
    }
    match endpoint {
        Endpoint::Inproc { name } => Ok(AnyListener::Inproc(inproc_transport::bind(
            name,
            snapshot.clone(),
            recv_notify.clone(),
            max_message_size,
        )?)),
        Endpoint::Ipc(_) => Ok(AnyListener::Ipc(IpcTransport::bind(endpoint).await?)),
        other => Err(Error::UnsupportedScheme(other.scheme().to_string())),
    }
}

/// Connect dispatch (single attempt). Used under `dial_with_backoff`.
pub(super) async fn connect_any(
    endpoint: &Endpoint,
    snapshot: &InprocPeerSnapshot,
    recv_notify: &std::sync::Arc<tokio::sync::Notify>,
    #[cfg(feature = "ws")] accept_invalid_certs: bool,
    #[cfg(feature = "ws")] mechanism: &omq_proto::MechanismSetup,
) -> Result<AnyConn> {
    if endpoint.is_tcp_family() {
        let s = TcpTransport::connect(&endpoint.underlying_tcp()).await?;
        let peer_ident = peer_ident_for_endpoint(endpoint);
        return Ok(AnyConn::ByteStream {
            stream: AnyStream::Tcp(s),
            peer_ident,
            leftover: bytes::Bytes::new(),
        });
    }
    #[cfg(feature = "ws")]
    if endpoint.is_ws_family() {
        let (host, port, path) = match endpoint {
            Endpoint::Ws {
                host, port, path, ..
            }
            | Endpoint::Wss {
                host, port, path, ..
            } => (host, *port, path.as_str()),
            _ => unreachable!(),
        };
        let connected = crate::transport::ws::connect(
            host,
            port,
            path,
            matches!(endpoint, Endpoint::Wss { .. }),
            accept_invalid_certs,
            mechanism,
        )
        .await?;
        let peer_ident = peer_ident_for_endpoint(endpoint);
        return Ok(AnyConn::ByteStream {
            stream: AnyStream::Ws(Box::new(connected.transport)),
            peer_ident,
            leftover: connected.leftover,
        });
    }
    match endpoint {
        Endpoint::Inproc { name } => {
            let conn =
                inproc_transport::connect(name, snapshot.clone(), recv_notify.clone()).await?;
            Ok(AnyConn::Inproc {
                conn,
                peer_ident: PeerIdent::Inproc(name.clone()),
            })
        }
        Endpoint::Ipc(_) => {
            let s = IpcTransport::connect(endpoint).await?;
            let peer_ident = peer_ident_for_endpoint(endpoint);
            Ok(AnyConn::ByteStream {
                stream: AnyStream::Ipc(s),
                peer_ident,
                leftover: bytes::Bytes::new(),
            })
        }
        other => Err(Error::UnsupportedScheme(other.scheme().to_string())),
    }
}

pub(super) fn peer_ident_for_endpoint(endpoint: &Endpoint) -> PeerIdent {
    match endpoint {
        Endpoint::Tcp { host, port } => PeerIdent::Path(format!("{host}:{port}")),
        Endpoint::Inproc { name } => PeerIdent::Inproc(name.clone()),
        other => PeerIdent::Path(other.to_string()),
    }
}

pub(super) fn peer_ident_socket_addr(ident: &PeerIdent) -> Option<std::net::SocketAddr> {
    match ident {
        PeerIdent::Socket(a) => Some(*a),
        _ => None,
    }
}

pub(super) use omq_proto::message::generated_identity;

use std::sync::{Arc, RwLock, atomic::Ordering};

use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::error::{Error, Result};
use omq_proto::proto::SocketType;

use crate::monitor::PeerIdent;
use crate::transport::driver::DriverCommand;
use crate::transport::inproc;
use crate::transport::stream_raw;
use crate::transport::tcp as tcp_transport;

use super::Socket;
use super::dial::{connect_ipc_with_reconnect, connect_tcp_with_reconnect};
use super::inner::SocketInner;
use super::inner::{PeerOut, PeerSlot, UdpDialerEntry, WirePeerHandle};
use super::install::install_inproc_peer;
use super::reject_encrypted_inproc;

impl Socket {
    /// Queue a connect attempt. Returns immediately; the background dial
    /// supervisor handles the initial connect and any retries per the
    /// configured [`ReconnectPolicy`](omq_proto::ReconnectPolicy).
    pub async fn connect(&self, endpoint: Endpoint) -> Result<()> {
        self.connect_inner(endpoint).await
    }

    #[expect(clippy::too_many_lines)]
    async fn connect_inner(&self, endpoint: Endpoint) -> Result<()> {
        reject_encrypted_inproc(&endpoint, &self.inner().options.mechanism)?;
        if self.inner().socket_type == SocketType::Stream {
            if !endpoint.is_tcp_family() {
                return Err(Error::Protocol(
                    "STREAM sockets only support tcp:// endpoints".into(),
                ));
            }
            return self.connect_stream_tcp(endpoint).await;
        }
        if endpoint.is_tcp_family() {
            use omq_proto::proto::connection::Role;
            let plain = endpoint.underlying_tcp();
            if let Endpoint::Tcp {
                host: Host::Name(name),
                port,
            } = &plain
            {
                tcp_transport::resolve_name(name, *port)?;
            }
            connect_tcp_with_reconnect(self.inner(), endpoint, Role::Client);
            return Ok(());
        }
        #[cfg(feature = "ws")]
        if endpoint.is_ws_family() {
            match &endpoint {
                Endpoint::Ws {
                    host: Host::Name(name),
                    port,
                    ..
                }
                | Endpoint::Wss {
                    host: Host::Name(name),
                    port,
                    ..
                } => {
                    tcp_transport::resolve_name(name, *port)?;
                }
                _ => {}
            }
            let inner = self.inner().clone();
            let ep = endpoint.clone();
            let mechanism = self.inner().options.mechanism.clone();
            let accept_invalid_certs = self.inner().options.wss_tls.accept_invalid_certs;
            compio::runtime::spawn(async move {
                let Ok(upgraded) =
                    crate::transport::ws::connect(&ep, &mechanism, accept_invalid_certs).await
                else {
                    return;
                };
                let conn_id = inner
                    .next_connection_id
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                inner.monitor.connected(
                    ep.clone(),
                    crate::monitor::PeerIdent::Path(ep.to_string()),
                    conn_id,
                );
                crate::socket::install::install_ws_peer(
                    &inner,
                    upgraded,
                    omq_proto::proto::connection::Role::Client,
                    ep,
                    conn_id,
                    None,
                );
            })
            .detach();
            return Ok(());
        }
        match endpoint {
            Endpoint::Inproc { name } => {
                let snapshot = self.inner().snapshot();
                let in_tx = self.inner().in_tx.clone();
                let recv_event = self.inner().inproc_recv_event.clone();
                let parked = self.inner().inproc_parked.clone();
                let conn_id = self
                    .inner()
                    .next_connection_id
                    .fetch_add(1, Ordering::Relaxed);

                if inproc::is_bound(&name) {
                    let conn = inproc::connect(&name, snapshot, in_tx, conn_id, recv_event, parked)
                        .await?;
                    finish_inproc_connect(self.inner(), name, conn, conn_id);
                } else {
                    let inner = self.inner().clone();
                    compio::runtime::spawn(async move {
                        let Ok(conn) =
                            inproc::connect(&name, snapshot, in_tx, conn_id, recv_event, parked)
                                .await
                        else {
                            return;
                        };
                        finish_inproc_connect(&inner, name, conn, conn_id);
                    })
                    .detach();
                }
                Ok(())
            }
            Endpoint::Ipc(_) => {
                use omq_proto::proto::connection::Role;
                connect_ipc_with_reconnect(self.inner(), endpoint, Role::Client);
                Ok(())
            }
            Endpoint::Udp { .. } => self.connect_udp(endpoint).await,
            _ => Err(Error::Protocol(
                "transport variant not enabled in this omq-compio build".into(),
            )),
        }
    }

    async fn connect_stream_tcp(&self, endpoint: Endpoint) -> Result<()> {
        let plain = endpoint.underlying_tcp();
        let stream = tcp_transport::connect(&plain).await?;
        if let Ok(poll_fd) = stream.to_poll_fd() {
            let _ = self.inner().options.tcp_keepalive.apply(&poll_fd);
            let _ = self.inner().options.apply_socket_buffers(&poll_fd);
        }
        let conn_id = self
            .inner()
            .next_connection_id
            .fetch_add(1, Ordering::Relaxed);
        self.inner().monitor.connected(
            endpoint.clone(),
            PeerIdent::Path(format!("{endpoint}")),
            conn_id,
        );
        let identity = stream_raw::generated_identity(conn_id);
        let cap = super::cmd_channel_capacity(&self.inner().options);
        let (cmd_tx, cmd_rx) = flume::bounded::<DriverCommand>(cap);
        let handle: WirePeerHandle = Arc::new(RwLock::new(cmd_tx));
        let inner = self.inner().clone();
        let (_, writer) = stream.clone().into_split();
        let slot_idx = inner.insert_peer_slot(
            PeerSlot {
                out: PeerOut::Wire(handle),
                direct_io: None,
                peer: Arc::new(RwLock::new(None)),
                connection_id: conn_id,
                endpoint: endpoint.clone(),
                info: Arc::new(RwLock::new(None)),
                peer_sub: None,
                peer_groups: None,
            },
            Some(&identity),
        );
        let in_tx = inner.in_tx.clone();
        compio::runtime::spawn(async move {
            stream_raw::run(stream, writer.into(), conn_id, in_tx, cmd_rx).await;
            inner.release_slot(slot_idx);
        })
        .detach();
        Ok(())
    }

    async fn connect_udp(&self, endpoint: Endpoint) -> Result<()> {
        if self.inner().socket_type != SocketType::Radio {
            return Err(Error::Protocol(
                "udp:// connect is only supported on RADIO sockets".into(),
            ));
        }
        let sock = crate::transport::udp::connect(&endpoint).await?;
        let conn_id = self
            .inner()
            .next_connection_id
            .fetch_add(1, Ordering::Relaxed);
        self.inner().monitor.connected(
            endpoint.clone(),
            PeerIdent::Path(format!("{endpoint}")),
            conn_id,
        );
        self.inner()
            .udp_dialers
            .write()
            .expect("udp_dialers lock")
            .push(UdpDialerEntry {
                endpoint: endpoint.clone(),
                sock: Arc::new(sock),
            });
        self.inner().on_peer_ready.notify(usize::MAX);
        Ok(())
    }
}

fn finish_inproc_connect(
    inner: &Arc<SocketInner>,
    name: String,
    conn: inproc::InprocConn,
    conn_id: u64,
) {
    let ep = Endpoint::Inproc { name: name.clone() };
    inner
        .monitor
        .connected(ep.clone(), PeerIdent::Inproc(name), conn_id);
    install_inproc_peer(inner, conn, ep, conn_id);
}

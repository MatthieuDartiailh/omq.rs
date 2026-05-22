use std::sync::{Arc, RwLock, atomic::Ordering};

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::proto::SocketType;

use crate::monitor::PeerIdent;
use crate::transport::driver::DriverCommand;
use crate::transport::inproc;
use crate::transport::stream_raw;
use crate::transport::tcp as tcp_transport;

use super::Socket;
use super::dial::{connect_ipc_with_reconnect, connect_tcp_with_reconnect};
use super::inner::{PeerOut, PeerSlot, UdpDialerEntry, WirePeerHandle};
use super::install::install_inproc_peer;
use super::reject_encrypted_inproc;

impl Socket {
    /// Queue a connect attempt. Returns immediately; the background dial
    /// supervisor handles the initial connect and any retries per the
    /// configured [`ReconnectPolicy`](omq_proto::ReconnectPolicy).
    pub async fn connect(&self, endpoint: Endpoint) -> Result<()> {
        self.connect_inner(
            endpoint,
            #[cfg(feature = "priority")]
            omq_proto::DEFAULT_PRIORITY,
        )
        .await
    }

    /// Like [`connect`], but applies the per-pipe options in `opts` to
    /// the new endpoint. Currently the only knob is `priority`
    /// (1..=255, lower number = higher priority; default 128).
    /// Strict semantics - see `omq_proto::ConnectOpts`.
    #[cfg(feature = "priority")]
    pub async fn connect_with(
        &self,
        endpoint: Endpoint,
        opts: omq_proto::ConnectOpts,
    ) -> Result<()> {
        self.connect_inner(endpoint, opts.priority.get()).await
    }

    #[allow(clippy::too_many_lines)]
    async fn connect_inner(
        &self,
        endpoint: Endpoint,
        #[cfg(feature = "priority")] priority: u8,
    ) -> Result<()> {
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
            connect_tcp_with_reconnect(
                self.inner(),
                endpoint,
                Role::Client,
                #[cfg(feature = "priority")]
                priority,
            );
            return Ok(());
        }
        #[cfg(feature = "ws")]
        if endpoint.is_ws_family() {
            let inner = self.inner().clone();
            let ep = endpoint.clone();
            compio::runtime::spawn(async move {
                let Ok(upgraded) = crate::transport::ws::connect(&ep).await else {
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
        #[allow(unreachable_patterns, clippy::match_wildcard_for_single_variants)]
        match endpoint {
            Endpoint::Inproc { name } => {
                let snapshot = self.inner().snapshot();
                let in_tx = self.inner().in_tx.clone();
                let recv_event = self.inner().inproc_recv_event.clone();
                let parked = self.inner().inproc_parked.clone();

                if inproc::is_bound(&name) {
                    let conn = inproc::connect(&name, snapshot, in_tx, recv_event, parked).await?;
                    let conn_id = self
                        .inner()
                        .next_connection_id
                        .fetch_add(1, Ordering::Relaxed);
                    let ep = Endpoint::Inproc { name: name.clone() };
                    self.inner()
                        .monitor
                        .connected(ep.clone(), PeerIdent::Inproc(name), conn_id);
                    install_inproc_peer(
                        self.inner(),
                        conn,
                        ep,
                        conn_id,
                        #[cfg(feature = "priority")]
                        priority,
                    );
                } else {
                    let inner = self.inner().clone();
                    let name_clone = name.clone();
                    #[cfg(feature = "priority")]
                    #[allow(clippy::redundant_locals)]
                    let priority = priority;
                    compio::runtime::spawn(async move {
                        let Ok(conn) =
                            inproc::connect(&name_clone, snapshot, in_tx, recv_event, parked).await
                        else {
                            return;
                        };
                        let conn_id = inner.next_connection_id.fetch_add(1, Ordering::Relaxed);
                        let ep = Endpoint::Inproc {
                            name: name_clone.clone(),
                        };
                        inner
                            .monitor
                            .connected(ep.clone(), PeerIdent::Inproc(name_clone), conn_id);
                        install_inproc_peer(
                            &inner,
                            conn,
                            ep,
                            conn_id,
                            #[cfg(feature = "priority")]
                            priority,
                        );
                    })
                    .detach();
                }
                Ok(())
            }
            Endpoint::Ipc(_) => {
                use omq_proto::proto::connection::Role;
                connect_ipc_with_reconnect(
                    self.inner(),
                    endpoint,
                    Role::Client,
                    #[cfg(feature = "priority")]
                    priority,
                );
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
        let slot_idx = {
            let mut peers = inner.out_peers.write().expect("peers lock");
            let idx = peers.insert(PeerSlot {
                out: PeerOut::Wire(handle),
                direct_io: None,
                peer: Arc::new(RwLock::new(None)),
                connection_id: conn_id,
                endpoint: endpoint.clone(),
                info: Arc::new(RwLock::new(None)),
                peer_sub: None,
                peer_groups: None,
                #[cfg(feature = "priority")]
                priority: omq_proto::DEFAULT_PRIORITY,
            });
            inner
                .peers_gen
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            idx
        };
        {
            let pipes = unsafe { &mut *inner.inproc_send_pipes.get() };
            while pipes.len() <= slot_idx {
                pipes.push(None);
            }
        }
        if !identity.is_empty()
            && let Some(old_idx) = inner
                .identity_to_slot
                .write()
                .expect("identity table")
                .insert(identity.clone(), slot_idx)
            && old_idx != slot_idx
        {
            inner.evict_peer_for_handover(old_idx);
        }
        inner.rebuild_peer_keys();
        inner.on_peer_ready.notify(usize::MAX);
        let in_tx = inner.in_tx.clone();
        compio::runtime::spawn(async move {
            stream_raw::run(stream, writer.into(), identity, in_tx, cmd_rx).await;
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

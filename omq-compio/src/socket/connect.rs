use std::sync::{Arc, atomic::Ordering};

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::proto::SocketType;

use crate::monitor::{MonitorEvent, PeerIdent};
use crate::transport::inproc;

use super::Socket;
use super::dial::{connect_ipc_with_reconnect, connect_tcp_with_reconnect};
use super::inner::UdpDialerEntry;
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

    async fn connect_inner(
        &self,
        endpoint: Endpoint,
        #[cfg(feature = "priority")] priority: u8,
    ) -> Result<()> {
        reject_encrypted_inproc(&endpoint, &self.inner().options.mechanism)?;
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
                    self.inner().monitor.publish(MonitorEvent::Connected {
                        endpoint: ep.clone(),
                        peer_ident: PeerIdent::Inproc(name),
                        connection_id: conn_id,
                    });
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
                        inner.monitor.publish(MonitorEvent::Connected {
                            endpoint: ep.clone(),
                            peer_ident: PeerIdent::Inproc(name_clone),
                            connection_id: conn_id,
                        });
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
        self.inner().monitor.publish(MonitorEvent::Connected {
            endpoint: endpoint.clone(),
            peer_ident: PeerIdent::Path(format!("{endpoint}")),
            connection_id: conn_id,
        });
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

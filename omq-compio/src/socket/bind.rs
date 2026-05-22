use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::proto::SocketType;

use crate::monitor::PeerIdent;
use crate::transport::driver::DriverCommand;
use crate::transport::inproc::{self, InprocFrame};
use crate::transport::ipc as ipc_transport;
use crate::transport::stream_raw;
use crate::transport::tcp as tcp_transport;

use super::Socket;
use super::inner::{ListenerEntry, PeerOut, PeerSlot, WirePeerHandle};
use super::install::{install_accepted_wire_peer, install_inproc_peer};
use super::reject_encrypted_inproc;

impl Socket {
    /// Bind to an endpoint. Returns the resolved endpoint once the
    /// listener is active. For wildcard binds (`tcp://...:0`) the
    /// returned endpoint contains the actual port.
    pub async fn bind(&self, endpoint: Endpoint) -> Result<Endpoint> {
        reject_encrypted_inproc(&endpoint, &self.inner().options.mechanism)?;
        if self.inner().socket_type == SocketType::Stream {
            if !endpoint.is_tcp_family() {
                return Err(Error::Protocol(
                    "STREAM sockets only support tcp:// endpoints".into(),
                ));
            }
            return self.bind_stream_tcp(endpoint).await;
        }
        if endpoint.is_tcp_family() {
            return self.bind_tcp(endpoint).await;
        }
        #[cfg(feature = "ws")]
        if endpoint.is_ws_family() {
            return self.bind_ws(endpoint).await;
        }
        #[allow(unreachable_patterns, clippy::match_wildcard_for_single_variants)]
        match endpoint {
            Endpoint::Inproc { name } => self.bind_inproc(name).await,
            Endpoint::Ipc(_) => self.bind_ipc(endpoint).await,
            Endpoint::Udp { .. } => self.bind_udp(endpoint).await,
            _ => Err(Error::Protocol(
                "transport variant not enabled in this omq-compio build".into(),
            )),
        }
    }

    #[allow(clippy::unused_async)]
    async fn bind_inproc(&self, name: String) -> Result<Endpoint> {
        let snapshot = self.inner().snapshot();
        let listener = inproc::bind(
            &name,
            snapshot,
            self.inner().in_tx.clone(),
            self.inner().inproc_recv_event.clone(),
            self.inner().inproc_parked.clone(),
        )?;
        let resolved = Endpoint::Inproc { name: name.clone() };
        self.inner().monitor.listening(resolved.clone());
        let inner = self.inner().clone();
        let ep_for_task = resolved.clone();
        let name_for_ident = name;
        let task = compio::runtime::spawn(async move {
            while let Ok(conn) = listener.accept().await {
                let conn_id = inner.next_connection_id.fetch_add(1, Ordering::Relaxed);
                inner.monitor.accepted(
                    ep_for_task.clone(),
                    PeerIdent::Inproc(name_for_ident.clone()),
                    conn_id,
                );
                install_inproc_peer(
                    &inner,
                    conn,
                    ep_for_task.clone(),
                    conn_id,
                    #[cfg(feature = "priority")]
                    omq_proto::DEFAULT_PRIORITY,
                );
            }
        });
        let ret = resolved.clone();
        self.inner()
            .listeners
            .write()
            .expect("listeners lock")
            .push(ListenerEntry {
                endpoint: resolved,
                _task: task,
            });
        Ok(ret)
    }

    async fn bind_tcp(&self, endpoint: Endpoint) -> Result<Endpoint> {
        let wrapper = endpoint.clone();
        let plain = endpoint.underlying_tcp();
        let (listener, local) = tcp_transport::bind(&plain).await?;
        let resolved = wrapper.rewrap_tcp(Endpoint::Tcp {
            host: omq_proto::endpoint::Host::Ip(local.ip()),
            port: local.port(),
        });
        self.inner().monitor.listening(resolved.clone());
        let inner = self.inner().clone();
        let ep_for_task = resolved.clone();
        let task = compio::runtime::spawn(async move {
            use omq_proto::proto::connection::Role;
            while let Ok((stream, addr)) = listener.accept().await {
                let _ = stream.set_nodelay(true);
                if let Ok(poll_fd) = stream.to_poll_fd() {
                    let _ = inner.options.tcp_keepalive.apply(&poll_fd);
                    let _ = inner.options.apply_socket_buffers(&poll_fd);
                }
                let conn_id = inner.next_connection_id.fetch_add(1, Ordering::Relaxed);
                inner
                    .monitor
                    .accepted(ep_for_task.clone(), PeerIdent::Socket(addr), conn_id);
                let read_clone = stream.clone();
                let Ok(read_fd) = compio::runtime::fd::AsyncFd::new(read_clone) else {
                    continue;
                };
                let (_, writer) = stream.into_split();
                install_accepted_wire_peer(
                    &inner,
                    read_fd.into(),
                    writer.into(),
                    Role::Server,
                    ep_for_task.clone(),
                    conn_id,
                    Some(addr),
                );
            }
        });
        let ret = resolved.clone();
        self.inner()
            .listeners
            .write()
            .expect("listeners lock")
            .push(ListenerEntry {
                endpoint: resolved,
                _task: task,
            });
        Ok(ret)
    }

    async fn bind_ipc(&self, endpoint: Endpoint) -> Result<Endpoint> {
        let listener = ipc_transport::bind(&endpoint).await?;
        let resolved = endpoint.clone();
        self.inner().monitor.listening(resolved.clone());
        let inner = self.inner().clone();
        let ep_for_task = resolved.clone();
        let ident_path = match &resolved {
            Endpoint::Ipc(p) => format!("{p}"),
            _ => String::new(),
        };
        let task = compio::runtime::spawn(async move {
            use omq_proto::proto::connection::Role;
            while let Ok((stream, _addr)) = listener.inner.accept().await {
                if let Ok(poll_fd) = stream.to_poll_fd() {
                    let _ = inner.options.apply_socket_buffers(&poll_fd);
                }
                let conn_id = inner.next_connection_id.fetch_add(1, Ordering::Relaxed);
                inner.monitor.accepted(
                    ep_for_task.clone(),
                    PeerIdent::Path(ident_path.clone()),
                    conn_id,
                );
                let read_clone = stream.clone();
                let Ok(read_fd) = compio::runtime::fd::AsyncFd::new(read_clone) else {
                    continue;
                };
                let (_, writer) = stream.into_split();
                install_accepted_wire_peer(
                    &inner,
                    read_fd.into(),
                    writer.into(),
                    Role::Server,
                    ep_for_task.clone(),
                    conn_id,
                    None,
                );
            }
        });
        let ret = resolved.clone();
        self.inner()
            .listeners
            .write()
            .expect("listeners lock")
            .push(ListenerEntry {
                endpoint: resolved,
                _task: task,
            });
        Ok(ret)
    }

    async fn bind_stream_tcp(&self, endpoint: Endpoint) -> Result<Endpoint> {
        let plain = endpoint.underlying_tcp();
        let (listener, local) = tcp_transport::bind(&plain).await?;
        let resolved = endpoint.rewrap_tcp(Endpoint::Tcp {
            host: omq_proto::endpoint::Host::Ip(local.ip()),
            port: local.port(),
        });
        self.inner().monitor.listening(resolved.clone());
        let inner = self.inner().clone();
        let ep = resolved.clone();
        let task = compio::runtime::spawn(async move {
            while let Ok((stream, addr)) = listener.accept().await {
                let _ = stream.set_nodelay(true);
                if let Ok(poll_fd) = stream.to_poll_fd() {
                    let _ = inner.options.tcp_keepalive.apply(&poll_fd);
                    let _ = inner.options.apply_socket_buffers(&poll_fd);
                }
                let conn_id = inner.next_connection_id.fetch_add(1, Ordering::Relaxed);
                inner
                    .monitor
                    .accepted(ep.clone(), PeerIdent::Socket(addr), conn_id);
                let identity = stream_raw::generated_identity(conn_id);
                let cap = super::cmd_channel_capacity(&inner.options);
                let (cmd_tx, cmd_rx) = flume::bounded::<DriverCommand>(cap);
                let handle: WirePeerHandle = Arc::new(RwLock::new(cmd_tx));
                let (_, writer) = stream.clone().into_split();
                let slot_idx = inner.insert_peer_slot(
                    PeerSlot {
                        out: PeerOut::Wire(handle),
                        direct_io: None,
                        peer: Arc::new(RwLock::new(None)),
                        connection_id: conn_id,
                        endpoint: ep.clone(),
                        info: Arc::new(RwLock::new(None)),
                        peer_sub: None,
                        peer_groups: None,
                        #[cfg(feature = "priority")]
                        priority: omq_proto::DEFAULT_PRIORITY,
                    },
                    Some(&identity),
                );
                let inner2 = inner.clone();
                let in_tx = inner.in_tx.clone();
                compio::runtime::spawn(async move {
                    stream_raw::run(stream, writer.into(), identity, in_tx, cmd_rx).await;
                    inner2.release_slot(slot_idx);
                })
                .detach();
            }
        });
        let ret = resolved.clone();
        self.inner()
            .listeners
            .write()
            .expect("listeners lock")
            .push(ListenerEntry {
                endpoint: resolved,
                _task: task,
            });
        Ok(ret)
    }

    async fn bind_udp(&self, endpoint: Endpoint) -> Result<Endpoint> {
        if self.inner().socket_type != SocketType::Dish {
            return Err(Error::Protocol(
                "udp:// bind is only supported on DISH sockets".into(),
            ));
        }
        let sock = crate::transport::udp::bind(&endpoint).await?;
        let local = sock.local_addr().map_err(Error::Io)?;
        let resolved = match &endpoint {
            Endpoint::Udp { group, .. } => Endpoint::Udp {
                group: group.clone(),
                host: omq_proto::endpoint::Host::Ip(local.ip()),
                port: local.port(),
            },
            _ => unreachable!("checked above"),
        };
        self.inner().monitor.listening(resolved.clone());
        let inner = self.inner().clone();
        let task = compio::runtime::spawn(async move {
            let mut buf = vec![0u8; crate::transport::udp::MAX_DATAGRAM_SIZE];
            loop {
                let compio::BufResult(res, returned) = sock.recv_from(buf).await;
                buf = returned;
                let Ok((n, _from)) = res else { break };
                let Some((group, body)) = crate::transport::udp::decode_datagram(&buf[..n]) else {
                    continue;
                };
                let joined_now = {
                    let g = inner.joined_groups.read().expect("joined_groups lock");
                    g.contains(&group)
                };
                if !joined_now {
                    continue;
                }
                let msg = Message::multipart([group, body]);
                let frame =
                    InprocFrame::Message(Box::new(crate::transport::inproc::InprocFullMessage {
                        peer_identity: None,
                        msg,
                    }));
                if inner.in_tx.send_async(frame).await.is_err() {
                    break;
                }
            }
        });
        let ret = resolved.clone();
        self.inner()
            .listeners
            .write()
            .expect("listeners lock")
            .push(ListenerEntry {
                endpoint: resolved,
                _task: task,
            });
        Ok(ret)
    }

    #[cfg(feature = "ws")]
    async fn bind_ws(&self, endpoint: Endpoint) -> Result<Endpoint> {
        use crate::transport::ws as ws_transport;

        let tls_acc = if matches!(endpoint, Endpoint::Wss { .. }) {
            let cert = self
                .inner()
                .options
                .wss_tls
                .server_cert_pem
                .as_deref()
                .ok_or_else(|| {
                    omq_proto::error::Error::Protocol(
                        "wss:// bind requires server_cert_pem in WssTls options".into(),
                    )
                })?;
            let key = self
                .inner()
                .options
                .wss_tls
                .server_key_pem
                .as_deref()
                .ok_or_else(|| {
                    omq_proto::error::Error::Protocol(
                        "wss:// bind requires server_key_pem in WssTls options".into(),
                    )
                })?;
            Some(ws_transport::build_tls_acceptor(cert, key)?)
        } else {
            None
        };
        let ws_listener = ws_transport::bind(&endpoint, tls_acc).await?;
        let local = ws_listener.local_addr;
        let resolved = match &endpoint {
            Endpoint::Ws { path, .. } => Endpoint::Ws {
                host: omq_proto::endpoint::Host::Ip(local.ip()),
                port: local.port(),
                path: path.clone(),
            },
            Endpoint::Wss { path, .. } => Endpoint::Wss {
                host: omq_proto::endpoint::Host::Ip(local.ip()),
                port: local.port(),
                path: path.clone(),
            },
            _ => unreachable!("checked above"),
        };
        self.inner().monitor.listening(resolved.clone());
        let inner = self.inner().clone();
        let ep_for_task = resolved.clone();
        let tls_acc = ws_listener.tls_acceptor.clone();
        let task = compio::runtime::spawn(async move {
            while let Ok((stream, addr)) = ws_listener.inner.accept().await {
                let inner = inner.clone();
                let ep = ep_for_task.clone();
                let tls_acc = tls_acc.clone();
                compio::runtime::spawn(async move {
                    let Ok(upgraded) = ws_transport::accept(stream, tls_acc.as_ref()).await else {
                        return;
                    };
                    let conn_id = inner
                        .next_connection_id
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    inner.monitor.accepted(
                        ep.clone(),
                        crate::monitor::PeerIdent::Socket(addr),
                        conn_id,
                    );
                    crate::socket::install::install_ws_peer(
                        &inner,
                        upgraded,
                        omq_proto::proto::connection::Role::Server,
                        ep,
                        conn_id,
                        Some(addr),
                    );
                })
                .detach();
            }
        });
        let ret = resolved.clone();
        self.inner()
            .listeners
            .write()
            .expect("listeners lock")
            .push(ListenerEntry {
                endpoint: resolved,
                _task: task,
            });
        Ok(ret)
    }
}

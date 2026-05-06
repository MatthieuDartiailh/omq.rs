//! Public [`Socket`] handle and the `impl Socket` block - every
//! `&self` method on the public API surface lives here.

use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

use bytes::Bytes;

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::{Event, SocketType};

use crate::monitor::{MonitorEvent, MonitorStream, PeerIdent};
use crate::transport::driver::DriverCommand;
use crate::transport::inproc::{self, InprocFrame};
use crate::transport::ipc as ipc_transport;
use crate::transport::peer_io::PeerIo;
use crate::transport::tcp as tcp_transport;

use super::dial::{connect_ipc_with_reconnect, connect_tcp_with_reconnect};
use super::inner::{
    DirectIoState, ListenerEntry, PeerOut, SocketInner, UdpDialerEntry, WirePeerHandle,
};
use super::install::{install_accepted_wire_peer, install_inproc_peer};
use super::reject_encrypted_inproc;

/// Mirror of [`super::send::pre_send_needs_type_state`] for the recv path:
/// REQ / REP touch state, DISH validates two-frame `[group, body]`
/// shape. Everything else just returns the message unchanged.
fn post_recv_needs_type_state(t: SocketType) -> bool {
    matches!(t, SocketType::Req | SocketType::Rep | SocketType::Dish)
}

/// Identity-routed recv: the user-visible message is `[peer_identity,
/// body...]` rather than `[body...]`. ROUTER, SERVER and PEER all
/// identify their peers this way so a reply can be addressed back.
fn is_identity_recv(t: SocketType) -> bool {
    matches!(
        t,
        SocketType::Router | SocketType::Server | SocketType::Peer
    )
}

/// Recv-direct fast path eligibility. The path is taken when there's
/// exactly one wire peer and the socket type's recv stream is "user
/// data only" — no ROUTER identity prefix, no XPUB/XSUB subscribe
/// command surfacing, no UDP-only DISH. Other shapes go through the
/// driver's inproc-frame hop.
fn direct_recv_eligible(t: SocketType) -> bool {
    matches!(
        t,
        SocketType::Pull | SocketType::Sub | SocketType::Rep | SocketType::Pair | SocketType::Req
    )
}

/// Outcome of the multi-shot recv stream pull-and-feed select arm.
/// Materialising the cases as an enum lets us complete the
/// extract-buffer-and-feed-codec sequence synchronously inside the
/// arm (no `.await` between buffer extract and `handle_input`),
/// preserving the cancellation-safety invariant: dropping the recv
/// future at any earlier `.await` does not lose any kernel bytes.
enum PullOutcome {
    /// `handle_input` consumed a non-empty buffer.
    Fed,
    /// Stream terminated cleanly (peer closed) or yielded an empty buffer.
    Eof,
    /// Codec rejected input as a protocol error.
    ProtoErr,
    /// Stream errored. ENOBUFS is recoverable via re-arm; other errors
    /// are surfaced as fatal by the caller.
    Err(std::io::Error),
}

/// RAII guard for the [`DirectIoState`] recv claim. Released on drop:
/// resets `recv_claim` to 0 (idle) and wakes the driver via
/// `recv_state_changed` so it re-evaluates and resumes reading.
struct ClaimGuard<'a> {
    state: &'a Arc<DirectIoState>,
}

impl Drop for ClaimGuard<'_> {
    fn drop(&mut self) {
        self.state.recv_claim.store(0, Ordering::Release);
        self.state.recv_state_changed.notify(usize::MAX);
    }
}

/// A ZMQ-style socket. Clone-able; all clones talk to the same underlying
/// driver tasks. Close happens via the explicit [`Socket::close`] method
/// (the last handle drop cancels the background tasks without draining).
#[derive(Clone)]
pub struct Socket {
    inner: Arc<SocketInner>,
    /// Sentinel held only by user-facing `Socket` handles, never by internal
    /// tasks (drivers, dial supervisors, accept loops). When the last `Socket`
    /// drops, `Arc::strong_count` reaches 1 and `Drop` cancels the background
    /// tasks so TCP connections are torn down and peer-side drivers see EOF.
    user_life: Arc<()>,
}

impl std::fmt::Debug for Socket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Socket")
            .field("socket_type", &self.inner.socket_type)
            .finish_non_exhaustive()
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        // Cancel background tasks when the last user handle drops. Dial
        // supervisors hold Arc<SocketInner> clones independently of
        // _user_life, so without this they keep TCP connections alive even
        // after all user Sockets are gone. That prevents peer-side drivers
        // from ever seeing EOF, causing them to keep their shared_msg_rx
        // clones and steal messages meant for other peers.
        if Arc::strong_count(&self.user_life) == 1 {
            if let Ok(mut d) = self.inner.dialers.write() {
                d.clear();
            }
            if let Ok(mut l) = self.inner.listeners.write() {
                l.clear();
            }
            if let Ok(mut u) = self.inner.udp_dialers.write() {
                u.clear();
            }
        }
    }
}

impl Socket {
    /// Create a new socket of the given type with the given options. Background
    /// tasks (listener accept loops, dial supervisors, per-connection drivers)
    /// are spawned lazily by [`bind`](Self::bind) and [`connect`](Self::connect)
    /// on the current compio runtime.
    pub fn new(socket_type: SocketType, options: Options) -> Self {
        assert!(
            !options.conflate || crate::socket::supports_conflate(socket_type),
            "Options::conflate(true) is not valid for socket type {socket_type:?} \
             (no per-peer ordering invariant to preserve; conflate is \
             meaningless here)"
        );
        Self {
            inner: SocketInner::new(socket_type, options),
            user_life: Arc::new(()),
        }
    }

    /// Internal accessor used by sibling modules (send.rs, recv.rs)
    /// that hold extension methods on `Socket`. Not part of the
    /// public API.
    pub(super) fn inner(&self) -> &Arc<SocketInner> {
        &self.inner
    }

    /// The socket type this handle was created with.
    pub fn socket_type(&self) -> SocketType {
        self.inner.socket_type
    }

    /// Subscribe to connection-lifecycle events. Each call returns a
    /// fresh stream that observes events from this point onward.
    pub fn monitor(&self) -> MonitorStream {
        self.inner.monitor.subscribe()
    }

    /// Bind to an endpoint. Returns once the listener is active (or, for UDP
    /// DISH, once the socket is bound and the recv loop is spawned).
    pub async fn bind(&self, endpoint: Endpoint) -> Result<()> {
        reject_encrypted_inproc(&endpoint, &self.inner.options.mechanism)?;
        // The Lz4Tcp / ZstdTcp arms are gated on this crate's own
        // feature; Cargo feature unification can still surface those
        // variants (e.g. when a workspace neighbor enables `omq-proto/lz4`
        // for its own tests but pulls us in as a dev-dep without our lz4),
        // so we add a wildcard runtime fallback.
        #[allow(unreachable_patterns)]
        match endpoint {
            Endpoint::Inproc { name } => self.bind_inproc(name).await,
            Endpoint::Tcp { .. } => self.bind_tcp(endpoint).await,
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Tcp { .. } => self.bind_tcp(endpoint).await,
            #[cfg(feature = "zstd")]
            Endpoint::ZstdTcp { .. } => self.bind_tcp(endpoint).await,
            Endpoint::Ipc(_) => self.bind_ipc(endpoint).await,
            Endpoint::Udp { .. } => self.bind_udp(endpoint).await,
            _ => Err(Error::Protocol(
                "transport variant not enabled in this omq-compio build".into(),
            )),
        }
    }

    #[allow(clippy::unused_async)]
    async fn bind_inproc(&self, name: String) -> Result<()> {
        let snapshot = self.inner.snapshot();
        let listener = inproc::bind(&name, snapshot, self.inner.in_tx.clone())?;
        let resolved = Endpoint::Inproc { name: name.clone() };
        self.inner.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });
        let inner = self.inner.clone();
        let ep_for_task = resolved.clone();
        let name_for_ident = name;
        let task = compio::runtime::spawn(async move {
            while let Ok(conn) = listener.accept().await {
                let conn_id = inner.next_connection_id.fetch_add(1, Ordering::Relaxed);
                inner.monitor.publish(MonitorEvent::Accepted {
                    endpoint: ep_for_task.clone(),
                    peer_ident: PeerIdent::Inproc(name_for_ident.clone()),
                    connection_id: conn_id,
                });
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
        self.inner
            .listeners
            .write()
            .expect("listeners lock")
            .push(ListenerEntry {
                endpoint: resolved,
                _task: task,
            });
        Ok(())
    }

    async fn bind_tcp(&self, endpoint: Endpoint) -> Result<()> {
        let wrapper = endpoint.clone();
        let plain = endpoint.underlying_tcp();
        let (listener, local) = tcp_transport::bind(&plain).await?;
        let resolved = wrapper.rewrap_tcp(Endpoint::Tcp {
            host: omq_proto::endpoint::Host::Ip(local.ip()),
            port: local.port(),
        });
        self.inner.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });
        let inner = self.inner.clone();
        let ep_for_task = resolved.clone();
        let task = compio::runtime::spawn(async move {
            use omq_proto::proto::connection::Role;
            while let Ok((stream, addr)) = listener.accept().await {
                let _ = stream.set_nodelay(true);
                if let Ok(poll_fd) = stream.to_poll_fd() {
                    let _ = inner.options.tcp_keepalive.apply(&poll_fd);
                }
                let conn_id = inner.next_connection_id.fetch_add(1, Ordering::Relaxed);
                inner.monitor.publish(MonitorEvent::Accepted {
                    endpoint: ep_for_task.clone(),
                    peer_ident: PeerIdent::Socket(addr),
                    connection_id: conn_id,
                });
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
        self.inner
            .listeners
            .write()
            .expect("listeners lock")
            .push(ListenerEntry {
                endpoint: resolved,
                _task: task,
            });
        Ok(())
    }

    async fn bind_ipc(&self, endpoint: Endpoint) -> Result<()> {
        let listener = ipc_transport::bind(&endpoint).await?;
        let resolved = endpoint.clone();
        self.inner.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });
        let inner = self.inner.clone();
        let ep_for_task = resolved.clone();
        let ident_path = match &resolved {
            Endpoint::Ipc(p) => format!("{p}"),
            _ => String::new(),
        };
        let task = compio::runtime::spawn(async move {
            use omq_proto::proto::connection::Role;
            while let Ok((stream, _addr)) = listener.inner.accept().await {
                let conn_id = inner.next_connection_id.fetch_add(1, Ordering::Relaxed);
                inner.monitor.publish(MonitorEvent::Accepted {
                    endpoint: ep_for_task.clone(),
                    peer_ident: PeerIdent::Path(ident_path.clone()),
                    connection_id: conn_id,
                });
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
        self.inner
            .listeners
            .write()
            .expect("listeners lock")
            .push(ListenerEntry {
                endpoint: resolved,
                _task: task,
            });
        Ok(())
    }

    async fn bind_udp(&self, endpoint: Endpoint) -> Result<()> {
        if self.inner.socket_type != SocketType::Dish {
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
        self.inner.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });
        let inner = self.inner.clone();
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
                let mut msg = Message::new();
                msg.push_part(omq_proto::message::Payload::from_bytes(group));
                msg.push_part(omq_proto::message::Payload::from_bytes(body));
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
        self.inner
            .listeners
            .write()
            .expect("listeners lock")
            .push(ListenerEntry {
                endpoint: resolved,
                _task: task,
            });
        Ok(())
    }

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
        reject_encrypted_inproc(&endpoint, &self.inner.options.mechanism)?;
        match endpoint {
            Endpoint::Inproc { name } => {
                let snapshot = self.inner.snapshot();
                let conn = inproc::connect(&name, snapshot, self.inner.in_tx.clone()).await?;
                let conn_id = self
                    .inner
                    .next_connection_id
                    .fetch_add(1, Ordering::Relaxed);
                let ep = Endpoint::Inproc { name: name.clone() };
                self.inner.monitor.publish(MonitorEvent::Connected {
                    endpoint: ep.clone(),
                    peer_ident: PeerIdent::Inproc(name),
                    connection_id: conn_id,
                });
                install_inproc_peer(
                    &self.inner,
                    conn,
                    ep,
                    conn_id,
                    #[cfg(feature = "priority")]
                    priority,
                );
                Ok(())
            }
            Endpoint::Tcp { .. } => {
                use omq_proto::proto::connection::Role;
                connect_tcp_with_reconnect(
                    &self.inner,
                    endpoint,
                    Role::Client,
                    #[cfg(feature = "priority")]
                    priority,
                );
                Ok(())
            }
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Tcp { .. } => {
                use omq_proto::proto::connection::Role;
                connect_tcp_with_reconnect(
                    &self.inner,
                    endpoint,
                    Role::Client,
                    #[cfg(feature = "priority")]
                    priority,
                );
                Ok(())
            }
            #[cfg(feature = "zstd")]
            Endpoint::ZstdTcp { .. } => {
                use omq_proto::proto::connection::Role;
                connect_tcp_with_reconnect(
                    &self.inner,
                    endpoint,
                    Role::Client,
                    #[cfg(feature = "priority")]
                    priority,
                );
                Ok(())
            }
            Endpoint::Ipc(_) => {
                use omq_proto::proto::connection::Role;
                connect_ipc_with_reconnect(
                    &self.inner,
                    endpoint,
                    Role::Client,
                    #[cfg(feature = "priority")]
                    priority,
                );
                Ok(())
            }
            Endpoint::Udp { .. } => self.connect_udp(endpoint).await,
            #[allow(unreachable_patterns)]
            _ => Err(Error::Protocol(
                "transport variant not enabled in this omq-compio build".into(),
            )),
        }
    }

    async fn connect_udp(&self, endpoint: Endpoint) -> Result<()> {
        if self.inner.socket_type != SocketType::Radio {
            return Err(Error::Protocol(
                "udp:// connect is only supported on RADIO sockets".into(),
            ));
        }
        let sock = crate::transport::udp::connect(&endpoint).await?;
        let conn_id = self
            .inner
            .next_connection_id
            .fetch_add(1, Ordering::Relaxed);
        self.inner.monitor.publish(MonitorEvent::Connected {
            endpoint: endpoint.clone(),
            peer_ident: PeerIdent::Path(format!("{endpoint}")),
            connection_id: conn_id,
        });
        self.inner
            .udp_dialers
            .write()
            .expect("udp_dialers lock")
            .push(UdpDialerEntry {
                endpoint: endpoint.clone(),
                sock: Arc::new(sock),
            });
        self.inner.on_peer_ready.notify(usize::MAX);
        Ok(())
    }

    /// Tear down a previously-established bind. Cancels the listener
    /// (or DISH UDP recv loop) by dropping its task handle; already-
    /// accepted peers stay connected. Returns `Error::Unroutable` if
    /// no listener at `endpoint` is registered.
    #[allow(clippy::unused_async)]
    pub async fn unbind(&self, endpoint: Endpoint) -> Result<()> {
        let mut listeners = self.inner.listeners.write().expect("listeners lock");
        let before = listeners.len();
        listeners.retain(|l| l.endpoint != endpoint);
        if listeners.len() < before {
            Ok(())
        } else {
            Err(Error::Unroutable)
        }
    }

    /// Tear down a previously-started connect. Cancels the dial loop
    /// (or the dial supervisor that owns the per-connection driver)
    /// by dropping its task handle. UDP RADIO dialers are also
    /// dropped here. Existing handshaked peers from this dialer
    /// remain in `out_peers` for as long as the driver task they
    /// owned was held by the supervisor - under
    /// `ReconnectPolicy::Disabled` the driver IS the dialer task,
    /// so disconnecting tears the peer down too. Returns
    /// `Error::Unroutable` if no dialer matches.
    #[allow(clippy::unused_async)]
    pub async fn disconnect(&self, endpoint: Endpoint) -> Result<()> {
        let mut dialers = self.inner.dialers.write().expect("dialers lock");
        let mut udp = self.inner.udp_dialers.write().expect("udp_dialers lock");
        let before = dialers.len() + udp.len();
        dialers.retain(|d| d.endpoint != endpoint);
        udp.retain(|d| d.endpoint != endpoint);
        if dialers.len() + udp.len() < before {
            Ok(())
        } else {
            Err(Error::Unroutable)
        }
    }

    /// Snapshot the live status of one connected peer by
    /// `connection_id`. `Ok(None)` means no peer with that id
    /// exists (never connected, or already disconnected).
    #[allow(clippy::unused_async)]
    pub async fn connection_info(
        &self,
        connection_id: u64,
    ) -> Result<Option<crate::monitor::ConnectionStatus>> {
        let peers = self.inner.out_peers.read().expect("peers lock");
        for p in peers.iter() {
            if p.connection_id == connection_id {
                return Ok(Some(crate::monitor::ConnectionStatus {
                    connection_id: p.connection_id,
                    endpoint: p.endpoint.clone(),
                    identity: p
                        .info
                        .read()
                        .expect("info lock")
                        .as_ref()
                        .and_then(|i| i.peer_identity.clone())
                        .unwrap_or_default(),
                    peer_info: p.info.read().expect("info lock").clone(),
                }));
            }
        }
        Ok(None)
    }

    /// Snapshot every currently-connected peer. Empty vec when no
    /// peers are live. Useful for introspection / health checks.
    #[allow(clippy::unused_async)]
    pub async fn connections(&self) -> Result<Vec<crate::monitor::ConnectionStatus>> {
        let peers = self.inner.out_peers.read().expect("peers lock");
        Ok(peers
            .iter()
            .map(|p| crate::monitor::ConnectionStatus {
                connection_id: p.connection_id,
                endpoint: p.endpoint.clone(),
                identity: p
                    .info
                    .read()
                    .expect("info lock")
                    .as_ref()
                    .and_then(|i| i.peer_identity.clone())
                    .unwrap_or_default(),
                peer_info: p.info.read().expect("info lock").clone(),
            })
            .collect())
    }

    /// Subscribe to a topic prefix. Updates the local matcher and
    /// queues a SUBSCRIBE command to every currently-handshaked
    /// publisher; new peers replay our subscriptions when their own
    /// handshake completes. Must be called on a SUB / XSUB socket.
    pub async fn subscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        if !matches!(self.inner.socket_type, SocketType::Sub | SocketType::XSub) {
            return Err(Error::Protocol(
                "subscribe is only valid on SUB / XSUB sockets".into(),
            ));
        }
        let prefix = prefix.into();
        self.inner
            .subscriptions
            .write()
            .expect("subscriptions lock")
            .add(prefix.clone());
        {
            let mut subs = self.inner.our_subs.write().expect("our_subs lock");
            if !subs.iter().any(|p| p == &prefix) {
                subs.push(prefix.clone());
            }
        }
        let cmd = omq_proto::proto::Command::Subscribe(prefix);
        let peers = self.snapshot_peers_now();
        for p in peers {
            let _ = p.send_command(cmd.clone()).await;
        }
        Ok(())
    }

    /// Cancel a previously-registered subscription prefix. No-op if
    /// the prefix wasn't subscribed.
    pub async fn unsubscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        if !matches!(self.inner.socket_type, SocketType::Sub | SocketType::XSub) {
            return Err(Error::Protocol(
                "unsubscribe is only valid on SUB / XSUB sockets".into(),
            ));
        }
        let prefix = prefix.into();
        self.inner
            .subscriptions
            .write()
            .expect("subscriptions lock")
            .remove(&prefix);
        {
            let mut subs = self.inner.our_subs.write().expect("our_subs lock");
            if let Some(pos) = subs.iter().position(|p| p == &prefix) {
                subs.remove(pos);
            }
        }
        let cmd = omq_proto::proto::Command::Cancel(prefix);
        let peers = self.snapshot_peers_now();
        for p in peers {
            let _ = p.send_command(cmd.clone()).await;
        }
        Ok(())
    }

    /// Join a group (DISH only). UDP DISH never sends JOIN over the
    /// wire (RFC 48); the local set drives the receive-time filter in
    /// [`bind_udp`]'s recv loop.
    pub async fn join(&self, group: impl Into<Bytes>) -> Result<()> {
        if !matches!(self.inner.socket_type, SocketType::Dish) {
            return Err(Error::Protocol("join is only valid on DISH sockets".into()));
        }
        let group = group.into();
        self.inner
            .joined_groups
            .write()
            .expect("joined_groups lock")
            .insert(group.clone());
        // Propagate over the wire to every connected RADIO peer
        // (TCP/IPC/inproc); UDP RADIO never sees JOIN per RFC 48
        // and is filtered locally in bind_udp's recv loop. New
        // peers replay our joined_groups on handshake (see
        // spawn_wire_driver's snap listener).
        let cmd = omq_proto::proto::Command::Join(group);
        let peers = self.snapshot_peers_now();
        for p in peers {
            let _ = p.send_command(cmd.clone()).await;
        }
        Ok(())
    }

    /// Leave a previously-joined group. No-op if not joined.
    pub async fn leave(&self, group: impl Into<Bytes>) -> Result<()> {
        if !matches!(self.inner.socket_type, SocketType::Dish) {
            return Err(Error::Protocol(
                "leave is only valid on DISH sockets".into(),
            ));
        }
        let group = group.into();
        self.inner
            .joined_groups
            .write()
            .expect("joined_groups lock")
            .remove(&group[..]);
        let cmd = omq_proto::proto::Command::Leave(group);
        let peers = self.snapshot_peers_now();
        for p in peers {
            let _ = p.send_command(cmd.clone()).await;
        }
        Ok(())
    }

    /// Receive a message. The direct path is tried first when eligible
    /// (single wire peer, supported socket type) — reads the FD inline
    /// so the driver's read-side hop is skipped, saving one task wake
    /// per RTT. On non-eligible sockets, contention on `recv_claim`,
    /// or an early bailout (e.g. pre-handshake), the `in_rx` loop runs
    /// unchanged.
    ///
    /// Cancellation: dropping the returned future after `read_ready`
    /// has fired but before the read SQE returns may forfeit a small
    /// amount of in-flight bytes (~5 µs window). The codec stays
    /// consistent and the connection remains usable; the next
    /// `recv()` continues from there.
    #[allow(clippy::too_many_lines)]
    pub async fn recv(&self) -> Result<Message> {
        let st = self.inner.socket_type;
        if direct_recv_eligible(st)
            && let Some(msg) = self.try_direct_recv().await?
        {
            return Ok(msg);
        }
        loop {
            let frame = self
                .inner
                .in_rx
                .recv_async()
                .await
                .map_err(|_| Error::Closed)?;
            match frame {
                InprocFrame::SinglePart {
                    peer_identity,
                    body,
                } => {
                    if let Some(max) = self.inner.options.max_message_size
                        && body.len() > max
                    {
                        continue;
                    }
                    // Reconstruct the user-visible Message after the
                    // channel hop. The slot in flight was ~64 B (just
                    // a Bytes + Option<Bytes>) instead of the full
                    // ~624 B Message struct.
                    let msg = if is_identity_recv(st) {
                        let id = peer_identity.unwrap_or_default();
                        let mut wrapped = Message::new();
                        wrapped.push_part(omq_proto::message::Payload::from_bytes(id));
                        wrapped.push_part(omq_proto::message::Payload::from_bytes(body));
                        wrapped
                    } else {
                        Message::single(body)
                    };
                    if !self.matches_subscription(&msg) {
                        continue;
                    }
                    if post_recv_needs_type_state(st) {
                        let transformed = self
                            .inner
                            .type_state
                            .lock()
                            .expect("type_state lock")
                            .post_recv(st, msg)?;
                        if let Some(out) = transformed {
                            return Ok(out);
                        }
                    } else {
                        return Ok(msg);
                    }
                }
                InprocFrame::Message(boxed) => {
                    let crate::transport::inproc::InprocFullMessage { peer_identity, msg } = *boxed;
                    if let Some(max) = self.inner.options.max_message_size
                        && msg.byte_len() > max
                    {
                        continue;
                    }
                    if !self.matches_subscription(&msg) {
                        continue;
                    }
                    let msg = if is_identity_recv(st) {
                        let id = peer_identity.unwrap_or_default();
                        let mut wrapped = Message::new();
                        wrapped.push_part(omq_proto::message::Payload::from_bytes(id));
                        for p in msg.parts() {
                            wrapped.push_part(p.clone());
                        }
                        wrapped
                    } else {
                        msg
                    };
                    if post_recv_needs_type_state(st) {
                        let transformed = self
                            .inner
                            .type_state
                            .lock()
                            .expect("type_state lock")
                            .post_recv(st, msg)?;
                        if let Some(out) = transformed {
                            return Ok(out);
                        }
                    } else {
                        return Ok(msg);
                    }
                }
                InprocFrame::Command(c) => {
                    // For XPUB: surface SUBSCRIBE / CANCEL as messages
                    // (`\x01<topic>` / `\x00<topic>`) per the spec.
                    if matches!(st, SocketType::XPub) {
                        use omq_proto::proto::Command;
                        let body = match c {
                            Command::Subscribe(p) => {
                                let mut buf = bytes::BytesMut::with_capacity(1 + p.len());
                                buf.extend_from_slice(&[0x01]);
                                buf.extend_from_slice(&p);
                                Some(buf.freeze())
                            }
                            Command::Cancel(p) => {
                                let mut buf = bytes::BytesMut::with_capacity(1 + p.len());
                                buf.extend_from_slice(&[0x00]);
                                buf.extend_from_slice(&p);
                                Some(buf.freeze())
                            }
                            _ => None,
                        };
                        if let Some(b) = body {
                            let mut m = Message::new();
                            m.push_part(omq_proto::message::Payload::from_bytes(b));
                            return Ok(m);
                        }
                    }
                }
            }
        }
    }

    /// Process one `InprocFrame` from the inbound channel. Returns `Ok(Some(msg))`
    /// when a user-visible message is ready, `Ok(None)` for frames that should be
    /// silently skipped (filtered subscriptions, commands on non-XPUB sockets,
    /// etc.), or `Err` on protocol/state errors.
    fn process_inbound_frame(&self, frame: InprocFrame) -> Result<Option<Message>> {
        let st = self.inner.socket_type;
        match frame {
            InprocFrame::SinglePart {
                peer_identity,
                body,
            } => {
                if let Some(max) = self.inner.options.max_message_size
                    && body.len() > max
                {
                    return Ok(None);
                }
                let msg = if is_identity_recv(st) {
                    let id = peer_identity.unwrap_or_default();
                    let mut wrapped = Message::new();
                    wrapped.push_part(omq_proto::message::Payload::from_bytes(id));
                    wrapped.push_part(omq_proto::message::Payload::from_bytes(body));
                    wrapped
                } else {
                    Message::single(body)
                };
                if !self.matches_subscription(&msg) {
                    return Ok(None);
                }
                if post_recv_needs_type_state(st) {
                    self.inner
                        .type_state
                        .lock()
                        .expect("type_state lock")
                        .post_recv(st, msg)
                } else {
                    Ok(Some(msg))
                }
            }
            InprocFrame::Message(boxed) => {
                let crate::transport::inproc::InprocFullMessage { peer_identity, msg } = *boxed;
                if let Some(max) = self.inner.options.max_message_size
                    && msg.byte_len() > max
                {
                    return Ok(None);
                }
                if !self.matches_subscription(&msg) {
                    return Ok(None);
                }
                let msg = if is_identity_recv(st) {
                    let id = peer_identity.unwrap_or_default();
                    let mut wrapped = Message::new();
                    wrapped.push_part(omq_proto::message::Payload::from_bytes(id));
                    for p in msg.parts() {
                        wrapped.push_part(p.clone());
                    }
                    wrapped
                } else {
                    msg
                };
                if post_recv_needs_type_state(st) {
                    self.inner
                        .type_state
                        .lock()
                        .expect("type_state lock")
                        .post_recv(st, msg)
                } else {
                    Ok(Some(msg))
                }
            }
            InprocFrame::Command(c) => {
                if matches!(st, SocketType::XPub) {
                    use omq_proto::proto::Command;
                    let body = match c {
                        Command::Subscribe(p) => {
                            let mut buf = bytes::BytesMut::with_capacity(1 + p.len());
                            buf.extend_from_slice(&[0x01]);
                            buf.extend_from_slice(&p);
                            Some(buf.freeze())
                        }
                        Command::Cancel(p) => {
                            let mut buf = bytes::BytesMut::with_capacity(1 + p.len());
                            buf.extend_from_slice(&[0x00]);
                            buf.extend_from_slice(&p);
                            Some(buf.freeze())
                        }
                        _ => None,
                    };
                    if let Some(b) = body {
                        let mut m = Message::new();
                        m.push_part(omq_proto::message::Payload::from_bytes(b));
                        return Ok(Some(m));
                    }
                }
                Ok(None)
            }
        }
    }

    /// Non-blocking receive. Returns `Err(Error::WouldBlock)` if no message is
    /// currently queued. Does not perform I/O; only messages already buffered
    /// by the background driver are visible.
    pub fn try_recv(&self) -> Result<Message> {
        loop {
            let frame = self.inner.in_rx.try_recv().map_err(|e| match e {
                flume::TryRecvError::Empty => Error::WouldBlock,
                flume::TryRecvError::Disconnected => Error::Closed,
            })?;
            if let Some(msg) = self.process_inbound_frame(frame)? {
                return Ok(msg);
            }
        }
    }

    /// Snapshot the [`DirectIoState`] iff this socket has exactly one
    /// connected wire peer. Returns `None` for inproc-only sockets,
    /// no-peer sockets, or multi-peer sockets - any of which forces
    /// the caller to fall back to the slow `in_rx` path.
    fn snapshot_direct_io_single_peer(&self) -> Option<Arc<DirectIoState>> {
        let peers = self.inner.out_peers.read().expect("peers lock");
        if peers.len() != 1 {
            return None;
        }
        let p = &peers[0];
        let handle = p.direct_io.as_ref()?;
        handle.read().expect("direct_io handle lock").clone()
    }

    /// Walk the codec's user-facing event stream once under the
    /// [`PeerIo`] lock. Returns `Ok(Some(msg))` for the first
    /// `Event::Message` (with transform-decode applied), `Ok(None)`
    /// if the codec produced only commands or nothing.
    ///
    /// Commands like Ping / Pong / Error / Unknown are silently
    /// consumed - the slow `in_rx` path's monitor publishing for
    /// Error / Unknown is sacrificed on the direct path (rare;
    /// documented in the stripped-Stage-5 plan).
    ///
    /// `HandshakeSucceeded` is treated defensively: we gate entry to
    /// the direct-read loop on `handshake_done == true` while
    /// holding the lock, so this branch shouldn't normally fire.
    /// Flip the flag and continue if it does.
    #[allow(clippy::unused_self)]
    fn drain_one_user_event(&self, io: &mut PeerIo) -> Result<Option<Message>> {
        while let Some(ev) = io.codec.poll_event() {
            match ev {
                Event::Message(m) => {
                    let m = if let Some(dec) = io.decoder.as_mut() {
                        match dec.decode(m)? {
                            Some(plain) => plain,
                            None => continue,
                        }
                    } else {
                        m
                    };
                    return Ok(Some(m));
                }
                Event::Command(_) => {}
                Event::HandshakeSucceeded { .. } => {
                    io.handshake_done = true;
                }
            }
        }
        Ok(None)
    }

    /// Apply post-receive socket-type processing: SUB / XSUB
    /// subscription filtering, REQ / REP type-state. Mirrors the
    /// `in_rx` loop's handling. Returns `None` on filtered messages
    /// (caller continues the read loop), `Some(msg)` to surface.
    fn post_recv_apply(&self, msg: Message) -> Result<Option<Message>> {
        if !self.matches_subscription(&msg) {
            return Ok(None);
        }
        let st = self.inner.socket_type;
        if post_recv_needs_type_state(st) {
            Ok(self
                .inner
                .type_state
                .lock()
                .expect("type_state lock")
                .post_recv(st, msg)?)
        } else {
            Ok(Some(msg))
        }
    }

    /// Process one `InprocFrame` from `in_rx` for the direct-recv
    /// fallback race. Only handles the variants that the
    /// direct-recv-eligible socket types (Pull / Sub / Rep / Pair
    /// / Req) actually receive: `SinglePart` and Message. Command is
    /// XPub-only and not eligible for direct recv anyway.
    fn process_inproc_frame_for_direct(&self, frame: InprocFrame) -> Result<Option<Message>> {
        let max = self.inner.options.max_message_size;
        match frame {
            InprocFrame::SinglePart { body, .. } => {
                if max.is_some_and(|m| body.len() > m) {
                    return Ok(None);
                }
                self.post_recv_apply(Message::single(body))
            }
            InprocFrame::Message(boxed) => {
                let crate::transport::inproc::InprocFullMessage { msg, .. } = *boxed;
                if max.is_some_and(|m| msg.byte_len() > m) {
                    return Ok(None);
                }
                self.post_recv_apply(msg)
            }
            InprocFrame::Command(_) => Ok(None),
        }
    }

    /// Direct-recv: read straight off the wire instead of going
    /// through the driver's read arm + `in_rx` channel hop. Saves
    /// ~12 µs (one task wake) per RTT on the inbound side.
    ///
    /// Bails (returns `Ok(None)` to the caller, which falls back to
    /// the `in_rx` loop) when:
    ///   - the socket has zero or many peers,
    ///   - the peer has no `DirectIoState` (inproc / UDP),
    ///   - another `recv()` already holds the claim,
    ///   - the handshake hasn't completed yet (driver still owns
    ///     the read path until handshake).
    #[allow(clippy::too_many_lines)]
    async fn try_direct_recv(&self) -> Result<Option<Message>> {
        use futures::FutureExt;

        // Fall back if the driver has already buffered messages into
        // in_rx - they must be drained in arrival order.
        if !self.inner.in_rx.is_empty() {
            return Ok(None);
        }
        let Some(state) = self.snapshot_direct_io_single_peer() else {
            return Ok(None);
        };
        if state
            .recv_claim
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Acquire)
            .is_err()
        {
            return Ok(None);
        }
        let guard = ClaimGuard { state: &state };
        // Race-safe recheck: between the first peek and the claim
        // flip, the driver could have enqueued into in_rx (it stops
        // reading on its next iteration). Bail if so.
        if !self.inner.in_rx.is_empty() {
            return Ok(None);
        }

        loop {
            // 1) Drain any user-facing events the driver left in the
            //    codec. Bail out (Ok(None)) if the handshake hasn't
            //    completed - we hold the lock so this is race-free.
            let drained = {
                let mut io = state.peer_io.lock().expect("peer_io");
                if !io.handshake_done {
                    return Ok(None);
                }
                self.drain_one_user_event(&mut io)?
            };
            if let Some(msg) = drained {
                if let Some(out) = self.post_recv_apply(msg)? {
                    return Ok(Some(out));
                }
                continue;
            }

            // 2) Race the multi-shot recv stream against an in_rx
            //    fire. The driver may have parsed an event under the
            //    [`PeerIo`] lock while we were releasing it (small
            //    window, but real on a single-threaded runtime:
            //    drain-events-under-lock → release → forward-to-in_rx
            //    is two steps). If in_rx wins, drop the claim and
            //    process the buffered frame inline so we never deadlock
            //    waiting on the stream while the driver has a message
            //    in its hand.
            //
            //    The multi-shot stream is cancel-safe by construction:
            //    its persistent SQE survives consumer-future drops, so
            //    losing this select arm to in_rx does not lose any
            //    bytes - they remain queued in the BUF_RING for the
            //    next consumer to pick up. `recv_async` on flume is
            //    cancel-safe by construction.
            //    Cancel-safety invariant: NO `.await` between extracting
            //    the BufferRef from the stream and feeding it to
            //    `handle_input`. If the recv future is dropped in that
            //    window, `BufferRef::drop` returns the slot to the ring
            //    with its bytes unfed, desyncing ZMTP framing. We
            //    achieve atomicity by deferring the peer_io lock to
            //    AFTER `stream.next()` returns - and `peer_io` is a
            //    sync mutex (`std::sync::Mutex`) so its acquire is not
            //    an `.await`. The "never hold peer_io across `.await`"
            //    invariant elsewhere keeps the sync lock from blocking.
            let pull_and_feed = async {
                let mut sguard = state.recv_stream.0.lock().await;
                // See driver.rs: a previous iteration's one-shot
                // recv path may have dropped the stream and been
                // interrupted before rebuilding. Treat as EOF.
                let Some(cs) = sguard.as_mut() else {
                    return PullOutcome::Eof;
                };
                // See driver.rs `stream_arm`: `.with_cancel` registers
                // the SubmitMulti op key with the per-stream token so
                // the large-frame switch can drain pending CQEs.
                let buf = compio::runtime::FutureExt::with_cancel(
                    futures::StreamExt::next(&mut cs.stream),
                    cs.cancel.clone(),
                )
                .await;
                match buf {
                    None => PullOutcome::Eof,
                    Some(Err(e)) => PullOutcome::Err(e),
                    Some(Ok(buf)) => {
                        if buf.is_empty() {
                            return PullOutcome::Eof;
                        }
                        let handle_result = {
                            let mut io = state.peer_io.lock().expect("peer_io");
                            let bytes = bytes::Bytes::copy_from_slice(&buf[..]);
                            drop(buf);
                            io.codec.handle_input(bytes)
                        };
                        if handle_result.is_err() {
                            PullOutcome::ProtoErr
                        } else {
                            state.last_input_nanos.store(
                                state.hb_epoch.elapsed().as_nanos() as u64,
                                Ordering::Relaxed,
                            );
                            match crate::socket::try_one_shot_large_recv(&state, &mut sguard).await
                            {
                                crate::socket::OneShotLargeRecvOutcome::Skipped
                                | crate::socket::OneShotLargeRecvOutcome::Took => PullOutcome::Fed,
                                crate::socket::OneShotLargeRecvOutcome::IoErr(e) => {
                                    PullOutcome::Err(e)
                                }
                                crate::socket::OneShotLargeRecvOutcome::ProtoErr(_) => {
                                    PullOutcome::ProtoErr
                                }
                            }
                        }
                    }
                }
            };
            let inrx_fut = self.inner.in_rx.recv_async();
            // Driver may win the stream pull race by sampling claim=0
            // before we set it. After feeding, it notifies this event
            // so we break out of pull_and_feed and re-drain the codec.
            // Listener must be created BEFORE entering the select arm
            // body so the signal isn't lost on a notify between
            // "claim acquired" and "listener attached".
            let codec_ready_fut = state.recv_codec_ready.listen();
            futures::pin_mut!(pull_and_feed);
            futures::pin_mut!(inrx_fut);
            futures::pin_mut!(codec_ready_fut);
            let outcome = futures::select_biased! {
                frame = inrx_fut.fuse() => {
                    let frame = frame.map_err(|_| Error::Closed)?;
                    // Drop claim via RAII before returning.
                    drop(guard);
                    return self.process_inproc_frame_for_direct(frame);
                }
                () = codec_ready_fut.fuse() => PullOutcome::Fed,
                outcome = pull_and_feed.fuse() => outcome,
            };

            match outcome {
                PullOutcome::Fed => {
                    // Ordering: while we held the recv_stream lock the
                    // driver may have parsed events into in_rx. Those
                    // events are older than anything we just fed, so
                    // bail to the channel path before draining
                    // codec-fresh events that would jump the queue.
                    if !self.inner.in_rx.is_empty() {
                        drop(guard);
                        return Ok(None);
                    }
                    // Loop back to drain the freshly-parsed events.
                }
                PullOutcome::Eof => {
                    state.eof_signal.notify(usize::MAX);
                    return Err(Error::Closed);
                }
                PullOutcome::ProtoErr => {
                    // Codec rejected input (e.g. MessageTooLarge).
                    // Mirror the driver: drop this connection silently
                    // rather than surfacing a codec-level protocol error
                    // to the user. Notify eof_signal so the driver -
                    // parked on it while our claim is held - wakes and
                    // exits cleanly.
                    state.eof_signal.notify(usize::MAX);
                    return Ok(None);
                }
                PullOutcome::Err(e) => {
                    // Linux ENOBUFS = 105. Multi-shot recv is
                    // terminated by the kernel when the BUF_RING is
                    // exhausted; rearm to keep draining. Other errors
                    // are fatal.
                    if e.raw_os_error() != Some(105) {
                        state.eof_signal.notify(usize::MAX);
                        return Err(Error::Closed);
                    }
                    if state.recv_stream.rearm(&state.peer_io).await.is_err() {
                        state.eof_signal.notify(usize::MAX);
                        return Err(Error::Closed);
                    }
                }
            }
            // Flush any codec output from handle_input (e.g. auto-PONGs).
            // Acquire the writer lock FIRST and hold it across the entire
            // snapshot → write → advance sequence; the driver's step 3a
            // flush follows the same discipline. Without that serialization,
            // the driver's heartbeat PING (enqueued under the codec lock and
            // flushed in step 3a) could be cloned by both paths and written
            // twice, then double-advanced, panicking in `advance_transmit`.
            // The driver path (when its select-arm fires while we hold the
            // claim) blocks on `state.writer` until we drop our guard.
            loop {
                let mut writer = state.writer.lock().await;
                let chunks = {
                    let io = state.peer_io.lock().expect("peer_io");
                    if !io.codec.has_pending_transmit() {
                        break;
                    }
                    let mut c = io.codec.clone_transmit_chunks();
                    if c.len() > 1024 {
                        c.truncate(1024);
                    }
                    c
                };
                if chunks.is_empty() {
                    break;
                }
                let (res, _returned) = writer.write_vectored(chunks).await;
                let written = res.map_err(Error::Io)?;
                if written == 0 {
                    state.eof_signal.notify(usize::MAX);
                    return Err(Error::Closed);
                }
                state
                    .peer_io
                    .lock()
                    .expect("peer_io")
                    .codec
                    .advance_transmit(written);
            }
            // Loop back to drain the freshly-parsed events.
        }
    }

    /// Graceful close. Drains pending sends up to `Options::linger`,
    /// then tears peers down. After return, subsequent send/recv on
    /// this clone (or other clones still alive) returns
    /// `Error::Closed`. `Options::linger == None` waits forever;
    /// `Some(Duration::ZERO)` (the libzmq default) drops pending
    /// sends immediately.
    pub async fn close(self) -> Result<()> {
        let was_closed = self.inner.closed.swap(true, Ordering::SeqCst);
        if was_closed {
            return Ok(());
        }
        let deadline = self
            .inner
            .options
            .linger
            .map(|d| std::time::Instant::now() + d);
        // Cancel listener tasks immediately — they hold Arc<SocketInner>
        // and keep the OS port alive. Dialer tasks are deferred until
        // after the linger drain below: each dialer supervisor awaits its
        // driver's JoinHandle, so canceling the supervisor early would
        // also cancel the driver before it has flushed pending sends to
        // the wire. Dialer supervisors check `inner.closed` after their
        // driver exits and exit without reconnecting.
        self.inner
            .listeners
            .write()
            .expect("listeners lock")
            .clear();
        self.inner
            .udp_dialers
            .write()
            .expect("udp_dialers lock")
            .clear();
        // Round-robin shared queue: drop our last sender clone so
        // pumps see the queue go disconnected after they've drained
        // remaining messages. Pumps then exit and their
        // PeerOut::send was already through the per-peer cmd_tx, so
        // the per-driver Close handling below picks up everything.
        if let Some(tx) = self
            .inner
            .shared_send_tx
            .write()
            .expect("shared_send_tx lock")
            .take()
        {
            drop(tx);
        }
        // Wire peers: send DriverCommand::Close so the driver flushes
        // pending_cmds + any in-flight transmit before exiting. Then
        // poll cmd_tx.is_disconnected() - flume flips that flag once
        // the receiver (held by the driver task) drops, which only
        // happens on driver exit. Inproc peers don't run a driver:
        // their drain is synchronous (send awaits the peer's
        // receiver) so emptiness of the cmd channel is sufficient.
        let wire_handles: Vec<WirePeerHandle> = {
            let peers = self.inner.out_peers.read().expect("peers lock");
            peers
                .iter()
                .filter_map(|p| match &p.out {
                    PeerOut::Wire(handle) => Some(handle.clone()),
                    PeerOut::Inproc { .. } => None,
                })
                .collect()
        };
        for handle in &wire_handles {
            let tx = handle.read().expect("wire peer handle lock").clone();
            let _ = tx.send_async(DriverCommand::Close).await;
        }

        loop {
            let inproc_pending = {
                let peers = self.inner.out_peers.read().expect("peers lock");
                peers.iter().any(|p| match &p.out {
                    PeerOut::Inproc { sender, .. } => {
                        !sender.is_empty() && !sender.is_disconnected()
                    }
                    PeerOut::Wire(_) => false,
                })
            };
            let wire_alive = wire_handles.iter().any(|handle| {
                !handle
                    .read()
                    .expect("wire peer handle lock")
                    .is_disconnected()
            });
            if !inproc_pending && !wire_alive {
                break;
            }
            if let Some(d) = deadline
                && std::time::Instant::now() >= d
            {
                break;
            }
            compio::time::sleep(Duration::from_millis(5)).await;
        }
        // Now that all drivers have flushed (or the linger deadline
        // expired), cancel the dialer supervisor tasks. They check
        // `inner.closed` and exit without reconnecting once their
        // driver's JoinHandle resolves (which it already has, or will
        // very shortly — the cancel lands at a yield point only).
        self.inner.dialers.write().expect("dialers lock").clear();
        {
            let mut peers = self.inner.out_peers.write().expect("peers lock");
            peers.clear();
        }
        self.inner.monitor.publish(MonitorEvent::Closed);
        Ok(())
    }

    fn matches_subscription(&self, msg: &Message) -> bool {
        // Fast path: types that accept all messages skip the lock entirely.
        if !matches!(
            self.inner.socket_type,
            SocketType::Sub | SocketType::XSub | SocketType::Dish
        ) {
            return true;
        }
        match self.inner.socket_type {
            SocketType::Sub | SocketType::XSub => {
                let topic = msg
                    .parts()
                    .first()
                    .map(omq_proto::Payload::as_bytes)
                    .unwrap_or_default();
                self.inner
                    .subscriptions
                    .read()
                    .expect("subscriptions lock")
                    .matches(&topic)
            }
            SocketType::Dish => {
                // RFC 48: DISH receives `[group, body]`; drop messages
                // whose group is not in `joined_groups`. Inproc and
                // wire RADIO peers fan out to every connection without
                // a per-peer filter; the DISH side does the filtering.
                let group = msg
                    .parts()
                    .first()
                    .map(omq_proto::Payload::as_bytes)
                    .unwrap_or_default();
                self.inner
                    .joined_groups
                    .read()
                    .expect("joined_groups lock")
                    .contains(&group[..])
            }
            _ => true,
        }
    }

    pub(super) fn snapshot_peers_now(&self) -> Vec<PeerOut> {
        let peers = self.inner.out_peers.read().expect("peers lock");
        peers.iter().map(|p| p.out.clone()).collect()
    }
}
